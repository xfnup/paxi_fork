package paxi

// 这段代码定义了一个传输层接口和具体的传输实现，包括TCP、UDP和进程内通信（channels）。
import (
	"bytes"
	"encoding/gob"
	"errors"
	"flag"
	"net"
	"net/url"
	"strings"
	"sync"

	"github.com/ailidani/paxi/log"
)

// 从命令行获取配置参数
var scheme = flag.String("transport", "tcp", "transport scheme (tcp, udp, chan), default tcp")

// Transport = transport + pipe + client + server
type Transport interface {
	// Scheme returns tranport scheme
	Scheme() string

	// Send sends message into t.send chan
	Send(interface{})

	// Recv waits for message from t.recv chan
	Recv() interface{}

	// Dial connects to remote server non-blocking once connected
	Dial() error

	// Listen waits for connections, non-blocking once listener starts
	Listen()

	// Close closes send channel and stops listener
	Close()
}

// 根据给定的地址创建一个新的传输对象。它支持不同的传输方案，如 tcp、udp 和 chan
// NewTransport creates new transport object with url
func NewTransport(addr string) Transport {
	if !strings.Contains(addr, "://") {
		addr = *scheme + "://" + addr
	}
	uri, err := url.Parse(addr)
	if err != nil {
		log.Fatalf("error parsing address %s : %s\n", addr, err)
	}

	transport := &transport{
		uri:   uri,
		send:  make(chan interface{}, config.ChanBufferSize),
		recv:  make(chan interface{}, config.ChanBufferSize),
		close: make(chan struct{}),
	}

	switch uri.Scheme {
	case "chan":
		t := new(channel)
		t.transport = transport
		return t
	case "tcp":
		t := new(tcp)
		t.transport = transport
		return t
	case "udp":
		t := new(udp)
		t.transport = transport
		return t
	default:
		log.Fatalf("unknown scheme %s", uri.Scheme)
	}
	return nil
}

// transport 结构体及其方法提供了基础的网络传输功能。通过 Send 方法发送消息，通过 Recv 方法接收消息，通过 Close 方法关闭传输.
// Dial 方法用于建立与远程服务器的 TCP 连接，并在后台 goroutine 中处理消息发送。整个实现使用了 gob 编码器对消息进行编码，从而支持任意类型的消息传输。
type transport struct {
	uri   *url.URL
	send  chan interface{}
	recv  chan interface{}
	close chan struct{}
}

func (t *transport) Send(m interface{}) {
	t.send <- m
}

func (t *transport) Recv() interface{} {
	return <-t.recv
}

func (t *transport) Close() {
	close(t.send)
	close(t.close)
}

func (t *transport) Scheme() string {
	return t.uri.Scheme
}

func (t *transport) Dial() error {
	conn, err := net.Dial(t.Scheme(), t.uri.Host) // 使用 net.Dial 函数尝试建立到远程服务器的连接。
	if err != nil {
		return err
	}

	go func(conn net.Conn) { // 启动一个新的 goroutine 来处理连接
		// w := bufio.NewWriter(conn)
		// codec := NewCodec(config.Codec, conn)
		encoder := gob.NewEncoder(conn)
		defer conn.Close()
		for m := range t.send {
			err := encoder.Encode(&m) // 将消息编码为 gob 格式并发送到远程服务器
			if err != nil {
				log.Error(err)
			}
		}
	}(conn)

	return nil
}

/*
*****************************
/*     TCP communication      *
/*****************************
*/
type tcp struct {
	*transport
}

func (t *tcp) Listen() {
	log.Debug("start listening ", t.uri.Port())
	listener, err := net.Listen("tcp", ":"+t.uri.Port()) // 启动一个 TCP 监听器
	if err != nil {
		log.Fatal("TCP Listener error: ", err)
	}

	go func(listener net.Listener) { // 启动一个新的 goroutine 来运行监听器。
		defer listener.Close()
		for {
			conn, err := listener.Accept() // 监听器发现新连接
			if err != nil {
				log.Error("TCP Accept error: ", err)
				continue
			}

			go func(conn net.Conn) { // 启动一个新的goroutine来处理新连接
				// codec := NewCodec(config.Codec, conn)
				decoder := gob.NewDecoder(conn) // 为连接创建一个解码器
				defer conn.Close()
				//r := bufio.NewReader(conn)
				for {
					select {
					case <-t.close:
						return
					default:
						var m interface{}
						err := decoder.Decode(&m) // 解码收到的消息
						if err != nil {
							log.Error(err)
							continue
						}
						t.recv <- m // 将解码后的数据发送到recv通道
					}
				}
			}(conn)

		}
	}(listener)
}

/*
*****************************
/*     UDP communication      *
/*****************************
*/
type udp struct {
	*transport
}

func (u *udp) Dial() error {
	addr, err := net.ResolveUDPAddr("udp", u.uri.Host)
	if err != nil {
		log.Fatal("UDP resolve address error: ", err)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}

	go func(conn *net.UDPConn) {
		// packet := make([]byte, 1500)
		// w := bytes.NewBuffer(packet)
		w := new(bytes.Buffer)
		for m := range u.send {
			gob.NewEncoder(w).Encode(&m)
			_, err := conn.Write(w.Bytes())
			if err != nil {
				log.Error(err)
			}
			w.Reset()
		}
	}(conn)

	return nil
}

func (u *udp) Listen() {
	addr, err := net.ResolveUDPAddr("udp", ":"+u.uri.Port())
	if err != nil {
		log.Fatal("UDP resolve address error: ", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal("UDP Listener error: ", err)
	}
	go func(conn *net.UDPConn) {
		packet := make([]byte, 1500)
		defer conn.Close()
		for {
			select {
			case <-u.close:
				return
			default:
				_, err := conn.Read(packet)
				if err != nil {
					log.Error(err)
					continue
				}
				r := bytes.NewReader(packet)
				var m interface{}
				gob.NewDecoder(r).Decode(&m)
				u.recv <- m
			}
		}
	}(conn)
}

/*******************************
/* Intra-process communication *
/*******************************/

var chans = make(map[string]chan interface{})
var chansLock sync.RWMutex

type channel struct {
	*transport
}

func (c *channel) Scheme() string {
	return "chan"
}

func (c *channel) Dial() error {
	chansLock.RLock()
	defer chansLock.RUnlock()
	conn, ok := chans[c.uri.Host]
	if !ok {
		return errors.New("server not ready")
	}
	go func(conn chan<- interface{}) {
		for m := range c.send {
			conn <- m
		}
	}(conn)
	return nil
}

func (c *channel) Listen() {
	chansLock.Lock()
	defer chansLock.Unlock()
	chans[c.uri.Host] = make(chan interface{}, config.ChanBufferSize)
	go func(conn <-chan interface{}) {
		for {
			select {
			case <-c.close:
				return
			case m := <-conn:
				c.recv <- m
			}
		}
	}(chans[c.uri.Host])
}
