package paxi

// 总的来说，这段代码实现了一个分布式系统中节点的基本功能，包括接收和处理客户端请求、转发请求、以及通过 HTTP 提供 RESTful API 服务。
// 通过注册处理函数和启动节点，Node 可以接收、处理和转发请求，并通过 HTTP 提供 RESTful API 服务。
import (
	"net/http"
	"reflect"
	"sync"

	"github.com/ailidani/paxi/log"
)

// Node 接口定义了每个节点必须实现的方法。它继承了 Socket 和 Database 接口，并添加了一些节点特有的方法。
// Node is the primary access point for every replica
// it includes networking, state machine and RESTful API server
type Node interface {
	Socket
	Database
	ID() ID
	Run()
	Retry(r Request)
	Forward(id ID, r Request)
	Register(m interface{}, f interface{})
}

// node 结构体实现了Node接口操作并提供了具体的实现细节。
// node implements Node interface
type node struct {
	id ID

	Socket
	Database
	MessageChan chan interface{}
	handles     map[string]reflect.Value
	server      *http.Server

	sync.RWMutex
	forwards map[string]*Request
}

// NewNode creates a new Node object from configuration
func NewNode(id ID) Node {
	return &node{
		id:          id,
		Socket:      NewSocket(id, config.Addrs),
		Database:    NewDatabase(),
		MessageChan: make(chan interface{}, config.ChanBufferSize),
		handles:     make(map[string]reflect.Value),
		forwards:    make(map[string]*Request),
	}
}

func (n *node) ID() ID {
	return n.id
}

func (n *node) Retry(r Request) {
	log.Debugf("node %v retry reqeust %v", n.id, r)
	n.MessageChan <- r
}

// 为每种消息类型注册一个处理函数。使用反射机制检查传入的处理函数是否符合预期的签名，并将其存储在node的handles字段中
// Register a handle function for each message type
func (n *node) Register(m interface{}, f interface{}) {
	t := reflect.TypeOf(m)
	fn := reflect.ValueOf(f)
	if fn.Kind() != reflect.Func || fn.Type().NumIn() != 1 || fn.Type().In(0) != t {
		panic("register handle function error")
	}
	n.handles[t.String()] = fn
}

// 启动replica
// Run start and run the node
func (n *node) Run() {
	log.Infof("node %v start running", n.id)
	if len(n.handles) > 0 {
		go n.handle()
		go n.recv()
	}
	n.http() // 启动HTTP服务器
}

// recv receives messages from socket and pass to message channel
func (n *node) recv() {
	for {
		m := n.Recv()
		switch m := m.(type) {
		case Request: // 若是请求就将其交给相应的处理函数，并将回复写到请求的一个通道属性后，发送回请求源地址
			m.c = make(chan Reply, 1) // 为 Request 类型的消息创建一个缓冲区大小为1的 chan Reply，用于接收回复。
			go func(r Request) {      // func(r Request) 是一个匿名函数，接受一个 Request 类型的参数 r
				n.Send(r.NodeID, <-r.c) // 等待 m.c 中的值并将其发送到 r.NodeID。这个 goroutine 会一直阻塞，直到 m.c 接收到 Reply 类型的消息。
			}(m) // 调用匿名函数并传递 m 作为参数。
			n.MessageChan <- m // 写入通道中，让其他函数处理该请求
			continue

		case Reply:
			n.RLock()
			r := n.forwards[m.Command.String()] // 应该是之前的请求的回复，所以可以找到原始请求
			log.Debugf("node %v received reply %v", n.id, m)
			n.RUnlock()
			r.Reply(m) // 请求的reply函数
			continue
		}
		n.MessageChan <- m
	}
}

// 通过反射机制处理从 MessageChan 接收到的消息
// handle receives messages from message channel and calls handle function using refection
func (n *node) handle() {
	for {
		msg := <-n.MessageChan
		v := reflect.ValueOf(msg)
		name := v.Type().String()
		f, exists := n.handles[name] // 找到处理函数
		if !exists {
			log.Fatalf("no registered handle function for message type %v", name)
		}
		f.Call([]reflect.Value{v}) // 使用反射机制调用 f 函数
	}
}

/*
func (n *node) Forward(id ID, m Request) {
	key := m.Command.Key
	url := config.HTTPAddrs[id] + "/" + strconv.Itoa(int(key))

	log.Debugf("Node %v forwarding %v to %s", n.ID(), m, id)

	method := http.MethodGet
	var body io.Reader
	if !m.Command.IsRead() {
		method = http.MethodPut
		body = bytes.NewBuffer(m.Command.Value)
	}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		log.Error(err)
		return
	}
	req.Header.Set(HTTPClientID, string(n.id))
	req.Header.Set(HTTPCommandID, strconv.Itoa(m.Command.CommandID))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Error(err)
		m.Reply(Reply{
			Command: m.Command,
			Err:     err,
		})
		return
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		b, err := ioutil.ReadAll(res.Body)
		if err != nil {
			log.Error(err)
		}
		m.Reply(Reply{
			Command: m.Command,
			Value:   Value(b),
		})
	} else {
		m.Reply(Reply{
			Command: m.Command,
			Err:     errors.New(res.Status),
		})
	}
}
*/

func (n *node) Forward(id ID, m Request) {
	log.Debugf("Node %v forwarding %v to %s", n.ID(), m, id)
	m.NodeID = n.id
	n.Lock()
	n.forwards[m.Command.String()] = &m // 记录请求，recv函数收到reply时找到对应请求
	n.Unlock()
	n.Send(id, m)
}
