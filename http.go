package paxi

// 这段代码实现了一个 HTTP 服务器，提供了一些 API 接口来处理客户端请求，如处理命令、获取历史记录、模拟节点崩溃和消息丢弃。
import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/ailidani/paxi/log"
)

// http request header names
const (
	HTTPClientID  = "Id"
	HTTPCommandID = "Cid"
	HTTPTimestamp = "Timestamp"
	HTTPNodeID    = "Id"
)

// serve serves the http REST API request from clients
func (n *node) http() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", n.handleRoot) // 将不同的URL路径与对应的处理函数进行关联,客户端的正常请求url是/key结尾的，服务器会调用handleRoot进行响应
	mux.HandleFunc("/history", n.handleHistory)
	mux.HandleFunc("/crash", n.handleCrash)
	mux.HandleFunc("/drop", n.handleDrop)
	// http string should be in form of ":8080"
	url, err := url.Parse(config.HTTPAddrs[n.id])
	if err != nil {
		log.Fatal("http url parse error: ", err)
	}
	port := ":" + url.Port()
	n.server = &http.Server{ // 创建http.Server实例
		Addr:    port,
		Handler: mux,
	}
	log.Info("http server starting on ", port)
	log.Fatal(n.server.ListenAndServe()) // 启动http服务器并开始监听传入的请求
}

func (n *node) handleRoot(w http.ResponseWriter, r *http.Request) {
	var req Request
	var cmd Command
	var err error

	// get all http headers
	req.Properties = make(map[string]string)
	for k := range r.Header {
		if k == HTTPClientID {
			cmd.ClientID = ID(r.Header.Get(HTTPClientID)) // 请求头中的客户ID写到cmd里
			continue
		}
		if k == HTTPCommandID {
			cmd.CommandID, err = strconv.Atoi(r.Header.Get(HTTPCommandID)) // 请求头中的命令ID写到cmd里
			if err != nil {
				log.Error(err)
			}
			continue
		}
		req.Properties[k] = r.Header.Get(k) // 将其余请求头写到req里
	}

	// get command key and value
	if len(r.URL.Path) > 1 {
		i, err := strconv.Atoi(r.URL.Path[1:])
		if err != nil {
			http.Error(w, "invalid path", http.StatusBadRequest)
			log.Error(err)
			return
		}
		cmd.Key = Key(i) // 设置key
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				log.Error("error reading body: ", err)
				http.Error(w, "cannot read body", http.StatusBadRequest)
				return
			}
			cmd.Value = Value(body) // 设置value
		}
	} else {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Error("error reading body: ", err)
			http.Error(w, "cannot read body", http.StatusBadRequest)
			return
		}
		json.Unmarshal(body, &cmd)
	}

	req.Command = cmd
	req.Timestamp = time.Now().UnixNano()
	req.NodeID = n.id // TODO does this work when forward twice
	req.c = make(chan Reply, 1)

	n.MessageChan <- req // 将请求req发送到节点的消息通道，

	reply := <-req.c // 然后等待从 req.c 通道接收到回复。

	if reply.Err != nil {
		http.Error(w, reply.Err.Error(), http.StatusInternalServerError)
		return
	}

	// set all http headers
	w.Header().Set(HTTPClientID, string(reply.Command.ClientID))
	w.Header().Set(HTTPCommandID, strconv.Itoa(reply.Command.CommandID))
	for k, v := range reply.Properties {
		w.Header().Set(k, v)
	}

	_, err = io.WriteString(w, string(reply.Value)) // 将reply.Value转换为字符串并写入w，w是HTTP响应写入器(http.ResponseWriter)。这实际上将数据发送回客户端。
	if err != nil {
		log.Error(err)
	}
}

func (n *node) handleHistory(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(HTTPNodeID, string(n.id))
	k, err := strconv.Atoi(r.URL.Query().Get("key"))
	if err != nil {
		log.Error(err)
		http.Error(w, "invalide key", http.StatusBadRequest)
		return
	}
	h := n.Database.History(Key(k))
	b, _ := json.Marshal(h)
	_, err = w.Write(b)
	if err != nil {
		log.Error(err)
	}
}

func (n *node) handleCrash(w http.ResponseWriter, r *http.Request) {
	t, err := strconv.Atoi(r.URL.Query().Get("t"))
	if err != nil {
		log.Error(err)
		http.Error(w, "invalide time", http.StatusBadRequest)
		return
	}
	n.Socket.Crash(t)
	// timer := time.NewTimer(time.Duration(t) * time.Second)
	// go func() {
	// 	n.server.Close()
	// 	<-timer.C
	// 	log.Error(n.server.ListenAndServe())
	// }()
}

func (n *node) handleDrop(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	t, err := strconv.Atoi(r.URL.Query().Get("t"))
	if err != nil {
		log.Error(err)
		http.Error(w, "invalide time", http.StatusBadRequest)
		return
	}
	n.Drop(ID(id), t)
}
