package paxi

// 解析命令行参数。设置日志记录配置。加载配置信息。配置 HTTP 客户端的最大空闲连接数。
import (
	"flag"
	"net/http"

	"github.com/ailidani/paxi/log"
)

// Init setup paxi package
func Init() {
	flag.Parse()
	log.Setup()
	config.Load()
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 1000
}
