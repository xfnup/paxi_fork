package paxi

import (
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/ailidani/paxi/log"
)

// 客户端接口包含了客户端要实现的功能
// DB is general interface implemented by client to call client library
type DB interface {
	Init() error
	Read(key int) (int, error)
	Write(key, value int) error
	Stop() error
}

// Bconfig holds all benchmark configuration
type Bconfig struct {
	T                    int     // total number of running time in seconds
	N                    int     // total number of requests
	K                    int     // key sapce
	W                    float64 // write ratio
	Throttle             int     // requests per second throttle, unused if 0
	Concurrency          int     // number of simulated clients
	Distribution         string  // distribution
	LinearizabilityCheck bool    // run linearizability checker at the end of benchmark
	// rounds       int    // repeat in many rounds sequentially

	// conflict distribution
	Conflicts int // percentage of conflicting keys
	Min       int // min key

	// normal distribution
	Mu    float64 // mu of normal distribution
	Sigma float64 // sigma of normal distribution
	Move  bool    // moving average (mu) of normal distribution
	Speed int     // moving speed in milliseconds intervals per key

	// zipfian distribution
	ZipfianS float64 // zipfian s parameter
	ZipfianV float64 // zipfian v parameter

	// exponential distribution
	Lambda float64 // rate parameter
}

// DefaultBConfig returns a default benchmark config
func DefaultBConfig() Bconfig {
	return Bconfig{
		T:                    60,
		N:                    0,
		K:                    1000,
		W:                    0.5,
		Throttle:             0,
		Concurrency:          1,
		Distribution:         "uniform",
		LinearizabilityCheck: true,
		Conflicts:            100,
		Min:                  0,
		Mu:                   0,
		Sigma:                60,
		Move:                 false,
		Speed:                500,
		ZipfianS:             2,
		ZipfianV:             1,
		Lambda:               0.01,
	}
}

// 对客户端的benchmark
// Benchmark is benchmarking tool that generates workload and collects operation history and latency
type Benchmark struct {
	db DB // read/write operation interface
	Bconfig
	*History // 客户端对每个key的操作历史

	rate      *Limiter
	latency   []time.Duration // latency per operation
	startTime time.Time
	zipf      *rand.Zipf // 代表了一个 Zipf 分布,提供了一种方法，能够从该分布中生成符合特定参数的随机数。
	counter   int

	wait sync.WaitGroup // waiting for all generated keys to complete
}

// NewBenchmark returns new Benchmark object given implementation of DB interface
func NewBenchmark(db DB) *Benchmark {
	b := new(Benchmark)
	b.db = db
	b.Bconfig = config.Benchmark
	b.History = NewHistory()
	if b.Throttle > 0 {
		b.rate = NewLimiter(b.Throttle)
	}
	rand.Seed(time.Now().UTC().UnixNano())
	r := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	b.zipf = rand.NewZipf(r, b.ZipfianS, b.ZipfianV, uint64(b.K)) // 生成符合 Zipf 分布的随机数
	return b
}

// 对每个key都进行一次写操作
// 用于将所有键初始化到数据库中，确保数据库在基准测试之前已经加载了必要的数据。
// 设置写操作比例为 1.0 (b.W = 1.0)，意味着它只进行写操作，以便将所有键写入数据库。
// Load will create all K keys to DB
func (b *Benchmark) Load() {
	b.W = 1.0
	b.Throttle = 0

	b.db.Init()
	// keys和latencies是两个带缓冲区的通道
	keys := make(chan int, b.Concurrency)
	latencies := make(chan time.Duration, 1000)
	defer close(latencies)
	go b.collect(latencies)

	b.startTime = time.Now()
	for i := 0; i < b.Concurrency; i++ {
		go b.worker(keys, latencies)
	}
	for i := b.Min; i < b.Min+b.K; i++ {
		b.wait.Add(1) // 增加计数器
		keys <- i     // 往通道里写b.K个值
	}
	t := time.Now().Sub(b.startTime)

	b.db.Stop()
	close(keys)
	b.wait.Wait() // 等待所有操作完成,WaitGroup的计数器变为零
	stat := Statistic(b.latency)

	log.Infof("Benchmark took %v\n", t)
	log.Infof("Throughput %f\n", float64(len(b.latency))/t.Seconds())
	log.Info(stat)
}

// 实际执行基准测试，生成读写请求，收集操作的延迟数据，并计算和记录统计信息。
// 使用配置的写操作比例 (b.W)，执行基准测试时会根据这个比例决定进行读操作还是写操作
// Run starts the main logic of benchmarking
func (b *Benchmark) Run() {
	var stop chan bool // 这个通道用于控制定时任务的停止。
	if b.Move {
		move := func() { b.Mu = float64(int(b.Mu+1) % b.K) }           // 名为 move 的匿名函数,作用是更新 b.Mu 的值。
		stop = Schedule(move, time.Duration(b.Speed)*time.Millisecond) // 定时调用move
		defer close(stop)                                              // 在当前函数结束时停止定时任务
	}

	b.latency = make([]time.Duration, 0)
	keys := make(chan int, b.Concurrency)
	latencies := make(chan time.Duration, 1000)
	defer close(latencies)
	go b.collect(latencies)

	for i := 0; i < b.Concurrency; i++ {
		go b.worker(keys, latencies)
	}

	b.db.Init()
	b.startTime = time.Now()
	// 如果 b.T 大于 0，代码会在 b.T 秒内持续生成请求；否则，它会生成 b.N 个请求
	if b.T > 0 {
		timer := time.NewTimer(time.Second * time.Duration(b.T)) // 创建一个定时器 timer，该定时器将在 b.T 秒后触发
	loop:
		for {
			select {
			case <-timer.C: // timer.C 通道接收到一个值（即定时器触发）
				break loop
			default:
				b.wait.Add(1)
				keys <- b.next()
			}
		}
	} else {
		for i := 0; i < b.N; i++ {
			b.wait.Add(1)
			keys <- b.next()
		}
		b.wait.Wait()
	}
	t := time.Now().Sub(b.startTime)

	b.db.Stop()
	close(keys)
	stat := Statistic(b.latency)
	log.Infof("Concurrency = %d", b.Concurrency)
	log.Infof("Write Ratio = %f", b.W)
	log.Infof("Number of Keys = %d", b.K)
	log.Infof("Benchmark Time = %v\n", t)
	log.Infof("Throughput = %f\n", float64(len(b.latency))/t.Seconds())
	log.Info(stat)

	stat.WriteFile("latency")
	b.History.WriteFile("history")

	if b.LinearizabilityCheck {
		n := b.History.Linearizable()
		if n == 0 {
			log.Info("The execution is linearizable.")
		} else {
			log.Info("The execution is NOT linearizable.")
			log.Infof("Total anomaly read operations are %d", n)
			log.Infof("Anomaly percentage is %f", float64(n)/float64(stat.Size))
		}
	}
}

// generates key based on distribution
func (b *Benchmark) next() int {
	var key int
	switch b.Distribution {
	case "order":
		b.counter = (b.counter + 1) % b.K
		key = b.counter + b.Min

	case "uniform":
		key = rand.Intn(b.K) + b.Min

	case "conflict":
		if rand.Intn(100) < b.Conflicts {
			key = 0
		} else {
			b.counter = (b.counter + 1) % b.K
			key = b.counter + b.Min
		}

	case "normal":
		key = int(rand.NormFloat64()*b.Sigma + b.Mu)
		for key < 0 {
			key += b.K
		}
		for key > b.K {
			key -= b.K
		}

	case "zipfan":
		key = int(b.zipf.Uint64())

	case "exponential":
		key = int(rand.ExpFloat64() / b.Lambda)

	default:
		log.Fatalf("unknown distribution %s", b.Distribution)
	}

	if b.Throttle > 0 {
		b.rate.Wait()
	}

	return key
}

// a worker等于client的一个数据库连接，从通道keys中读取key，随机进行数据库读写操作。
func (b *Benchmark) worker(keys <-chan int, result chan<- time.Duration) {
	var s time.Time
	var e time.Time
	var v int
	var err error
	for k := range keys {
		op := new(operation)
		if rand.Float64() < b.W {
			v = rand.Int()
			s = time.Now()
			err = b.db.Write(k, v) // 根据client结构体实现的方法进行put操作
			e = time.Now()
			op.input = v
		} else {
			s = time.Now()
			v, err = b.db.Read(k) // 根据client结构体实现的方法进行get操作
			e = time.Now()
			op.output = v
		}
		op.start = s.Sub(b.startTime).Nanoseconds()
		if err == nil {
			op.end = e.Sub(b.startTime).Nanoseconds()
			result <- e.Sub(s)
		} else {
			op.end = math.MaxInt64
			log.Error(err)
		}
		b.History.AddOperation(k, op)
	}
}

func (b *Benchmark) collect(latencies <-chan time.Duration) {
	for t := range latencies {
		b.latency = append(b.latency, t)
		b.wait.Done() // 减少 WaitGroup 的计数器
	}
}
