package main

import "log"
import "flag"
import "net"
import "io"
import "bufio"
import "time"
import "github.com/miolini/godaemon"
import "compress/gzip"
import "runtime"
import nsq "github.com/bitly/go-nsq"
import "strings"

const (
	RECV_BUF_LEN = 2048
)

var (
	listenAddr = flag.String("l", "localhost:7777", "listen addr host:port")
	hosts      = flag.String("h", "localhost:4150", "comma-separated list of nsq host:port")
	topic      = flag.String("t", "t-simpletcp", "send all messages to this nsq topic")
	batchSize  = flag.Int("b", 1000, "batch size to publish")
	cpus       = flag.Int("c", runtime.NumCPU(), "max using cpu cores")
	publishers = flag.Int("p", 2, "connection numbers to eash nsqd node")
	decompress = flag.Bool("d", false, "enable gzip decompress of incoming data")
	pidFile    = flag.String("pidfile", "", "path to pid file")
)

func main() {
	log.Printf("nsq-simpletcp transmitter")

	flag.Parse()

	log.Printf("listenAddr: %s", *listenAddr)
	log.Printf("hosts:      %s", *hosts)
	log.Printf("topic:      %s", *topic)
	log.Printf("batchSize:  %d", *batchSize)
	log.Printf("cpus:       %d", *cpus)
	log.Printf("publishers: %d", *publishers)
	log.Printf("decompress: %b", *decompress)
	log.Printf("pidfile:    %s", *pidFile)

	runtime.GOMAXPROCS(*cpus)
	godaemon.WritePidFile(*pidFile)
	dataChan := make(chan string, 1000)
	statChan := NewStatCounter("batchs", 1)

	godaemon.NewWorker("listener", 1, time.Millisecond).Start(func(worker *godaemon.Worker) (err error) {
		log.Printf("start listener on %s", *listenAddr)
		return workerListener(*listenAddr, dataChan, *decompress, *batchSize)
	})

	hostList := strings.Split(*hosts, ",")
	for _, host := range hostList {
		runWorkerPublisher(host, *topic, dataChan, *publishers)
	}

	<-make(chan bool)
}

func runWorkerPublisher(host, topic string, dataChan chan string, publishers int) {
	godaemon.NewWorker("publisher", publishers, time.Second * 15).Start(func(worker *godaemon.Worker) (err error) {
		log.Printf("start publisher to %s", host)
		return workerPublisher(host, topic, dataChan)
	})
}

func readClient(reader io.Reader, dataChan chan string, compress bool, batchSize int) {
	var (
		scanner *bufio.Scanner
		line    string
		batch   string
		counter int
	)
	if compress {
		gzipReader, err := gzip.NewReader(reader)
		if err != nil {
			log.Printf("gzip error: %s", err)
			return
		}
		scanner = bufio.NewScanner(gzipReader)
	} else {
		scanner = bufio.NewScanner(reader)
	}
	for scanner.Scan() {
		line = scanner.Text()
		batch += line + "\n"
		counter++
		if counter < batchSize {
			continue
		}
		dataChan <- batch
		counter = 0
		batch = ""
	}
}

func workerListener(addr string, dataChan chan string, compress bool, batchSize int) (err error) {
	var (
		listener net.Listener
		conn     net.Conn
	)
	if listener, err = net.Listen("tcp", addr); err != nil {
		return
	}
	for {
		if conn, err = listener.Accept(); err != nil {
			return
		}
		log.Printf("new connection")
		go readClient(conn, dataChan, compress, batchSize)
	}
}

func workerPublisher(hosts string, topic string, dataChan chan string) (err error) {
	config := nsq.NewConfig()
	w, err := nsq.NewProducer(hosts, config)
	if err != nil {
		return
	}
	for {
		select {
		case data := <-dataChan:
			err = w.Publish(topic, []byte(data))
			if err != nil {
				go func(msg string) {
					dataChan <- msg
				}(data)
				return
			}
		}
	}
}

func NewStatCounter(label string, interval int) (chan int) {
    statChan := make(chan int)
    counter := 0
    timer := time.Now()
    mutex := sync.Mutex{}
    go func() {
        for volume := range statChan {
            mutex.Lock()
            counter += volume
            mutex.Unlock()
        }
    }()
    go func () {
        for {
            mutex.Lock()
            log.Printf("speed %s %d msg/sec", label, counter / interval)
            counter = 0
            timer = time.Now()
            mutex.Unlock()
            time.Sleep(time.Second * time.Duration(interval))
        }
    }()
    return statChan
}
