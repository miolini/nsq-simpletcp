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
	log.Printf("decompress: %s", *decompress)
	log.Printf("pidfile:    %s", *pidFile)

	godaemon.WritePidFile(*pidFile)

	dataChan := make(chan string, 1000)

	godaemon.NewWorker("listener", 1, time.Second).Start(func(worker *godaemon.Worker) (err error) {
		log.Printf("start listener on %s")
		return workerListener(*listenAddr, dataChan, *decompress)
	})

	godaemon.NewWorker("publisher", 4, time.Second).Start(func(worker *godaemon.Worker) (err error) {
		log.Printf("start publisher")
		return workerPublisher(*hosts, *topic, *batchSize, dataChan)
	})

	<-make(chan bool)
}

func readClient(reader io.Reader, dataChan chan string, compress bool) {
	var scanner *bufio.Scanner
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
		dataChan <- scanner.Text()
	}
}

func workerListener(addr string, dataChan chan string, compress bool) (err error) {
    var (
        listener net.Listener
        conn net.Conn
    )
	if listener, err = net.Listen("tcp", addr); err != nil {
		return
	}
	for {
		if conn, err = listener.Accept(); err != nil {
			return
		}
		log.Printf("new connection")
		go readClient(conn, dataChan, compress)
	}
}

func workerPublisher(hosts string, topic string, batchSize int, dataChan chan string) (err error) {
	config := nsq.NewConfig()
    w, err := nsq.NewProducer(hosts, config)
    if err != nil {
        return
    }
    var batch []string
	for {
		select {
		case data := <-dataChan:
            batch = append(batch, data)
            if len(batch) < batchSize {
                continue
            }
            err = w.Publish(topic, []byte(strings.Join(batch, "\n")))
            if err != nil {
                return
            }
            batch = []string{}
		}
	}
}
