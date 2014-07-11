all: depends clean build

clean:
	rm -rf nsq-simpletcp

build:
	go build -o nsq-simpletcp nsq-simpletcp.go

depends:
	go get -d