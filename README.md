# NSQ-SimpleTCP

This is daemon writen on Go language for receiver over TCP text stream CRLF separated and 
publish each line as a message in NSQ topic.

## Install

```
# git clone https://github.com/miolini/nsq-simpletcp
# make depends
# make
```

## Usage

```
# ./nsq-simpletcp --help
Usage of ./nsq-simpletcp:
  -b=1000: batch size to publish
  -c=4: max using cpu cores
  -d=false: enable gzip decompress of incoming data
  -h="localhost:4150": comma-separated list of nsq host:port
  -l="localhost:7777": listen addr host:port
  -pidfile="": path to pid file
  -t="t-simpletcp": send all messages to this nsq topic
```

## Support

If you have any questions, please, send email to me: mio@volmy.com.


Enjoy!