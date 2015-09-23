package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

var outboxes = make(map[string]*outbox)
var globalLock = &lock{}

type lock struct {
	ModifiedIndex uint64
	sync.RWMutex
}

func etcdAPI(endpoints []string) (client.KeysAPI, error) {
	c, err := client.New(client.Config{
		Endpoints: endpoints,
		Transport: client.DefaultTransport,
	})

	if err != nil {
		return nil, err
	}
	return client.NewKeysAPI(c), nil
}

type callback func(node *client.Node) error

func get(kapi client.KeysAPI, key string, fn callback) {
	resp, err := kapi.Get(context.TODO(), key, nil)

	if err != nil {
		log.Printf("!! ERR: %v\n", err)
		return
	}

	if err := fn(resp.Node); err != nil {
		log.Printf("!! ERR: %v\n", err)
		log.Printf("!! Calling get again")
		get(kapi, key, fn)
	}
}

func watch(kapi client.KeysAPI, key string, fn callback) {
	watcher := kapi.Watcher(key, nil)

	for {
		resp, err := watcher.Next(context.TODO())
		if err != nil {
			log.Printf("!! ERR: %v\n", err)
			break
		}
		if err := fn(resp.Node); err != nil {
			// we got an outdated node. we should trigger a get
			// to ensure we get the latest value.
			get(kapi, key, fn)
		}
	}

}

func main() {
	log.Println("Starting...")
	kapi, err := etcdAPI([]string{"http://0.0.0.0:2379"})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Started with 0.0.0.0:2379")

	token := flag.String("token", "", "token to watch")
	flag.Parse()

	go get(kapi, *token, func(node *client.Node) error {
		log.Printf("get: apply called with %s -> %d\n", node.Value, node.ModifiedIndex)
		return apply(node)
	})

	go watch(kapi, *token, func(node *client.Node) error {
		log.Printf("watch: apply called with %s -> %d\n", node.Value, node.ModifiedIndex)
		return apply(node)
	})

	streamLogLines()
}

func newOutbox(url string) *outbox {
	return &outbox{url: url, ch: make(chan []byte, 100)}
}

type outbox struct {
	url    string
	ch     chan []byte
	closed bool
}

func (o *outbox) send(msg []byte) {
	if o.closed {
		return
	}
	o.ch <- msg
}

func (o *outbox) start() {
	log.Printf("Starting outbox:%s\n", o.url)
	for line := range o.ch {
		log.Printf("send(%s, %s)\n", o.url, line)
		send(o.url, line)
	}
}

func (o *outbox) stop() {
	log.Printf("Stopping outbox:%s\n", o.url)
	o.closed = true
	close(o.ch)
}

func send(url string, line []byte) {
	resp, err := http.Post(url, "application/logplex-1", bytes.NewReader(line))
	if err != nil {
		log.Printf("!! ERR: %s\n", url)
		return
	}
	defer resp.Body.Close()
}

func streamLogLines() {
	buf := bufio.NewReader(os.Stdin)

	for {
		line, err := buf.ReadBytes('\n')
		if len(line) > 0 {
			log.Printf("enqueue(%s)\n", line)
			enqueue(line)
		}

		if err != nil {
			log.Fatal(err)
		}
	}
}

func enqueue(line []byte) {
	globalLock.RLock()
	defer globalLock.RUnlock()

	for _, outbox := range outboxes {
		outbox.send(line)
	}
}

func errNotFound(err error) bool {
	if e, ok := err.(client.Error); ok {
		if e.Code == 100 {
			return true
		}
	}
	return false
}

func apply(node *client.Node) error {
	log.Printf("apply(%+v)\n", node)

	var v []string
	json.Unmarshal([]byte(node.Value), &v)

	globalLock.Lock()
	defer globalLock.Unlock()

	if node.ModifiedIndex < globalLock.ModifiedIndex {
		return fmt.Errorf("Trying to update stale data")
	}

	if node.ModifiedIndex == globalLock.ModifiedIndex {
		log.Printf("Got the same index; no need to do `apply`")
		return nil
	}

	for _, url := range v {
		if _, ok := outboxes[url]; !ok {
			outboxes[url] = newOutbox(url)
			go outboxes[url].start()
		}
	}

	for url, outbox := range outboxes {
		if !contains(v, url) {
			delete(outboxes, url)
			outbox.stop()
		}
	}

	return nil
}

func contains(list []string, elem string) bool {
	for _, e := range list {
		if e == elem {
			return true
		}
	}
	return false
}
