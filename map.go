package noise

import (
	"container/list"
	"errors"
	"math"
	"sync"
	"fmt"
)

type clientMapEntry struct {
	el     *list.Element
	client *Client
}

type clientMap struct {
	sync.Mutex

	cap     uint
	order   *list.List
	entries map[string]clientMapEntry
}

func newClientMap(cap uint) *clientMap {
	return &clientMap{
		cap:     cap,
		order:   list.New(),
		entries: make(map[string]clientMapEntry, cap),
	}
}

//PrintClientMap prints the client map
func (c *clientMap) PrintClientMap(myAdd string) {
	fmt.Println(".")
	fmt.Println("Printing the client map...",myAdd)
	for key, val := range c.entries {
		fmt.Println("addr is: ", key, " client is: ", val.client.ID().Address)
	}
	fmt.Println(".")

}

//CheckEntry checks if entry exist in the client map
func (c *clientMap) CheckEntry(n *Node, addr string) (*Client, bool) {
	entry, exists := c.entries[addr]
	if exists {
		return entry.client, true
	}

	return nil, false
}

func (c *clientMap) get(n *Node, addr string) (*Client, bool) {
	c.Lock()
	defer c.Unlock()

	entry, exists := c.entries[addr]
	// fmt.Println("entry against adde: ",addr,entry.client.ID())
	// n.logger.Info(" \n Connection Entries for Address", zap.String("Addr ", addr),  c.entries[addr])
	if !exists {
		// fmt.Printf(" \n Connection not exist in Entries for Address %v %v \n", addr, c.entries)
		if uint(len(c.entries)) == n.maxInboundConnections {
			el := c.order.Back()
			evicted := c.order.Remove(el).(string)

			e := c.entries[evicted]
			delete(c.entries, evicted)

			e.client.close()
			e.client.waitUntilClosed()
		}

		entry.el = c.order.PushFront(addr)
		entry.client = newClient(n)

		c.entries[addr] = entry
	} else {
		c.order.MoveToFront(entry.el)
	}

	// fmt.Printf(" \n Connection Entries for Address %v %v \n", addr, c.entries)
	return entry.client, exists
}

func (c *clientMap) remove(addr string) {
	c.Lock()
	defer c.Unlock()

	entry, exists := c.entries[addr]
	if !exists {
		return
	}

	c.order.Remove(entry.el)
	delete(c.entries, addr)
}

func (c *clientMap) release() {
	c.Lock()

	entries := c.entries
	c.entries = make(map[string]clientMapEntry, c.cap)
	c.order.Init()

	c.Unlock()

	for _, e := range entries {
		e.client.close()
		e.client.waitUntilClosed()
	}
}

func (c *clientMap) slice() []*Client {
	c.Lock()
	defer c.Unlock()

	clients := make([]*Client, 0, len(c.entries))
	for el := c.order.Front(); el != nil; el = el.Next() {
		clients = append(clients, c.entries[el.Value.(string)].client)
	}

	return clients
}

type requestMap struct {
	sync.Mutex
	entries map[uint64]chan message
	nonce   uint64
}

func newRequestMap() *requestMap {
	return &requestMap{entries: make(map[uint64]chan message)}
}

func (r *requestMap) nextNonce() (<-chan message, uint64, error) {
	r.Lock()
	defer r.Unlock()

	if r.nonce == math.MaxUint64 {
		r.nonce = 0
	}

	r.nonce++
	nonce := r.nonce

	if _, exists := r.entries[nonce]; exists {
		return nil, 0, errors.New("ran out of available nonce to use for making a new request")
	}

	ch := make(chan message, 1)
	r.entries[nonce] = ch

	return ch, nonce, nil
}

func (r *requestMap) markRequestFailed(nonce uint64) {
	r.Lock()
	defer r.Unlock()

	close(r.entries[nonce])
	delete(r.entries, nonce)
}

func (r *requestMap) findRequest(nonce uint64) chan<- message {
	r.Lock()
	defer r.Unlock()

	ch, exists := r.entries[nonce]
	if exists {
		delete(r.entries, nonce)
	}

	return ch
}

func (r *requestMap) close() {
	r.Lock()
	defer r.Unlock()

	for nonce := range r.entries {
		close(r.entries[nonce])
		delete(r.entries, nonce)
	}
}
