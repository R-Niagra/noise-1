// Package gossip is a simple implementation of a gossip protocol for noise. It keeps track of a cache of messages
// sent/received to/from peers to avoid re-gossiping particular messages to specific peers.
package broadcast

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync/atomic"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/R-Niagra/noise-1"
	"github.com/R-Niagra/noise-1/kademlia"
)

const (
	relayChanSize = 64
)

// Protocol implements a simple gossiping protocol that avoids resending messages to peers that it already believes
// is aware of particular messages that are being gossiped.
type Protocol struct {
	node           *noise.Node
	overlay        *kademlia.Protocol
	events         Events
	relayChan      chan Message
	msgSentCounter uint32
	Logging        bool

	seen *fastcache.Cache
}

// New returns a new instance of a gossip protocol with 32MB of in-memory cache instantiated.
func New(overlay *kademlia.Protocol, log bool, opts ...Option) *Protocol {
	p := &Protocol{
		overlay:   overlay,
		seen:      fastcache.New(32 << 20),
		relayChan: make(chan Message, relayChanSize),
		Logging:   log,
	}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

// Protocol returns a noise.Protocol that may registered to a node via (*noise.Node).Bind.
func (p *Protocol) Protocol() noise.Protocol {
	return noise.Protocol{
		VersionMajor: 0,
		VersionMinor: 0,
		VersionPatch: 0,
		Bind:         p.Bind,
	}
}

// Bind registers a single message gossip.Message, and handles them by registering the (*Protocol).Handle Handler.
func (p *Protocol) Bind(node *noise.Node) error {
	p.node = node

	node.RegisterMessage(Message{}, UnmarshalMessage)
	node.Handle(p.Handle)

	return nil
}

// Push gossips a single message concurrently to all peers this node is aware of, on the condition that this node
// believes that the aforementioned peer has not received data before. A context may be provided to cancel Push, as it
// blocks the current goroutine until the gossiping of a single message is done. Any errors pushing a message to a
// particular peer is ignored.
func (p *Protocol) Push(ctx context.Context, msg Message, changeRandomN bool, Fromid noise.ID) {
	if changeRandomN {
		msg.randomN = p.msgSentCounter
		atomic.AddUint32(&p.msgSentCounter, 1)
		if p.Logging {
			fmt.Printf("Sending BroadCast Msg at Node %v - %v\n", p.node.Addr(), msg.String())
		}
	}

	data := msg.Marshal()
	// p.seen.SetBig(p.hash(p.node.ID(), data), nil)
	dataHash := p.hash(data)
	if !p.seen.Has(dataHash) {
		p.seen.SetBig(dataHash, nil)
	}

	peers := p.overlay.Table().Entries()
	// var wg sync.WaitGroup
	// wg.Add(len(peers))

	for _, id := range peers {
		// id, key := id, p.hash(id, data)
		id, _ := id, p.hash(data)
		if Fromid.ID.String() == id.ID.String() {
			continue
		}
		go func() {
			// defer wg.Done()

			// if p.seen.Has(key) {
			// 	return
			// }

			if err := p.node.SendMessage(ctx, id.Address, msg); err != nil {
				return
			}

			// p.seen.SetBig(key, nil)
		}()
	}

	// wg.Wait()
}

// Handle implements noise.Protocol and handles gossip.Message messages.
func (p *Protocol) Handle(ctx noise.HandlerContext) error {
	// fmt.Println("Gosip Handle")
	if ctx.IsRequest() {
		return nil
	}

	obj, err := ctx.DecodeMessage()
	if err != nil {
		return nil
	}

	msg, ok := obj.(Message)
	if !ok {
		return nil
	}

	data := msg.Marshal()
	// p.seen.SetBig(p.hash(ctx.ID(), data), nil) // Mark that the sender already has this data.
	dataHash := p.hash(data)
	if p.seen.Has(dataHash) {
		return nil
	}

	p.seen.SetBig(dataHash, nil) // Mark that the sender already has this data.

	// self := p.hash(data)
	// self := p.hash(p.node.ID(), data)
	// p.seen.SetBig(self, nil) // Mark that we already have this data.

	p.relayChan <- msg
	if p.Logging {
		fmt.Printf("BroadCast Msg Received at Node %v From Peer %v - %v\n", p.node.Addr(), ctx.ID(), msg.String())
	}
	// if p.events.OnGossipReceived != nil {
	// 	if err := p.events.OnGossipReceived(p.node, ctx.ID(), msg); err != nil {
	// 		return err
	// 	}
	// }

	p.Push(context.Background(), msg, false, ctx.ID())

	return nil
}

func (p *Protocol) hash(data []byte) []byte {
	hasher := sha256.New()
	hasher.Write(data)
	return hasher.Sum(nil)
	// return append(id.ID[:], data...)
}

func (p *Protocol) GetBroadcastChan() chan Message {
	return p.relayChan
}
