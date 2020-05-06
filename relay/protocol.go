// Package gossip is a simple implementation of a gossip protocol for noise. It keeps track of a cache of messages
// sent/received to/from peers to avoid re-gossiping particular messages to specific peers.
package relay

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync/atomic"

	"github.com/R-Niagra/noise-1"
	"github.com/R-Niagra/noise-1/kademlia"
	"github.com/VictoriaMetrics/fastcache"
)

const (
	DefaultPeerToSendRelay = 16
	broadcastChanSize      = 64 // default broadcast channel buffer size
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
	seen           *fastcache.Cache
}

// New returns a new instance of a gossip protocol with 32MB of in-memory cache instantiated.
func New(overlay *kademlia.Protocol, log bool, opts ...Option) *Protocol {
	p := &Protocol{
		overlay:        overlay,
		seen:           fastcache.New(32 << 20),
		relayChan:      make(chan Message, broadcastChanSize),
		msgSentCounter: 0,
		Logging:        log,
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

func (p *Protocol) setSeen(k, v []byte) {
	p.seen.SetBig(k, v)
}

func (p *Protocol) getSeen(k []byte) bool {
	return p.seen.Has(k)
}

func getShaHash(data []byte) []byte {
	h := sha1.New()
	h.Write([]byte(data))
	return h.Sum(nil)
}

// Push gossips a single message concurrently to all peers this node is aware of, on the condition that this node
// believes that the aforementioned peer has not received data before. A context may be provided to cancel Push, as it
// blocks the current goroutine until the gossiping of a single message is done. Any errors pushing a message to a
// particular peer is ignored.

func (p *Protocol) printMsgData(msg Message) {
	fmt.Printf("From: %v - To: %v - CODE: %v", msg.From.String(), msg.To, msg.Code)
}

func (p *Protocol) Relay(ctx context.Context, msg Message, changeRandomN bool, Fromid noise.ID) {
	// fmt.Println("Relay 1")
	if changeRandomN {
		msg.randomN = p.msgSentCounter
		atomic.AddUint32(&p.msgSentCounter, 1)
		if p.Logging {
			fmt.Printf("Sending Relay Msg at Node %v - %v\n", p.node.Addr(), msg.String())
		}
	}

	data := msg.Marshal()
	dataHash := p.hash(data)
	if !p.seen.Has(dataHash) {
		p.seen.SetBig(dataHash, nil)
	}

	localPeerAddress := p.overlay.Table().AddressFromPK(msg.To)
	// fmt.Println(".")
	// fmt.Println("Local peer add for relay:- ", localPeerAddress, "by str comp:- ", p.overlay.Table().AddressFromPK2(msg.To))
	// fmt.Println("Closest: ", p.overlay.Table().FindClosest(msg.To, DefaultPeerToSendRelay))
	// fmt.Println(".")

	if localPeerAddress != "" {
		if err := p.node.SendMessage(ctx, localPeerAddress, msg); err != nil {
			fmt.Printf("Relay local send msg Fucked %v\n", err)
		}
		return
	}

	// peers := p.overlay.Find(msg.To)
	peers := p.overlay.Table().FindClosest(msg.To, DefaultPeerToSendRelay)
	// fmt.Println("Peers to send to:", peers)

	// for _, p := range peers{

	// }

	// fmt.Printf("Relay Peers %v\n", peers)
	// var wg sync.WaitGroup
	// wg.Add(len(peers))

	for _, id := range peers {
		id, key := id, dataHash
		// key := p.hash(id, data)
		if Fromid.ID.String() == id.ID.String() {
			continue
		}

		go func() {
			// defer wg.Done()

			if p.seen.Has(key) {
				// fmt.Printf("Relay ID %v Alread seen Msg!! %v\n", id, hex.EncodeToString(key))
				return
			}
			if len(key) > 64000 {
				fmt.Printf("WARN torture Relay ID %v hash %v\n", id, len(key))
				p.printMsgData(msg)
			} else {
				fmt.Printf("torture Relay ID %v size %v ,hash %v\n", id, len(key), hex.EncodeToString(key))
				p.printMsgData(msg)
			}
			if err := p.node.SendMessage(ctx, id.Address, msg); err != nil {
				// fmt.Printf("Relay send msg Fucked %v\n", err)
				return
			}

			// p.seen.SetBig(key, nil)
		}()
	}

	// wg.Wait()
}

// Handle implements noise.Protocol and handles gossip.Message messages.
func (p *Protocol) Handle(ctx noise.HandlerContext) error {
	// fmt.Printf("Relay Handle Enter, ctx.ID %v msg: %v\n", ctx.ID(), ctx.Message().String())
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

	// fmt.Printf("Handle received msg %v\n", msg.String())
	data := msg.Marshal()
	dataHash := p.hash(data)
	if p.seen.Has(dataHash) {
		return nil
	}

	// p.seen.SetBig(p.hash(ctx.ID(), data), nil) // Mark that the sender already has this data.
	p.seen.SetBig(dataHash, nil) // Mark that the sender already has this data.
	// fmt.Printf("Seen Hash set in Handle  for ID %v and data %v  %v\n", ctx.ID(), hex.EncodeToString(p.hash(ctx.ID(), data)))

	// self := p.hash(p.node.ID(), data)

	// if p.seen.Has(self) {
	// 	return nil
	// }

	// p.seen.SetBig(self, nil) // Mark that we already have this data.

	if msg.To == p.node.ID().ID {
		if p.Logging {
			fmt.Printf("Relay Msg Received at Node %v From Peer %v - %v\n", p.node.Addr(), ctx.ID(), msg.String())
		}
		p.relayChan <- msg
	} else {
		// fmt.Println("Relay Handle Relaying further")
		go p.Relay(context.TODO(), msg, false, ctx.ID())
	}

	// if p.events.OnGossipReceived != nil {
	// 	if err := p.events.OnGossipReceived(ctx.ID(), msg); err != nil {
	// 		return err
	// 	}
	// }

	return nil
}

// func (p *Protocol) hash(id noise.ID, data []byte) []byte {
func (p *Protocol) hash(data []byte) []byte {
	hasher := sha256.New()
	hasher.Write(data)
	return hasher.Sum(nil)
	// return append(id.ID[:], data...)
}

func (p *Protocol) GetRelayChan() chan Message {
	return p.relayChan
}
