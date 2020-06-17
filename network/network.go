package network

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/R-Niagra/noise-1"
	"github.com/R-Niagra/noise-1/broadcast"
	"github.com/R-Niagra/noise-1/kademlia"
	"github.com/R-Niagra/noise-1/relay"
	"go.uber.org/zap"
	// "github.com/R-Niagra/noise-1/broadcast"
	// "github.com/R-Niagra/noise-1/relay"
)

const (
	// DefaultBootstrapTimeout is the default timeout for bootstrapping with each peer.
	DefaultBootstrapTimeout = 10 * time.Second
	// DefaultPeerThreshold is the default threshold above which bootstrapping is considered successful
	DefaultPeerThreshold = 8
	MsgChanSize          = 64
)

// Network encapsulates the communication in a noise p2p network.
type Network struct {
	node          *noise.Node
	overlay       *kademlia.Protocol
	broadcastHub  *broadcast.Protocol
	relayHub      *relay.Protocol
	relayChan     chan relay.Message
	broadcastChan chan broadcast.Message
}

// //HandlePeerDisconnection registers the dinconnection callback with the interface
// func (ntw *Network) HandlePeerDisconnection(peerCleanup disconCleanInterface) {

// 	node := ntw.node

// 	node.OnPeerDisconnected(func(node *noise.Node, peer *noise.Peer) error {
// 		fmt.Println("PEER HAS BEEN DISCONNECTED: ", (protocol.PeerID(peer).(kad.ID)).Address())
// 		// fmt.Println(kad.Table(node).GetPeers())
// 		kad.Table(node).Delete(protocol.PeerID(peer)) //Deleting it from the table where it is used to send messages e.g broadcasting
// 		// fmt.Println("After delete: ", kad.Table(node).GetPeers())
// 		peerCleanup.OnDisconnectCleanup(protocol.PeerID(peer).(kad.ID))
// 		return nil
// 	})

// }

// check panics if err is not nil.
func check(err error) {
	if err != nil {
		panic(err)
	}
}

// printedLength is the total prefix length of a public key associated to a chat users ID.
const printedLength = 8

// New creates and returns a new network instance.
func New(hostStr string, port uint16, privatekey noise.PrivateKey, logger *zap.Logger, logging bool) (*Network, error) {
	// Set up node and policy.
	host := net.ParseIP(hostStr)
	if host == nil {
		return nil, errors.New("host in provided public address is invalid (must be IPv4/IPv6)")
	}

	node, err := noise.NewNode(
		noise.WithNodeBindHost(host), //net.ParseIP(host)),
		noise.WithNodeBindPort(port),
		noise.WithNodeLogger(logger),
		noise.WithNodePrivateKey(privatekey),
		// noise.WithNodeAddress(*addressFlag),
	)

	check(err)
	events := kademlia.Events{
		OnPeerAdmitted: func(id noise.ID) {
			fmt.Printf("Learned about a new peer %s(%s).\n", id.Address, id.ID.String()[:printedLength])
		},
		OnPeerEvicted: func(id noise.ID) {
			fmt.Printf("Forgotten a peer %s(%s).\n", id.Address, id.ID.String()[:printedLength])
		},
	}

	overlay := kademlia.New(kademlia.WithProtocolEvents(events))
	// Bind Kademlia to the node.
	// GosipEvents := gossip.Events{
	// 	OnGossipReceived: func(node *noise.Node, sender noise.ID, data []byte) error {
	// 		msg, err := unmarshalChatMessage(data)
	// 		if err != nil {
	// 			return nil
	// 		}

	// 		if len(msg.Data) == 0 {
	// 			return nil
	// 		}

	// 		fmt.Printf("Gosip MSG REceieved")
	// 		// node.MsgChan <- msg
	// 		fmt.Printf("%s(%s)> %s\n", sender.Address, sender.ID.String()[:printedLength], msg.String())
	// 		return nil
	// 	},
	// }

	broadcastHub := broadcast.New(overlay, logging) //gossip.WithEvents(GosipEvents)
	relayHub := relay.New(overlay, logging)

	node.Bind(
		overlay.Protocol(),
		broadcastHub.Protocol(),
		relayHub.Protocol(),
	)

	node.Listen()
	return &Network{
		node:          node,
		overlay:       overlay,
		broadcastHub:  broadcastHub,
		relayHub:      relayHub,
		broadcastChan: broadcastHub.GetBroadcastChan(),
		relayChan:     relayHub.GetRelayChan(),
	}, nil
}

// Bootstrap bootstraps a network using a list of peer addresses and returns whether bootstrap finished before timeout.
func (ntw *Network) Bootstrap(peerAddrs []string, timeout time.Duration, peerThreshold int) bool {
	if len(peerAddrs) == 0 {
		return false
	}

	for _, addr := range peerAddrs {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		_, err := ntw.node.Ping(ctx, addr)
		cancel()

		if err != nil {
			fmt.Printf("Failed to ping bootstrap node (%s). Skipping... [error: %s]\n", addr, err)
			continue
		}

		if ntw.GetNumPeers() >= peerThreshold {
			return true
		}

	}
	fmt.Println("Bootstrap after ping no of peers: ", ntw.GetNumPeers())

	if ntw.GetNumPeers() <= peerThreshold {
		ntw.Discover()
	}
	fmt.Println("Bootstrap after discover no of peers: ", ntw.GetNumPeers())

	return true
}

func (ntw *Network) Discover() {
	ids := ntw.overlay.Discover(kademlia.WithIteratorMaxNumResults(3))

	var str []string
	for _, id := range ids {
		str = append(str, fmt.Sprintf("%s(%s)", id.Address, id.ID.String()[:printedLength]))
	}

	if len(ids) > 0 {
		fmt.Printf("Discovered %d peer(s): [%v]\n", len(ids), strings.Join(str, ", "))
	} else {
		fmt.Printf("Did not discover any peers.\n")
	}
}

//DisconWithPeers discionnects with the given ids
func (ntw *Network) DisconWithPeers(ids []noise.ID) {
	// for _, id := range ids {
	// _, _ = ntw.overlay.Table().Delete(id.ID) //Delete overlay entries from the node peers
	// if done == true {
	// 	fmt.Println("Success Disconnection with ", id)
	// } else {
	// 	fmt.Println("Disconnection failed with ", id)
	// 	}
	// }

	for _, id := range ids {
		// _, done := ntw.overlay.Table().Delete(id.ID) //Delete overlay entries from the node peers
		ntw.node.DisconWithPeer(id.Address)
	}

	for _, id := range ids {
		_, _ = ntw.overlay.Table().Delete(id.ID) //Delete overlay entries from the node peers
		// if done == true {
		// 	fmt.Println("Success Disconnection with ", id)
		// } else {
		// 	fmt.Println("Disconnection failed with ", id)
		// }
	}

}

// peers prints out all peers we are already aware of.
func (ntw *Network) GetPeerAddrs() []noise.ID {
	return ntw.overlay.Table().Peers()
}

// BootstrapDefault runs Bootstrap with default parameters.
func (ntw *Network) BootstrapDefault(peerAddrs []string) bool {
	return ntw.Bootstrap(peerAddrs, DefaultBootstrapTimeout, DefaultPeerThreshold)
}

// GetRelayChan returns the channel for relay messages.
func (ntw *Network) GetRelayChan() chan relay.Message {
	return ntw.relayChan
}

// GetBroadcastChan returns the channel for broadcast messages.
func (ntw *Network) GetBroadcastChan() chan broadcast.Message {
	return ntw.broadcastChan
}

// GetNodeID returns the network node's skademlia ID.
func (ntw *Network) GetNodeID() noise.ID {
	return ntw.node.ID()
}

//AddressFromPK returns the address associated with the public key
func (ntw *Network) AddressFromPK(publicKey noise.PublicKey) string {
	return ntw.overlay.Table().AddressFromPK(publicKey)
}

// GetNumPeers returns the number of peers the network node has.
func (ntw *Network) GetNumPeers() int {
	return len(ntw.overlay.Table().Peers())
}

// Broadcast broadcasts data to the entire p2p network.
func (ntw *Network) Broadcast(code byte, data []byte) {
	// ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	// fmt.Println("Broad CAST HERE")
	msg := broadcast.Message{}
	msg.From = ntw.Node().ID()
	msg.Data = data
	msg.Code = code

	ntw.broadcastHub.Push(context.TODO(), msg, true, msg.From)
	// cancel()
}

func (ntw *Network) FindPeer(target noise.PublicKey) []noise.ID {
	return ntw.overlay.Table().FindClosest(target, 16)
}

func (ntw *Network) RelayToPB(peerID noise.PublicKey, code byte, data []byte) {
	// ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	msg := relay.Message{}
	msg.From = ntw.node.ID()
	msg.Data = data
	msg.To = peerID
	msg.Code = code

	ntw.relayHub.Relay(context.TODO(), msg, true, msg.From)
	// cancel()
}

func (ntw *Network) Relay(peerID noise.ID, code byte, data []byte) {
	ntw.RelayToPB(peerID.ID, code, data)
}

func (ntw *Network) Close() {
	ntw.node.Close()
}

func (ntw *Network) Node() *noise.Node {
	return ntw.node
}

func (ntw *Network) Process() {
	for {
		select {
		case msg := <-ntw.relayChan:
			fmt.Printf("Relay Msg : %v\n", msg.String())
			break
		case msg := <-ntw.broadcastChan:
			fmt.Printf("Broadcast msg: %v\n", msg.String())
			break
		}
	}
}

func (ntw *Network) ExternalAddress() string {
	return ntw.node.ExternalAddress()
}
