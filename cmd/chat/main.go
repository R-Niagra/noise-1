package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/R-Niagra/noise-1"
	"github.com/R-Niagra/noise-1/kademlia"
	"github.com/R-Niagra/noise-1/payload"
	"github.com/spf13/pflag"
)

var (
	hostFlag    = pflag.IPP("host", "h", nil, "binding host")
	portFlag    = pflag.Uint16P("port", "p", 0, "binding port")
	addressFlag = pflag.StringP("address", "a", "", "publicly reachable network address")
)

type Message struct {
	From   noise.ID
	Code   byte // 0 for relay, 1 for broadcast
	Data   []byte
	SeqNum byte
}

func (msg Message) Marshal() []byte {
	writer := payload.NewWriter(nil)
	writer.Write(msg.From.Marshal())
	writer.WriteByte(msg.Code)
	writer.WriteByte(msg.SeqNum)
	writer.WriteUint32(uint32(len(msg.Data)))
	writer.Write(msg.Data)
	return writer.Bytes()
}

func (m Message) String() string {
	return m.From.String() + " Code: " + strconv.Itoa(int(m.Code)) + " SeqNum: " + strconv.Itoa(int(m.SeqNum)) + " msg: " + string(m.Data)
}

func unmarshalChatMessage(buf []byte) (Message, error) {
	msg := Message{}
	msg.From, _ = noise.UnmarshalID(buf)

	buf = buf[msg.From.Size():]
	reader := payload.NewReader(buf)
	code, err := reader.ReadByte()
	if err != nil {
		panic(err)
	}
	msg.Code = code

	seqNum, err := reader.ReadByte()
	if err != nil {
		panic(err)
	}
	msg.SeqNum = seqNum

	data, err := reader.ReadBytes()
	if err != nil {
		panic(err)
	}
	msg.Data = data
	return msg, nil
}

// check panics if err is not nil.
func check(err error) {
	if err != nil {
		panic(err)
	}
}

// printedLength is the total prefix length of a public key associated to a chat users ID.
const printedLength = 8

func GetLocalIP() net.IP {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP
			}
		}
	}
	return nil
}

// An example chat application on Noise.
func main() {
	// Parse flags/options.
	pflag.Parse()

	logger, err := zap.NewDevelopment(zap.AddStacktrace(zap.PanicLevel))
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	// Create a new configured node.
	localIP := GetLocalIP()
	node, err := noise.NewNode(
		noise.WithNodeBindHost(localIP),
		noise.WithNodeBindPort(*portFlag),
		noise.WithNodeLogger(logger),
		// noise.WithNodeAddress(*addressFlag),
	)
	check(err)

	// Release resources associated to node at the end of the program.
	defer node.Close()

	// Register the Message Go type to the node with an associated unmarshal function.
	node.RegisterMessage(Message{}, unmarshalChatMessage)

	// Register a message handler to the node.
	node.Handle(handle)

	// Instantiate Kademlia.
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
	node.Bind(overlay.Protocol())

	// Have the node start listening for new peers.
	check(node.Listen())

	// Print out the nodes ID and a help message comprised of commands.
	help(node)

	// Ping nodes to initially bootstrap and discover peers from.
	bootstrap(node, pflag.Args()...)

	// Attempt to discover peers if we are bootstrapped to any nodes.
	discover(overlay)

	// Accept chat message inputs and handle chat commands in a separate goroutine.
	go input(func(line string) {
		chat(node, overlay, line)
	})

	// Wait until Ctrl+C or a termination call is done.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	// Close stdin to kill the input goroutine.
	check(os.Stdin.Close())

	// Empty println.
	println()
}

// input handles inputs from stdin.
func input(callback func(string)) {
	r := bufio.NewReader(os.Stdin)

	for {
		buf, _, err := r.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}

			check(err)
		}

		line := string(buf)
		if len(line) == 0 {
			continue
		}

		callback(line)
	}
}

// handle handles and prints out valid chat messages from peers.
func handle(ctx noise.HandlerContext) error {
	obj, err := ctx.DecodeMessage()
	if err != nil {
		return nil
	}

	msg, ok := obj.(Message)
	if !ok {
		return nil
	}

	if len(msg.Data) == 0 {
		return nil
	}

	fmt.Printf("%s(%s)> %s\n", ctx.ID().Address, ctx.ID().ID.String()[:printedLength], msg.String())

	if ctx.IsRequest() {
		msg2 := Message{}
		msg2.From = ctx.ID()
		msg2.Data = []byte("GOT: " + string(msg.Data))
		msg2.Code = 2
		msg2.SeqNum = 1
		return ctx.SendMessage(msg2)
	}

	return nil
}

// help prints out the users ID and commands available.
func help(node *noise.Node) {
	fmt.Printf("Your ID is %s(%s). Type '/discover' to attempt to discover new "+
		"peers, or '/peers' to list out all peers you are connected to.\n",
		node.ID().Address,
		node.ID().ID.String()[:printedLength],
	)
}

// bootstrap pings and dials an array of network addresses which we may interact with and  discover peers from.
func bootstrap(node *noise.Node, addresses ...string) {
	for _, addr := range addresses {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := node.Ping(ctx, addr)
		cancel()

		if err != nil {
			fmt.Printf("Failed to ping bootstrap node (%s). Skipping... [error: %s]\n", addr, err)
			continue
		}
	}
}

// discover uses Kademlia to discover new peers from nodes we already are aware of.
func discover(overlay *kademlia.Protocol) {
	ids := overlay.Discover()

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

// peers prints out all peers we are already aware of.
func peers(overlay *kademlia.Protocol) {
	ids := overlay.Table().Peers()

	var str []string
	for _, id := range ids {
		str = append(str, fmt.Sprintf("%s(%s)", id.Address, id.ID.String()[:printedLength]))
	}

	fmt.Printf("You know %d peer(s): [%v]\n", len(ids), strings.Join(str, ", "))
}

// chat handles sending chat messages and handling chat commands.
func chat(node *noise.Node, overlay *kademlia.Protocol, line string) {
	switch line {
	case "/discover":
		discover(overlay)
		return
	case "/peers":
		peers(overlay)
		return
	case "/request":
		for _, id := range overlay.Table().Peers() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			msg := Message{}
			msg.From = node.ID()
			msg.SeqNum = byte(int(0))
			msg.Code = byte(int(1))
			msg.Data = []byte(line)
			msg2, err := node.RequestMessage(ctx, id.Address, msg)
			if err != nil {
				fmt.Printf("Failed to send message to %s(%s). Skipping... [error: %s]\n",
					id.Address,
					id.ID.String()[:printedLength],
					err,
				)
				continue
			} else {
				fmt.Printf("GOR RESPONSE for Request %v", msg2.(Message).String())
			}
			cancel()
		}
		return
	default:
	}

	if strings.HasPrefix(line, "/") {
		help(node)
		return
	}

	for _, id := range overlay.Table().Peers() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		msg := Message{}
		msg.From = node.ID()
		msg.Data = []byte(line)
		msg.SeqNum = byte(3)
		msg.Code = byte(3)
		fmt.Printf("msg %v", msg.String())
		err := node.SendMessage(ctx, id.Address, msg)
		cancel()

		if err != nil {
			fmt.Printf("Failed to send message to %s(%s). Skipping... [error: %s]\n",
				id.Address,
				id.ID.String()[:printedLength],
				err,
			)
			continue
		}
	}
}
