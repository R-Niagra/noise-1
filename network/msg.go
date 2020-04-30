package network

import (
	"strconv"

	"github.com/R-Niagra/noise-1"
	"github.com/R-Niagra/noise-1/payload"
)

type Message struct {
	From   noise.ID
	Code   byte // 0 for relay, 1 for broadcast
	Data   []byte
	SeqNum byte
	To     noise.PublicKey
}

func (msg Message) Marshal() []byte {
	writer := payload.NewWriter(nil)
	writer.Write(msg.From.Marshal())
	writer.WriteByte(msg.Code)
	writer.WriteByte(msg.SeqNum)
	writer.WriteUint32(uint32(len(msg.To[:])))
	writer.Write([]byte(msg.To[:]))
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

	to, err := reader.ReadBytes()
	if err != nil {
		panic(err)
	}
	copy(msg.To[:], to)

	data, err := reader.ReadBytes()
	if err != nil {
		panic(err)
	}
	msg.Data = data
	return msg, nil
}
