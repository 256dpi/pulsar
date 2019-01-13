package frame

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/256dpi/pulsar/pb"
)

var magicByte = []byte{0x0e, 0x01}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type Type int

const (
	CONNECT                           = Type(pb.BaseCommand_CONNECT)
	CONNECTED                         = Type(pb.BaseCommand_CONNECTED)
	SUBSCRIBE                         = Type(pb.BaseCommand_SUBSCRIBE)
	PRODUCER                          = Type(pb.BaseCommand_PRODUCER)
	SEND                              = Type(pb.BaseCommand_SEND)
	SEND_RECEIPT                      = Type(pb.BaseCommand_SEND_RECEIPT)
	SEND_ERROR                        = Type(pb.BaseCommand_SEND_ERROR)
	MESSAGE                           = Type(pb.BaseCommand_MESSAGE)
	ACK                               = Type(pb.BaseCommand_ACK)
	FLOW                              = Type(pb.BaseCommand_FLOW)
	UNSUBSCRIBE                       = Type(pb.BaseCommand_UNSUBSCRIBE)
	SUCCESS                           = Type(pb.BaseCommand_SUCCESS)
	ERROR                             = Type(pb.BaseCommand_ERROR)
	CLOSE_PRODUCER                    = Type(pb.BaseCommand_CLOSE_PRODUCER)
	CLOSE_CONSUMER                    = Type(pb.BaseCommand_CLOSE_CONSUMER)
	PRODUCER_SUCCESS                  = Type(pb.BaseCommand_PRODUCER_SUCCESS)
	PING                              = Type(pb.BaseCommand_PING)
	PONG                              = Type(pb.BaseCommand_PONG)
	REDELIVER_UNACKNOWLEDGED_MESSAGES = Type(pb.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES)
	PARTITIONED_METADATA              = Type(pb.BaseCommand_PARTITIONED_METADATA)
	PARTITIONED_METADATA_RESPONSE     = Type(pb.BaseCommand_PARTITIONED_METADATA_RESPONSE)
	LOOKUP                            = Type(pb.BaseCommand_LOOKUP)
	LOOKUP_RESPONSE                   = Type(pb.BaseCommand_LOOKUP_RESPONSE)
	CONSUMER_STATS                    = Type(pb.BaseCommand_CONSUMER_STATS)
	CONSUMER_STATS_RESPONSE           = Type(pb.BaseCommand_CONSUMER_STATS_RESPONSE)
	REACHED_END_OF_TOPIC              = Type(pb.BaseCommand_REACHED_END_OF_TOPIC)
	SEEK                              = Type(pb.BaseCommand_SEEK)
	GET_LAST_MESSAGE_ID               = Type(pb.BaseCommand_GET_LAST_MESSAGE_ID)
	GET_LAST_MESSAGE_ID_RESPONSE      = Type(pb.BaseCommand_GET_LAST_MESSAGE_ID_RESPONSE)
	ACTIVE_CONSUMER_CHANGE            = Type(pb.BaseCommand_ACTIVE_CONSUMER_CHANGE)
	GET_TOPICS_OF_NAMESPACE           = Type(pb.BaseCommand_GET_TOPICS_OF_NAMESPACE)
	GET_TOPICS_OF_NAMESPACE_RESPONSE  = Type(pb.BaseCommand_GET_TOPICS_OF_NAMESPACE_RESPONSE)
	GET_SCHEMA                        = Type(pb.BaseCommand_GET_SCHEMA)
	GET_SCHEMA_RESPONSE               = Type(pb.BaseCommand_GET_SCHEMA_RESPONSE)
)

type Frame interface {
	Type() Type
}

type SimpleDecoder interface {
	Decode(*pb.BaseCommand) error
}

type SimpleEncoder interface {
	Encode() (*pb.BaseCommand, error)
}

type PayloadDecoder interface {
	Decode(*pb.BaseCommand, *pb.MessageMetadata, []byte) error
}

type PayloadEncoder interface {
	Encode() (*pb.BaseCommand, *pb.MessageMetadata, []byte, error)
}

func Read(reader io.Reader) (Frame, error) {
	// read total size
	totalSizeBytes := make([]byte, 4)
	_, err := io.ReadFull(reader, totalSizeBytes)
	if err != nil {
		return nil, err
	}

	// get total size
	totalSize := int(binary.BigEndian.Uint32(totalSizeBytes))

	// read complete frame
	frameBytes := make([]byte, totalSize)
	_, err = io.ReadFull(reader, frameBytes)
	if err != nil {
		return nil, err
	}

	// decode frame
	f, err := Decode(frameBytes)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func Write(frame Frame, writer io.Writer) error {
	// encode frame
	data, err := Encode(frame)
	if err != nil {
		return err
	}

	// write buffer
	_, err = writer.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func Decode(data []byte) (Frame, error) {
	// get total size
	totalSize := len(data)
	if totalSize < 4 {
		return nil, fmt.Errorf("expected frame to be at least 4 bytes")
	}

	// get command size
	commandSize := int(binary.BigEndian.Uint32(data[:4]))
	if totalSize < 4+commandSize {
		return nil, fmt.Errorf("not enough data received to decode command")
	}

	// read command
	commandBytes := data[4 : commandSize+4]

	// decode command
	base := &pb.BaseCommand{}
	err := base.Unmarshal(commandBytes)
	if err != nil {
		return nil, err
	}

	// prepare metadata and payload
	var metadata *pb.MessageMetadata
	var payload []byte

	// compute rest of frame
	restOfFrame := totalSize - 4 - commandSize

	// check rest of frame
	if restOfFrame > 0 {
		// get magic number
		magicNumber := data[commandSize+4 : commandSize+4+2]
		if !bytes.Equal(magicNumber, magicByte) {
			return nil, fmt.Errorf("invalid magic number")
		}

		// get checksum
		checksum := binary.BigEndian.Uint32(data[commandSize+4+2 : commandSize+4+2+4])

		// check checksum
		if checksum != crc32.Checksum(data[commandSize+4+2+4:], crcTable) {
			return nil, fmt.Errorf("checksum mismatch")
		}

		// get metadata size
		metadataSize := int(binary.BigEndian.Uint32(data[commandSize+4+2+4 : commandSize+4+2+4+4]))

		// get metadata bytes
		metadataBytes := data[commandSize+4+2+4+4 : commandSize+4+2+4+4+metadataSize]

		// decode metadata
		metadata := &pb.MessageMetadata{}
		err = metadata.Unmarshal(metadataBytes)
		if err != nil {
			return nil, err
		}

		// get payload
		payload = data[commandSize+4+2+4+4+metadataSize:]
	}

	// set command
	switch base.GetType() {
	case pb.BaseCommand_CONNECT:
		// not supported
	case pb.BaseCommand_CONNECTED:
		connected := &Connected{}
		return connected, connected.Decode(base)
	case pb.BaseCommand_SUBSCRIBE:
		// not supported
	case pb.BaseCommand_PRODUCER:
		// not supported
	case pb.BaseCommand_SEND:
		// not supported
	case pb.BaseCommand_SEND_RECEIPT:
		sendReceipt := &SendReceipt{}
		return sendReceipt, sendReceipt.Decode(base)
	case pb.BaseCommand_SEND_ERROR:
		sendError := &SendError{}
		return sendError, sendError.Decode(base)
	case pb.BaseCommand_MESSAGE:
		message := &Message{}
		return message, message.Decode(base, metadata, payload)
	case pb.BaseCommand_ACK:
		// not supported
	case pb.BaseCommand_FLOW:
		// not supported
	case pb.BaseCommand_UNSUBSCRIBE:
		// not supported
	case pb.BaseCommand_SUCCESS:
		success := &Success{}
		return success, success.Decode(base)
	case pb.BaseCommand_ERROR:
		_error := &Error{}
		return _error, _error.Decode(base)
	case pb.BaseCommand_CLOSE_PRODUCER:
		closeProducer := &CloseProducer{}
		return closeProducer, closeProducer.Decode(base)
	case pb.BaseCommand_CLOSE_CONSUMER:
		closeConsumer := &CloseConsumer{}
		return closeConsumer, closeConsumer.Decode(base)
	case pb.BaseCommand_PRODUCER_SUCCESS:
		producerSuccess := &ProducerSuccess{}
		return producerSuccess, producerSuccess.Decode(base)
	case pb.BaseCommand_PING:
		ping := &Ping{}
		return ping, ping.Decode(base)
	case pb.BaseCommand_PONG:
		pong := &Pong{}
		return pong, pong.Decode(base)
	case pb.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES:
		// not supported
	case pb.BaseCommand_PARTITIONED_METADATA:
		// not supported
	case pb.BaseCommand_PARTITIONED_METADATA_RESPONSE:
		// TODO: Add support.
	case pb.BaseCommand_LOOKUP:
		// not supported
	case pb.BaseCommand_LOOKUP_RESPONSE:
		lookupResponse := &LookupResponse{}
		return lookupResponse, lookupResponse.Decode(base)
	case pb.BaseCommand_CONSUMER_STATS:
		// not supported
	case pb.BaseCommand_CONSUMER_STATS_RESPONSE:
		// TODO: Add support.
	case pb.BaseCommand_REACHED_END_OF_TOPIC:
		// TODO: Add support.
	case pb.BaseCommand_SEEK:
		// not supported
	case pb.BaseCommand_GET_LAST_MESSAGE_ID:
		// not supported
	case pb.BaseCommand_GET_LAST_MESSAGE_ID_RESPONSE:
		// TODO: Add support.
	case pb.BaseCommand_ACTIVE_CONSUMER_CHANGE:
		// TODO: Add support.
	case pb.BaseCommand_GET_TOPICS_OF_NAMESPACE:
		// not supported
	case pb.BaseCommand_GET_TOPICS_OF_NAMESPACE_RESPONSE:
		// TODO: Add support.
	case pb.BaseCommand_GET_SCHEMA:
		// not supported
	case pb.BaseCommand_GET_SCHEMA_RESPONSE:
		// TODO: Add support.
	}

	return nil, fmt.Errorf("unsupported command type %d", base.GetType())
}

func Encode(frame Frame) ([]byte, error) {
	// handle simple encoder
	if sce, ok := frame.(SimpleEncoder); ok {
		// encode frame
		base, err := sce.Encode()
		if err != nil {
			return nil, err
		}

		// marshal base
		baseBytes, err := base.Marshal()
		if err != nil {
			return nil, err
		}

		// get command size
		commandSize := len(baseBytes)

		// compute total size
		totalSize := 4 + commandSize

		// allocate final slice
		data := make([]byte, 4+totalSize)

		// write total size
		binary.BigEndian.PutUint32(data, uint32(totalSize))

		// write command size
		binary.BigEndian.PutUint32(data[4:], uint32(commandSize))

		// write command
		copy(data[8:], baseBytes)

		return data, nil
	}

	// handle payload encoder
	if pce, ok := frame.(PayloadEncoder); ok {
		// encode frame
		base, metadata, payload, err := pce.Encode()
		if err != nil {
			return nil, err
		}

		// marshal base
		baseBytes, err := base.Marshal()
		if err != nil {
			return nil, err
		}

		// marshal metadata
		metadataBytes, err := metadata.Marshal()
		if err != nil {
			return nil, err
		}

		// get command, metadata and payload size
		commandSize := len(baseBytes)
		metadataSize := len(metadataBytes)
		payloadSize := len(payload)

		// compute total size
		totalSize := 4 + commandSize + 2 + 4 + 4 + metadataSize + payloadSize

		// allocate final slice
		data := make([]byte, 4+totalSize)

		// write total size
		binary.BigEndian.PutUint32(data, uint32(totalSize))

		// write command size
		binary.BigEndian.PutUint32(data[4:], uint32(commandSize))

		// write command
		copy(data[4+4:], baseBytes)

		// write magic number
		copy(data[4+4+commandSize:], magicByte)

		// write metadata size
		binary.BigEndian.PutUint32(data[4+4+commandSize+2+4:], uint32(metadataSize))

		// write metadata
		copy(data[4+4+commandSize+2+4+4:], metadataBytes)

		// write payload
		copy(data[4+4+commandSize+2+4+4+metadataSize:], payload)

		// write checksum
		binary.BigEndian.PutUint32(data[4+4+commandSize+2:], crc32.Checksum(data[4+4+commandSize+2+4:], crcTable))

		return data, nil
	}

	return nil, fmt.Errorf("unable to encode frame")
}
