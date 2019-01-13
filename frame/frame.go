package frame

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/256dpi/pulsar/pb"
)

type Frame interface{}

type SimpleCommandDecoder interface {
	Decode(*pb.BaseCommand) error
}

type SimpleCommandEncoder interface {
	Encode() (*pb.BaseCommand, error)
}

type PayloadCommandDecoder interface {
	Decode(*pb.BaseCommand, *pb.MessageMetadata, []byte) error
}

type PayloadCommandEncoder interface {
	Encode() (*pb.BaseCommand, *pb.MessageMetadata, []byte, error)
}

func Decode(data []byte) (interface{}, error) {
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
		if !bytes.Equal(magicNumber, []byte{0x0e, 0x01}) {
			return nil, fmt.Errorf("invalid magic number")
		}

		// get checksum
		// checksum := data[commandSize+4+2:commandSize+4+2+4]

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
		payload = data[commandSize+4+2+4+4+metadataSize : len(data)-1]
	}

	// set command
	switch base.GetType() {
	case pb.BaseCommand_CONNECT:
	case pb.BaseCommand_CONNECTED:
		connected := &Connected{}
		return connected, connected.Decode(base)
	case pb.BaseCommand_SUBSCRIBE:
	case pb.BaseCommand_PRODUCER:
	case pb.BaseCommand_SEND:
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
	case pb.BaseCommand_FLOW:
	case pb.BaseCommand_UNSUBSCRIBE:
	case pb.BaseCommand_SUCCESS:
		success := &Success{}
		return success, success.Decode(base)
	case pb.BaseCommand_ERROR:
		_error := &Error{}
		return _error, _error.Decode(base)
	case pb.BaseCommand_CLOSE_PRODUCER:
		// TODO: Add support.
	case pb.BaseCommand_CLOSE_CONSUMER:
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
	case pb.BaseCommand_PARTITIONED_METADATA:
	case pb.BaseCommand_PARTITIONED_METADATA_RESPONSE:
	case pb.BaseCommand_LOOKUP:
	case pb.BaseCommand_LOOKUP_RESPONSE:
		lookupResponse := &LookupResponse{}
		return lookupResponse, lookupResponse.Decode(base)
	case pb.BaseCommand_CONSUMER_STATS:
	case pb.BaseCommand_CONSUMER_STATS_RESPONSE:
	case pb.BaseCommand_REACHED_END_OF_TOPIC:
	case pb.BaseCommand_SEEK:
	case pb.BaseCommand_GET_LAST_MESSAGE_ID:
	case pb.BaseCommand_GET_LAST_MESSAGE_ID_RESPONSE:
	case pb.BaseCommand_ACTIVE_CONSUMER_CHANGE:
	case pb.BaseCommand_GET_TOPICS_OF_NAMESPACE:
	case pb.BaseCommand_GET_TOPICS_OF_NAMESPACE_RESPONSE:
	case pb.BaseCommand_GET_SCHEMA:
	case pb.BaseCommand_GET_SCHEMA_RESPONSE:
	}

	return nil, fmt.Errorf("unsupported command type %d", base.GetType())
}

func Encode(frame interface{}) ([]byte, error) {
	// handle simple encoder
	if sce, ok := frame.(SimpleCommandEncoder); ok {
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
	if _, ok := frame.(PayloadCommandEncoder); ok {

	}

	return nil, fmt.Errorf("unable to encode frame")
}
