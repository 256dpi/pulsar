package frame

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/256dpi/pulsar/api"
)

var magicByte = []byte{0x0e, 0x01}

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// SizeLimit is the maximum allowed size of a frame.
const SizeLimit = 5 * 1024 * 1024 // 5MB

// Type defines the different frame types.
type Type int

// All available frame types.
const (
	ConnectFrame                         = Type(api.BaseCommand_CONNECT)
	ConnectedFrame                       = Type(api.BaseCommand_CONNECTED)
	SubscribeFrame                       = Type(api.BaseCommand_SUBSCRIBE)
	ProducerFrame                        = Type(api.BaseCommand_PRODUCER)
	SendFrame                            = Type(api.BaseCommand_SEND)
	SendReceiptFrame                     = Type(api.BaseCommand_SEND_RECEIPT)
	SendErrorFrame                       = Type(api.BaseCommand_SEND_ERROR)
	MessageFrame                         = Type(api.BaseCommand_MESSAGE)
	AckFrame                             = Type(api.BaseCommand_ACK)
	FlowFrame                            = Type(api.BaseCommand_FLOW)
	UnsubscribeFrame                     = Type(api.BaseCommand_UNSUBSCRIBE)
	SuccessFrame                         = Type(api.BaseCommand_SUCCESS)
	ErrorFrame                           = Type(api.BaseCommand_ERROR)
	CloseProducerFrame                   = Type(api.BaseCommand_CLOSE_PRODUCER)
	CloseConsumerFrame                   = Type(api.BaseCommand_CLOSE_CONSUMER)
	ProducerSuccessFrame                 = Type(api.BaseCommand_PRODUCER_SUCCESS)
	PingFrame                            = Type(api.BaseCommand_PING)
	PongFrame                            = Type(api.BaseCommand_PONG)
	RedeliverUnacknowledgedMessagesFrame = Type(api.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES)
	PartitionedMetadataFrame             = Type(api.BaseCommand_PARTITIONED_METADATA)
	PartitionedMetadataResponseFrame     = Type(api.BaseCommand_PARTITIONED_METADATA_RESPONSE)
	LookupFrame                          = Type(api.BaseCommand_LOOKUP)
	LookupResponseFrame                  = Type(api.BaseCommand_LOOKUP_RESPONSE)
	ConsumerStatsFrame                   = Type(api.BaseCommand_CONSUMER_STATS)
	ConsumerStatsResponseFrame           = Type(api.BaseCommand_CONSUMER_STATS_RESPONSE)
	ReachedEndOfTopicFrame               = Type(api.BaseCommand_REACHED_END_OF_TOPIC)
	SeekFrame                            = Type(api.BaseCommand_SEEK)
	GetLastMessageIDFrame                = Type(api.BaseCommand_GET_LAST_MESSAGE_ID)
	GetLastMessageIDResponseFrame        = Type(api.BaseCommand_GET_LAST_MESSAGE_ID_RESPONSE)
	ActiveConsumerChangeFrame            = Type(api.BaseCommand_ACTIVE_CONSUMER_CHANGE)
	GetTopicsOfNamespaceFrame            = Type(api.BaseCommand_GET_TOPICS_OF_NAMESPACE)
	GetTopicsOfNamespaceResponseFrame    = Type(api.BaseCommand_GET_TOPICS_OF_NAMESPACE_RESPONSE)
	GetSchemaFrame                       = Type(api.BaseCommand_GET_SCHEMA)
	GetSchemaResponseFrame               = Type(api.BaseCommand_GET_SCHEMA_RESPONSE)
)

// Frame is generic frame exchanged with the pulsar broker.
type Frame interface {
	// Type should return the frame type.
	Type() Type
}

// SimpleDecoder decodes simple frames.
type SimpleDecoder interface {
	// Decode should construct the frame from the specified components.
	Decode(*api.BaseCommand) error
}

// SimpleEncoder encodes simple frames.
type SimpleEncoder interface {
	// Encode should encode the frame and return its components.
	Encode() (*api.BaseCommand, error)
}

// PayloadDecoder decodes frames with metadata and a payload.
type PayloadDecoder interface {
	// Decode should construct the frame from the specified components.
	Decode(*api.BaseCommand, *api.MessageMetadata, []byte) error
}

// PayloadEncoder encodes frames with metadata and a payload.
type PayloadEncoder interface {
	// Encode should encode the frame and return its components.
	Encode() (*api.BaseCommand, *api.MessageMetadata, []byte, error)
}

// Read will block and try to read a frame from the provided reader.
func Read(reader io.Reader) (Frame, error) {
	// read total size
	totalSizeBytes := make([]byte, 4)
	_, err := io.ReadFull(reader, totalSizeBytes)
	if err != nil {
		return nil, err
	}

	// get total size
	totalSize := int(binary.BigEndian.Uint32(totalSizeBytes))

	// check frame size
	if 4+totalSize > SizeLimit {
		return nil, fmt.Errorf("received frame is too big")
	}

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

// Write will write the provided frame to the specified writer.
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

// Decode will decode a frame from the provided byte slice. The byte slice must
// not include the total size.
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
	base := &api.BaseCommand{}
	err := base.Unmarshal(commandBytes)
	if err != nil {
		return nil, err
	}

	// prepare metadata and payload
	var metadata *api.MessageMetadata
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
		metadata := &api.MessageMetadata{}
		err = metadata.Unmarshal(metadataBytes)
		if err != nil {
			return nil, err
		}

		// get payload
		payload = data[commandSize+4+2+4+4+metadataSize:]
	}

	// set command
	switch base.GetType() {
	case api.BaseCommand_CONNECT:
		// not supported
	case api.BaseCommand_CONNECTED:
		connected := &Connected{}
		return connected, connected.Decode(base)
	case api.BaseCommand_SUBSCRIBE:
		// not supported
	case api.BaseCommand_PRODUCER:
		// not supported
	case api.BaseCommand_SEND:
		// not supported
	case api.BaseCommand_SEND_RECEIPT:
		sendReceipt := &SendReceipt{}
		return sendReceipt, sendReceipt.Decode(base)
	case api.BaseCommand_SEND_ERROR:
		sendError := &SendError{}
		return sendError, sendError.Decode(base)
	case api.BaseCommand_MESSAGE:
		message := &Message{}
		return message, message.Decode(base, metadata, payload)
	case api.BaseCommand_ACK:
		// not supported
	case api.BaseCommand_FLOW:
		// not supported
	case api.BaseCommand_UNSUBSCRIBE:
		// not supported
	case api.BaseCommand_SUCCESS:
		success := &Success{}
		return success, success.Decode(base)
	case api.BaseCommand_ERROR:
		_error := &Error{}
		return _error, _error.Decode(base)
	case api.BaseCommand_CLOSE_PRODUCER:
		closeProducer := &CloseProducer{}
		return closeProducer, closeProducer.Decode(base)
	case api.BaseCommand_CLOSE_CONSUMER:
		closeConsumer := &CloseConsumer{}
		return closeConsumer, closeConsumer.Decode(base)
	case api.BaseCommand_PRODUCER_SUCCESS:
		producerSuccess := &ProducerSuccess{}
		return producerSuccess, producerSuccess.Decode(base)
	case api.BaseCommand_PING:
		ping := &Ping{}
		return ping, ping.Decode(base)
	case api.BaseCommand_PONG:
		pong := &Pong{}
		return pong, pong.Decode(base)
	case api.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES:
		// not supported
	case api.BaseCommand_PARTITIONED_METADATA:
		// not supported
	case api.BaseCommand_PARTITIONED_METADATA_RESPONSE:
		// TODO: Add support.
	case api.BaseCommand_LOOKUP:
		// not supported
	case api.BaseCommand_LOOKUP_RESPONSE:
		lookupResponse := &LookupResponse{}
		return lookupResponse, lookupResponse.Decode(base)
	case api.BaseCommand_CONSUMER_STATS:
		// not supported
	case api.BaseCommand_CONSUMER_STATS_RESPONSE:
		// TODO: Add support.
	case api.BaseCommand_REACHED_END_OF_TOPIC:
		// TODO: Add support.
	case api.BaseCommand_SEEK:
		// not supported
	case api.BaseCommand_GET_LAST_MESSAGE_ID:
		// not supported
	case api.BaseCommand_GET_LAST_MESSAGE_ID_RESPONSE:
		// TODO: Add support.
	case api.BaseCommand_ACTIVE_CONSUMER_CHANGE:
		// TODO: Add support.
	case api.BaseCommand_GET_TOPICS_OF_NAMESPACE:
		// not supported
	case api.BaseCommand_GET_TOPICS_OF_NAMESPACE_RESPONSE:
		// TODO: Add support.
	case api.BaseCommand_GET_SCHEMA:
		// not supported
	case api.BaseCommand_GET_SCHEMA_RESPONSE:
		// TODO: Add support.
	}

	return nil, fmt.Errorf("unsupported command type %d", base.GetType())
}

// Encode will encode the provided frame and return a byte slice. The byte slice
// already includes the total size at the beginning.
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

		// check frame size
		if 4+totalSize > SizeLimit {
			return nil, fmt.Errorf("to be written frame is too big")
		}

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

		// check frame size
		if 4+totalSize > SizeLimit {
			return nil, fmt.Errorf("to be written frame is too big")
		}

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

func getType(t api.BaseCommand_Type) *api.BaseCommand_Type {
	return &t
}
