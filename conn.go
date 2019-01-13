package pulsar

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/256dpi/pulsar/frame"

	"github.com/256dpi/mercury"
)

type Conn interface {
	Send(cmd frame.Frame) error
	Receive() (frame.Frame, error)
	Close() error
}

type IOConn struct {
	reader *bufio.Reader
	writer *mercury.Writer
	closer io.Closer

	sMutex sync.Mutex
	rMutex sync.Mutex
}

func Dial(addr string) (*IOConn, error) {
	// set default addr
	if addr == "" {
		addr = "pulsar://localhost:6650"
	}

	// parse address
	loc, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}

	// create connection
	conn, err := net.Dial("tcp", loc.Host)
	if err != nil {
		return nil, err
	}

	return NewIOConn(conn), nil
}

func NewIOConn(carrier io.ReadWriteCloser) *IOConn {
	return &IOConn{
		reader: bufio.NewReader(carrier),
		closer: carrier,
		writer: mercury.NewWriter(carrier, time.Millisecond),
	}
}

func (c *IOConn) Send(f frame.Frame) error {
	c.sMutex.Lock()
	defer c.sMutex.Unlock()

	// encode frame
	data, err := frame.Encode(f)
	if err != nil {
		return err
	}

	// write buffer
	_, err = c.writer.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func (c *IOConn) Receive() (frame.Frame, error) {
	c.rMutex.Lock()
	defer c.rMutex.Unlock()

	// read total size
	totalSizeBytes, err := c.readSlice(4)
	if err != nil {
		return nil, err
	}

	// get total size
	totalSize := int(binary.BigEndian.Uint32(totalSizeBytes))

	// read complete frame
	frameBytes, err := c.readSlice(totalSize)
	if err != nil {
		return nil, err
	}

	// decode frame
	f, err := frame.Decode(frameBytes)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (c *IOConn) readSlice(len int) ([]byte, error) {
	// TODO: Reuse some kind of buffer?

	// prepare slice
	slice := make([]byte, len)

	// read bytes
	_, err := io.ReadFull(c.reader, slice)
	if err != nil {
		return nil, err
	}

	return slice, nil
}

func (c *IOConn) Close() error {
	c.sMutex.Lock()
	defer c.sMutex.Unlock()

	// flush buffer
	err1 := c.writer.Flush()

	// close connection
	err2 := c.closer.Close()

	// handle errors
	if err1 != nil {
		return err1
	} else if err2 != nil {
		return err2
	}

	return nil
}
