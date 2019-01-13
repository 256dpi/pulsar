package pulsar

import (
	"bufio"
	"io"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/256dpi/pulsar/frame"

	"github.com/256dpi/mercury"
)

// Conn is a low level connection to a pulsar broker that can send and receive
// frames.
type Conn struct {
	reader *bufio.Reader
	writer *mercury.Writer
	closer io.Closer

	sMutex sync.Mutex
	rMutex sync.Mutex
}

// Dial will connect to the specified broker and establish a connection.
func Dial(addr string) (*Conn, error) {
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

	return NewConn(conn), nil
}

// NewConn will create a connection from a ReadWriteCloser.
func NewConn(carrier io.ReadWriteCloser) *Conn {
	return &Conn{
		reader: bufio.NewReader(carrier),
		closer: carrier,
		writer: mercury.NewWriter(carrier, time.Millisecond),
	}
}

// Send will send the specified frame.
func (c *Conn) Send(f frame.Frame) error {
	c.sMutex.Lock()
	defer c.sMutex.Unlock()

	// write frame
	err := frame.Write(f, c.writer)
	if err != nil {
		return err
	}

	return nil
}

// Receive will block and receive the next frame.
func (c *Conn) Receive() (frame.Frame, error) {
	c.rMutex.Lock()
	defer c.rMutex.Unlock()

	// read next frame
	f, err := frame.Read(c.reader)
	if err != nil {
		return nil, err
	}

	return f, nil
}

// Close will close the connection.
func (c *Conn) Close() error {
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
