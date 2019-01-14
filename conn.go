package pulsar

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/256dpi/pulsar/frame"

	"github.com/256dpi/mercury"
)

const defaultHost = "localhost"
const defaultPort = "6650"

var defaultAddr = fmt.Sprintf("pulsar://%s:%s", defaultHost, defaultPort)

// Conn is a low level connection to a pulsar broker that is used to send and
// receive frames. It is safe for concurrent use.
type Conn struct {
	reader *bufio.Reader
	writer *mercury.Writer
	closer io.Closer

	sMutex sync.Mutex
	rMutex sync.Mutex
}

// Dial will connect to the specified broker and establish a connection. It will
// fallback to the default address "pulsar://localhost:6650" if missing.
func Dial(addr string) (*Conn, error) {
	// set default addr
	if addr == "" {
		addr = defaultAddr
	}

	// parse address
	loc, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}

	// check scheme
	if loc.Scheme != "pulsar" {
		return nil, fmt.Errorf("address scheme is not 'pulsar'")
	}

	// split host port
	host, port, err := net.SplitHostPort(loc.Host)
	if err != nil {
		return nil, err
	}

	// check host
	if host == "" {
		host = defaultHost
	}

	// check port
	if port == "" {
		port = defaultPort
	}

	// create connection
	conn, err := net.Dial("tcp", net.JoinHostPort(host, port))
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
