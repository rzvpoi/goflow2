package tcp

import (
	"flag"
	"fmt"
	"net"
	"sync"

	"github.com/netsampler/goflow2/v2/transport"
)

type TCPDriver struct {
	destination   string
	conn          net.Conn
	lineSeparator string
	lock          *sync.RWMutex
	q             chan bool
}

func (d *TCPDriver) Prepare() error {
	flag.StringVar(&d.destination, "transport.tcp", "localhost:5044", "TCP address to send data")
	flag.StringVar(&d.lineSeparator, "transport.tcp.sep", "\n", "Line separator")
	return nil
}

func (d *TCPDriver) openTCPConnection() error {
	conn, err := net.Dial("tcp", d.destination)
	if err != nil {
		return err
	}
	d.conn = conn
	return nil
}

func (d *TCPDriver) Init() error {
	d.q = make(chan bool, 1)

	if d.destination == "" {
		return fmt.Errorf("no TCP destination specified")
	}

	err := d.openTCPConnection()
	if err != nil {
		return fmt.Errorf("failed to connect: %v", err)
	}

	return nil
}

func (d *TCPDriver) Send(key, data []byte) error {
	d.lock.RLock()
	conn := d.conn
	d.lock.RUnlock()

	_, err := fmt.Fprint(conn, string(data)+d.lineSeparator)
	return err
}

func (d *TCPDriver) Close() error {

	d.lock.Lock()
	defer d.lock.Unlock()

	if d.conn != nil {
		d.conn.Close()
	}

	close(d.q)
	return nil
}

func init() {
	// Register the TCP driver with the GoFlow2 transport
	d := &TCPDriver{
		lock: &sync.RWMutex{},
	}
	transport.RegisterTransportDriver("tcp", d)
}
