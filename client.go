package beanstalkd

import (
	"net"
	"bufio"
	"time"
	"fmt"
	"strings"
	"io"
	"errors"
)

const minLenToBuf = 1500 //minimum data len to send using bufio, otherwise use TCPConn

type Conn struct {
	conn      net.Conn
	addr      string
	bufReader *bufio.Reader
	bufWriter *bufio.Writer
}

func newConn(conn net.Conn, addr string) (*Conn, error) {
	c := new(Conn)
	c.conn = conn
	c.addr = addr
	c.bufReader = bufio.NewReader(conn)
	c.bufWriter = bufio.NewWriter(conn)

	return c, nil
}

func Dial(addr string) (*Conn, error) {
	kon, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	c, err := newConn(kon, addr)

	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Conn) Use(tubeName string) error {

	//check parameter
	if len(tubeName) > 200 {
		return errors.New("tube name is unavailable")
	}

	cmd := fmt.Sprintf("use %s\r\n", tubeName)
	expected := fmt.Sprintf("USING %s\r\n", tubeName)
	return sendAndGetExpect(c, cmd, expected)
}

func (c *Conn) Watch(tubeName string) (int, error) {
	cmd := fmt.Sprintf("watch %s\r\n", tubeName)

	resp, err := send(c, cmd)
	if err != nil {
		return -1, err
	}

	var tubeCount int
	_, err = fmt.Sscanf(resp, "WATCHING %d\r\n", &tubeCount)
	if err != nil {
		return -1, parseError(resp)
	}
	return tubeCount, nil
}

func (c *Conn) Put(data []byte, level uint32, delay, ttr time.Duration) (uint64, error) {
	cmd := fmt.Sprintf("put %d %d %d %d\r\n", level, uint64(delay.Seconds()), uint64(ttr.Seconds()), len(data))
	cmd = cmd + string(data) + "\r\n"

	resp, err := send(c, cmd)
	if err != nil {
		return 0, err
	}

	//parse Put response
	switch {
	case strings.HasPrefix(resp, "INSERTED"):
		var id uint64
		_, parseErr := fmt.Sscanf(resp, "INSERTED %d\r\n", &id)
		return id, parseErr
	case strings.HasPrefix(resp, "BURIED"):
		var id uint64
		fmt.Sscanf(resp, "BURIED %d\r\n", &id)
		return id, ErrBuried
	default:
		return 0, parseError(resp)
	}
}

//Reserve Job, with an optional timeout
func (c *Conn) Reserve(timeout ...time.Duration) (*Job, error) {

	cmd := "reserve\r\n"
	if len(timeout) > 0 {
		cmd = fmt.Sprintf("reserve-with-timeout %d\r\n", int(timeout[0].Seconds()))
	}

	resp, err := send(c, cmd)
	if err != nil {
		return nil, err
	}

	var id uint64
	var bodyLen int

	switch {
	case strings.HasPrefix(resp, "RESERVED"):
		_, err = fmt.Sscanf(resp, "RESERVED %d %d\r\n", &id, &bodyLen)
		if err != nil {
			return nil, err
		}
	default:
		return nil, parseError(resp)
	}

	body, err := c.readBody(bodyLen)
	return &Job{ID: id, Body: body}, err
}

func (c *Conn) Release(id uint64, pri uint32, delay time.Duration) error {
	cmd := fmt.Sprintf("release %d %d %d\r\n", id, pri, uint64(delay.Seconds()))
	expected := "RELEASED\r\n"
	return sendAndGetExpect(c, cmd, expected)
}

func (c *Conn) Delete(id uint64) error {
	cmd := fmt.Sprintf("delete %d\r\n", id)
	expected := "DELETED\r\n"
	return sendAndGetExpect(c, cmd, expected)
}

func (c *Conn) Bury(id uint64, pri uint32) error {
	cmd := fmt.Sprintf("bury %d %d\r\n", id, pri)
	expected := "BURIED\r\n"
	return sendAndGetExpect(c, cmd, expected)
}

func (c *Conn) KickJob(id uint64) error {
	cmd := fmt.Sprintf("kick-job %d\r\n", id)
	expected := "KICKED\r\n"
	return sendAndGetExpect(c, cmd, expected)
}

//Quit close network connection.
func (c *Conn) Quit() {
	sendFull(c, []byte("quit \r\n"))
	c.conn.Close()
}

func (c *Conn) readBody(bodyLen int) ([]byte, error) {
	body := make([]byte, bodyLen+2) //+2 is for trailing \r\n
	n, err := io.ReadFull(c.bufReader, body)
	if err != nil {
		return nil, err
	}

	return body[:n-2], nil //strip \r\n trail
}

func (c *Conn) Close() error {
	return c.conn.Close()
}
