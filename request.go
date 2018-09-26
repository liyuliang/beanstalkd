package beanstalkd

// Send command and read response
func send(c *Conn, cmd string) (string, error) {

	c.mutex.Lock()
	defer c.mutex.Unlock()

	_, err := sendFull(c, []byte(cmd))
	if err != nil {
		return "", err
	}

	//wait for response
	resp, err := c.bufReader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return resp, nil
}

//request for expected results
func sendAndGetExpect(c *Conn, cmd, expectResult string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	resp, err := send(c, cmd)
	if err != nil {
		return err
	}

	if resp != expectResult {
		return parseError(resp)
	}
	return nil
}

//try to send all of data
//if data len < 1500, it use TCPConn.Write
//if data len >= 1500, it use bufio.Write
func sendFull(c *Conn, data []byte) (int, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	toWrite := data
	totWritten := 0
	var n int
	var err error
	for totWritten < len(data) {
		if len(toWrite) >= minLenToBuf {
			n, err = c.bufWriter.Write(toWrite)
			if err != nil && !isNetTempErr(err) {
				return totWritten, err
			}
			err = c.bufWriter.Flush()
			if err != nil && !isNetTempErr(err) {
				return totWritten, err
			}
		} else {
			n, err = c.conn.Write(toWrite)
			if err != nil && !isNetTempErr(err) {
				return totWritten, err
			}
		}
		totWritten += n
		toWrite = toWrite[n:]
	}
	return totWritten, nil
}
