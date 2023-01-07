package nifitest

import (
	"io"
	"bytes"
	"errors"
	"encoding/binary"
)

/*
   00000003
   00000004 70617468
   00000002 2e2f
   
   00000008 66696c656e616d65
   00000024 33663538396637372d323534342d343734362d623637382d373562396339663335333463
   
   00000004 75756964
   00000024 33663538396637372d323534342d343734362d623637382d373562396339663335333463

   0000000000000003 313233
 */
func deserializePacket(packet []byte) (string, error) {
	r := bytes.NewReader(packet)
	if r == nil {
		return "", errors.New("Could not get a byte reader")
	}

	var N int32
	err := binary.Read(r, binary.BigEndian, &N)
	if err != nil {
		return "", chainErrors(
			errors.New("Could not read the number of attributes"),
			err)
	}
	for i := 0; i < int(N); i++ {
		_, _, err := readKV(r)
		if err != nil {
			return "", err
		}
	}

	var S int64
	err = binary.Read(r, binary.BigEndian, &S)
	if err != nil {
		return "", chainErrors(
			errors.New("Could not read the data size"), err)
	}

	d := []byte{}
	var read int64 = 0
	for {
		buf := make([]byte, S - read)
		size, err := r.Read(buf)
		read += int64(size)
		if err != nil {
			if err != io.EOF {
				return "", chainErrors(
					errors.New("Reading data"), err)
			}
			if read < S {
				return "", errors.New("Premature EOF")
			}
			d = append(d, buf[0:size]...)
			break
		}
		d = append(d, buf[0:size]...)
		if read == S {
			size, err = r.Read(buf)
			if err == nil || err != io.EOF || size > 0 {
				return "", errors.New("Trailing garbage")
			}
			break
		}
	}

	return string(d), nil
}

func readKV(r io.Reader) (string, string, error) {
	k, err := readString(r)
	if err != nil {
		return "", "", err
	}
	v, err := readString(r)
	if err != nil {
		return k, "", err
	}
	return k, v, nil
}

func readString(r io.Reader) (string, error) {
	var S int32
	err := binary.Read(r, binary.BigEndian, &S)
	if err != nil {
		return "", chainErrors(
			errors.New("Could not read the string length"),	err)
	}
	b := make([]byte, S)
	n, err := r.Read(b)
	if n < int(S) {
		return string(b), errors.New("Incomplete string")
	}
	if err != nil && err != io.EOF {
		return string(b), err
	}
	return string(b), nil
}
