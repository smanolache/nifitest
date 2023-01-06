package main

import (
	"os"
	"fmt"
	"orange.com/nifitest/nifitest"
	"github.com/antihax/optional"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Missing nifi endpoint\n")
		os.Exit(1)
	}
	nifiEndpoint := os.Args[1]
	var user optional.String
	var pass optional.String
	if len(os.Args) > 2 {
		user = optional.NewString(os.Args[2])
		if len(os.Args) > 3 {
			pass = optional.NewString(os.Args[3])
		}
	}

	cfg := nifitest.Config{
		URL:                     nifiEndpoint,
		VerifyServerCertificate: false,
		Username:                user,
		Password:                pass,
		RemoveOutLinks:          false,
	}
	c, err := nifitest.New(&cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v", err)
		os.Exit(1)
	}

	testData := make(map[string]string)
	src := "e447fb30-0184-1000-15b5-2adea499753e"
	testData[src] = "123"
	exp := []byte{0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff, 0x33, 0x34, 0x32,
		6, 0, 0xd2, 0x63, 0x48, 0x88, 3, 0, 0, 0}
	expected := make(map[string]string)
	dst := "322b9410-0185-1000-fdc6-192740b4fd9c"
	expected[dst] = string(exp)
	err = c.Run("root", testData, expected)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
