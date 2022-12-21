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
	}
	c, err := nifitest.New(&cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v", err)
		os.Exit(1)
	}

	testData := make(map[nifitest.Id]string)
	src := nifitest.NewId("e2a26c9b-0184-1000-40ef-d2edcb8e63e2",
		"e447fb30-0184-1000-15b5-2adea499753e")
	testData[src] = "123"
	expected := make(map[nifitest.Id]string)
	dst := nifitest.NewId("e2a26c9b-0184-1000-40ef-d2edcb8e63e2",
		"322b9410-0185-1000-fdc6-192740b4fd9c")
	expected[dst] = "123"
	err = c.Run("root", testData, expected)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
