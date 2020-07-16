package main

import (
	"testing"
)

func TestHttpServerRun(t *testing.T) {
	node = "161.117.39.102:37101"
	var err error
	mongoClient, err = NewMongoClient("mongodb://admin:this is mongodb admin password@192.168.3.150:27017", "jy_chain_test")
	if err != nil {
		t.Fatal(err)
	}
	run()
}
