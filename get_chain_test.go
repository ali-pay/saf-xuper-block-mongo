package main

import (
	"fmt"
	"github.com/jason-cn-dev/xuperdata/utils"
	"testing"
)

func TestGetBlockByHeight(t *testing.T) {
	node = "161.117.39.102:37101"
	bcname = "xuper"
	block, err := GetBlockByHeight(100)
	if err != nil {
		t.Fatal(err)
	}
	utils.PrintBlock(block)
}

func TestGetUtxoTotal(t *testing.T) {
	node = "161.117.39.102:37101"
	bcname = "xuper"
	total, err := GetUtxoTotal()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(total)
}
