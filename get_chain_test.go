package main

import (
	"fmt"
	"github.com/jason-cn-dev/xuperdata/utils"
	"testing"
)

func TestGetBlockByHeight(t *testing.T) {
	node = ":37101"
	bcname = "xuper"
	block, err := GetBlockByHeight(1)
	if err != nil {
		t.Fatal(err)
	}
	utils.PrintBlock(block)
}

func TestGetUtxoTotal(t *testing.T) {
	node = ":37101"
	bcname = "xuper"
	total, height, err := GetUtxoTotalAndTrunkHeight()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(total, height)
}
