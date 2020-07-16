package main

import (
	"context"
	"encoding/hex"
	"errors"
	"github.com/jason-cn-dev/xuper-sdk-go/pb"
	"google.golang.org/grpc"
	"strconv"
	"time"
)

var (
	node   = ":37101"
	bcname = "xuper"
)

func GetUtxoTotal() (int64, error) {

	conn, err := grpc.Dial(node, grpc.WithInsecure(), grpc.WithMaxMsgSize(64<<20-1))
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15000*time.Millisecond)
	defer cancel()

	client := pb.NewXchainClient(conn)

	//查询单条链
	bcStatusPB := &pb.BCStatus{Bcname: bcname}
	bcStatus, err := client.GetBlockChainStatus(ctx, bcStatusPB)
	if err != nil {
		return -1, err
	}
	if bcStatus == nil {
		return -1, errors.New("GetBlockChainStatus: the chain is null")
	}
	if bcStatus.Header.Error != pb.XChainErrorEnum_SUCCESS {
		return -1, errors.New("GetBlockChainStatus: Header.Error is fail")
	}

	return strconv.ParseInt(bcStatus.UtxoMeta.UtxoTotal, 10, 64)
}

func GetBlockByHeight(height int64) (*pb.InternalBlock, error) {

	conn, err := grpc.Dial(node, grpc.WithInsecure(), grpc.WithMaxMsgSize(64<<20-1))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15000*time.Millisecond)
	defer cancel()

	client := pb.NewXchainClient(conn)
	blockHeightPB := &pb.BlockHeight{
		Bcname: bcname,
		Height: height,
	}

	reply, err := client.GetBlockByHeight(ctx, blockHeightPB)
	if err != nil {
		return nil, err
	}
	if reply == nil {
		return nil, errors.New("GetBlockByHeight: the reply is null")
	}
	if reply.Header.Error != pb.XChainErrorEnum_SUCCESS {
		return nil, errors.New("GetBlockByHeight: Header.Error is fail")
	}
	if reply.Block == nil {
		return nil, errors.New("GetBlockByHeight: the block is null")
	}
	return reply.Block, nil
}

func GetTxByTxId(txid string) (*pb.Transaction, error) {

	rawTxid, err := hex.DecodeString(txid)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(node, grpc.WithInsecure(), grpc.WithMaxMsgSize(64<<20-1))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15000*time.Millisecond)
	defer cancel()

	client := pb.NewXchainClient(conn)
	txstatus := &pb.TxStatus{
		Bcname: bcname,
		Txid:   rawTxid,
	}

	reply, err := client.QueryTx(ctx, txstatus)
	if err != nil {
		return nil, err
	}
	if reply == nil {
		return nil, errors.New("QueryTx: the reply is null")
	}

	if reply.Header.Error != pb.XChainErrorEnum_SUCCESS {
		return nil, errors.New("QueryTx: Header.Error is fail")
	}
	if reply.Tx == nil {
		return nil, errors.New("QueryTx: the tx is null")
	}
	return reply.Tx, nil
}
