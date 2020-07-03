package main

import (
	"context"
	"math/big"
	"time"

	"github.com/wxnacy/wgo/arrays"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/jason-cn-dev/xuperdata/utils"
)

type Count struct {
	//ID        primitive.ObjectID `bson:"_id,omitempty"`
	TxCount   int64  `bson:"tx_count"`   //交易总数
	CoinCount int64  `bson:"coin_count"` //全网金额
	AccCount  int64  `bson:"acc_count"`  //账户总数
	Accounts  bson.A `bson:"accounts"`   //账户列表
}

var counts *Count

func (m *MongoClient) SaveCount(txs []*utils.Transaction) error {
	countCol := m.Database.Collection("count")
	accCol := m.Database.Collection("account")

	//获取已有数据,缓存起来
	if counts == nil {
		counts = &Count{}

		//id必须有12个字节
		//获取统计数
		err := countCol.FindOne(m.ctx, bson.M{"_id": "chain_count"}).Decode(counts)
		if err != nil && err != mongo.ErrNoDocuments {
			return err
		}

		//获取账户地址
		cursor, err := accCol.Find(m.ctx, bson.M{})
		if err != nil && err != mongo.ErrNoDocuments {
			return err
		}
		if cursor != nil {
			err = cursor.All(m.ctx, &counts.Accounts)
		}

		//过滤key,减小体积
		for i, v := range counts.Accounts {
			counts.Accounts[i] = v.(bson.D).Map()["_id"]
		}
	}

	for _, tx := range txs {
		//统计交易总数
		counts.TxCount++

		//统计全网金额
		if tx.Coinbase || tx.VoteCoinbase {
			for _, output := range tx.TxOutputs {
				counts.CoinCount += (*big.Int)(&output.Amount).Int64()
			}
		}

		//统计账户
		for _, txOutput := range tx.TxOutputs {
			if txOutput.ToAddr == "$" {
				continue
			}
			i := arrays.Contains(counts.Accounts, txOutput.ToAddr)
			if i == -1 {
				//统计账户总数
				counts.AccCount++

				//缓存账户
				counts.Accounts = append(counts.Accounts, txOutput.ToAddr)

				//写入数据库
				_, err := accCol.InsertOne(m.ctx, bson.D{
					{"_id", txOutput.ToAddr},
					{"timestamp", tx.Timestamp},
				})
				if err != nil {
					return err
				}
			}
		}
	}

	up := true
	_, err := countCol.UpdateOne(m.ctx,
		bson.M{"_id": "chain_count"},
		&bson.D{{"$set", bson.D{
			{"tx_count", counts.TxCount},
			{"coin_count", counts.CoinCount},
			{"acc_count", counts.AccCount},
		}}},
		&options.UpdateOptions{Upsert: &up})

	return err
}

func (m *MongoClient) SaveBlock(block *utils.InternalBlock) error {

	//存统计
	err := m.SaveCount(block.Transactions)
	if err != nil {
		return err
	}

	//存交易
	err = m.SaveTx(block.Height, block.Transactions)
	if err != nil {
		return err
	}

	txids := []bson.D{}
	for _, v := range block.Transactions {
		txids = append(txids, bson.D{
			{"$ref", "tx"},
			{"$id", v.Txid},
		})
	}

	iblock := bson.D{
		{"_id", block.Height},
		{"blockid", block.Blockid},
		{"proposer", block.Proposer},
		{"transactions", txids},
		{"txCount", block.TxCount},
		{"preHash", block.PreHash},
		{"inTrunk", block.InTrunk},
		{"timestamp", block.Timestamp},
	}

	blockCol := m.Database.Collection("block")
	_, err = blockCol.InsertOne(m.ctx, iblock)
	return err
}

func (m *MongoClient) SaveTx(blockHeight int64, txs []*utils.Transaction) error {

	//索引 最新的交易
	//db.col.createIndex({"timestamp":-1}, {background: true})

	//记录交易
	sampleTxs := []interface{}{}

	//遍历交易
	for _, tx := range txs {
		sampleTxs = append(sampleTxs, bson.D{
			{"_id", tx.Txid},
			{"blockid", tx.Blockid},
			{"blockHeight", blockHeight},
			{"timestamp", tx.Timestamp},
			{"initiator", tx.Initiator},
			{"txInputs", tx.TxInputs},
			{"txOutputs", tx.TxOutputs},
			{"coinbase", tx.Coinbase},
			{"voteCoinbase", tx.VoteCoinbase}, //todo 需要修改pb文件
		})
	}

	txCol := m.Database.Collection("tx")
	_, err := txCol.InsertMany(m.ctx, sampleTxs)
	return err
}

type MongoClient struct {
	ctx context.Context
	*mongo.Client
	*mongo.Database
}

func NewMongoClient(dataSource, database string) (*MongoClient, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(dataSource))
	if err != nil {
		return nil, err
	}

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	//defer client.Disconnect(ctx)

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return nil, err
	}

	//databases, err := client.ListDatabaseNames(ctx, bson.M{})
	//if err != nil {
	//	return nil, err
	//}
	//fmt.Println(databases)

	return &MongoClient{ctx, client, client.Database(database)}, nil
}

func (m *MongoClient) Close() error {
	return m.Client.Disconnect(m.ctx)
}
