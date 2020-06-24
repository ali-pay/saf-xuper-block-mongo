package main

import (
	"context"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"

	"github.com/wxnacy/wgo/arrays"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/jason-cn-dev/xuperdata/utils"
)

var accounts bson.A

type Count struct {
	ID        primitive.ObjectID `bson:"_id,omitempty"`
	TxCount   int64              `bson:"tx_count"`   //交易总数
	CoinCount int64              `bson:"coin_count"` //全网金额
	AccCount  int64              `bson:"acc_count"`  //账户总数
	Accounts  []bson.D           `bson:"accounts"`   //账户列表
}

var counts *Count

func (m *MongoClient) SaveCount(txs []*utils.Transaction) error {
	countCol := m.Database.Collection("count")

	//获取已有数据,缓存起来
	if counts == nil {
		err := countCol.FindOne(m.ctx, bson.M{"_id": "counts"}).Decode(counts)
		if err != nil && err != mongo.ErrNoDocuments {
			return err
		}
	}

	for _, tx := range txs {

		//统计交易总数
		counts.TxCount++

		//统计全网金额
		if tx.Coinbase || tx.VoteCoinbase {
			for _, output := range tx.TxOutputs {
				counts.CoinCount += output.Amount
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
				counts.Accounts = append(counts.Accounts, bson.D{{"accounts", txOutput.ToAddr}})
				//写入数据库
				_, err := countCol.InsertOne(m.ctx, bson.D{{"accounts", txOutput.ToAddr}})
				if err != nil {
					return err
				}
			}
		}
	}

	up := true
	_, err := countCol.UpdateOne(m.ctx,
		bson.M{"_id": "counts"},
		&bson.D{{"$set", bson.D{
			{"tx_count", counts.TxCount},
			{"coin_count", counts.CoinCount},
			{"acc_count", counts.AccCount},
		}}},
		&options.UpdateOptions{Upsert: &up})

	return err
}

func (m *MongoClient) SaveAccount(txs []*utils.Transaction) error {

	accCol := m.Database.Collection("account")

	//获取已有账户,缓存起来
	var accountDoc bson.D
	if len(accounts) == 0 {
		err := accCol.FindOne(m.ctx, bson.M{"_id": "accounts"}).Decode(&accountDoc)
		if err != nil && err != mongo.ErrNoDocuments {
			return err
		}
		if accountDoc != nil {
			accounts = accountDoc.Map()["accounts"].(bson.A)
		}
	}

	//是否需要保存新的账户
	needSave := false
	for _, tx := range txs {
		for _, txOutput := range tx.TxOutputs {
			if txOutput.ToAddr == "$" {
				continue
			}
			i := arrays.Contains(accounts, txOutput.ToAddr)
			if i == -1 {
				accounts = append(accounts, txOutput.ToAddr)
				needSave = true
			}
		}
	}
	if needSave {
		needSave = false
		a := true
		_, err := accCol.UpdateOne(m.ctx,
			bson.M{"_id": "accounts"},
			&bson.D{{"$set", bson.D{{"accounts", accounts}}}},
			&options.UpdateOptions{Upsert: &a})
		if err != nil {
			return err
		}
	}

	//记录交易
	sampleTxs := []interface{}{}

	//遍历交易
	for _, tx := range txs {
		sampleTxs = append(sampleTxs, bson.D{
			{"_id", tx.Txid},
			{"blockid", tx.Blockid},
			{"timestamp", tx.Timestamp},
			{"initiator", tx.Initiator},
			{"txInputs", tx.TxInputs},
			{"txOutputs", tx.TxOutputs},
			{"coinbase", tx.Coinbase},
			{"voteCoinbase", tx.VoteCoinbase}, //todo 需要修改pb文件
		})
	}

	//记录账户交易
	//sampleTxs := []interface{}{}
	for _, tx := range txs {

		//记录转账人
		if tx.Initiator != "" {
			sampleTxs = append(sampleTxs, bson.D{
				{"account", tx.Initiator},
				{"timestamp", tx.Timestamp},
				//{"tx", bson.D{
				//	{"$ref", "tx"},
				//	{"$id", tx.Txid},
				//},
				//},
				{"tx", bson.D{
					{"_id", tx.Txid},
					{"blockid", tx.Blockid},
					{"timestamp", tx.Timestamp},
					{"initiator", tx.Initiator},
					{"txInputs", tx.TxInputs},
					{"txOutputs", tx.TxOutputs},
					{"coinbase", tx.Coinbase},
					{"voteCoinbase", tx.VoteCoinbase}, //todo 需要修改pb文件
				},
				},
			})
		}

		//记录收款人
		for _, output := range tx.TxOutputs {
			to := output.ToAddr
			if to == "$" || to == tx.Initiator {
				continue
			}
			sampleTxs = append(sampleTxs, bson.D{
				{"account", output.ToAddr},
				{"timestamp", tx.Timestamp},
				//{"tx", bson.D{
				//	{"$ref", "tx"},
				//	{"$id", tx.Txid},
				//},
				//},
				{"tx", bson.D{
					{"_id", tx.Txid},
					{"blockid", tx.Blockid},
					{"timestamp", tx.Timestamp},
					{"initiator", tx.Initiator},
					{"txInputs", tx.TxInputs},
					{"txOutputs", tx.TxOutputs},
					{"coinbase", tx.Coinbase},
					{"voteCoinbase", tx.VoteCoinbase}, //todo 需要修改pb文件
				},
				},
			})
		}
	}

	_, err := accCol.InsertMany(m.ctx, sampleTxs)
	return err
}

func (m *MongoClient) SaveBlock(block *utils.InternalBlock) error {

	//存账户
	err := m.SaveAccount(block.Transactions)
	if err != nil {
		return err
	}

	//存交易
	err = m.SaveTx(block.Height, block.Transactions)
	if err != nil {
		return err
	}

	//txids := []bson.D{}
	//for _, v := range block.Transactions {
	//	txids = append(txids, bson.D{
	//		{"$ref", "tx"},
	//		{"$id", v.Txid},
	//	})
	//}

	//记录交易
	sampleTxs := []interface{}{}

	//遍历交易
	for _, tx := range block.Transactions {
		sampleTxs = append(sampleTxs, bson.D{
			{"_id", tx.Txid},
			{"blockid", tx.Blockid},
			{"blockHeight", block.Height},
			{"timestamp", tx.Timestamp},
			{"initiator", tx.Initiator},
			{"txInputs", tx.TxInputs},
			{"txOutputs", tx.TxOutputs},
			{"coinbase", tx.Coinbase},
			{"voteCoinbase", tx.VoteCoinbase}, //todo 需要修改pb文件
		})
	}

	iblock := bson.D{
		{"_id", block.Height},
		{"blockid", block.Blockid},
		{"proposer", block.Proposer},
		//{"transactions", txids},
		{"transactions", sampleTxs},
		{"txCount", block.TxCount},
		{"preHash", block.PreHash},
		{"inTrunk", block.InTrunk},
		{"timestamp", block.Timestamp},
		{"failedTxs", block.FailedTxs},
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
