package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/wxnacy/wgo/arrays"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/jason-cn-dev/xuperdata/utils"
)

var (
	gosize      = 10        //苟柔婷数量
	mongoClient *MongoClient //全局的mongodb对象
)

type Count struct {
	//ID        primitive.ObjectID `bson:"_id,omitempty"`
	TxCount   int64  `bson:"tx_count"`   //交易总数
	CoinCount int64  `bson:"coin_count"` //全网金额
	AccCount  int64  `bson:"acc_count"`  //账户总数
	Accounts  bson.A `bson:"accounts"`   //账户列表
}

var counts *Count

func (m *MongoClient) SaveCount(block *utils.InternalBlock) error {
	countCol := m.Database.Collection("count")
	accCol := m.Database.Collection("account")

	//获取已有数据,缓存起来
	if counts == nil {
		counts = &Count{}

		//id必须有12个字节
		//获取统计数
		err := countCol.FindOne(nil, bson.M{"_id": "chain_count"}).Decode(counts)
		if err != nil && err != mongo.ErrNoDocuments {
			return err
		}

		//获取账户地址
		cursor, err := accCol.Find(nil, bson.M{})
		if err != nil && err != mongo.ErrNoDocuments {
			return err
		}
		if cursor != nil {
			err = cursor.All(nil, &counts.Accounts)
		}

		//过滤key,减小体积
		for i, v := range counts.Accounts {
			counts.Accounts[i] = v.(bson.D).Map()["_id"]
		}
	}

	//获取账户地址
	for _, tx := range block.Transactions {
		for _, txOutput := range tx.TxOutputs {
			//过滤矿工地址
			if txOutput.ToAddr == "$" {
				continue
			}
			//判断是否账户是否已存在
			i := arrays.Contains(counts.Accounts, txOutput.ToAddr)
			if i == -1 {
				//缓存账户
				counts.Accounts = append(counts.Accounts, txOutput.ToAddr)
				//写入数据库
				_, err := accCol.InsertOne(nil, bson.D{
					{"_id", txOutput.ToAddr},
					{"timestamp", tx.Timestamp},
				})
				if err != nil {
					return err
				}
			}
		}
	}

	//统计账户总数
	counts.AccCount = int64(len(counts.Accounts))
	//统计交易总数
	counts.TxCount += int64(block.TxCount)
	//统计全网金额
	total, err := GetUtxoTotal()
	if err != nil {
		log.Println(err)
	} else {
		counts.CoinCount = total
	}

	up := true
	_, err = countCol.UpdateOne(nil,
		bson.M{"_id": "chain_count"},
		&bson.D{{"$set", bson.D{
			{"tx_count", counts.TxCount},
			{"coin_count", counts.CoinCount},
			{"acc_count", counts.AccCount},
		}}},
		&options.UpdateOptions{Upsert: &up})

	return err
}

func (m *MongoClient) SaveTx(block *utils.InternalBlock) error {

	//索引 最新的交易
	//db.col.createIndex({"timestamp":-1}, {background: true})

	txCol := m.Database.Collection("tx")
	up := true
	var err error

	//遍历交易
	for _, tx := range block.Transactions {

		//该交易是否成功
		state := "fail"
		if tx.Blockid != "" {
			state = "success"
		}

		_, err = txCol.ReplaceOne(nil,
			bson.M{"_id": tx.Txid},
			bson.D{
				{"_id", tx.Txid},
				{"blockid", tx.Blockid},
				{"blockHeight", block.Height},
				{"timestamp", tx.Timestamp},
				{"initiator", tx.Initiator},
				{"txInputs", tx.TxInputs},
				{"txOutputs", tx.TxOutputs},
				{"coinbase", tx.Coinbase},
				{"voteCoinbase", tx.VoteCoinbase},
				{"state", state},
			},
			&options.ReplaceOptions{Upsert: &up})
	}

	//txCol := m.Database.Collection("tx")
	//_, err := txCol.InsertMany(m.ctx, sampleTxs)
	return err
}

func (m *MongoClient) SaveBlock(block *utils.InternalBlock) error {

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
	_, err := blockCol.InsertOne(nil, iblock)
	return err
}

//var once sync.Once

var two bool

func (m *MongoClient) Save(block *utils.InternalBlock) error {

	//只在启动程序第一次获取到区块的时候进行判断
	//once.Do(func() { m.GetLackBlocks(block) })
	//once里面的函数没有执行完成，状态不会置为1，所有只能通过一个标志字段来判断了
	if !two {
		two = true
		m.GetLackBlocks(block)
	}

	//存统计
	err := m.SaveCount(block)
	if err != nil {
		return err
	}

	//存交易
	err = m.SaveTx(block)
	if err != nil {
		return err
	}

	//存区块
	err = m.SaveBlock(block)
	if err != nil {
		return err
	}

	return nil
}

type MongoClient struct {
	*mongo.Client
	*mongo.Database
}

func NewMongoClient(dataSource, database string) (*MongoClient, error) {
	client, err := mongo.NewClient(options.Client().
		ApplyURI(dataSource).
		SetConnectTimeout(10 * time.Second))
	if err != nil {
		return nil, err
	}

	//ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	ctx := context.Background()
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

	return &MongoClient{client, client.Database(database)}, nil
}

func (m *MongoClient) Close() error {
	return m.Client.Disconnect(nil)
}

//找出缺少的区块
func findLacks(heights []int64) []int64 {
	lacks := make([]int64, 0)

	var i int64 = 0
	for ; i < heights[len(heights)-1]; i++ {
		//不存在,记录该值
		index := arrays.ContainsInt(heights, i)
		if index == -1 {
			lacks = append(lacks, i)
			continue
		}
		//存在,剔除该值
		heights = append(heights[:index], heights[index+1:]...)
		//fmt.Println("heights:", heights)
	}
	//fmt.Println("lacks:", lacks)
	return lacks
}

func (m *MongoClient) GetLackBlocks(block *utils.InternalBlock) error {

	blockCol := m.Database.Collection("block")

	//获取数据库中最后的区块高度
	sort := -1
	limit := int64(1)
	var heights []int64

again:
	{
		cursor, err := blockCol.Find(nil, bson.M{}, &options.FindOptions{
			Projection: bson.M{"_id": 1},
			Sort:       bson.M{"_id": sort},
			Limit:      &limit,
		})

		if err != nil && err != mongo.ErrNoDocuments {
			return err
		}
		var reply bson.A
		if cursor != nil {
			counts = &Count{}
			err = cursor.All(nil, &reply)
		}
		//fmt.Println("reply:", reply)

		//获取需要遍历的区块高度
		heights = make([]int64, len(reply))
		for i, v := range reply {
			heights[i] = v.(bson.D).Map()["_id"].(int64)
		}
		//fmt.Println("heights:", heights)
	}

	//高度不匹配,找出缺少的区块高度，并获取区块
	if len(heights) == 1 && heights[0] != block.Height-1 {
		sort = 1         //顺序排列
		limit = int64(0) //获取所有区块
		goto again
	}

	//添加一个值,避免空指针异常
	heights = append(heights, block.Height)
	//找到缺少的区块
	lacks := findLacks(heights)

	//用个协程池,避免控制并发量
	defer ants.Release()
	wg := sync.WaitGroup{}
	p, _ := ants.NewPoolWithFunc(gosize, func(i interface{}) {
		func(height int64) {
			iblock, err := GetBlockByHeight(height)
			if err != nil {
				log.Printf("GetBlockByHeight: %d, error: %s", height, err)
				return
			}

			err = m.Save(utils.FromInternalBlockPB(iblock))
			if err != nil {
				log.Println(err)
				return
			}
			//fmt.Println("succeed get lack block:", height)
		}(i.(int64))
		wg.Done()
	})
	defer p.Release()
	for _, height := range lacks {
		//fmt.Println("start get lack block:", height)
		wg.Add(1)
		_ = p.Invoke(height)

		//未使用协程池
		//go func(height int64) {
		//	defer wg.Done()
		//	iblock, err := GetBlockByHeight(height)
		//	if err != nil {
		//		log.Println(err)
		//		return
		//	}
		//
		//	err = m.Save(utils.FromInternalBlockPB(iblock))
		//	if err != nil {
		//		log.Println(err)
		//		return
		//	}
		//	fmt.Println("succeed get lack block:", height)
		//
		//}(height)
	}

	wg.Wait()
	//fmt.Println("get lack blocks finished")
	//fmt.Printf("running goroutines: %d\n", p.Running())
	return nil
}
