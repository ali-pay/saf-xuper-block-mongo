package main

import (
	"fmt"
	"github.com/jason-cn-dev/xuperdata/utils"
	"github.com/wxnacy/wgo/arrays"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
	"testing"
	"time"
)

func TestMongo(t *testing.T) {

	//mongodb客户端
	m, err := NewMongoClient(
		"mongodb://admin:this is mongodb admin password@192.168.3.150:27017",
		"jy_chain")
	if err != nil {
		t.Fatal(err)
	}

	//获取数据库列表，看能否连接上
	databases, err := m.ListDatabaseNames(nil, bson.M{})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(databases)

	//数据库中储存的区块总数
	blockCol := m.Database.Collection("block")
	count, err := blockCol.CountDocuments(nil, bson.M{})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("总区块数:", count)

	//获取区块高度
	//limit := int64(5)
	cursor, err := blockCol.Find(nil, bson.M{}, &options.FindOptions{
		Projection: bson.M{"_id": 1},
		//Sort:       bson.M{"_id": -1},
		//Limit: &limit,
	})
	if err != nil && err != mongo.ErrNoDocuments {
		t.Fatal(err)
	}
	var reply bson.A
	if cursor != nil {
		counts = &Count{}
		err = cursor.All(nil, &reply)
	}
	fmt.Println("区块id:", reply)

	//获取需要遍历的区块高度
	heights := make([]int64, len(reply))
	for i, v := range reply {
		heights[i] = v.(bson.D).Map()["_id"].(int64)
	}
	fmt.Println("简化区块id:", heights)

	//提供最高区块高度，判断数据库缺少了哪些区块
	findLacks(heights)
}

func TestFindLacks(t *testing.T) {
	lacks := make([]int64, 0)
	heights := []int64{0, 1, 2, 4, 5, 6, 7, 9, 100}

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
		fmt.Println(heights)
	}
	fmt.Println(lacks)
	fmt.Println("done")
}

func TestGetLackBlocks(t *testing.T) {
	node = ":37101"
retry:
	m, err := NewMongoClient("mongodb://admin:this is mongodb admin password@192.168.3.150:27017", "jy_chain_test")
	if err != nil {
		fmt.Println(err)
		time.Sleep(time.Second)
		if strings.Contains(err.Error(), "i/o timeout") {
			goto retry
		}
		//t.Fatal(err)
	}
	block := &utils.InternalBlock{
		Height: 3,
	}
	err = m.GetLackBlocks(block)
	if err != nil {
		t.Fatal(err)
	}
}
