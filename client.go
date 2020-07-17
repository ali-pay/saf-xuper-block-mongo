package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"

	"github.com/golang/protobuf/proto"
	"github.com/jason-cn-dev/xuper-sdk-go/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/jason-cn-dev/xuperdata/utils"
)

type Config struct {
	Type string `json:"type"`
	Args struct {
		Data string `json:"data"`
	} `json:"args"`
}

type TransactionEventRequest struct {
	Bcname      string `json:"bcname"`
	Initiator   string `json:"initiator"`
	AuthRequire string `json:"auth_require"`
	NeedContent bool   `json:"need_content"`
}

type BlockEventRequest struct {
	Bcname      string `json:"bcname"`
	Proposer    string `json:"proposer"`
	StartHeight int64  `json:"start_height"`
	EndHeight   int64  `json:"end_height"`
	NeedContent bool   `json:"need_content"`
}

type AccountEventRequest struct {
	Bcname      string `json:"bcname"`
	FromAddr    string `json:"from_addr"`
	ToAddr      string `json:"to_addr"`
	NeedContent bool   `json:"need_content"`
}

type PubsubClientCommand struct {
	DescFile   string
	Command    string
	EventID    string
	DestIP     string
	DataSource string
	Database   string
	HttpPort   int
	Gosize     int
}

// todo 添加重置数据库功能
func (cmd *PubsubClientCommand) addFlags() {
	flag.StringVar(&cmd.DescFile, "f", "json/block.json", "arg file to subscribe an event")
	flag.StringVar(&cmd.Command, "c", "subscribe", "option: subscribe|unsubscribe")
	flag.StringVar(&cmd.EventID, "id", "000", "eventID to unsubscribe")
	flag.StringVar(&cmd.DestIP, "h", "localhost:37101", "xchain node")
	flag.StringVar(&cmd.DataSource, "s", "mongodb://localhost:27017", "mongodb data source")
	flag.StringVar(&cmd.Database, "b", "jy_chain", "mongodb database")
	flag.IntVar(&cmd.HttpPort, "port", 8081, "port of http server")
	flag.IntVar(&cmd.Gosize, "gosize", 10, "goroutine size of sync block")
	flag.Parse()
}

func (cmd *PubsubClientCommand) Unsubscribe() {
	conn, err := grpc.Dial(cmd.DestIP, grpc.WithInsecure())
	if err != nil {
		fmt.Println("unsubscribe failed, err msg:", err)
		return
	}
	defer conn.Close()

	client := pb.NewPubsubServiceClient(conn)
	request := &pb.UnsubscribeRequest{
		Id: cmd.EventID,
	}
	response, err := client.Unsubscribe(context.Background(), request)
	if err != nil {
		fmt.Println("unsubscribe failed, err msg:", err)
	} else {
		fmt.Println("response:", response)
	}
}

func (cmd *PubsubClientCommand) Subscribe() {

	//初始化数据库连接
	var err error
	mongoClient, err = NewMongoClient(cmd.DataSource, cmd.Database)
	if err != nil {
		log.Println("can't connecting to mongodb, err:", err)
		return
	}
	defer mongoClient.Close()

	data, err := ioutil.ReadFile(cmd.DescFile)
	if err != nil {
		fmt.Println("subscribe failed, error:", err)
		return
	}
	conn, err := grpc.Dial(cmd.DestIP, grpc.WithInsecure())
	if err != nil {
		fmt.Println("unsubscribe failed, err msg:", err)
		return
	}
	defer conn.Close()

	client := pb.NewPubsubServiceClient(conn)
	request := &Config{}
	err = json.Unmarshal(data, request)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("json file:", request)

	requestArgs := request.Args.Data
	requestType := request.Type
	var requestBytes []byte
	var requestBytesErr error
	switch requestType {
	case "BLOCK":
		requestLocal := &BlockEventRequest{}
		err := json.Unmarshal([]byte(requestArgs), requestLocal)
		if err != nil {
			fmt.Println(err)
			return
		}
		requestPB := &pb.BlockEventRequest{
			Bcname:      requestLocal.Bcname,
			Proposer:    requestLocal.Proposer,
			StartHeight: requestLocal.StartHeight,
			EndHeight:   requestLocal.EndHeight,
			NeedContent: requestLocal.NeedContent,
		}
		requestBytes, requestBytesErr = proto.Marshal(requestPB)
		if requestBytesErr != nil {
			fmt.Println(requestBytesErr)
			return
		}

		node = cmd.DestIP            //节点
		bcname = requestLocal.Bcname //链名

	case "TRANSACTION":
		requestLocal := &TransactionEventRequest{}
		err := json.Unmarshal([]byte(requestArgs), requestLocal)
		if err != nil {
			fmt.Println(err)
			return
		}
		requestPB := &pb.TransactionEventRequest{
			Bcname:      requestLocal.Bcname,
			Initiator:   requestLocal.Initiator,
			AuthRequire: requestLocal.AuthRequire,
			NeedContent: requestLocal.NeedContent,
		}
		requestBytes, requestBytesErr = proto.Marshal(requestPB)
		if requestBytesErr != nil {
			fmt.Println(requestBytesErr)
			return
		}
	case "ACCOUNT":
		requestLocal := &AccountEventRequest{}
		err := json.Unmarshal([]byte(requestArgs), requestLocal)
		if err != nil {
			fmt.Println(err)
			return
		}
		requestPB := &pb.AccountEventRequest{
			Bcname:      requestLocal.Bcname,
			FromAddr:    requestLocal.FromAddr,
			ToAddr:      requestLocal.ToAddr,
			NeedContent: requestLocal.NeedContent,
		}
		requestBytes, requestBytesErr = proto.Marshal(requestPB)
		if requestBytesErr != nil {
			fmt.Println(requestBytesErr)
			return
		}
	default:
		fmt.Println("unexpected subscribe type", requestType)
		return
	}

	stream, streamErr := client.Subscribe(
		context.Background(), &pb.EventRequest{
			Type:    pb.EventType(pb.EventType_value[requestType]),
			Payload: requestBytes,
		},
	)
	if streamErr != nil {
		fmt.Println(streamErr)
		return
	}

	//上一次的事件id,避免输出太多重复数据
	tempID := ""

	for {
		reply, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println(err)
			return
		}

		eventType := reply.GetType()
		payload := reply.GetPayload()
		if tempID != reply.GetId() {
			tempID = reply.GetId()
			fmt.Println("eventID:", reply.GetId())
		}
		if pb.EventType_name[int32(eventType)] != requestType {
			fmt.Println("get unexpected msg, refuse to accept it")
			continue
		}
		switch eventType {
		case pb.EventType_TRANSACTION:
			test := &pb.TransactionEvent{}
			unmarshalErr := proto.Unmarshal(payload, test)
			if unmarshalErr != nil {
				continue
			}
			fmt.Println("I am TransactionEvent")
			fmt.Println("status:", reply.GetTxStatus())
			fmt.Println("payload", test.GetTx())

		case pb.EventType_BLOCK:
			test := &pb.BlockEvent{}
			unmarshalErr := proto.Unmarshal(payload, test)
			if unmarshalErr != nil {
				continue
			}
			//fmt.Println("I am BlockEvent")
			//fmt.Println("status:", reply.GetBlockStatus())
			//fmt.Println("payload", test.GetBlock())

			//fmt.Println("Recv block:", test.GetBlock().Height)
			//存数据
			err = mongoClient.Save(utils.FromInternalBlockPB(test.GetBlock()))
			if err != nil {
				log.Printf("save block to mongodb failed, height: %d, error: %s", test.GetBlock().Height, err)
			}

		case pb.EventType_ACCOUNT:
			test := &pb.TransactionEvent{}
			unmarshalErr := proto.Unmarshal(payload, test)
			if unmarshalErr != nil {
				continue
			}
			fmt.Println("I am AccountEvent")
			fmt.Println("status:", reply.GetAccountStatus())
			fmt.Println("payload", test.GetTx())
		default:
			fmt.Println("I am undefined")
		}
	}
}

func main() {
	cmd := &PubsubClientCommand{
		DescFile: "json/block.json",
		Command:  "subscribe",
		EventID:  "000",
		DestIP:   ":37101",
		//DataSource: "mongodb://192.168.3.150:27017",
		DataSource: "mongodb://admin:this is mongodb admin password@192.168.3.150:27017",
		Database:   "jy_chain_test",
		HttpPort:   8081,
	}
	cmd.addFlags()

	port = cmd.HttpPort
	go run() //开启http服务

	switch cmd.Command {
	case "subscribe":
		cmd.Subscribe()
	case "unsubscribe":
		cmd.Unsubscribe()
	default:
		return
	}
	fmt.Println(cmd)
}
