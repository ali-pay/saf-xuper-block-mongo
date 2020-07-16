package main

import (
	"encoding/json"
	"fmt"
	"github.com/jason-cn-dev/xuperdata/utils"
	"log"
	"net/http"
)

var (
	port = 8081
)

type Request struct {
	Txid string `json:"txid"`
}

func postTxid(w http.ResponseWriter, r *http.Request) {

	//fmt.Fprintf(w, "Hello golang http!")

	var req Request
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		log.Println(err)
		return
	}
	defer r.Body.Close()
	fmt.Fprintf(w, "you input txid is: %s", req.Txid)

	//获取交易
	//GetTxByTxId(req.Txid)
}

func getTxid(w http.ResponseWriter, r *http.Request) {
	txid := r.FormValue("txid")
	//fmt.Fprintf(w, "you input txid is: %s", txid)

	if len(txid) != 64 {
		fmt.Fprintf(w, "error! txid must be 64 char, you input txid is: %s", txid)
		return
	}

	tx, err := GetTxByTxId(txid)
	if err != nil {
		fmt.Fprintf(w, "txid: %s, get the tx is failed, errors is: %s", txid, err.Error())
		log.Println(err)
		return
	}

	err = mongoClient.SaveTx(&utils.InternalBlock{
		Transactions: []*utils.Transaction{utils.FullTx(tx)},
	})
	if err != nil {
		log.Println(err)
		fmt.Fprintf(w, "txid: %s, save to mongodb failed", txid)
		return
	}
	fmt.Fprintf(w, "txid: %s, save to mongodb successed", txid)
}

func run() {
	http.HandleFunc("/getTxid", getTxid)
	//http.HandleFunc("/postTxid", postTxid)

	log.Printf("http server is runing, listen in the port: %d", port)
	err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	if err != nil {
		log.Fatal(err)
	}
}
