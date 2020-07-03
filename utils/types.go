package utils

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/jason-cn-dev/xuper-sdk-go/pb"
	"github.com/jason-cn-dev/xuper-sdk-go/txhash"
)

type HexID []byte

func (h HexID) MarshalJSON() ([]byte, error) {
	hex := hex.EncodeToString(h)
	return json.Marshal(hex)
}

type TxInput struct {
	RefTxid   HexID  `json:"refTxid,omitempty"`
	RefOffset int32  `json:"refOffset,omitempty"`
	FromAddr  string `json:"fromAddr,omitempty"`
	Amount    BigInt `json:"amount,omitempty"`
}

type TxOutput struct {
	Amount BigInt `json:"amount,omitempty"`
	ToAddr string `json:"toAddr,omitempty"`
}

type TxInputExt struct {
	Bucket    string `json:"bucket,omitempty"`
	Key       string `json:"key,omitempty"`
	RefTxid   HexID  `json:"refTxid,omitempty"`
	RefOffset int32  `json:"refOffset,omitempty"`
}

type TxOutputExt struct {
	Bucket string `json:"bucket,omitempty"`
	Key    string `json:"key,omitempty"`
	Value  string `json:"value,omitempty"`
}

type ResourceLimit struct {
	Type  string `json:"type,omitempty"`
	Limit int64  `json:"limit,omitempty"`
}

type InvokeRequest struct {
	ModuleName    string            `json:"moduleName,omitempty"`
	ContractName  string            `json:"contractName,omitempty"`
	MethodName    string            `json:"methodName,omitempty"`
	Args          map[string]string `json:"args,omitempty"`
	ResouceLimits []ResourceLimit   `json:"resource_limits,omitempty"`
}

type GasPrice struct {
	CpuRate  int64 `json:"cpu_rate,omitempty"`
	MemRate  int64 `json:"mem_rate,omitempty"`
	DiskRate int64 `json:"disk_rate,omitempty"`
	XfeeRate int64 `json:"xfee_rate,omitempty"`
}

type SignatureInfo struct {
	PublicKey string `json:"publickey,omitempty"`
	Sign      HexID  `json:"sign,omitempty"`
}

type QCState int32

type SignInfo struct {
	Address   string `protobuf:"bytes,1,opt,name=Address,proto3" json:"Address,omitempty"`
	PublicKey string `protobuf:"bytes,2,opt,name=PublicKey,proto3" json:"PublicKey,omitempty"`
	Sign      []byte `protobuf:"bytes,3,opt,name=Sign,proto3" json:"Sign,omitempty"`
}

type QCSignInfos struct {
	QCSignInfos []*SignInfo `protobuf:"bytes,1,rep,name=QCSignInfos,proto3" json:"QCSignInfos,omitempty"`
}

type QuorumCert struct {
	ProposalId  string       `protobuf:"bytes,1,opt,name=ProposalId,proto3" json:"ProposalId,omitempty"`
	ProposalMsg []byte       `protobuf:"bytes,2,opt,name=ProposalMsg,proto3" json:"ProposalMsg,omitempty"`
	Type        QCState      `protobuf:"varint,3,opt,name=Type,proto3,enum=pb.QCState" json:"Type,omitempty"`
	ViewNumber  int64        `protobuf:"varint,4,opt,name=ViewNumber,proto3" json:"ViewNumber,omitempty"`
	SignInfos   *QCSignInfos `protobuf:"bytes,5,opt,name=SignInfos,proto3" json:"SignInfos,omitempty"`
}

type Transaction struct {
	Txid              HexID            `json:"txid,omitempty"`
	Blockid           HexID            `json:"blockid,omitempty"`
	TxInputs          []TxInput        `json:"txInputs,omitempty"`
	TxOutputs         []TxOutput       `json:"txOutputs,omitempty"`
	Desc              string           `json:"desc,omitempty"`
	Nonce             string           `json:"nonce,omitempty"`
	Timestamp         int64            `json:"timestamp,omitempty"`
	Version           int32            `json:"version,omitempty"`
	Autogen           bool             `json:"autogen,omitempty"`
	Coinbase          bool             `json:"coinbase,omitempty"`
	VoteCoinbase      bool             `json:"voteCoinbase,omitempty"`
	TxInputsExt       []TxInputExt     `json:"txInputsExt,omitempty"`
	TxOutputsExt      []TxOutputExt    `json:"txOutputsExt,omitempty"`
	ContractRequests  []*InvokeRequest `json:"contractRequests,omitempty"`
	Initiator         string           `json:"initiator,omitempty"`
	AuthRequire       []string         `json:"authRequire,omitempty"`
	InitiatorSigns    []SignatureInfo  `json:"initiatorSigns,omitempty"`
	AuthRequireSigns  []SignatureInfo  `json:"authRequireSigns,omitempty"`
	ReceivedTimestamp int64            `json:"receivedTimestamp,omitempty"`
	//ModifyBlock       ModifyBlock      `json:"modifyBlock,omitempty"`
}

type ModifyBlock struct {
	Marked          bool   `json:"marked,omitempty"`
	EffectiveHeight int64  `json:"effectiveHeight,omitempty"`
	EffectiveTxid   string `json:"effectiveTxid,omitempty"`
}

type BigInt big.Int

func FromAmountBytes(buf []byte) BigInt {
	n := big.Int{}
	n.SetBytes(buf)
	return BigInt(n)
}

func (b *BigInt) MarshalJSON() ([]byte, error) {
	str := (*big.Int)(b).String()
	return json.Marshal(str)
}

//精简了获取的交易数据，在获取状态的时候，尽可能的取消不必要的操作
func FromPBTx(tx *pb.Transaction) *Transaction {
	t := &Transaction{
		Txid:      tx.Txid,
		Timestamp: tx.Timestamp,
	}
	return t
}

type InternalBlock struct {
	Version      int32             `json:"version,omitempty"`
	Blockid      HexID             `json:"blockid,omitempty"`
	PreHash      HexID             `json:"preHash,omitempty"`
	Proposer     string            `json:"proposer,omitempty"`
	Sign         HexID             `json:"sign,omitempty"`
	Pubkey       string            `json:"pubkey,omitempty"`
	MerkleRoot   HexID             `json:"merkleRoot,omitempty"`
	Height       int64             `json:"height,omitempty"`
	Timestamp    int64             `json:"timestamp,omitempty"`
	Transactions []*Transaction    `json:"transactions,omitempty"`
	TxCount      int32             `json:"txCount,omitempty"`
	MerkleTree   []HexID           `json:"merkleTree,omitempty"`
	InTrunk      bool              `json:"inTrunk,omitempty"`
	NextHash     HexID             `json:"nextHash,omitempty"`
	FailedTxs    map[string]string `json:"failedTxs,omitempty"`
	CurTerm      int64             `json:"curTerm,omitempty"`
	CurBlockNum  int64             `json:"curBlockNum,omitempty"`
	Justify      *QuorumCert       `json:"justify,omitempty"`
	Nonce        int32             `json:"nonce,omitempty"`
	TargetBits   int32             `json:"targetBits,omitempty"`
}

func FromInternalBlockPB(block *pb.InternalBlock) *InternalBlock {
	iblock := &InternalBlock{
		//Version:     block.Version,
		Blockid:  block.Blockid,
		PreHash:  block.PreHash,
		Proposer: string(block.Proposer),
		//Sign:        block.Sign,
		//Pubkey:      string(block.Pubkey),
		//MerkleRoot:  block.MerkleRoot,
		Height:    block.Height,
		Timestamp: block.Timestamp,
		TxCount:   block.TxCount,
		InTrunk:   block.InTrunk,
		//NextHash:    block.NextHash,
		//FailedTxs:   block.FailedTxs,
		//CurTerm:     block.CurTerm,
		//CurBlockNum: block.CurBlockNum,
		Nonce:      block.Nonce,
		TargetBits: block.TargetBits,
	}
	//iblock.MerkleTree = make([]HexID, len(block.MerkleTree))
	//for i := range block.MerkleTree {
	//	iblock.MerkleTree[i] = block.MerkleTree[i]
	//}
	iblock.Transactions = make([]*Transaction, len(block.Transactions))
	for i := range block.Transactions {
		iblock.Transactions[i] = FromPBTx(block.Transactions[i])
	}
	//iblock.Justify = FromPBJustify(block.Justify)
	return iblock
}

func FromPBJustify(qc *pb.QuorumCert) *QuorumCert {
	justify := &QuorumCert{}
	if qc != nil {
		justify.ProposalId = hex.EncodeToString(qc.ProposalId)
		justify.ProposalMsg = qc.ProposalMsg
		justify.Type = QCState(int(qc.Type))
		justify.ViewNumber = qc.ViewNumber
		justify.SignInfos = &QCSignInfos{
			QCSignInfos: make([]*SignInfo, 0),
		}
		for _, sign := range qc.SignInfos.QCSignInfos {
			tmpSign := &SignInfo{
				Address:   sign.Address,
				PublicKey: sign.PublicKey,
				Sign:      sign.Sign,
			}
			justify.SignInfos.QCSignInfos = append(justify.SignInfos.QCSignInfos, tmpSign)
		}
	}
	return justify
}

type LedgerMeta struct {
	RootBlockid HexID `json:"rootBlockid,omitempty"`
	TipBlockid  HexID `json:"tipBlockid,omitempty"`
	TrunkHeight int64 `json:"trunkHeight,omitempty"`
}

type UtxoMeta struct {
	LatestBlockid            HexID           `json:"latestBlockid,omitempty"`
	LockKeyList              []string        `json:"lockKeyList,omitempty"`
	UtxoTotal                string          `json:"utxoTotal,omitempty"`
	AvgDelay                 int64           `json:"avgDelay,omitempty"`
	UnconfirmTxAmount        int64           `json:"unconfirmed,omitempty"`
	MaxBlockSize             int64           `json:"maxBlockSize,omitempty"`
	ReservedContracts        []InvokeRequest `json:"reservedContracts,omitempty"`
	ForbiddenContract        InvokeRequest   `json:"forbiddenContract,omitempty"`
	NewAccountResourceAmount int64           `json:"newAccountResourceAmount,omitempty"`
	TransferFeeAmount        int64           `json:"transfer_fee_amount,omitempty"`
	IrreversibleBlockHeight  int64           `json:"irreversibleBlockHeight,omitempty"`
	IrreversibleSlideWindow  int64           `json:"irreversibleSlideWindow,omitempty"`
	GasPrice                 GasPrice        `json:"gasPrice,omitempty"`
}

type ContractStatData struct {
	AccountCount  int64 `json:"accountCount,omitempty"`
	ContractCount int64 `json:"contractCount,omitempty"`
}

type ChainStatus struct {
	Name          string         `json:"name,omitempty"`
	LedgerMeta    LedgerMeta     `json:"ledger,omitempty"`
	//UtxoMeta      UtxoMeta       `json:"utxo,omitempty"`
	//BranchBlockid []string       `json:"branchBlockid,omitempty"`
	Block         *InternalBlock `json:"block,omitempty"` //增加区块数据
}

type SystemStatus struct {
	ChainStatus []ChainStatus `json:"blockchains,omitempty"`
	Peers       []string      `json:"peers,omitempty"`
	//Speeds      *pb.Speeds    `json:"speeds,omitempty"`
}

func FromSystemStatusPB(statuspb *pb.SystemsStatus) *SystemStatus {
	status := &SystemStatus{}
	for _, chain := range statuspb.GetBcsStatus() {
		ledgerMeta := chain.GetMeta()
		block := chain.GetBlock() //增加区块数据

		//utxoMeta := chain.GetUtxoMeta()
		//ReservedContracts := utxoMeta.GetReservedContracts()
		//rcs := []InvokeRequest{}
		//for _, rcpb := range ReservedContracts {
		//	args := map[string]string{}
		//	for k, v := range rcpb.GetArgs() {
		//		args[k] = string(v)
		//	}
		//	rc := InvokeRequest{
		//		ModuleName:   rcpb.GetModuleName(),
		//		ContractName: rcpb.GetContractName(),
		//		MethodName:   rcpb.GetMethodName(),
		//		Args:         args,
		//	}
		//	rcs = append(rcs, rc)
		//}
		//forbiddenContract := utxoMeta.GetForbiddenContract()
		//args := forbiddenContract.GetArgs()
		//originalArgs := map[string]string{}
		//for key, value := range args {
		//	originalArgs[key] = string(value)
		//}
		//forbiddenContractMap := InvokeRequest{
		//	ModuleName:   forbiddenContract.GetModuleName(),
		//	ContractName: forbiddenContract.GetContractName(),
		//	MethodName:   forbiddenContract.GetMethodName(),
		//	Args:         originalArgs,
		//}
		//gasPricePB := utxoMeta.GetGasPrice()
		//gasPrice := GasPrice{
		//	CpuRate:  gasPricePB.GetCpuRate(),
		//	MemRate:  gasPricePB.GetMemRate(),
		//	DiskRate: gasPricePB.GetDiskRate(),
		//	XfeeRate: gasPricePB.GetXfeeRate(),
		//}
		status.ChainStatus = append(status.ChainStatus, ChainStatus{
			Name: chain.GetBcname(),
			LedgerMeta: LedgerMeta{
				RootBlockid: ledgerMeta.GetRootBlockid(),
				TipBlockid:  ledgerMeta.GetTipBlockid(),
				TrunkHeight: ledgerMeta.GetTrunkHeight(),
			},

			//UtxoMeta: UtxoMeta{
			//	LatestBlockid:            utxoMeta.GetLatestBlockid(),
			//	LockKeyList:              utxoMeta.GetLockKeyList(),
			//	UtxoTotal:                utxoMeta.GetUtxoTotal(),
			//	AvgDelay:                 utxoMeta.GetAvgDelay(),
			//	UnconfirmTxAmount:        utxoMeta.GetUnconfirmTxAmount(),
			//	MaxBlockSize:             utxoMeta.GetMaxBlockSize(),
			//	NewAccountResourceAmount: utxoMeta.GetNewAccountResourceAmount(),
			//	TransferFeeAmount:        utxoMeta.GetTransferFeeAmount(),
			//	ReservedContracts:        rcs,
			//	ForbiddenContract:        forbiddenContractMap,
			//	IrreversibleBlockHeight:  utxoMeta.GetIrreversibleBlockHeight(),
			//	IrreversibleSlideWindow:  utxoMeta.GetIrreversibleSlideWindow(),
			//	GasPrice:                 gasPrice,
			//},

			//BranchBlockid: chain.GetBranchBlockid(),
			Block:         FromInternalBlockPB(block),
		})
	}
	status.Peers = statuspb.GetPeerUrls()
	//status.Speeds = statuspb.GetSpeeds()
	return status
}

type TriggerDesc struct {
	Module string      `json:"module,omitempty"`
	Method string      `json:"method,omitempty"`
	Args   interface{} `json:"args,omitempty"`
	Height int64       `json:"height,omitempty"`
}

type ContractDesc struct {
	Module  string      `json:"module,omitempty"`
	Method  string      `json:"method,omitempty"`
	Args    interface{} `json:"args,omitempty"`
	Trigger TriggerDesc `json:"trigger,omitempty"`
}

func SimpleTx(tx *pb.Transaction) *Transaction {
	t := &Transaction{
		Txid:      tx.Txid,
		Blockid:   tx.Blockid,
		Timestamp: tx.Timestamp,
		Initiator: tx.Initiator,
		Coinbase:  tx.Coinbase,
	}

	for _, input := range tx.TxInputs {
		t.TxInputs = append(t.TxInputs, TxInput{
			RefTxid:   input.RefTxid,
			RefOffset: input.RefOffset,
			FromAddr:  string(input.FromAddr),
			Amount:    FromAmountBytes(input.Amount),
		})
	}

	for _, output := range tx.TxOutputs {
		//过滤
		//to := string(output.ToAddr)
		//if to == "$" || to == v.Initiator {
		//	continue
		//}

		t.TxOutputs = append(t.TxOutputs, TxOutput{
			Amount: FromAmountBytes(output.Amount),
			ToAddr: string(output.ToAddr),
		})
	}
	return t
}

func SimpleTxs(txs []*pb.Transaction) []*Transaction {
	tempTxs := []*Transaction{}
	for _, tx := range txs {
		t := &Transaction{
			Txid:      tx.Txid,
			Blockid:   tx.Blockid,
			Timestamp: tx.Timestamp,
			Initiator: tx.Initiator,
			Coinbase:  tx.Coinbase,
		}

		for _, input := range tx.TxInputs {
			t.TxInputs = append(t.TxInputs, TxInput{
				RefTxid:   input.RefTxid,
				RefOffset: input.RefOffset,
				FromAddr:  string(input.FromAddr),
				Amount:    FromAmountBytes(input.Amount),
			})
		}

		for _, output := range tx.TxOutputs {
			//过滤
			//to := string(output.ToAddr)
			//if to == "$" || to == v.Initiator {
			//	continue
			//}

			t.TxOutputs = append(t.TxOutputs, TxOutput{
				Amount: FromAmountBytes(output.Amount),
				ToAddr: string(output.ToAddr),
			})
		}
		tempTxs = append(tempTxs, t)
	}
	return tempTxs
}

func SimpleBlock(block *pb.InternalBlock) *InternalBlock {
	iblock := &InternalBlock{
		Blockid:   block.Blockid,
		PreHash:   block.PreHash,
		Proposer:  string(block.Proposer),
		Height:    block.Height,
		Timestamp: block.Timestamp,
		TxCount:   block.TxCount,
		InTrunk:   block.InTrunk,
		FailedTxs: block.FailedTxs,
	}
	iblock.Transactions = make([]*Transaction, len(block.Transactions))
	for i := range block.Transactions {
		iblock.Transactions[i] = SimpleTx(block.Transactions[i])
	}
	return iblock
}

func SimpleBlocks(blocks []*pb.InternalBlock) []*InternalBlock {
	tempBlocks := []*InternalBlock{}
	for _, v := range blocks {
		block := &InternalBlock{
			Height:       v.Height,
			Blockid:      v.Blockid,
			Timestamp:    v.Timestamp,
			Proposer:     string(v.Proposer),
			PreHash:      v.PreHash,
			Transactions: SimpleTxs(v.Transactions),
			TxCount:      v.TxCount,
			InTrunk:      v.InTrunk,
		}
		tempBlocks = append(tempBlocks, block)
	}
	return tempBlocks
}

func FullTx(tx *pb.Transaction) *Transaction {
	t := &Transaction{
		Txid:              tx.Txid,
		Blockid:           tx.Blockid,
		Nonce:             tx.Nonce,
		Timestamp:         tx.Timestamp,
		Version:           tx.Version,
		Desc:              string(tx.Desc),
		Autogen:           tx.Autogen,
		Coinbase:          tx.Coinbase,
		VoteCoinbase:      tx.VoteCoinbase,
		Initiator:         tx.Initiator,
		ReceivedTimestamp: tx.ReceivedTimestamp,
	}
	for _, input := range tx.TxInputs {
		t.TxInputs = append(t.TxInputs, TxInput{
			RefTxid:   input.RefTxid,
			RefOffset: input.RefOffset,
			FromAddr:  string(input.FromAddr),
			Amount:    FromAmountBytes(input.Amount),
		})
	}
	for _, output := range tx.TxOutputs {
		t.TxOutputs = append(t.TxOutputs, TxOutput{
			Amount: FromAmountBytes(output.Amount),
			ToAddr: string(output.ToAddr),
		})
	}
	for _, inputExt := range tx.TxInputsExt {
		t.TxInputsExt = append(t.TxInputsExt, TxInputExt{
			Bucket:    inputExt.Bucket,
			Key:       string(inputExt.Key),
			RefTxid:   inputExt.RefTxid,
			RefOffset: inputExt.RefOffset,
		})
	}
	for _, outputExt := range tx.TxOutputsExt {
		v := string(outputExt.Value)
		if len(v) > 30 {
			v = "value too long"
		}
		t.TxOutputsExt = append(t.TxOutputsExt, TxOutputExt{
			Bucket: outputExt.Bucket,
			Key:    string(outputExt.Key),
			Value:  v,
		})
	}
	if tx.ContractRequests != nil {
		for i := 0; i < len(tx.ContractRequests); i++ {
			req := tx.ContractRequests[i]
			tmpReq := &InvokeRequest{
				ModuleName:   req.ModuleName,
				ContractName: req.ContractName,
				MethodName:   req.MethodName,
				Args:         map[string]string{},
			}
			for argKey, argV := range req.Args {
				v := string(argV)
				if len(argV) > 30 {
					v = "value too long"
				}
				tmpReq.Args[argKey] = v
			}
			for _, rlimit := range req.ResourceLimits {
				resource := ResourceLimit{
					Type:  rlimit.Type.String(),
					Limit: rlimit.Limit,
				}
				tmpReq.ResouceLimits = append(tmpReq.ResouceLimits, resource)
			}
			t.ContractRequests = append(t.ContractRequests, tmpReq)
		}
	}

	t.AuthRequire = append(t.AuthRequire, tx.AuthRequire...)

	for _, initsign := range tx.InitiatorSigns {
		t.InitiatorSigns = append(t.InitiatorSigns, SignatureInfo{
			PublicKey: initsign.PublicKey,
			Sign:      initsign.Sign,
		})
	}

	for _, authSign := range tx.AuthRequireSigns {
		t.AuthRequireSigns = append(t.AuthRequireSigns, SignatureInfo{
			PublicKey: authSign.PublicKey,
			Sign:      authSign.Sign,
		})
	}

	//if tx.ModifyBlock != nil {
	//	t.ModifyBlock = ModifyBlock{
	//		EffectiveHeight: tx.ModifyBlock.EffectiveHeight,
	//		Marked:          tx.ModifyBlock.Marked,
	//		EffectiveTxid:   tx.ModifyBlock.EffectiveTxid,
	//	}
	//}
	return t
}

func SumHash(tx *pb.Transaction) error {
	j, err := json.Marshal(tx)
	if err != nil {
		return err
	}
	fmt.Println("md5:", md5.Sum(j))
	fmt.Println("sha1:", sha1.Sum(j))
	fmt.Println("sha256:", sha256.Sum256(j))

	txid, err := txhash.MakeTransactionID(tx)
	if err != nil {
		return err
	}
	fmt.Println("txid:", hex.EncodeToString(txid))
	return nil
}

func PrintTx(tx *pb.Transaction) error {
	// print tx
	t := FullTx(tx)
	output, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(output))
	return nil
}
