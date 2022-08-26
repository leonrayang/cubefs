package metanode

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)
var partitionId uint64=10
var DirMode = 1 << (32 - 1) // d: is a directory

var ino = &Inode{
	Inode:1,
	Type:0,
	NLink:1,
	Extents:&SortedExtents{},
	ObjExtents: NewSortedObjExtents(),
}

var manager = &metadataManager{
}
var mp *metaPartition
//PartitionId   uint64              `json:"partition_id"`
//VolName       string              `json:"vol_name"`
//PartitionType int                 `json:"partition_type"`
var metaConf = &MetaPartitionConfig{
	PartitionId:10001,
	VolName:"testVol",
	PartitionType:proto.VolumeTypeHot,
}
const (
	ConfigKeyLogDir   = "logDir"
	ConfigKeyLogLevel = "logLevel"
)
var cfgJSON = `{
		"role": "meta",
		"logDir": "/tmp/chubaofs/Logs",
		"logLevel":"debug",
		"walDir":"/tmp/chubaofs/raft",
		"clusterName":"chubaofs"
	}`

func newPartition(conf *MetaPartitionConfig, manager *metadataManager) (mp *metaPartition){
	return &metaPartition{
		config:        conf,
		dentryTree:    NewBtree(),
		inodeTree:     NewBtree(),
		extendTree:    NewBtree(),
		multipartTree: NewBtree(),
		stopC:         make(chan bool),
		storeChan:     make(chan *storeMsg, 100),
		freeList:      newFreeList(),
		extDelCh:      make(chan []proto.ExtentKey, defaultDelExtentsCnt),
		extReset:      make(chan struct{}),
		vol:           NewVol(),
		manager:       manager,
		verSeq:        conf.VerSeq,
	}
}

func init() {
	cfg := config.LoadConfigString(cfgJSON)

	logDir := cfg.GetString(ConfigKeyLogDir)
	os.RemoveAll(logDir)

	if _, err := log.InitLog(logDir, "metanode", log.DebugLevel, nil); err != nil {
		fmt.Println("Fatal: failed to start the chubaofs daemon - ", err)
		return
	}
	log.LogDebugf("action start")
	mp = newPartition(metaConf, manager)
	mp.fsmCreateInode(ino)
	return
}


func buildExtentKey(seq uint64, foffset uint64, extid uint64, exteoffset uint64, size uint32 ) proto.ExtentKey{
	return proto.ExtentKey{
		FileOffset: foffset,
		PartitionId:partitionId,
		ExtentId:extid,
		ExtentOffset:exteoffset,  // offset in extent like tiny extent offset large than 0,normal is 0
		Size:size,  // extent real size?
		VerSeq:seq,
	}
}

func buildExtents(verSeq uint64, startFileOff uint64, extid uint64) (exts []proto.ExtentKey){
	var (
		i uint64
		extOff uint64 = 0
	)
	for ; i < 1; i ++ {
		ext1 := buildExtentKey(verSeq, startFileOff+i*1000, extid, extOff+i*1000, 1000)
		exts = append(exts, ext1)
	}

	return
}

func isExtEqual(ek1 proto.ExtentKey, ek2 proto.ExtentKey) bool {
	return ek1.ExtentId == ek2.ExtentId &&
		ek1.FileOffset == ek2.FileOffset &&
		ek1.Size == ek2.Size &&
		ek1.ExtentOffset == ek2.ExtentOffset &&
		ek1.PartitionId == ek2.PartitionId
}

func checkOffSetInSequnce(t *testing.T, eks []proto.ExtentKey) bool {
	if len(eks) < 2 {
		return true
	}

	var (
		lastFileOff uint64 = eks[0].FileOffset
		lastSize   uint32 = eks[0].Size
	)

	for idx, ext := range eks[1:] {
		//t.Logf("idx:%v ext:%v, lastFileOff %v, lastSize %v", idx, ext, lastFileOff, lastSize)
		if ext.FileOffset != lastFileOff + uint64(lastSize)	{
			t.Errorf("checkOffSetInSequnce not equal idx %v %v:(%v+%v) eks{%v}", idx, ext.FileOffset, lastFileOff, lastSize, eks)
			return false
		}
		lastFileOff = ext.FileOffset
		lastSize   = ext.Size
	}
	return true
}

func getExtList(t *testing.T, verRead uint64) (resp *proto.GetExtentsResponse) {
	reqExtList := &proto.GetExtentsRequest{
		VolName: metaConf.VolName,
		PartitionID: partitionId,
		Inode: ino.Inode,
	}
	packet := &Packet{}
	reqExtList.VerSeq = verRead
	assert.True(t, nil == mp.ExtentsList(reqExtList, packet))
	resp = &proto.GetExtentsResponse{}
	assert.True(t, nil == packet.UnmarshalData(resp))

	assert.True(t, packet.ResultCode == proto.OpOk)
	assert.True(t, checkOffSetInSequnce(t, resp.Extents))
	return
}

func checkExtList(t *testing.T, seqArr []uint64) bool {
	reqExtList := &proto.GetExtentsRequest{
		VolName: metaConf.VolName,
		PartitionID: partitionId,
		Inode: ino.Inode,
	}

	for idx, verRead := range seqArr {
		t.Logf("check extlist index %v ver %v", idx, verRead)
		reqExtList.VerSeq = verRead
		getExtRsp := getExtList(t, verRead)
		t.Logf("check extlist rsp %v size %v,%v", getExtRsp, getExtRsp.Size, ino.Size)
		assert.True(t, getExtRsp.Size == uint64(1000*idx))
	}
	return true
}


func TestAppendList(t *testing.T) {
	t.Logf("enter TestAppendList")
	mp.fsmCreateInode(ino)
	ct := uint64(time.Now().Unix())
	seqArr := []uint64{ct,ct+2111,ct+10333,	ct+53456}
	t.Logf("layer len %v, arr size %v, seqarr(%v)", len(ino.multiVersions), len(seqArr), seqArr)
	for idx, seq := range seqArr {
		exts := buildExtents(seq, uint64(idx*1000), uint64(idx))
		t.Logf("buildExtents exts[%v]", exts)
		iTmp := &Inode{
			Inode: ino.Inode,
			Extents: &SortedExtents{
				eks: exts,
			},
			ObjExtents: NewSortedObjExtents(),
			verSeq: seq,
		}
		mp.verSeq = seq

		if status := mp.fsmAppendExtentsWithCheck(iTmp, false); status != proto.OpOk {
			t.Errorf("status %v", status)
		}
	}
	t.Logf("layer len %v, arr size %v, seqarr(%v)", len(ino.multiVersions), len(seqArr), seqArr)
	assert.True(t, len(ino.multiVersions) == len(seqArr))
	assert.True(t, ino.verSeq == mp.verSeq)

	for i:=0 ;i < len(seqArr)-1; i++ {
		assert.True(t, ino.multiVersions[i].verSeq == seqArr[len(seqArr)-i-2])
		t.Logf("layer %v len %v content %v,seq %v, %v", i, len(ino.multiVersions[i].Extents.eks), ino.multiVersions[i].Extents.eks,
			ino.multiVersions[i].verSeq, seqArr[len(seqArr)-i-2])
		assert.True(t, len(ino.multiVersions[i].Extents.eks) == 0 )
	}

	//-------------   split at begin -----------------------------------------
	t.Logf("start split at begin")
	var splitSeq=ct+60000
	splitKey := buildExtentKey(	splitSeq, 0, 0, 128000, 10)
	extents:= &SortedExtents{}
	extents.eks = append(extents.eks, splitKey)

	iTmp := &Inode{
		Inode: ino.Inode,
		Extents: extents,
		verSeq: splitSeq,
	}
	mp.fsmAppendExtentsWithCheck(iTmp, true)
	t.Logf("in split at begin")
	assert.True(t, ino.multiVersions[0].Extents.eks[0].VerSeq == ino.multiVersions[3].verSeq)
	assert.True(t, ino.multiVersions[0].Extents.eks[0].FileOffset == 0)
	assert.True(t, ino.multiVersions[0].Extents.eks[0].ExtentId == 0)
	assert.True(t, ino.multiVersions[0].Extents.eks[0].ExtentOffset == 0)
	assert.True(t, ino.multiVersions[0].Extents.eks[0].Size == splitKey.Size)

	t.Logf("in split at begin")

	assert.True(t, isExtEqual(ino.Extents.eks[0],splitKey))
	assert.True(t, checkOffSetInSequnce(t, ino.Extents.eks))

	t.Logf("top layer len %v, layer 1 len %v arr size %v", len(ino.Extents.eks), len(ino.multiVersions[0].Extents.eks), len(seqArr))
	assert.True(t, len(ino.multiVersions[0].Extents.eks) == 1)
	assert.True(t, len(ino.Extents.eks) == len(seqArr)+1)

	checkExtList(t, seqArr)


	//--------  split at middle  -----------------------------------------------
	t.Logf("start split at middle")

	lastTopEksLen := len(ino.Extents.eks)
	t.Logf("split at middle lastTopEksLen %v", lastTopEksLen)

	splitSeq = ct+72344
	splitKey = buildExtentKey(splitSeq, 500, 0, 128100, 100)
	extents= &SortedExtents{}
	extents.eks = append(extents.eks, splitKey)

	iTmp = &Inode{
		Inode: ino.Inode,
		Extents: extents,
		verSeq:splitSeq,
	}
	t.Logf("split at middle multiVersions %v", len(ino.multiVersions))
	mp.fsmAppendExtentsWithCheck(iTmp, true)
	t.Logf("split at middle multiVersions %v", len(ino.multiVersions))

	getExtRsp := getExtList(t, ino.multiVersions[0].verSeq)
	t.Logf("split at middle getExtRsp len %v seq(%v), toplayer len:%v seq(%v)",
		len(getExtRsp.Extents), ino.multiVersions[0].verSeq, len(ino.Extents.eks), ino.verSeq)

	assert.True(t, len(getExtRsp.Extents) == lastTopEksLen+2)
	assert.True(t, len(ino.Extents.eks) == lastTopEksLen+2)
	assert.True(t, checkOffSetInSequnce(t, ino.Extents.eks))

	t.Logf("ino exts{%v}", ino.Extents.eks)




	//--------  split at end  -----------------------------------------------
	t.Logf("start split at middle")
	// split at end
	lastTopEksLen = len(ino.Extents.eks)
	splitSeq = ct+234424
	splitKey = buildExtentKey(splitSeq, 3900, 3, 129000, 100)
	extents= &SortedExtents{}
	extents.eks = append(extents.eks, splitKey)

	iTmp = &Inode{
		Inode: ino.Inode,
		Extents: extents,
		verSeq:splitSeq,
	}
	t.Logf("split key:%v", splitKey)
	getExtRsp = getExtList(t, ino.multiVersions[0].verSeq)
	t.Logf("split at middle multiVersions %v, extent %v, level 1 %v", len(ino.multiVersions), getExtRsp.Extents, ino.multiVersions[0].Extents.eks)

	mp.fsmAppendExtentsWithCheck(iTmp, true)
	t.Logf("split at middle multiVersions %v", len(ino.multiVersions))
	getExtRsp = getExtList(t, ino.multiVersions[0].verSeq)
	t.Logf("split at middle multiVersions %v, extent %v, level 1 %v", len(ino.multiVersions), getExtRsp.Extents, ino.multiVersions[0].Extents.eks)

	t.Logf("split at middle getExtRsp len %v seq(%v), toplayer len:%v seq(%v)",
		len(getExtRsp.Extents), ino.multiVersions[0].verSeq, len(ino.Extents.eks), ino.verSeq)

	assert.True(t, len(getExtRsp.Extents) == lastTopEksLen+1)
	assert.True(t, len(ino.Extents.eks) == lastTopEksLen+1)
	assert.True(t, isExtEqual(ino.Extents.eks[lastTopEksLen], splitKey))
	//assert.True(t, false)
}

func TestSplitList(t *testing.T) {

}