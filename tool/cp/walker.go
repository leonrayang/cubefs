package cp

import (
	"fmt"
	"io"
	"io/fs"
	"log"
	"time"

	// syslog "log"
	"os"
	"path"
	"strings"
	"sync"

	clog "github.com/chubaofs/chubaofs/util/log"
)

type opType int

const (
	CopyOp  opType = 0
	SyncOp  opType = 1
	CheckOp opType = 2
	DelteOp opType = 3 // not imp
)

type opTask struct {
	op            opType
	src           string
	dest          string
	srcParentIno  uint64
	destParentIno uint64
	mode          fs.FileMode
}

type Walker struct {
	srcApi  FsApi
	destApi FsApi
	Conf

	traverJobCh chan bool
	taskCh      chan opTask
	processTask func(task opTask)
	wg          sync.WaitGroup
}

func InitWalker(cfg Conf) *Walker {
	w := &Walker{
		Conf:        cfg,
		traverJobCh: make(chan bool, cfg.TraverseJobCnt),
		taskCh:      make(chan opTask, cfg.TaskCnt),
	}

	w.srcApi = initFs(cfg.SrcDir)
	w.destApi = initFs(cfg.DestDir)
	w.wg = sync.WaitGroup{}
	if cfg.Op == CopyOp {
		w.processTask = w.copyTask
	} else if cfg.Op == SyncOp {
		w.processTask = w.syncTask
	}

	return w
}

func (w *Walker) Execute() {
	defer clog.LogFlush()

	srcCfg := w.Conf.SrcDir
	destCfg := w.Conf.DestDir

	clog.LogDebugf("start excute task, src %s dest %s", srcCfg.dir, destCfg.dir)

	srcInode, err := w.srcApi.getParentInoByPath(srcCfg.dir)
	if err != nil {
		log.Fatalf("get src inode by path failed, src %s, err %s", srcCfg.dir, err.Error())
	}

	destIno, err := w.destApi.getParentInoByPath(destCfg.dir)
	if err != nil {
		log.Fatalf("get dest inode by path failed, src %s, err %s", destCfg.dir, err.Error())
	}

	srcParentStat, err := w.destApi.statByInode(srcInode)
	if err != nil {
		log.Fatal("get src inode failed", destCfg.dir, err.Error())
	}

	err = checkMode(srcParentStat, read)
	if err != nil {
		log.Fatalf("src dir %s, err %s", srcCfg.dir, err.Error())
	}

	destParentStat, err := w.destApi.statByInode(destIno)
	if err != nil {
		log.Fatal("get dest inode failed", destCfg.dir, err.Error())
	}

	err = checkMode(destParentStat, write)
	if err != nil {
		log.Fatalf("dest dir %s, err %s", srcCfg.dir, err.Error())
	}

	srcStat, err := w.srcApi.statFile(srcCfg.dir, srcInode)
	if err != nil {
		clog.LogFatalf("stat src dir failed, srcDir %s, err %s", srcCfg.dir, err.Error())
	}

	mode := fileMode(srcStat.Mode)
	if !mode.IsDir() {

		task := opTask{
			op:            w.Op,
			src:           srcCfg.dir,
			dest:          destCfg.dir,
			srcParentIno:  srcInode,
			destParentIno: destIno,
		}

		w.processTask(task)
	} else {
		for idx := 0; idx < w.workerCnt; idx++ {
			w.wg.Add(1)
			go w.consumeTask()
		}

		w.traverseDir(srcCfg.dir, srcInode, destIno, w.Op)

		close(w.taskCh)
		w.wg.Wait()
	}

	time.Sleep(time.Second)
	log.Println("success")
}

func (w *Walker) getDestPath(src string) string {
	newPath := strings.Replace(src, w.SrcDir.dir, w.DestDir.dir+"/", 1)
	return path.Clean(newPath)
}

func (w *Walker) createDir(src, dest string, srcParentIno, destParentIno uint64) {
	// if w.DestDir.dir == dest {
	// 	clog.LogDebugf("[createDir] dest dir %s is baseDestDir, no need create again", dest)
	// }

	err := w.destApi.mkdir(dest, destParentIno)
	if err != nil {
		clog.LogFatalf("mkdir %s failed %s", dest, err.Error())
	}

	srcStat, err := w.srcApi.statFile(src, srcParentIno)
	if err != nil {
		clog.LogFatalf("stat src %s failed %s", src, err.Error())
	}

	err = w.destApi.updateStat(dest, srcStat, destParentIno)
	if err != nil {
		clog.LogFatalf("update dir dest stat failed, dest %s err %s", dest, err.Error())
	}
}

func (w *Walker) addTask(src, dest string, srcParentIno, destParentIno uint64, op opType, mode os.FileMode) {
	task := opTask{
		src:           src,
		dest:          dest,
		srcParentIno:  srcParentIno,
		destParentIno: destParentIno,
		op:            op,
		mode:          mode,
	}

	w.taskCh <- task
}

func (w *Walker) traverseDir(src string, srcParentIno, destParentIno uint64, op opType) {
	dest := w.getDestPath(src)

	if op == SyncOp || op == CopyOp {
		w.createDir(src, dest, srcParentIno, destParentIno)
	}

	newSrcParentIno, err := w.srcApi.getIno(src, srcParentIno)
	if err != nil {
		log.Fatalf("get src inode failed, src %s err %s", src, err.Error())
	}

	newDestParentIno, err := w.destApi.getIno(dest, destParentIno)
	if err != nil {
		log.Fatalf("get dest inode failed, dest %s err %s", dest, err.Error())
	}

	ents, err := w.srcApi.readDir(src, newSrcParentIno)
	if err != nil {
		log.Fatalf("read src dir failed, src %s, err %s", src, err.Error())
	}

	if len(ents) == 0 {
		return
	}

	srcParentStat, err := w.destApi.statByInode(newSrcParentIno)
	if err != nil {
		log.Fatal("get src inode failed", src, err.Error())
	}

	err = checkMode(srcParentStat, read)
	if err != nil {
		fmt.Printf("src dir %s, err %s", src, err.Error())
	}

	destParentStat, err := w.destApi.statByInode(newDestParentIno)
	if err != nil {
		log.Fatal("get dest inode failed", dest, err.Error())
	}

	err = checkMode(destParentStat, write)
	if err != nil {
		fmt.Printf("dest dir %s, err %s", dest, err.Error())
	}

	localCh := make(chan bool, 1)
	wg := sync.WaitGroup{}

	asyncFunc := func(subDir string, ch chan bool) {
		defer wg.Done()
		w.traverseDir(subDir, newSrcParentIno, newDestParentIno, op)
		<-ch
	}

	for _, ent := range ents {
		subDir := path.Join(src, ent.Name)

		if !ent.isDir() {
			subDestPath := path.Join(dest, ent.Name)
			w.addTask(subDir, subDestPath, newSrcParentIno, newDestParentIno, op, ent.Mode)
			continue
		}

		select {
		case w.traverJobCh <- true:
			wg.Add(1)
			go asyncFunc(subDir, w.traverJobCh)
		case localCh <- true:
			wg.Add(1)
			go asyncFunc(subDir, localCh)
		}
	}

	wg.Wait()

}

func (w *Walker) consumeTask() {
	defer w.wg.Done()
	for task := range w.taskCh {
		w.processTask(task)
	}
}

func (w *Walker) copyFile(task opTask) {
	srcFd, err := w.srcApi.openFile(task.src, false, task.srcParentIno)
	if err != nil {
		log.Fatalf("open src path %s failed %s", task.src, err.Error())
	}
	defer srcFd.Close()

	destFd, err := w.destApi.openFile(task.dest, true, task.destParentIno)
	if err != nil {
		log.Fatalf("create dest path %s failed %s", task.dest, err.Error())
	}

	defer destFd.Close()

	data := make([]byte, w.BlkSize)
	offset := int64(0)
	for {
		size, err := srcFd.ReadAt(data, offset)
		if err != nil && err != io.EOF {
			log.Fatalf("read data from %s failed, err %s", task.src, err.Error())
		}

		if size == 0 {
			break
		}

		size, err = destFd.WriteAt(data[:size], offset)
		if err != nil {
			log.Fatalf("write data to %s failed, err %s", task.dest, err.Error())
		}

		if size < len(data) || err == io.EOF {
			break
		}

		offset += int64(size)
	}
}

func (w *Walker) copyLink(task opTask) {
	target, err := w.srcApi.readlink(task.src, task.srcParentIno)
	if err != nil {
		log.Fatalf("read src link failed, src %s, err %s", task.src, err.Error())
	}

	err = w.destApi.symlink(target, task.dest, task.destParentIno)
	if err != nil {
		log.Fatalf("execute symlink failed, target %s dest %s err %s", target, task.dest, err.Error())
	}
}

func (w *Walker) copyTask(task opTask) {
	if task.mode&os.ModeSymlink != 0 {
		w.copyLink(task)
	} else {
		w.copyFile(task)
	}

	srcStat, err := w.srcApi.statFile(task.src, task.srcParentIno)
	if err != nil {
		log.Fatalf("stat src %s failed, err %s", task.src, err.Error())
	}

	err = w.destApi.updateStat(task.dest, srcStat, task.destParentIno)
	if err != nil {
		log.Fatalf("update dest stat error, path %s, err %s", task.dest, err.Error())
	}
}

func (w *Walker) syncTask(task opTask) {
	srcStat, err := w.srcApi.statFile(task.src, task.srcParentIno)
	if err != nil {
		log.Fatalf("stat src file failed, path %s err %s", task.src, err.Error())
	}

	destStat, err := w.destApi.statFile(task.dest, task.destParentIno)
	if err != nil && err != os.ErrNotExist {
		log.Fatalf("stat dest file failed, path %s err %s", task.dest, err.Error())
	}

	if err == os.ErrNotExist || srcStat.Mtim.Sec != destStat.Mtim.Sec || srcStat.Size != destStat.Size {
		w.copyTask(task)
	}
}

func (w *Walker) checkTask(task opTask) {
	// TODO
}
