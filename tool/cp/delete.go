package cp

import (
	"log"
	"os"
	"path"
	"sync"
	"time"

	clog "github.com/chubaofs/chubaofs/util/log"
)

func (w *Walker) ExecuteDel() {
	defer clog.LogFlush()

	srcCfg := w.Conf.SrcDir

	clog.LogDebugf("start excute del task, src %s", srcCfg.dir)

	srcInode, err := w.srcApi.getParentInoByPath(srcCfg.dir)
	if err != nil {
		log.Fatalf("get src inode by path failed, src %s, err %s", srcCfg.dir, err.Error())
	}

	srcParentStat, err := w.srcApi.statByInode(srcInode)
	if err != nil {
		log.Fatal("get src inode failed", srcCfg.dir, err.Error())
	}

	err = checkMode(srcParentStat, write)
	if err != nil {
		log.Fatalf("delete src dir %s, err %s", srcCfg.dir, err.Error())
	}

	srcStat, err := w.srcApi.statFile(srcCfg.dir, srcInode)
	if err != nil {
		log.Fatalf("stat src dir failed, srcDir %s, err %s", srcCfg.dir, err.Error())
	}

	mode := fileMode(srcStat.Mode)
	if !mode.IsDir() {
		task := opTask{
			src:          srcCfg.dir,
			srcParentIno: srcInode,
		}
		w.processTask(task)
	} else {

		w.traverseDel(srcCfg.dir, srcInode)
		task := opTask{
			src:          srcCfg.dir,
			srcParentIno: srcInode,
			mode:         os.ModeDir,
		}
		w.processTask(task)
	}

	time.Sleep(time.Second)
	log.Printf("delete %s success\n", srcCfg.dir)
}

func (w *Walker) traverseDel(src string, srcParentIno uint64) {
	newSrcParentIno, err := w.srcApi.getIno(src, srcParentIno)
	if err != nil {
		log.Fatalf("get src inode failed, src %s err %s", src, err.Error())
	}

	ents, err := w.srcApi.readDir(src, newSrcParentIno)
	if err != nil {
		log.Fatalf("read src dir failed, src %s, err %s", src, err.Error())
	}

	if len(ents) != 0 {
		srcParentStat, err := w.srcApi.statByInode(newSrcParentIno)
		if err != nil {
			log.Fatal("get src inode failed", src, err.Error())
		}
		err = checkMode(srcParentStat, write)
		if err != nil {
			log.Fatalf("src dir %s, delete err %s", src, err.Error())
		}
	}

	localCh := make(chan bool, 1)
	wg := sync.WaitGroup{}

	asyncFunc := func(subDir string, ch chan bool) {
		defer wg.Done()
		w.traverseDel(subDir, newSrcParentIno)
		<-ch
	}

	for _, ent := range ents {
		if !ent.Mode.IsDir() {
			continue
		}

		subDir := path.Join(src, ent.Name)
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

	delWg := sync.WaitGroup{}
	delLocalCh := make(chan struct{}, 1)
	deleteFunc := func(subDir string, mode os.FileMode, ch chan struct{}) {
		defer delWg.Done()
		task := opTask{
			src:          subDir,
			srcParentIno: newSrcParentIno,
			mode:         mode,
		}
		w.processTask(task)
		<-ch
	}

	// fmt.Println("start delete current dir")

	for _, ent := range ents {
		subDir := path.Join(src, ent.Name)

		select {
		case w.deleteJobCh <- struct{}{}:
			delWg.Add(1)
			go deleteFunc(subDir, ent.Mode, w.deleteJobCh)
		case delLocalCh <- struct{}{}:
			delWg.Add(1)
			go deleteFunc(subDir, ent.Mode, delLocalCh)
		}
	}

	delWg.Wait()

}

func (w *Walker) deleteTask(task opTask) {
	err := w.srcApi.delete(task.src, task.srcParentIno, task.mode.IsDir())
	if err == nil {
		return
	}

	log.Printf("delete path %s err %s, continue", task.src, err.Error())
}
