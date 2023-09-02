package server

import (
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	migrateProto "github.com/cubefs/cubefs/util/migrate/proto"
	"go.uber.org/zap"
	gopath "path"
	"strings"
	"sync"
	"syscall"
)

func (job *MigrateJob) walkDir(srcDir, dstDir string, svr *MigrateServer) {
	var (
		logger = job.logger
		err    error
		wg     sync.WaitGroup
	)
	defer func() {
		if err != nil {
			job.saveWalkFailedTask(srcDir, dstDir, err)
			return
		}
	}()
	//理论上这里是否就应该判断是否为已经成功的

	//创建目标目录,目标的根目录应该存在，所以不用创建
	err = job.createDestDir(srcDir, dstDir)
	if err != nil {
		logger.Debug("createDestDir failed", zap.Any("dstDir", dstDir), zap.Any("err", err))
		return
	}
	children, err := job.srcSDK.ReadDir(srcDir)
	if err != nil {
		logger.Debug("ReadDir failed", zap.Any("srcDir", srcDir), zap.Any("err", err))
		err = errors.New(fmt.Sprintf("ReadDir failed, dir %s[%s]", srcDir, err.Error()))
		return
	}
	traverFunc := func(srcDir, dstDir string, svr *MigrateServer) {
		defer wg.Done()
		job.walkDir(srcDir, dstDir, svr)
		svr.ReleaseTraverseToken()
	}
	////反正所有文件都要遍历，可以在这里统计个数和总大小,会降低server效率
	//var (
	//	totalSize uint64 = 0
	//	fileCnt   uint64 = 0
	//)
	for _, child := range children {
		if !job.srcSDK.IsDirByDentry(child) {
			//srcInfo, err := job.srcSDK.LookupFileWithParentCache(gopath.Join(srcDir, child.Name))
			//if err != nil {
			//	logger.Warn("Get file size failed", zap.Any("file", gopath.Join(srcDir, child.Name)),
			//		zap.Any("err", err))
			//} else {
			//	fileCnt++
			//	totalSize += srcInfo.Size
			//}
			continue
		}
		traverCh := svr.TraverseDirGoroutineLimit
		select {
		case traverCh <- true:
			wg.Add(1)
			go traverFunc(gopath.Join(srcDir, child.Name), gopath.Join(dstDir, child.Name), svr)
		case <-job.stopCh:
			logger.Warn("receive stop signal, exit traverseDir")
			return
		default:
			job.walkDir(gopath.Join(srcDir, child.Name), gopath.Join(dstDir, child.Name), svr)
		}
	}
	wg.Wait()
	//每个task就有自己的totalSize
	var taskType = migrateProto.NormalTask
	//if totalSize != 0 && fileCnt != 0 && totalSize/fileCnt <= migrateProto.TinyFile {
	//	taskType = migrateProto.TinyTask
	//}
	task := job.newTask(srcDir, dstDir, 0, taskType, job.overWrite)
	//ok, successTask := svr.alreadySuccess(task.TaskId)
	////如果不存在或者开启overWrite则覆盖
	//if !ok || job.overWrite {
	//	job.sendTask(task, svr)
	//} else {
	//	job.updateCompleteSize(successTask)
	//	job.addTotalSize(successTask.MigrateSize)
	//}
	job.sendTask(task, svr)
}

// 传入的是绝对路径，因为需要对应目录权限
func (job *MigrateJob) createDestDir(srcDir, dstDir string) (err error) {
	if job.dstSDK == nil {
		panic(fmt.Sprintf("dstSDK should not be nil"))
	}
	logger := job.logger
	//获取源目录的信息
	srcDirInfo, err := job.srcSDK.LookupPathWithCache(gopath.Clean(srcDir))
	if err != nil {
		logger.Error("LookupPath src failed", zap.Any("err", err))
		return err
	}
	//获取目录路径的父目录信息
	dstParentDir, dirName := gopath.Split(gopath.Clean(dstDir))
	dstParentInfo, err := job.dstSDK.LookupPathWithCache(gopath.Clean(dstParentDir))
	if err != nil {
		logger.Error("LookupPath dstParentDir failed", zap.Any("err", err))
		return err
	}
	//创建目标文件
	_, err = job.dstSDK.CreateDirectory(dstParentInfo.Inode, dirName, srcDirInfo.Mode, srcDirInfo.Uid, srcDirInfo.Gid)
	if err != nil {
		if err != syscall.EEXIST {
			logger.Error("Create target dir failed", zap.Any("dstDir", dstDir), zap.Any("err", err))
			return errors.New(fmt.Sprintf("Create target failed, dstDir %s[%s]", dstDir, err.Error()))
		}
		//logger.Debug("Target dir is already exist", zap.Any("dstDir", dstDir))
	}
	//	logger.Debug("CreateDestDir success", zap.Any("dstDir", dstDir))
	return nil
}

func (job *MigrateJob) saveWalkFailedTask(srcPath, dstPath string, err error) {
	task := job.newTask(srcPath, dstPath, 0, migrateProto.NormalTask, job.overWrite)
	task.ErrorMsg = fmt.Sprintf("create dst dir failed:%v", err.Error())
	job.logger.Debug("saveWalkFailedTask", zap.Any("task", task))
	job.saveFailedMigratingTask(task)
}

func (job *MigrateJob) mkParentDir(dirPath string) (err error) {
	dirPath = gopath.Clean(dirPath)
	subs := strings.Split(dirPath, "/")
	//至少/volumes/mlp/code/personal/肯定是创建好的
	base := gopath.Join("/", subs[1], subs[2], subs[3], subs[4])

	for index, _ := range subs {
		if index <= 4 {
			continue
		}
		targetDir := gopath.Join(base, subs[index])
		if !job.dstSDK.PathIsExist(targetDir) {
			//不存在则创建对应目录,根据源路径的权限
			var srcDirInfo, targetDirInfo *proto.InodeInfo
			srcDirInfo, err = job.srcSDK.LookupPath(gopath.Clean(targetDir))
			if err != nil {
				job.logger.Error("mkParentDir get src dir failed", zap.Any("targetDir", targetDir), zap.Any("err", err))
				return
			}
			//获取目标路径的父目录
			targetDirInfo, err = job.dstSDK.LookupPath(gopath.Clean(base))
			if err != nil {
				job.logger.Error("mkParentDir get dst dir failed", zap.Any("base", base), zap.Any("err", err))
				return
			}
			_, err = job.dstSDK.CreateDirectory(targetDirInfo.Inode, subs[index], srcDirInfo.Mode, srcDirInfo.Uid, srcDirInfo.Gid)
			if err != nil {
				job.logger.Error("mkParentDir create dir failed", zap.Any("targetDir", targetDir), zap.Any("err", err))
				return
			}
		}
		//如果已经存在，但是不是目录也报错
		var isDir bool
		isDir, err = job.dstSDK.IsDir(targetDir)
		if err != nil {
			job.logger.Error("check targetDir failed", zap.Any("targetDir", targetDir), zap.Any("err", err))
			return err
		}
		if !isDir {
			job.logger.Error("targetDir is not dir", zap.Any("targetDir", targetDir), zap.Any("err", err))
			return err
		}
		base = targetDir
	}
	return
}
