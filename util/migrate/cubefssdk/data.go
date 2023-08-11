package cubefssdk

import (
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data/stream"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/migrate/util"
	"go.uber.org/zap"
	"io"
	gopath "path"
	"syscall"
	"time"
)

type DataApi struct {
	ec *stream.ExtentClient
}

func NewDataApi(volName, endpoint string, mw *meta.MetaWrapper, logger *zap.Logger) (*DataApi, error) {
	var (
		ec      *stream.ExtentClient
		err     error
		masters []string
	)

	masters = append(masters, endpoint)
	if ec, err = stream.NewExtentClient(&stream.ExtentConfig{
		Volume:            volName,
		Masters:           masters,
		FollowerRead:      false,
		OnAppendExtentKey: mw.AppendExtentKey,
		OnGetExtents:      mw.GetExtents,
		OnTruncate:        mw.Truncate,
	}); err != nil {
		logger.Error("newClient NewExtentClient failed", zap.Any("volName", volName),
			zap.Any("masters", masters), zap.Any("err", err))
		return nil, err
	}
	dataAPI := &DataApi{
		ec: ec,
	}
	return dataAPI, nil
}

func (sdk *CubeFSSdk) CopyFileToDir(srcPath, dstRoot string, dstSdk *CubeFSSdk, taskId string) (err error) {
	var (
		srcEC  = sdk.ecApi.ec
		srcMW  = sdk.mwApi.mw
		dstEC  = dstSdk.ecApi.ec
		start  = time.Now()
		logger = sdk.logger
		eks    []proto.ExtentKey
	)
	//获取源文件的inode信息。
	//logger.Debug("CopyFileToDir lookup src", zap.Any("TaskId", taskId))
	srcInfo, err := sdk.LookupPath(srcPath)
	if err != nil {
		logger.Warn("LookupPath source failed", zap.Any("TaskId", taskId), zap.Any("srcVol", sdk.volName), zap.Any("err", err))
		return err
	}
	srcEC.OpenStream(srcInfo.Inode)
	//logger.Debug("CopyFileToDir IsSymlink", zap.Any("TaskId", taskId))
	//如果是软连接
	if proto.IsSymlink(srcInfo.Mode) {
		return sdk.CopySymlinkToDir(srcPath, dstRoot, dstSdk)
	}
	//获取源文件名
	_, fileName := gopath.Split(gopath.Clean(srcPath))
	//获取目标路径信息
	//logger.Debug("CopyFileToDir lookup dst", zap.Any("TaskId", taskId))
	dstParentInfo, err := dstSdk.LookupPath(dstRoot)
	if err != nil {
		logger.Error("LookupPathWithCache target failed", zap.Any("TaskId", taskId), zap.Any("dstVol", dstSdk.volName), zap.Any("err", err))
		return err
	}
	//dstSdk.ic.Put(dstRoot, dstParentInfo)
	//logger.Debug("CopyFileToDir PathIsExist", zap.Any("TaskId", taskId))
	var dstInfo *proto.InodeInfo
	//如果目标文件已经存在，则要删除
	if ino, isExist := dstSdk.PathIsExistWithIno(gopath.Join(dstRoot, fileName)); isExist {
		//logger.Debug("CopyFileToDir open exist", zap.Any("TaskId", taskId))
		dstInfo, err = dstSdk.LookupPath(gopath.Join(dstRoot, fileName))
		if err != nil {
			logger.Error("Open target failed", zap.Any("TaskId", taskId), zap.Any("dstVol", dstSdk.volName), zap.Any("err", err))
			return errors.New(fmt.Sprintf("Open exist target failed, dstRoot %s vol %s [%s]", dstRoot, dstSdk.volName, err.Error()))
		}
		dstEC.OpenStream(dstInfo.Inode)
		//logger.Debug("CopyFileToDir DeleteFile", zap.Any("TaskId", taskId))
		if err = dstSdk.Truncate(dstParentInfo.Inode, ino); err != nil {
			logger.Error("Delete exist target failed", zap.Any("TaskId", taskId), zap.Any("dstVol", dstSdk.volName),
				zap.Any("dstFile", gopath.Join(dstRoot, fileName)), zap.Any("err", err),
				zap.Any("dstParentInfo.Inode", dstParentInfo.Inode), zap.Any("ino", ino))
			return err
		}
	} else {
		//创建目标文件
		dstInfo, err = dstSdk.CreateFile(dstParentInfo.Inode, fileName, srcInfo.Mode, srcInfo.Uid, srcInfo.Gid)
		if err != nil {
			logger.Error("Create target failed", zap.Any("TaskId", taskId), zap.Any("dstRoot", dstRoot), zap.Any("dstVol", dstSdk.volName), zap.Any("err", err))
			return errors.New(fmt.Sprintf("Create target failed, dstRoot %s vol %s[%s]", dstRoot, dstSdk.volName, err.Error()))
		}
		dstEC.OpenStream(dstInfo.Inode)
	}
	//logger.Debug("CopyFileToDir GetExtents", zap.Any("TaskId", taskId))
	//

	_, _, eks, err = srcMW.GetExtents(srcInfo.Inode)
	if err != nil {
		dstEC.CloseStream(dstInfo.Inode)
		srcEC.CloseStream(srcInfo.Inode)
		logger.Error("GetExtents for source failed", zap.Any("TaskId", taskId), zap.Any("srcPath", srcPath), zap.Any("srcVol", sdk.volName), zap.Any("err", err))
		return errors.New(fmt.Sprintf("GetExtents for source failed %s vol %s[%s]", srcPath, sdk.volName, err.Error()))
	}
	//logger.Debug("CopyFileToDir copy extents", zap.Any("TaskId", taskId))
	for _, ek := range eks {
		size := ek.Size
		var buf = util.Alloc(int(size))
		var n int
		n, err = sdk.read(srcInfo.Inode, int(ek.FileOffset), buf)
		if err != nil {
			logger.Error("Read extent failed", zap.Any("TaskId", taskId),
				zap.Any("FileOffset", ek.FileOffset), zap.Any("srcVol", sdk.volName), zap.Any("err", err))
			util.Free(buf)
			dstEC.CloseStream(dstInfo.Inode)
			srcEC.CloseStream(srcInfo.Inode)
			return errors.New(fmt.Sprintf("Read FileOffset %d from source %s vol %s [%s]",
				ek.FileOffset, srcPath, sdk.volName, err.Error()))
		}

		if uint32(n) != size {
			logger.Error("Read wrong size", zap.Any("TaskId", taskId), zap.Any("FileOffset", ek.FileOffset), zap.Any("expect size", size),
				zap.Any("actual size", n))
			util.Free(buf)
			dstEC.CloseStream(dstInfo.Inode)
			srcEC.CloseStream(srcInfo.Inode)
			return errors.New(fmt.Sprintf("Read wrong size from source %s, %d[expect %d]",
				srcPath, n, size))
		}
		_, err = dstSdk.write(dstInfo.Inode, dstParentInfo.Inode, int(ek.FileOffset), buf, 0)
		if err != nil {
			util.Free(buf)
			dstEC.CloseStream(dstInfo.Inode)
			srcEC.CloseStream(srcInfo.Inode)
			logger.Error("Write extent failed", zap.Any("TaskId", taskId),
				zap.Any("FileOffset", ek.FileOffset), zap.Any("dstVol", dstSdk.volName), zap.Any("err", err))
			return errors.New(fmt.Sprintf("Write FileOffset %d to  dst %s vol %s [%s]",
				ek.FileOffset, srcPath, dstSdk.volName, err.Error()))
		}
		util.Free(buf)
	}
	//logger.Debug("CopyFileToDir lookup src", zap.Any("TaskId", taskId))
	dstEC.CloseStream(dstInfo.Inode)
	srcEC.CloseStream(srcInfo.Inode)
	//检查文件大小是否一致,这里不能用缓存
	srcInfo, err = sdk.LookupPath(srcPath)
	if err != nil {
		logger.Warn("LookupPath source to check failed", zap.Any("TaskId", taskId), zap.Any("srcVol", sdk.volName), zap.Any("err", err))
		return err
	}
	//logger.Debug("CopyFileToDir lookup dst", zap.Any("TaskId", taskId))
	dstInfo, err = dstSdk.LookupPath(gopath.Join(dstRoot, fileName))
	if err != nil {
		logger.Warn("LookupPath dst to check failed", zap.Any("TaskId", taskId), zap.Any("dstVol", dstSdk.volName), zap.Any("err", err))
	}
	if srcInfo.Size != dstInfo.Size {
		return errors.New(fmt.Sprintf("Copy size not the same %s[%s]", srcPath, gopath.Join(dstRoot, fileName)))
	}
	logger.Debug("Copy success", zap.Any("TaskId", taskId), zap.Any("srcPath", srcPath), zap.Any("srcVol", sdk.volName), zap.Any("dstPath", gopath.Join(dstRoot, fileName)),
		zap.Any("dstVol", dstSdk.volName), zap.Any("size", dstInfo.Size),
		zap.Any("cost", time.Now().Sub(start).String()))
	return nil
}

func (sdk *CubeFSSdk) CopySymlinkToDir(srcPath, dstRoot string, dstSdk *CubeFSSdk) (err error) {
	logger := sdk.logger
	//获取源文件的inode信息。
	srcInfo, err := sdk.LookupPath(srcPath)
	if err != nil {
		logger.Warn("LookupPath source failed", zap.Any("err", err))
		return err
	}
	//获取源文件名
	_, fileName := gopath.Split(gopath.Clean(srcPath))
	//获取目标路径信息
	dstParentInfo, err := dstSdk.LookupPath(dstRoot)
	if err != nil {
		logger.Error("LookupPath target failed", zap.Any("err", err))
		return err
	}
	//创建目标文件
	dstInfo, err := dstSdk.CreateSymlink(dstParentInfo.Inode, fileName, srcInfo.Mode, srcInfo.Uid, srcInfo.Gid, srcInfo.Target)
	if err != nil {
		if err != syscall.EEXIST {
			logger.Error("Create target failed", zap.Any("dstRoot", dstRoot), zap.Any("err", err))
			return errors.New(fmt.Sprintf("Create target failed, dstRoot %s[%s]", dstRoot, err.Error()))
		}
		dstInfo, err = dstSdk.LookupPath(gopath.Join(dstRoot, fileName))
		if err != nil {
			logger.Error("Open target failed", zap.Any("err", err))
			return errors.New(fmt.Sprintf("Open exist target failed, dstRoot %s[%s]", dstRoot, err.Error()))
		}
	}
	if string(dstInfo.Target) != string(srcInfo.Target) {
		err = errors.New(fmt.Sprintf("Src[%v]target[%v]not equal dst[%v]target[%v]",
			srcPath, string(srcInfo.Target), gopath.Join(dstRoot, fileName), string(dstInfo.Target)))
		logger.Error("target not equal", zap.Any("err", err))
		return
	}
	logger.Debug("Copy Symlink success", zap.Any("srcPath", srcPath), zap.Any("srcVol", sdk.volName),
		zap.Any("dstPath", gopath.Join(dstRoot, fileName)), zap.Any("dstVol", dstSdk.volName))
	return nil
}
func (sdk *CubeFSSdk) read(ino uint64, offset int, data []byte) (n int, err error) {
	ec := sdk.ecApi.ec
	n, err = ec.Read(ino, data, offset, len(data))
	if err != nil && err != io.EOF {
		return 0, err
	}
	return n, nil
}

func (sdk *CubeFSSdk) write(ino, pino uint64, offset int, data []byte, flags int) (n int, err error) {
	ec := sdk.ecApi.ec
	ec.GetStreamer(ino).SetParentInode(pino)
	checkFunc := func() error {
		if !sdk.mwApi.mw.EnableQuota {
			return nil
		}

		if ok := sdk.ecApi.ec.UidIsLimited(0); ok {
			return syscall.ENOSPC
		}

		if sdk.mwApi.mw.IsQuotaLimitedById(ino, true, false) {
			return syscall.ENOSPC
		}
		return nil
	}
	n, err = ec.Write(ino, offset, data, flags, checkFunc)
	if err != nil {
		return 0, err
	}
	return n, nil
}
