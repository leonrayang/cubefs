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
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type DataApi struct {
	ec *stream.ExtentClient
}

type EnableDebugMsg func() bool

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

// 仅测试用
func extractNumberFromFileName(fileName string) (int, error) {
	// 提取文件名部分，不包括扩展名
	fileName = strings.TrimSuffix(filepath.Base(fileName), filepath.Ext(fileName))

	// 查找最后一个下划线的位置
	index := strings.LastIndex(fileName, "_")
	if index == -1 {
		return 0, fmt.Errorf("Invalid file name format")
	}

	// 提取下划线后的数字部分
	numberStr := fileName[index+1:]

	// 解析为整数
	number, err := strconv.Atoi(numberStr)
	if err != nil {
		return 0, err
	}

	return number, nil
}

func (sdk *CubeFSSdk) CopyFileToDir(srcPath, dstRoot string, dstSdk *CubeFSSdk, taskId string,
	debugFunc EnableDebugMsg, copyLogger *zap.Logger, overWrite bool, address string) (err error) {
	var (
		srcEC              = sdk.ecApi.ec
		srcMW              = sdk.mwApi.mw
		dstEC              = dstSdk.ecApi.ec
		start              = time.Now()
		logger             = sdk.logger
		eks                []proto.ExtentKey
		srcStreamHasClosed = false
		dstStreamHasClosed = false
	)
	//tmpName := gopath.Base(srcPath)
	//if tmpName == "123.jpg" {
	//	return errors.New(fmt.Sprintf("chi test errors "))
	//}
	//tmpName := gopath.Base(srcPath)
	//index, _ := extractNumberFromFileName(tmpName)
	//if index%10 == 0 {
	//	return errors.New(fmt.Sprintf("chi errors for index %d", index))
	//}
	//获取源文件的inode信息。
	logger.Debug("CopyFileToDir enter", zap.Any("TaskId", taskId), zap.Any("srcPath", srcPath))
	//这里查找文件应该缓存父目录的元数据
	//	stepStart := time.Now()
	srcInfo, err := sdk.LookupFileWithParentCache(srcPath)
	if err != nil {
		logger.Warn("LookupPath source failed", zap.Any("TaskId", taskId), zap.Any("srcVol", sdk.volName), zap.Any("err", err))
		return err
	}

	srcEC.OpenStream(srcInfo.Inode)
	defer func() {
		if !srcStreamHasClosed {
			srcEC.CloseStream(srcInfo.Inode)
			srcEC.EvictStream(srcInfo.Inode)
		}
	}()
	//if debugFunc() {
	//	logger.Debug("OpenStream src", zap.Any("srcPath", srcPath), zap.Any("srcIno", srcInfo.Inode),
	//		zap.Any("cost", time.Now().Sub(stepStart).String()))
	//}
	//logger.Debug("CopyFileToDir IsSymlink", zap.Any("TaskId", taskId))
	//获取源文件名
	_, fileName := gopath.Split(gopath.Clean(srcPath))
	//如果是软连接
	if proto.IsSymlink(srcInfo.Mode) {
		return sdk.CopySymlinkToDir(srcInfo, srcPath, dstRoot, dstSdk, debugFunc, taskId, copyLogger, overWrite, address)
	}
	//	stepStart = time.Now()

	//获取目标路径信息
	//logger.Debug("CopyFileToDir lookup dst", zap.Any("TaskId", taskId))
	dstParentInfo, err := dstSdk.LookupPathWithCache(dstRoot)
	if err != nil {
		logger.Error("LookupPathWithCache target failed", zap.Any("TaskId", taskId), zap.Any("dstVol", dstSdk.volName), zap.Any("err", err))
		return err
	}
	//logger.Debug("CopyFileToDir PathIsExist", zap.Any("TaskId", taskId))
	var dstInfo *proto.InodeInfo
	//如果目标文件已经存在，则要删除

	if ino, isExist := dstSdk.PathIsExistWithIno(dstParentInfo.Inode, gopath.Join(dstRoot, fileName)); isExist {
		//logger.Debug("CopyFileToDir open exist", zap.Any("TaskId", taskId))
		//如果目标文件已经存在，则判断modifyTime是否一致，一致则忽略，不一致则告警
		dstInfo, err = dstSdk.getInodeInfo(ino)
		if err != nil {
			logger.Error("Open target failed", zap.Any("TaskId", taskId), zap.Any("dstVol", dstSdk.volName), zap.Any("err", err))
			return errors.New(fmt.Sprintf("Open exist target failed, dstRoot %s vol %s [%s]", dstRoot, dstSdk.volName, err.Error()))
		}
		if overWrite == false {
			if dstInfo.ModifyTime == srcInfo.ModifyTime {
				return nil
			} else {
				//记录到task的日志
				if copyLogger != nil {
					copyLogger.Debug("modify time not the same,please check", zap.Any("TaskId", taskId), zap.Any("fileName", srcPath))
				}

				return errors.New(fmt.Sprintf("modify time not the same,please check file list %v on %v",
					taskId, address))
			}
		} else {
			dstEC.OpenStream(dstInfo.Inode)
			defer func() {
				if !dstStreamHasClosed {
					dstEC.CloseStream(dstInfo.Inode)
					dstEC.EvictStream(dstInfo.Inode)
				}
			}()
			//logger.Debug("CopyFileToDir DeleteFile", zap.Any("TaskId", taskId))
			if err = dstSdk.Truncate(dstParentInfo.Inode, ino); err != nil {
				logger.Error("Delete exist target failed", zap.Any("TaskId", taskId), zap.Any("dstVol", dstSdk.volName),
					zap.Any("dstFile", gopath.Join(dstRoot, fileName)), zap.Any("err", err),
					zap.Any("dstParentInfo.Inode", dstParentInfo.Inode), zap.Any("ino", ino))
				return err
			}
			//logger.Error("delete old file success", zap.Any("file", gopath.Join(dstRoot, fileName)))
		}
	} else {
		//创建目标文件
		dstInfo, err = dstSdk.CreateFile(dstParentInfo.Inode, fileName, srcInfo.Mode, srcInfo.Uid, srcInfo.Gid)
		if err != nil {
			logger.Error("Create target failed", zap.Any("TaskId", taskId), zap.Any("dstRoot", dstRoot), zap.Any("dstVol", dstSdk.volName), zap.Any("err", err))
			return errors.New(fmt.Sprintf("Create target failed, dstRoot %s vol %s[%s]", dstRoot, dstSdk.volName, err.Error()))
		}
		dstEC.OpenStream(dstInfo.Inode)
		defer func() {
			if !dstStreamHasClosed {
				dstEC.CloseStream(dstInfo.Inode)
				dstEC.EvictStream(dstInfo.Inode)
			}
		}()
		//logger.Error("create new file success", zap.Any("file", gopath.Join(dstRoot, fileName)))
	}
	//logger.Debug("CopyFileToDir GetExtents", zap.Any("TaskId", taskId))
	//
	//if debugFunc() {
	//	logger.Debug("OpenStream dst", zap.Any("dstPath", gopath.Join(dstRoot, fileName)), zap.Any("dstIno", dstInfo.Inode),
	//		zap.Any("cost", time.Now().Sub(stepStart).String()), zap.Any("from start", time.Now().Sub(start).String()))
	//}
	//stepStart = time.Now()
	_, _, eks, err = srcMW.GetExtents(srcInfo.Inode)
	//if debugFunc() {
	//	logger.Debug("GetExtents", zap.Any("srcPath", srcPath), zap.Any("srcIno", srcInfo.Inode),
	//		zap.Any("cost", time.Now().Sub(stepStart).String()), zap.Any("from start", time.Now().Sub(start).String()))
	//}
	if err != nil {
		logger.Error("GetExtents for source failed", zap.Any("TaskId", taskId), zap.Any("srcPath", srcPath), zap.Any("srcVol", sdk.volName), zap.Any("err", err))
		return errors.New(fmt.Sprintf("GetExtents for source failed %s vol %s[%s]", srcPath, sdk.volName, err.Error()))
	}
	//stepStart = time.Now()
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
			return errors.New(fmt.Sprintf("Read FileOffset %d from source %s vol %s [%s]",
				ek.FileOffset, srcPath, sdk.volName, err.Error()))
		}

		if uint32(n) != size {
			logger.Error("Read wrong size", zap.Any("TaskId", taskId), zap.Any("FileOffset", ek.FileOffset), zap.Any("expect size", size),
				zap.Any("actual size", n))
			util.Free(buf)
			return errors.New(fmt.Sprintf("Read wrong size from source %s, %d[expect %d]",
				srcPath, n, size))
		}
		_, err = dstSdk.write(dstInfo.Inode, dstParentInfo.Inode, int(ek.FileOffset), buf, 0)
		if err != nil {
			util.Free(buf)
			logger.Error("Write extent failed", zap.Any("TaskId", taskId),
				zap.Any("FileOffset", ek.FileOffset), zap.Any("dstVol", dstSdk.volName), zap.Any("err", err))
			return errors.New(fmt.Sprintf("Write FileOffset %d to  dst %s vol %s [%s]",
				ek.FileOffset, srcPath, dstSdk.volName, err.Error()))
		}
		util.Free(buf)
	}
	//if debugFunc() {
	//	logger.Debug("CopyEks", zap.Any("srcPath", srcPath), zap.Any("srcIno", srcInfo.Inode),
	//		zap.Any("cost", time.Now().Sub(stepStart).String()), zap.Any("from start", time.Now().Sub(start).String()))
	//}
	//stepStart = time.Now()
	//logger.Debug("CopyFileToDir lookup src", zap.Any("TaskId", taskId))
	//close traverse 不然会定时器泄漏
	dstEC.CloseStream(dstInfo.Inode)
	dstEC.EvictStream(dstInfo.Inode)
	srcEC.CloseStream(srcInfo.Inode)
	srcEC.EvictStream(srcInfo.Inode)
	srcStreamHasClosed = true
	dstStreamHasClosed = true
	//if debugFunc() {
	//	logger.Debug("CloseStream", zap.Any("srcPath", srcPath), zap.Any("srcIno", srcInfo.Inode),
	//		zap.Any("cost", time.Now().Sub(stepStart).String()), zap.Any("from start", time.Now().Sub(start).String()))
	//	logger.Debug("CloseStream", zap.Any("dstPath", gopath.Join(dstRoot, fileName)), zap.Any("dstIno", dstInfo.Inode),
	//		zap.Any("cost", time.Now().Sub(stepStart).String()), zap.Any("from start", time.Now().Sub(start).String()))
	//}
	//stepStart = time.Now()
	//检查文件大小是否一致,这里不能用缓存
	srcInfo2, err := sdk.LookupFileWithParentCache(srcPath)
	if err != nil {
		logger.Warn("LookupPath source to check failed", zap.Any("TaskId", taskId), zap.Any("srcVol", sdk.volName), zap.Any("err", err))
		return err
	}
	//if debugFunc() {
	//	logger.Debug("LookupFileWithParentCache src", zap.Any("srcPath", srcPath), zap.Any("srcIno", srcInfo2.Inode),
	//		zap.Any("cost", time.Now().Sub(stepStart).String()), zap.Any("from start", time.Now().Sub(start).String()))
	//}
	//stepStart = time.Now()
	//logger.Debug("CopyFileToDir lookup dst", zap.Any("TaskId", taskId))
	dstInfo2, err := dstSdk.LookupFileWithParentCache(gopath.Join(dstRoot, fileName))
	if err != nil {
		logger.Warn("LookupPath dst to check failed", zap.Any("TaskId", taskId), zap.Any("dstVol", dstSdk.volName), zap.Any("err", err))
		return err
	}
	//if debugFunc() {
	//	logger.Debug("LookupFileWithParentCache dst", zap.Any("dstPath", gopath.Join(dstRoot, fileName)), zap.Any("dstIno", dstInfo2.Inode),
	//		zap.Any("cost", time.Now().Sub(stepStart).String()), zap.Any("from start", time.Now().Sub(start).String()))
	//}
	if srcInfo2.Size != dstInfo2.Size {
		return errors.New(fmt.Sprintf("Copy size not the same src[%s:%v]   dst[%s:%v]", srcPath, srcInfo2.Size,
			gopath.Join(dstRoot, fileName), dstInfo2.Size))
	}
	//优化打开
	//if debugFunc() {
	if debugFunc() {
		logger.Debug("Copy success", zap.Any("TaskId", taskId), zap.Any("srcPath", srcPath), zap.Any("srcVol", sdk.volName), zap.Any("dstPath", gopath.Join(dstRoot, fileName)),
			zap.Any("dstVol", dstSdk.volName), zap.Any("size", dstInfo.Size),
			zap.Any("cost", time.Now().Sub(start).String()))
	}
	return nil
}

func (sdk *CubeFSSdk) CopySymlinkToDir(srcInfo *proto.InodeInfo, srcPath, dstRoot string, dstSdk *CubeFSSdk,
	debugFunc EnableDebugMsg, taskId string, copyLogger *zap.Logger, overWrite bool, address string) (err error) {
	logger := sdk.logger
	var (
		dstInfo *proto.InodeInfo
		start   = time.Now()
		//stepStart = time.Now()
	)
	_, fileName := gopath.Split(gopath.Clean(srcPath))
	//setpStart = time.Now()
	dstParentInfo, err := dstSdk.LookupPathWithCache(dstRoot)
	if err != nil {
		logger.Error("LookupPathWithCache target failed", zap.Any("TaskId", taskId), zap.Any("dstVol", dstSdk.volName), zap.Any("err", err))
		return err
	}
	//logger.Error("CopySymlinkToDir LookupPathWithCache ", zap.Any("cost", zap.Any("cost", time.Now().Sub(setpStart).String())))
	//logger.Debug("CopySymlinkToDir", zap.Any("srcPath", srcPath))

	//如果目标文件已经存在，则要删除
	if ino, isExist := dstSdk.PathIsExistWithIno(dstParentInfo.Inode, gopath.Join(dstRoot, fileName)); isExist {
		//logger.Debug("CopyFileToDir open exist", zap.Any("TaskId", taskId))
		//如果目标文件已经存在，则判断modifyTime是否一致，一致则忽略，不一致则告警
		dstInfo, err = dstSdk.getInodeInfo(ino)
		if err != nil {
			logger.Error("Open target failed", zap.Any("TaskId", taskId), zap.Any("dstVol", dstSdk.volName), zap.Any("err", err))
			return errors.New(fmt.Sprintf("Open exist target failed, dstRoot %s vol %s [%s]", dstRoot, dstSdk.volName, err.Error()))
		}
		if overWrite == false {
			if dstInfo.ModifyTime == srcInfo.ModifyTime {
				return nil
			} else {
				//记录到task的日志
				if copyLogger != nil {
					copyLogger.Debug("modify time not the same,please check", zap.Any("TaskId", taskId), zap.Any("fileName", fileName))
				}

				return errors.New(fmt.Sprintf("modify time not the same,please check file list %v on %v",
					taskId, address))
			}
		} else {
			//setpStart = time.Now()
			if err = dstSdk.DeleteFile(dstParentInfo.Inode, fileName); err != nil {
				logger.Error("Delete exist target failed", zap.Any("TaskId", taskId), zap.Any("dstVol", dstSdk.volName),
					zap.Any("dstFile", gopath.Join(dstRoot, fileName)), zap.Any("err", err),
					zap.Any("dstParentInfo.Inode", dstParentInfo.Inode), zap.Any("ino", ino))
				return err
			}
			//logger.Error("CopySymlinkToDir DeleteFile ", zap.Any("cost", zap.Any("cost", time.Now().Sub(setpStart).String())))
			//dstEC.OpenStream(dstInfo.Inode)
			//defer func() {
			//	dstEC.CloseStream(dstInfo.Inode)
			//	dstEC.EvictStream(dstInfo.Inode)
			//}()
			////logger.Debug("CopyFileToDir DeleteFile", zap.Any("TaskId", taskId))
			//if err = dstSdk.Truncate(dstParentInfo.Inode, ino); err != nil {
			//	logger.Error("Delete exist target failed", zap.Any("TaskId", taskId), zap.Any("dstVol", dstSdk.volName),
			//		zap.Any("dstFile", gopath.Join(dstRoot, fileName)), zap.Any("err", err),
			//		zap.Any("dstParentInfo.Inode", dstParentInfo.Inode), zap.Any("ino", ino))
			//	return err
			//}
			//logger.Error("delete old file success", zap.Any("file", gopath.Join(dstRoot, fileName)))
		}
	}

	//创建目标文件
	//setpStart = time.Now()
	dstInfo, err = dstSdk.CreateSymlink(dstParentInfo.Inode, fileName, srcInfo.Mode, srcInfo.Uid, srcInfo.Gid, srcInfo.Target)
	//logger.Error("CopySymlinkToDir CreateSymlink ", zap.Any("cost", zap.Any("cost", time.Now().Sub(setpStart).String())))
	if err != nil {
		logger.Error("CreateSymlink failed", zap.Any("err", err), zap.Any("TaskId", taskId))
		return errors.New(fmt.Sprintf("CreateSymlink failed, dstRoot %s[%s]", dstRoot, err.Error()))
	}
	if string(dstInfo.Target) != string(srcInfo.Target) {
		err = errors.New(fmt.Sprintf("Src[%v]target[%v]not equal dst[%v]target[%v]",
			srcPath, string(srcInfo.Target), gopath.Join(dstRoot, fileName), string(dstInfo.Target)))
		logger.Error("target not equal", zap.Any("err", err), zap.Any("TaskId", taskId))
		return
	}
	//if debugFunc() {
	if true {
		logger.Debug("Copy Symlink success", zap.Any("srcPath", srcPath), zap.Any("srcVol", sdk.volName),
			zap.Any("dstPath", gopath.Join(dstRoot, fileName)), zap.Any("dstVol", dstSdk.volName),
			zap.Any("cost", time.Now().Sub(start).String()), zap.Any("TaskId", taskId))
	}

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
	streamer := ec.GetStreamer(ino)
	if streamer == nil {
		return 0, errors.New(fmt.Sprintf("Streamer for ino %v is nil", ino))
	} else {
		streamer.SetParentInode(pino)
	}
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

func (sdk *CubeFSSdk) GetStreamerLen() (inoArray []uint64, total int) {
	return sdk.ecApi.ec.GetStreamerLen()
}
