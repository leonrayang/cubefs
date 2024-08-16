package cp

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"syscall"

	"bazil.org/fuse"
	cfs "github.com/chubaofs/chubaofs/client/fs"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/sdk/meta"
	clog "github.com/chubaofs/chubaofs/util/log"
)

type FsApi interface {
	readDir(dir string, parentIno uint64) (dirItmes []DirItem, err error)
	statFile(filePath string, parentIno uint64) (info *syscall.Stat_t, err error)
	statByInode(inode uint64) (info *syscall.Stat_t, err error)
	openFile(filePath string, create bool, parentIno uint64) (fd FdApi, err error)
	getIno(filePath string, parentIno uint64) (ino uint64, err error)
	getParentInoByPath(filePath string) (ino uint64, err error)
	symlink(oldname, newname string, parentIno uint64) error
	readlink(name string, parentIno uint64) (string, error)
	mkdir(dir string, parentIno uint64) error
	mkdirall(dir string) error
	updateStat(dir string, stat *syscall.Stat_t, parentIno uint64) error
	delete(filepath string, parentIno uint64, isDir bool) error
}

type DirItem struct {
	Mode fs.FileMode
	Name string
	Ino  uint64
}

func (d *DirItem) isDir() bool {
	return d.Mode.IsDir()
}

// func (d *DirItem) isSymlink() bool {
// 	return d.Mode&fs.ModeSymlink != 0
// }

// func (d *DirItem) IsRegular() bool {
// 	return d.Mode.IsRegular()
// }

type FsType int

const (
	OsTyp     FsType = 0
	CubeFsTyp FsType = 1
)

func checkPermission(opt *proto.MountOptions) (err error) {
	var mc = master.NewMasterClientFromString(opt.Master, false)

	// Check user access policy is enabled
	if opt.AccessKey != "" {
		var userInfo *proto.UserInfo
		if userInfo, err = mc.UserAPI().GetAKInfo(opt.AccessKey); err != nil {
			return
		}
		if userInfo.SecretKey != opt.SecretKey {
			err = proto.ErrNoPermission
			return
		}
		var policy = userInfo.Policy
		if policy.IsOwn(opt.Volname) {
			return
		}
		if policy.IsAuthorized(opt.Volname, opt.SubDir, proto.POSIXWriteAction) &&
			policy.IsAuthorized(opt.Volname, opt.SubDir, proto.POSIXReadAction) {
			return
		}
		if policy.IsAuthorized(opt.Volname, opt.SubDir, proto.POSIXReadAction) &&
			!policy.IsAuthorized(opt.Volname, opt.SubDir, proto.POSIXWriteAction) {
			opt.Rdonly = true
			return
		}
		err = proto.ErrNoPermission
		return
	}
	return
}

func initFs(cfg *pathCfg) (api FsApi) {
	if cfg.tp == OsTyp {
		return &OsFs{}
	}

	opt := cfg.option
	err := checkPermission(opt)
	if err != nil {
		log.Fatalf("check cubefs perm failed, check your ak&sk&owner err %s", err.Error())
	}

	super, err := cfs.NewSuper(opt)
	if err != nil {
		log.Fatalf("config illegal, new super failed, err %s", err.Error())
	}

	var masters = strings.Split(opt.Master, meta.HostsSeparator)
	var metaConfig = &meta.MetaConfig{
		Volume:        opt.Volname,
		Owner:         opt.Owner,
		Masters:       masters,
		Authenticate:  opt.Authenticate,
		ValidateOwner: opt.Authenticate || opt.AccessKey == "",
	}
	mw, err := meta.NewMetaWrapper(metaConfig)
	if err != nil {
		log.Fatalf("config illegal, new metaWraaper failed, err %s, cfg %v", err.Error(), *metaConfig)
	}

	cf := &CubeFs{
		super: super,
		mw:    mw,
	}

	return cf
}

type OsFs struct {
}

func (f *OsFs) mkdirall(dir string) error {
	err := os.MkdirAll(dir, 0755)
	if err != os.ErrExist {
		return err
	}

	return nil
}

func (f *OsFs) getParentInoByPath(filePath string) (ino uint64, err error) {
	dir, _ := path.Split(filePath)
	if dir == "" || dir == "/" {
		return
	}

	stat, err := f.statFile(dir, 0)
	if err == nil {
		return stat.Ino, err
	}

	return 0, err
}

func (f *OsFs) delete(filepath string, parentIno uint64, isDir bool) error {
	err := os.Remove(filepath)
	clog.LogWarnf("finish delete file, path %s, err %v", filepath, err)
	return err
}

func (f *OsFs) statByInode(inode uint64) (info *syscall.Stat_t, err error) {
	info = &syscall.Stat_t{}
	info.Mode = 0777
	return
}

func (f *OsFs) readlink(name string, parentIno uint64) (string, error) {
	return os.Readlink(name)
}

func (f *OsFs) updateStat(destDir string, stat *syscall.Stat_t, parentIno uint64) error {
	mode := fileMode(stat.Mode)
	if mode&os.ModeSymlink != 0 { // link do nothing
		return nil
	}

	err := syscall.Chmod(destDir, stat.Mode)
	if err != nil {
		return err
	}

	// os.Chown()
	syscall.Chown(destDir, int(stat.Uid), int(stat.Gid))
	// if err != nil {
	// 	return err
	// }

	err = syscall.UtimesNano(destDir, []syscall.Timespec{stat.Atim, stat.Mtim})
	if err != nil {
		return err
	}

	return nil
}

func (f *OsFs) mkdir(dir string, parentIno uint64) error {
	info, err := os.Lstat(dir)
	if err == nil {
		if !info.IsDir() {
			log.Fatalf("target file is already exist, but not dir, %s", dir)
		}

		return nil
	}

	return os.Mkdir(dir, 0755)
}

func (f *OsFs) symlink(oldname, newname string, parentIno uint64) error {
	err := os.Symlink(oldname, newname)
	if os.IsExist(err) {
		// link same same
		oldLink, err := f.readlink(newname, parentIno)
		if err != nil {
			return err
		}

		if oldLink != oldname {
			return fmt.Errorf("dest path %s is exist, with link %s", newname, oldLink)
		}

		return nil
	}

	return err
}

func (f *OsFs) readDir(dir string, parentIno uint64) (dirItmes []DirItem, err error) {
	entrys, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	dirItmes = make([]DirItem, 0, len(entrys))
	for _, ent := range entrys {
		dirItmes = append(dirItmes, DirItem{
			Mode: ent.Type(),
			Name: ent.Name(),
		})
	}

	return
}

func (f *OsFs) statFile(filePath string, parentIno uint64) (stat *syscall.Stat_t, err error) {
	info, err := os.Lstat(filePath)
	if err != nil {
		pathErr, ok := err.(*os.PathError)
		if ok && pathErr.Err == syscall.ENOENT {
			return nil, os.ErrNotExist
		}

		return nil, err
	}

	stat = info.Sys().(*syscall.Stat_t)
	return
}

func (f *OsFs) getIno(filePath string, parentIno uint64) (ino uint64, err error) {
	stat, err := f.statFile(filePath, parentIno)
	if err != nil {
		return
	}

	return stat.Ino, err
}

func (f *OsFs) openFile(filePath string, create bool, parentIno uint64) (fd FdApi, err error) {
	if !create {
		return os.Open(filePath)
	}

	return os.Create(filePath)
}

type CubeFs struct {
	super *cfs.Super
	mw    *meta.MetaWrapper
}

func (f *CubeFs) mkdirall(dirPath string) error {
	var err error
	dirPath = path.Clean(dirPath)
	if isRootDIr(dirPath) {
		return err
	}

	user := getUser()
	uid, _ := strconv.ParseInt(user.Uid, 10, 64)
	gid, _ := strconv.ParseInt(user.Gid, 10, 64)
	log.Printf("uid %d gid %d\n", uid, gid)

	ino := proto.RootIno
	dirs := strings.Split(dirPath, "/")
	var stat, parentStat *syscall.Stat_t

	for _, dir := range dirs {
		if dir == "/" || dir == "" {
			continue
		}

		parentStat, err = f.getInodeStat(ino)
		if err != nil {
			return err
		}

		err = checkMode(parentStat, read)
		if err != nil {
			return err
		}

		stat, err = f.statFile(dir, ino)
		if err == nil {
			newMode := fileMode(stat.Mode)
			if !newMode.IsDir() {
				return fmt.Errorf("target %s is exist and not directory", dir)
			}

			ino = stat.Ino
			continue
		}

		if err != os.ErrNotExist {
			return err
		}

		err = checkMode(parentStat, write)
		if err != nil {
			return err
		}

		err = f.mkdir(dir, ino)
		if err != nil {
			return err
		}

		newIno, err := f.getIno(dir, ino)
		if err != nil {
			return err
		}

		valid := proto.AttrUid | proto.AttrGid
		err = f.mw.Setattr(newIno, valid, 0755, uint32(uid), uint32(gid), 0, 0)
		if err != nil {
			return err
		}

		ino = newIno
	}
	return nil
}

func (f *CubeFs) readlink(name string, parentIno uint64) (string, error) {
	ino, err := f.getIno(name, parentIno)
	if err != nil {
		return "", err
	}

	fd := cfs.NewFile(f.super, &proto.InodeInfo{Inode: ino}, parentIno).(*cfs.File)
	target, err := fd.Readlink(ctx, &fuse.ReadlinkRequest{})

	return target, err
}

func (f *CubeFs) delete(filepath string, parentIno uint64, isDir bool) error {
	_, name := path.Split(filepath)
	info, err := f.mw.Delete_ll(parentIno, name, isDir)
	if err != nil {
		return err
	}

	f.mw.Evict(info.Inode)
	return err
}

func (f *CubeFs) statByInode(inode uint64) (info *syscall.Stat_t, err error) {
	inodeInfo, err := f.super.InodeGet(inode)
	if err != nil {
		return info, err
	}

	return buildStatFromInode(inodeInfo), nil
}

func fileMode(unixMode uint32) os.FileMode {
	mode := os.FileMode(unixMode & 0777)
	switch unixMode & syscall.S_IFMT {
	case syscall.S_IFREG:
		// nothing
	case syscall.S_IFDIR:
		mode |= os.ModeDir
	case syscall.S_IFLNK:
		mode |= os.ModeSymlink
	default:
		// no idea
		mode |= os.ModeDevice
	}

	if unixMode&syscall.S_ISUID != 0 {
		mode |= os.ModeSetuid
	}
	if unixMode&syscall.S_ISGID != 0 {
		mode |= os.ModeSetgid
	}

	return mode
}

type modeOp int

const (
	read = iota
	write
)

func checkMode(stat *syscall.Stat_t, op modeOp) error {
	modeS := fileMode(stat.Mode).String()
	mode := modeS[len(modeS)-9:]

	check := func(m string, op modeOp) bool {
		if op == read {
			return m[0] == 'r' && m[2] == 'x'
		}

		return m == "rwx"
	}

	user := getUser()
	if user.Uid == "0" {
		return nil
	}

	uidS := fmt.Sprintf("%d", stat.Uid)
	if user.Uid == uidS && check(mode[:3], op) {
		return nil
	}

	gidS := fmt.Sprintf("%d", stat.Gid)
	checkGrp := func(gid string) bool {
		if gid == gidS && check(mode[3:6], op) {
			return true
		}
		return false
	}

	if checkGrp(user.Gid) {
		return nil
	}

	for _, gid := range user.groupIds {
		if checkGrp(gid) {
			return nil
		}
	}

	if check(mode[6:], op) {
		return nil
	}

	return fmt.Errorf("permission not allowed")
}

func getUint32(id string) uint32 {
	uid, _ := strconv.Atoi(id)
	return uint32(uid)
}

func (f *CubeFs) updateStat(dir string, srcStat *syscall.Stat_t, parentIno uint64) error {
	clog.LogDebugf("start cfs.updateStat, dir %s", dir)
	if isRootDIr(dir) {
		return nil
	}

	valid := proto.AttrMode | proto.AttrUid | proto.AttrGid | proto.AttrAccessTime | proto.AttrModifyTime
	ino, err := f.getIno(dir, parentIno)
	if err != nil {
		return err
	}

	// in case dest dir can't visist
	uid := getUint32(u.Uid)
	gid := getUint32(u.Gid)
	if uid == 0 {
		uid = srcStat.Uid
	}
	if gid == 0 {
		gid = srcStat.Gid
	}

	mode := fileMode(srcStat.Mode)
	err = f.mw.Setattr(ino, valid, proto.Mode(mode), uid, gid, srcStat.Atim.Sec, srcStat.Mtim.Sec)
	if err != nil {
		return err
	}

	return nil
}

func isRootDIr(dir string) bool {
	return dir == "/"
}

func (f *CubeFs) mkdir(dir string, parentIno uint64) (err error) {
	clog.LogDebugf("start cubefs.mkdir, dir %s", dir)
	if isRootDIr(dir) {
		return
	}

	stat, err := f.statFile(dir, parentIno)
	if err == nil {
		newMode := fileMode(stat.Mode)
		if !newMode.IsDir() {
			return fmt.Errorf("target %s is exist and not directory", dir)
		}

		return
	}

	if err != nil && err != os.ErrNotExist {
		return err
	}

	_, filename := path.Split(dir)
	dirf := cfs.NewDir(f.super, &proto.InodeInfo{Inode: parentIno}).(*cfs.Dir)
	_, err = dirf.Mkdir(ctx, &fuse.MkdirRequest{Name: filename, Mode: 0755})
	return err
}

func (f *CubeFs) symlink(oldname, newname string, parentIno uint64) error {
	if isRootDIr(newname) {
		return nil
	}

	_, filename := path.Split(newname)
	dirf := cfs.NewDir(f.super, &proto.InodeInfo{Inode: parentIno}).(*cfs.Dir)
	_, err := dirf.Symlink(ctx, &fuse.SymlinkRequest{NewName: filename, Target: oldname})

	if err == fuse.EEXIST {
		// link same
		oldLink, err := f.readlink(newname, parentIno)
		if err != nil {
			return err
		}

		if oldLink != oldname {
			return fmt.Errorf("dest path %s is exist, with link %s", newname, oldLink)
		}

		return nil
	}

	return err
}

func getModeTyp(typ uint32) fs.FileMode {
	if proto.IsDir(typ) {
		return fs.ModeDir
	}

	if proto.IsSymlink(typ) {
		return fs.ModeSymlink
	}

	return 0
}

func (f *CubeFs) readDir(dir string, ino uint64) (dirItmes []DirItem, err error) {
	clog.LogDebugf("start cubefs.readDir,dir %s, ino %d", dir, ino)
	entrys, err := f.mw.ReadDir_ll(ino)
	if err != nil {
		return
	}

	dirItmes = make([]DirItem, 0, len(entrys))
	for _, ent := range entrys {

		dirItmes = append(dirItmes, DirItem{
			Mode: getModeTyp(ent.Type),
			Name: ent.Name,
			Ino:  ent.Inode,
		})
	}

	return
}

func buildStatFromInode(inode *proto.InodeInfo) (stat *syscall.Stat_t) {
	mode := proto.OsMode(inode.Mode)

	newMode := uint32(mode) & 0777
	switch {
	default:
		newMode |= syscall.S_IFREG
	case mode&os.ModeDir != 0:
		newMode |= syscall.S_IFDIR
	case mode&os.ModeSymlink != 0:
		newMode |= syscall.S_IFLNK
	}

	if mode&os.ModeSetuid != 0 {
		newMode |= syscall.S_ISUID
	}
	if mode&os.ModeSetgid != 0 {
		newMode |= syscall.S_ISGID
	}

	stat = &syscall.Stat_t{
		Ino:   inode.Inode,
		Nlink: uint64(inode.Nlink),
		Mode:  newMode,
		Uid:   inode.Uid,
		Gid:   inode.Gid,
		Size:  int64(inode.Size),
		Atim:  syscall.Timespec{Sec: inode.AccessTime.Unix(), Nsec: int64(inode.AccessTime.Nanosecond())},
		Mtim:  syscall.Timespec{Sec: inode.ModifyTime.Unix(), Nsec: int64(inode.ModifyTime.Nanosecond())},
		Ctim:  syscall.Timespec{Sec: inode.CreateTime.Unix(), Nsec: int64(inode.CreateTime.Nanosecond())},
	}

	if proto.IsSymlink(inode.Mode) {
		stat.Size = int64(len(inode.Target))
	}

	return
}

func (f *CubeFs) statFile(filePath string, parentIno uint64) (stat *syscall.Stat_t, err error) {
	clog.LogDebugf("start cubefs.statFile, path %s", filePath)
	defer func() {
		if err != nil {
			err = cfs.ParseError(err)
			if err == fuse.ENOENT {
				err = os.ErrNotExist
			}
		}
	}()

	inode := parentIno
	if !isRootDIr(filePath) {
		_, filename := path.Split(filePath)
		inode, _, err = f.mw.Lookup_ll(parentIno, filename)
		if err != nil {
			return
		}
	}

	info, err := f.super.InodeGet(inode)
	if err != nil {
		return
	}

	stat = buildStatFromInode(info)
	return
}

func (f *CubeFs) getInodeStat(inode uint64) (stat *syscall.Stat_t, err error) {
	info, err := f.super.InodeGet(inode)
	if err != nil {
		return
	}

	stat = buildStatFromInode(info)
	return
}

func (f *CubeFs) getIno(filePath string, parentIno uint64) (ino uint64, err error) {
	clog.LogDebugf("start cubefs.getIno, path %s", filePath)

	if isRootDIr(filePath) {
		return proto.RootIno, nil
	}

	_, filename := path.Split(filePath)
	inode, _, err := f.mw.Lookup_ll(parentIno, filename)
	if err != nil {
		return
	}

	return inode, err
}

func (f *CubeFs) getParentInoByPath(filePath string) (ino uint64, err error) {
	clog.LogDebugf("start cubefs.getInoByPath, path %s", filePath)
	dir, _ := path.Split(filePath)
	if isRootDIr(dir) {
		return proto.RootIno, nil
	}

	var stat, parentStat *syscall.Stat_t

	ino = proto.RootIno
	dirs := strings.Split(dir, "/")
	for _, dir := range dirs {
		if dir == "/" || dir == "" {
			continue
		}

		parentStat, err = f.getInodeStat(ino)
		if err != nil {
			return 0, err
		}

		err = checkMode(parentStat, read)
		if err != nil {
			return 0, err
		}

		stat, err = f.statFile(dir, ino)
		if err == nil {
			newMode := fileMode(stat.Mode)
			if !newMode.IsDir() {
				return 0, fmt.Errorf("target %s is exist and not directory", dir)
			}

			ino = stat.Ino
			continue
		}

		return 0, err

		// if err != os.ErrNotExist {
		// 	return 0, err
		// }

		// err = checkMode(parentStat, write)
		// if err != nil {
		// 	return 0, err
		// }

		// err = f.mkdir(dir, ino)
		// if err != nil {
		// 	return
		// }

		// newIno, err := f.getIno(dir, ino)
		// if err != nil {
		// 	return 0, err
		// }

		// ino = newIno
	}

	return ino, nil
}

func (f *CubeFs) openFile(filePath string, create bool, parentIno uint64) (fd FdApi, err error) {
	clog.LogDebugf("start cubefs.openFile, path %s", filePath)
	_, fileName := path.Split(filePath)
	if !create {
		inode, _, err := f.mw.Lookup_ll(parentIno, fileName)
		if err != nil {
			return nil, err
		}

		file := cfs.NewFile(f.super, &proto.InodeInfo{Inode: inode}, parentIno).(*cfs.File)
		req := &fuse.OpenRequest{}
		file.Open(ctx, req, &fuse.OpenResponse{})
		return &CubeFd{fd: file}, err
	}

	dirf := cfs.NewDir(f.super, &proto.InodeInfo{Inode: parentIno}).(*cfs.Dir)
	child, _, err := dirf.Create(ctx, &fuse.CreateRequest{Name: fileName, Mode: 0777},
		&fuse.CreateResponse{})
	if err != nil && err != fuse.EEXIST {
		return
	}

	if err == nil {
		file := child.(*cfs.File)
		return &CubeFd{fd: file}, err
	}

	// err == fuse.EEXIST
	// truncate exist file firse
	ino, err := f.getIno(filePath, parentIno)
	if err != nil {
		return
	}

	err = f.mw.Truncate(ino, 0)
	if err != nil {
		return
	}

	return f.openFile(filePath, false, parentIno)

}

type FdApi interface {
	WriteAt(b []byte, offset int64) (n int, err error)
	ReadAt(b []byte, offset int64) (n int, err error)
	Close() (err error)
}

type CubeFd struct {
	fd *cfs.File
}

func (fd *CubeFd) Close() (err error) {
	clog.LogDebugf("start cubefs.Close")
	req := &fuse.ReleaseRequest{}
	return fd.fd.Release(ctx, req)
}

func (fd *CubeFd) ReadAt(b []byte, offset int64) (n int, err error) {
	clog.LogDebugf("start cubefs.ReadAt, offset %d", offset)
	req := &fuse.ReadRequest{
		Offset: offset,
		Size:   len(b),
	}

	resp := &fuse.ReadResponse{
		Data: make([]byte, len(b)+fuse.OutHeaderSize),
	}

	err = fd.fd.Read(ctx, req, resp)
	if err != nil {
		return
	}

	copy(b, resp.Data[fuse.OutHeaderSize:])

	return len(resp.Data) - fuse.OutHeaderSize, err
}

func (fd *CubeFd) WriteAt(b []byte, offset int64) (n int, err error) {
	clog.LogDebugf("start cubefs.WriteAt")
	req := fuse.WriteRequest{
		Data:   b,
		Offset: offset,
		Flags:  fuse.WriteFlags(fuse.OpenAppend),
	}
	resp := fuse.WriteResponse{}

	err = fd.fd.Write(ctx, &req, &resp)
	if err != nil {
		return
	}

	return resp.Size, err
}

var ctx = context.Background()
