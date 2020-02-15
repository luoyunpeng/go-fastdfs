package model

import (
	"fmt"
	"os"
	"runtime/debug"

	"github.com/luoyunpeng/go-fastdfs/internal/config"
	"github.com/luoyunpeng/go-fastdfs/pkg"
	log "github.com/sirupsen/logrus"
	levelDBUtil "github.com/syndtr/goleveldb/leveldb/util"
)

// Read: BackUpMetaDataByDate back up the file 'files.md5' and 'meta.data' in the directory name with 'date'
func BackUpMetaDataByDate(date string, conf *config.Config) {
	defer func() {
		if re := recover(); re != nil {
			buffer := debug.Stack()
			log.Error("BackUpMetaDataByDate")
			log.Error(re)
			log.Error(string(buffer))
		}
	}()

	var (
		err          error
		keyPrefix    string
		msg          string
		name         string
		fileInfo     FileInfo
		logFileName  string
		fileLog      *os.File
		fileMeta     *os.File
		metaFileName string
		fi           os.FileInfo
	)

	logFileName = conf.DataDir() + "/" + date + "/" + conf.FileMd5()
	conf.LockMap().LockKey(logFileName)
	defer conf.LockMap().UnLockKey(logFileName)

	metaFileName = "/" + date + "/" + "meta.data"
	_ = os.MkdirAll(conf.DataDir()+"/"+date, 0775)
	if pkg.Exist(logFileName) {
		_ = os.Remove(logFileName)
	}
	if pkg.Exist(metaFileName) {
		_ = os.Remove(metaFileName)
	}
	fileLog, err = os.OpenFile(logFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
	if err != nil {
		log.Error(err)
		return
	}

	defer func() {
		_ = fileLog.Close()
	}()
	fileMeta, err = os.OpenFile(metaFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0664)
	if err != nil {
		log.Error(err)
		return
	}
	defer func() {
		_ = fileMeta.Close()
	}()

	keyPrefix = "%s_%s_"
	keyPrefix = fmt.Sprintf(keyPrefix, date, conf.FileMd5())
	iter := conf.LevelDB().NewIterator(levelDBUtil.BytesPrefix([]byte(keyPrefix)), nil)
	defer iter.Release()

	for iter.Next() {
		if err = config.Json.Unmarshal(iter.Value(), &fileInfo); err != nil {
			continue
		}

		name = fileInfo.Name
		if fileInfo.ReName != "" {
			name = fileInfo.ReName
		}
		msg = fmt.Sprintf("%s\t%s\n", fileInfo.Md5, string(iter.Value()))
		if _, err = fileMeta.WriteString(msg); err != nil {
			log.Error(err)
		}

		msg = fmt.Sprintf("%s\t%s\n", pkg.MD5(fileInfo.Path+"/"+name), string(iter.Value()))
		if _, err = fileMeta.WriteString(msg); err != nil {
			log.Error(err)
		}

		msg = fmt.Sprintf("%s|%d|%d|%s\n", fileInfo.Md5, fileInfo.Size, fileInfo.TimeStamp, fileInfo.Path+"/"+name)
		if _, err = fileLog.WriteString(msg); err != nil {
			log.Error(err)
		}
	}

	if fi, err = fileLog.Stat(); err != nil {
		log.Error(err)
	} else if fi.Size() == 0 {
		_ = fileLog.Close()
		_ = os.Remove(logFileName)
	}

	if fi, err = fileMeta.Stat(); err != nil {
		log.Error(err)
	} else if fi.Size() == 0 {
		_ = fileMeta.Close()
		_ = os.Remove(metaFileName)
	}
}
