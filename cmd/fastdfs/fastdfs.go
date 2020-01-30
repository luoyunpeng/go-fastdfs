package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"

	"github.com/luoyunpeng/go-fastdfs/internal/config"
	"github.com/luoyunpeng/go-fastdfs/internal/model"
	"github.com/luoyunpeng/go-fastdfs/internal/server"
	"github.com/luoyunpeng/go-fastdfs/pkg"
)

var (
	FOLDERS     = []string{config.DataDir, config.StoreDir, config.ConfDir, config.StaticDir}
	VERSION     string
	BuildTime   string
	GoVersion   string
	GitVersion  string
	versionInfo = flag.Bool("v", false, "display version")
)

func init() {
	flag.Parse()
	if *versionInfo {
		fmt.Printf("%s\n%s\n%s\n%s\n", VERSION, BuildTime, GoVersion, GitVersion)
		os.Exit(0)
	}
	appDir, e1 := pkg.GetFileServerRunningAbsDir(os.Args[0])
	curDir, e2 := filepath.Abs(".")
	if e1 == nil && e2 == nil && appDir != curDir {
		msg := fmt.Sprintf("please switch directory to '%s' start fileserver\n", appDir)
		panic(msg)
	}

	FOLDERS = []string{config.DataDir, config.StoreDir, config.ConfDir, config.StaticDir}
	for _, folder := range FOLDERS {
		_ = os.MkdirAll(folder, 0775)
	}
	config.LoadDefaultConfig()
	config.CommonConfig.AbsRunningDir = appDir

	model.Svr = model.NewServer()
	model.Svr.InitComponent(false)
}

func main() {
	svr := model.Svr
	go func() {
		for {
			svr.CheckFileAndSendToPeer(pkg.GetToDay(), config.Md5ErrorFileName, false)
			//fmt.Println("CheckFileAndSendToPeer")
			time.Sleep(time.Second * time.Duration(config.CommonConfig.RefreshInterval))
			//svr.pkg.RemoveEmptyDir(STORE_DIR)
		}
	}()
	go svr.CleanAndBackUp()
	go model.CheckClusterStatus()
	go svr.LoadQueueSendToPeer()
	go svr.ConsumerPostToPeer()
	go svr.ConsumerLog()
	go svr.ConsumerDownLoad()
	go svr.ConsumerUpload()
	go svr.RemoveDownloading()
	if config.CommonConfig.EnableFsnotify {
		go svr.WatchFilesChange()
	}
	//go svr.LoadSearchDict()
	if config.CommonConfig.EnableMigrate {
		go svr.RepairFileInfoFromFile()
	}
	if config.CommonConfig.AutoRepair {
		go func() {
			for {
				time.Sleep(time.Minute * 3)
				svr.AutoRepair(false)
				time.Sleep(time.Minute * 60)
			}
		}()
	}
	go func() { // force free memory
		for {
			time.Sleep(time.Minute * 1)
			debug.FreeOSMemory()
		}
	}()

	server.Start("")
}
