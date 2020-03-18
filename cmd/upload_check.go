package main

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

var (
	pool sync.Pool
)

func init() {
	pool.New = func() interface{} {
		return make([]byte, 1024*1024*8)
	}

	pool.Put(make([]byte, 1024*1024*8))
}

func main() {
	app := setRouter()

	app.Run()
}

func setRouter() *gin.Engine {
	app := gin.Default()

	app.POST("/file_buf", uploadBuffer)
	app.POST("/file_normal", uploadNormal)
	app.POST("/file_pool", uploadPool)

	return app
}

func uploadBuffer(ctx *gin.Context) {
	customName := ctx.PostForm("name")

	fileHeader, err := ctx.FormFile("file")
	if err != nil {
		log.Println("upload err: ", err)
		ctx.JSON(http.StatusBadRequest, err.Error())
		return
	}

	fileName := filepath.Base(fileHeader.Filename)

	if customName != "" {
		fileName = customName
	}

	uploadDir := "./tmp/upload/"
	uploadFileName := uploadDir + fileName

	tmpFile, err := fileHeader.Open()
	if err != nil {
		log.Println("open err: ", err)
		ctx.JSON(http.StatusBadRequest, err.Error())
		return
	}
	defer tmpFile.Close()

	uploadFile, err := os.Create(uploadFileName)
	if err != nil {
		log.Println("CopyN err: ", err)
		ctx.JSON(http.StatusBadRequest, err.Error())
		return
	}
	defer uploadFile.Close()

	buf := make([]byte, 1024*1024*8)
	n, err := io.CopyBuffer(uploadFile, tmpFile, buf)
	if err != nil {
		log.Println("CopyN err: ", err)
		ctx.JSON(http.StatusBadRequest, err.Error())
		return
	}

	log.Printf("copy %d bytes", n)
}

func uploadNormal(ctx *gin.Context) {
	customName := ctx.PostForm("name")

	fileHeader, err := ctx.FormFile("file")
	if err != nil {
		log.Println("upload err: ", err)
		ctx.JSON(http.StatusBadRequest, err.Error())
		return
	}

	fileName := filepath.Base(fileHeader.Filename)

	if customName != "" {
		fileName = customName
	}

	uploadDir := "./tmp/upload/"
	uploadFileName := uploadDir + fileName

	tmpFile, err := fileHeader.Open()
	if err != nil {
		log.Println("open err: ", err)
		ctx.JSON(http.StatusBadRequest, err.Error())
		return
	}
	defer tmpFile.Close()

	uploadFile, err := os.Create(uploadFileName)
	if err != nil {
		log.Println("CopyN err: ", err)
		ctx.JSON(http.StatusBadRequest, err.Error())
		return
	}
	defer uploadFile.Close()

	n, err := io.Copy(uploadFile, tmpFile)
	if err != nil {
		log.Println("CopyN err: ", err)
		ctx.JSON(http.StatusBadRequest, err.Error())
		return
	}

	log.Printf("copy %d bytes", n)
}

func uploadPool(ctx *gin.Context) {
	customName := ctx.PostForm("name")

	fileHeader, err := ctx.FormFile("file")
	if err != nil {
		log.Println("upload err: ", err)
		ctx.JSON(http.StatusBadRequest, err.Error())
		return
	}

	fileName := filepath.Base(fileHeader.Filename)

	if customName != "" {
		fileName = customName
	}

	uploadDir := "./tmp/upload/"
	uploadFileName := uploadDir + fileName

	tmpFile, err := fileHeader.Open()
	if err != nil {
		log.Println("open err: ", err)
		ctx.JSON(http.StatusBadRequest, err.Error())
		return
	}
	defer tmpFile.Close()

	uploadFile, err := os.Create(uploadFileName)
	if err != nil {
		log.Println("CopyN err: ", err)
		ctx.JSON(http.StatusBadRequest, err.Error())
		return
	}
	defer uploadFile.Close()

	buf := pool.Get().([]byte)
	n, err := io.CopyBuffer(uploadFile, tmpFile, buf)
	if err != nil {
		log.Println("CopyN err: ", err)
		ctx.JSON(http.StatusBadRequest, err.Error())
		return
	}

	log.Printf("copy %d bytes", n)
}
