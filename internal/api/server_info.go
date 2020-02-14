package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/luoyunpeng/go-fastdfs/internal/config"
	"github.com/luoyunpeng/go-fastdfs/internal/model"
)

// Addr return current host addr
func Addr(uri string, router *gin.RouterGroup, conf *config.Config) {
	router.GET(uri, func(ctx *gin.Context) {
		r := ctx.Request
		if !model.IsPeer(r, conf) {
			ctx.JSON(http.StatusNotAcceptable, model.GetClusterNotPermitMessage(r))
			return
		}

		ctx.JSON(http.StatusOK, conf.Addr())
	})
}

// Peers return all peer
func Peers(uri string, router *gin.RouterGroup, conf *config.Config) {
	router.GET(uri, func(ctx *gin.Context) {
		r := ctx.Request
		if !model.IsPeer(r, conf) {
			ctx.JSON(http.StatusNotAcceptable, model.GetClusterNotPermitMessage(r))
			return
		}

		ctx.JSON(http.StatusOK, conf.Peers())
	})
}

// PeerID return current peer id
func PeerID(uri string, router *gin.RouterGroup, conf *config.Config) {
	router.GET(uri, func(ctx *gin.Context) {
		r := ctx.Request
		if !model.IsPeer(r, conf) {
			ctx.JSON(http.StatusNotAcceptable, model.GetClusterNotPermitMessage(r))
			return
		}

		ctx.JSON(http.StatusOK, conf.PeerId())
	})
}
