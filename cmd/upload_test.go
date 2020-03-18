package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/astaxie/beego/httplib"
	"golang.org/x/exp/errors/fmt"
)

func BenchmarkUpload(b *testing.B) {
	router := setRouter()
	for i := 0; i < b.N; i++ {

		w := httptest.NewRecorder()
		customName := fmt.Sprintf("testFile-%d", i)

		req := httplib.Post("http://localhost:8080/file_buf")
		req.PostFile("file", "/opt/golang/architecture.svg")
		req.Param("name", customName)

		router.ServeHTTP(w, req.GetRequest())
	}
}

func BenchmarkUploadNormal(b *testing.B) {
	router := setRouter()
	for i := 0; i < b.N; i++ {

		w := httptest.NewRecorder()
		customName := fmt.Sprintf("testFile-%d", i)

		req := httplib.Post("http://localhost:8080/file_normal")
		req.PostFile("file", "/opt/golang/architecture.svg")
		req.Param("name", customName)

		router.ServeHTTP(w, req.GetRequest())
	}
}

func BenchmarkUploadPool(b *testing.B) {
	router := setRouter()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		customName := fmt.Sprintf("testFile-%d", i)

		req := httplib.Post("http://localhost:8080/file_pool")
		req.PostFile("file", "/opt/golang/architecture.svg")
		req.Param("name", customName)

		router.ServeHTTP(w, req.GetRequest())
	}
}

func BenchmarkUploadYes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		req := httplib.Post("http://localhost:8080/file_normal")
		req.PostFile("file", "/opt/golang/architecture.svg") //注意不是全路径
		req.Param("output", "json")
		req.Param("scene", "")
		req.Param("path", "")
		req.Response()
	}
}

func BenchmarkUploadYes4(b *testing.B) {
	router := setRouter()

	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/file_normal", nil)
		req.Header.Set("Content-Type", "multipart/form-data")
		req.FormFile("/opt/golang/architecture.svg")
		router.ServeHTTP(w, req)
	}
}
