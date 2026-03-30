package web

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

//go:embed static/*
var assets embed.FS

func Handler() (http.Handler, error) {
	root, err := fs.Sub(assets, "static")
	if err != nil {
		return nil, err
	}
	fileServer := http.FileServer(http.FS(root))
	indexHTML, err := fs.ReadFile(root, "index.html")
	if err != nil {
		return nil, err
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqPath := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
		if reqPath == "." || reqPath == "" {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write(indexHTML)
			return
		}

		if _, err := fs.Stat(root, reqPath); err == nil {
			fileServer.ServeHTTP(w, r)
			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	}), nil
}
