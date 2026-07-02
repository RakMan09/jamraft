// Package web embeds the static party UI assets so the node binary is
// self-contained.
package web

import "embed"

//go:embed index.html app.js style.css
var FS embed.FS
