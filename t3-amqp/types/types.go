package types

import "time"

// FileNode represents a file or directory in the file system
type FileNode struct {
	ID       string      `json:"id"`
	Name     string      `json:"name"`
	IsDir    bool        `json:"is_dir"`
	Children []*FileNode `json:"children,omitempty"`
	Path     string      `json:"path"`
}

// FileData represents the structure to encapsulate file contents
type FileData struct {
	Path      string    `json:"path"`
	Name      string    `json:"name"`
	Type      string    `json:"type"`
	MimeType  string    `json:"mimetype"`
	Contents  string    `json:"contents"`
	Timestamp time.Time `json:"timestamp"`
}
