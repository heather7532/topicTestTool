package persistence

import (
	"bufio"
	"encoding/base64"
	log "github.com/sirupsen/logrus"
	"io"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"t3-amqp/types"
	"unicode"
)

// ReadDirectory reads the contents of a directory given a path and returns a FileNode struct
func ReadDirectory(path string) (*types.FileNode, error) {
	root := &types.FileNode{Name: filepath.Base(path), IsDir: true}
	root.Path = path
	err := readDirRecursive(path, root)
	if err != nil {
		return nil, err
	}
	log.Info("root: ", root)
	return root, nil
}

// readDirRecursive is a helper function to recursively read directory contents
func readDirRecursive(path string, node *types.FileNode) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		childNode := &types.FileNode{
			Name: entry.Name(), IsDir: entry.IsDir(), Path: filepath.Join(path, entry.Name()),
		}
		if entry.IsDir() {
			err := readDirRecursive(filepath.Join(path, entry.Name()), childNode)
			if err != nil {
				return err
			}
		}
		node.Children = append(node.Children, childNode)
	}
	return nil
}

// IsBinaryFile reads a portion of the file and checks if it contains non-text characters.
func IsBinaryFile(filename string) (bool, error) {
	file, err := os.Open(filename)
	if err != nil {
		return false, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	const sampleSize = 512 // Read the first 512 bytes
	sample, err := reader.Peek(sampleSize)
	if err != nil && err != bufio.ErrBufferFull && err != io.EOF {
		return false, err
	}

	// Check if there are non-printable characters in the sample
	for _, b := range sample {
		// If the byte is not ASCII and not printable, it may be binary
		if b > unicode.MaxASCII || (b < 32 && b != '\n' && b != '\r' && b != '\t') {
			return true, nil // It's likely a binary file
		}
	}

	return false, nil // It's likely a text file
}

// IsBinaryMimeType returns true if the MIME type is likely binary
func IsBinaryMimeType(mimeType string) bool {
	// Check for common text MIME types
	if strings.HasPrefix(mimeType, "text/") ||
		mimeType == "application/json" ||
		mimeType == "application/xml" ||
		mimeType == "application/javascript" ||
		mimeType == "application/x-www-form-urlencoded" {
		return false
	}

	// By default, consider other types as binary
	return true
}

func GetMimeType(filename string) string {
	// Get the file extension
	ext := filepath.Ext(filename)

	// Use mime.TypeByExtension to get the MIME type
	mimeType, _, err := mime.ParseMediaType(mime.TypeByExtension(ext))
	if err != nil {
		log.Error("Error parsing media type: ", err)
	}

	// If TypeByExtension returns an empty string, default to application/octet-stream
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	return mimeType
}

// ReadFile reads the contents of a file and returns a FileData struct after determining the MIME type
func ReadFile(filename string) (*types.FileData, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Determine the MIME type of the file
	mimeType := GetMimeType(filename)

	// Check if the file is binary
	isBinary, err := IsBinaryFile(filename)
	if err != nil {
		return nil, err
	}

	// Read the contents of the file based on whether it is binary or text and return a FileData struct with proper encoding
	var contents string

	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	if isBinary {
		contents = base64.StdEncoding.EncodeToString(data)
	} else {
		contents = string(data)
	}

	return &types.FileData{
		Path: filename, Name: filepath.Base(filename), Type: filepath.Ext(filename),
		MimeType: mimeType, Contents: contents,
	}, nil
}

// WriteFile writes FileData struct contents to the path.  if file exists it will overwrite it.
// If it is a new file it will create it.  If the directory does not exist it will create it.
// It will take into account the file type and write the contents in the appropriate format.
func WriteFile(fileData types.FileData) error {
	// Create the directory if it does not exist
	err := os.MkdirAll(filepath.Dir(fileData.Path), 0755)
	if err != nil {
		return err
	}

	// Decode the contents if it is a binary file
	var contents []byte
	if IsBinaryMimeType(fileData.MimeType) {
		contents, err = base64.StdEncoding.DecodeString(fileData.Contents)
		if err != nil {
			return err
		}
	} else {
		contents = []byte(fileData.Contents)
	}

	// Write the contents to the file
	err = os.WriteFile(fileData.Path, contents, 0644)
	if err != nil {
		return err
	}

	return nil
}
