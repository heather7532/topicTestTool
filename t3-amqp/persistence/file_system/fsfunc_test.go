package persistence

import (
	"os"
	"path/filepath"
	"t3-amqp/types"
	"testing"
	"time"
)

// Helper function to create a test directory structure
func setupTestDir(t *testing.T) string {
	root := t.TempDir()

	subDir := filepath.Join(root, "subdir")
	err := os.Mkdir(subDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create subdir: %v", err)
	}

	file1 := filepath.Join(root, "file1.txt")
	_, err = os.Create(file1)
	if err != nil {
		t.Fatalf("Failed to create file1: %v", err)
	}

	file2 := filepath.Join(subDir, "file2.txt")
	_, err = os.Create(file2)
	if err != nil {
		t.Fatalf("Failed to create file2: %v", err)
	}

	return root
}

func TestReadDirectory(t *testing.T) {
	root := setupTestDir(t)

	node, err := ReadDirectory(root)
	if err != nil {
		t.Fatalf("ReadDirectory failed: %v", err)
	}

	if node.Name != filepath.Base(root) {
		t.Errorf("Expected root name %s, got %s", filepath.Base(root), node.Name)
	}

	if node.Path != root {
		t.Errorf("Expected root path %s, got %s", root, node.Path)
	}

	if !node.IsDir {
		t.Errorf("Expected root to be a directory")
	}

	if len(node.Children) != 2 {
		t.Errorf("Expected 2 children, got %d", len(node.Children))
	}

	subDirFound := false
	file1Found := false
	for _, child := range node.Children {
		if child.Name == "subdir" && child.IsDir {
			subDirFound = true
			if child.Path != filepath.Join(root, "subdir") {
				t.Errorf(
					"Expected subdir path %s, got %s", filepath.Join(root, "subdir"), child.Path,
				)
			}
			if len(child.Children) != 1 || child.Children[0].Name != "file2.txt" {
				t.Errorf("Expected subdir to contain file2.txt")
			}
			if child.Children[0].Path != filepath.Join(root, "subdir", "file2.txt") {
				t.Errorf(
					"Expected file2.txt path %s, got %s",
					filepath.Join(root, "subdir", "file2.txt"), child.Children[0].Path,
				)
			}
		}
		if child.Name == "file1.txt" && !child.IsDir {
			file1Found = true
			if child.Path != filepath.Join(root, "file1.txt") {
				t.Errorf(
					"Expected file1.txt path %s, got %s", filepath.Join(root, "file1.txt"),
					child.Path,
				)
			}
		}
	}

	if !subDirFound {
		t.Errorf("Expected to find subdir")
	}

	if !file1Found {
		t.Errorf("Expected to find file1.txt")
	}
}

func TestIsBinaryFile(t *testing.T) {
	root := setupTestDir(t)

	textFile := filepath.Join(root, "file1.txt")
	binaryFile := filepath.Join(root, "file1.bin")

	// Create a binary file
	err := os.WriteFile(binaryFile, []byte{0x00, 0x01, 0x02, 0x03}, 0644)
	if err != nil {
		t.Fatalf("Failed to create binary file: %v", err)
	}

	// Define the test table
	tests := []struct {
		filename string
		expected bool
	}{
		{filename: textFile, expected: false},
		{filename: binaryFile, expected: true},
	}

	for _, test := range tests {
		isBinary, err := IsBinaryFile(test.filename)
		if err != nil {
			t.Fatalf("IsBinaryFile failed: %v", err)
		}
		if isBinary != test.expected {
			t.Errorf(
				"Expected IsBinaryFile(%s) to be %v, got %v", test.filename, test.expected,
				isBinary,
			)
		}
	}
}

func TestIsBinaryMimeType(t *testing.T) {
	tests := []struct {
		mimeType string
		expected bool
	}{
		{"text/plain", false},
		{"application/json", false},
		{"application/octet-stream", true},
		{"image/png", true},
	}

	for _, test := range tests {
		isBinary := IsBinaryMimeType(test.mimeType)
		if isBinary != test.expected {
			t.Errorf(
				"Expected IsBinaryMimeType(%s) to be %v, got %v", test.mimeType, test.expected,
				isBinary,
			)
		}
	}
}

func TestGetMimeType(t *testing.T) {
	tests := []struct {
		filename string
		expected string
	}{
		{"file.txt", "text/plain"},
		{"file.json", "application/json"},
		{"file.unknown", "application/octet-stream"},
	}

	for _, test := range tests {
		mimeType := GetMimeType(test.filename)
		if mimeType != test.expected {
			t.Errorf(
				"Expected GetMimeType(%s) to be %s, got %s", test.filename, test.expected, mimeType,
			)
		}
	}
}

func TestReadFile(t *testing.T) {
	root := setupTestDir(t)

	textFile := filepath.Join(root, "file1.txt")
	binaryFile := filepath.Join(root, "file1.bin")

	// Create a binary file
	err := os.WriteFile(binaryFile, []byte{0x00, 0x01, 0x02, 0x03}, 0644)
	if err != nil {
		t.Fatalf("Failed to create binary file: %v", err)
	}

	tests := []struct {
		filename string
		expected *types.FileData
	}{
		{
			textFile,
			&types.FileData{
				Name:      "file1.txt",
				Type:      ".txt",
				MimeType:  "text/plain",
				Contents:  "",
				Timestamp: time.Now(),
			},
		},
		{
			binaryFile,
			&types.FileData{
				Name:      "file1.bin",
				Type:      ".bin",
				MimeType:  "application/octet-stream",
				Contents:  "AAECAw==",
				Timestamp: time.Now(),
			},
		},
	}

	for _, test := range tests {
		fileData, err := ReadFile(test.filename)
		if err != nil {
			t.Fatalf("ReadFile failed: %v", err)
		}
		if fileData.Name != test.expected.Name {
			t.Errorf("Expected file name %s, got %s", test.expected.Name, fileData.Name)
		}
		if fileData.Type != test.expected.Type {
			t.Errorf("Expected file type %s, got %s", test.expected.Type, fileData.Type)
		}
		if fileData.MimeType != test.expected.MimeType {
			t.Errorf("Expected MIME type %s, got %s", test.expected.MimeType, fileData.MimeType)
		}
		if fileData.Contents != test.expected.Contents {
			t.Errorf("Expected contents %s, got %s", test.expected.Contents, fileData.Contents)
		}
	}
}
