package artifacts

import (
	"t3-amqp/db"
	"time"
)

// Folder struct that embeds ArtifactImpl and contains other Folders and Artifacts
type Folder struct {
	ArtifactImpl
	SubFolders []Folder
	Artifacts  []Artifact
}

// NewFolder function to create a new Folder
func NewFolder(name, artifactType string, owner db.User, group int) *Folder {
	return &Folder{
		ArtifactImpl: ArtifactImpl{
			Name:     name,
			Type:     artifactType,
			Owner:    owner,
			Group:    group,
			Created:  time.Now(),
			Modified: time.Now(),
		},
		SubFolders: []Folder{},
		Artifacts:  []Artifact{},
	}
}

// AddSubFolder method to add a subfolder to a Folder
func (f *Folder) AddSubFolder(subFolder Folder) {
	f.SubFolders = append(f.SubFolders, subFolder)
	f.Modified = time.Now()
}

// AddArtifact method to add an artifact to a Folder
func (f *Folder) AddArtifact(artifact Artifact) {
	f.Artifacts = append(f.Artifacts, artifact)
	f.Modified = time.Now()
}
