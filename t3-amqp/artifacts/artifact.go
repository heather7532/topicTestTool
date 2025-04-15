package artifacts

import (
	"os"
	"t3-amqp/db" // Adjust the import path as necessary
	"time"
)

// Artifact interface with the specified fields and methods
type Artifact interface {
	GetName() string
	GetType() string
	GetOwner() db.User
	GetGroup() int
	GetCreated() time.Time
	GetModified() time.Time
	GetPermissions() os.FileMode
	New(name, artifactType string, owner db.User, group int) Artifact
	Delete() error
	View() error
	Save() error
	Move(newGroup int) error
	Copy() Artifact
}

// ArtifactImpl struct that implements the Artifact interface
type ArtifactImpl struct {
	Name        string
	Type        string
	Owner       db.User
	Group       int
	Created     time.Time
	Modified    time.Time
	Permissions os.FileMode
}

func (a *ArtifactImpl) GetName() string {
	return a.Name
}

func (a *ArtifactImpl) GetType() string {
	return a.Type
}

func (a *ArtifactImpl) GetOwner() db.User {
	return a.Owner
}

func (a *ArtifactImpl) GetGroup() int {
	return a.Group
}

func (a *ArtifactImpl) GetCreated() time.Time {
	return a.Created
}

func (a *ArtifactImpl) GetModified() time.Time {
	return a.Modified
}

func (a *ArtifactImpl) GetPermissions() os.FileMode {
	return a.Permissions
}

func (a *ArtifactImpl) New(name, artifactType string, owner db.User, group int) Artifact {
	return &ArtifactImpl{
		Name:        name,
		Type:        artifactType,
		Owner:       owner,
		Group:       group,
		Created:     time.Now(),
		Modified:    time.Now(),
		Permissions: 0644, // Default permissions
	}
}

func (a *ArtifactImpl) Delete() error {
	// Implement delete logic here
	return nil
}

func (a *ArtifactImpl) View() error {
	// Implement view logic here
	return nil
}

func (a *ArtifactImpl) Save() error {
	// Implement save logic here
	return nil
}

func (a *ArtifactImpl) Move(newGroup int) error {
	a.Group = newGroup
	a.Modified = time.Now()
	return nil
}

func (a *ArtifactImpl) Copy() Artifact {
	return &ArtifactImpl{
		Name:        a.Name,
		Type:        a.Type,
		Owner:       a.Owner,
		Group:       a.Group,
		Created:     a.Created,
		Modified:    a.Modified,
		Permissions: a.Permissions,
	}
}
