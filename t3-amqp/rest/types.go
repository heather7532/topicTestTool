package rest

type SchemaRequest struct {
	Name       string `json:"name" binding:"required"`
	Type       string `json:"type" binding:"required"`
	Version    string `json:"version" binding:"required"`
	SchemaData string `json:"schemaData" binding:"required"`
}

type UserRequest struct {
	ID       int
	Username string
	Password string
	Email    string
}
