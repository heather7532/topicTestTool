package db

import "time"

type QueryArgs struct {
	Name       string
	Type       string
	Version    string
	SchemaData string
}

type Schema struct {
	ID         int
	Name       string
	Type       string
	Version    string
	SchemaData string
	Created    time.Time
	Modified   time.Time
}
