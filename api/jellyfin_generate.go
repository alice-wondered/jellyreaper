package api

//go:generate go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen -generate types,client -include-tags ActivityLog,Items -package jellyfingen -o ../internal/jellyfin/gen/openapi.gen.go external/jellyfin-openapi-stable.json
