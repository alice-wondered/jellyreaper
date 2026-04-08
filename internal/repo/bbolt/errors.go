package bbolt

import "errors"

var (
	ErrNotFound      = errors.New("record not found")
	ErrConflict      = errors.New("conflict")
	ErrAlreadyExists = errors.New("record already exists")
	ErrInvalidInput  = errors.New("invalid input")
)
