package jobs

import (
	"fmt"
	"sync"

	"jellyreaper/internal/domain"
)

type Registry struct {
	mu       sync.RWMutex
	handlers map[domain.JobKind]JobHandler
}

func NewRegistry(handlers ...JobHandler) (*Registry, error) {
	r := &Registry{handlers: make(map[domain.JobKind]JobHandler, len(handlers))}
	for _, handler := range handlers {
		if err := r.Register(handler); err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (r *Registry) Register(handler JobHandler) error {
	if handler == nil {
		return fmt.Errorf("register handler: nil handler")
	}

	kind := handler.Kind()
	if kind == "" {
		return fmt.Errorf("register handler: empty job kind")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[kind]; exists {
		return fmt.Errorf("register handler: kind %q already registered", kind)
	}

	r.handlers[kind] = handler
	return nil
}

func (r *Registry) MustRegister(handler JobHandler) {
	if err := r.Register(handler); err != nil {
		panic(err)
	}
}

func (r *Registry) Get(kind domain.JobKind) (JobHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	handler, ok := r.handlers[kind]
	return handler, ok
}
