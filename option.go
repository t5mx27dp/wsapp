package wshub

import "context"

type Option func(*Hub)

func WithContext(ctx context.Context) Option {
	return func(h *Hub) {
		h.ctx, h.cancel = context.WithCancel(ctx)
	}
}

func WithLogger(logger Logger) Option {
	return func(h *Hub) {
		h.logger = logger
	}
}

func WithDebug() Option {
	return func(h *Hub) {
		h.debug = true
	}
}
