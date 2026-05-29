package wshub

type Option func(*Hub)

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
