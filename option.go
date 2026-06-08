package wshub

type Option func(*Hub)

func WithLogger(logger Logger) Option {
	return func(h *Hub) {
		h.logger = logger
	}
}

func WithReadingBufferSize(size uint32) Option {
	return func(h *Hub) {
		h.reading = make(chan Message, size)
	}
}

func WithWritingBufferSize(size uint32) Option {
	return func(h *Hub) {
		h.writing = make(chan Message, size)
	}
}

func WithDebug() Option {
	return func(h *Hub) {
		h.debug = true
	}
}
