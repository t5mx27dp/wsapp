package wshub

type Option func(*WSHub)

func WithLogger(logger Logger) Option {
	return func(h *WSHub) {
		h.logger = logger
	}
}

func WithDebug() Option {
	return func(h *WSHub) {
		h.debug = true
	}
}

func WithReadingBufferSize(size uint32) Option {
	return func(h *WSHub) {
		h.reading = make(chan Message, size)
	}
}

func WithWritingBufferSize(size uint32) Option {
	return func(h *WSHub) {
		h.writing = make(chan Message, size)
	}
}
