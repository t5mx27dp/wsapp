package wshub

import "github.com/t5mx27dp/app"

type Option func(*config)

func WithLogger(logger app.Logger) Option {
	return func(c *config) {
		c.logger = logger
	}
}

func WithReadingBufferSize(size uint32) Option {
	return func(c *config) {
		c.reading = make(chan Message, size)
	}
}

func WithWritingBufferSize(size uint32) Option {
	return func(c *config) {
		c.writing = make(chan Message, size)
	}
}

func WithDebug() Option {
	return func(c *config) {
		c.debug = true
	}
}
