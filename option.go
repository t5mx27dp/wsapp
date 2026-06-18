package wsapp

import (
	"github.com/t5mx27dp/wsapp/message"
)

type Option func(*App)

func WithReadingBufferSize(size uint32) Option {
	return func(a *App) {
		a.reading = make(chan message.Message, size)
	}
}

func WithWritingBufferSize(size uint32) Option {
	return func(a *App) {
		a.writing = make(chan message.Message, size)
	}
}
