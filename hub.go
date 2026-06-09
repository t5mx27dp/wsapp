package wshub

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/t5mx27dp/app"
)

type MessageType string

type Message interface {
	GetType() MessageType
	Marshal() ([]byte, error)
}

type Decoder func([]byte) (Message, error)

type Handler func(ctx context.Context, message Message, writing chan<- Message)

type config struct {
	logger app.Logger

	reading chan Message
	writing chan Message

	debug bool
}

type Hub struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	conn *websocket.Conn

	decoder Decoder

	handlers map[MessageType]Handler

	config *config
}

func New(conn *websocket.Conn, decoder Decoder, handlers map[MessageType]Handler, opts ...Option) *Hub {
	cfg := &config{}

	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.reading == nil {
		cfg.reading = make(chan Message, 100)
	}

	if cfg.writing == nil {
		cfg.writing = make(chan Message, 100)
	}

	return &Hub{
		conn:     conn,
		decoder:  decoder,
		handlers: handlers,
		config:   cfg,
	}
}

func (h *Hub) Writing() chan<- Message {
	return h.config.writing
}

func (h *Hub) Run(ctx context.Context) {
	h.ctx, h.cancel = context.WithCancel(ctx)

	h.wg.Add(3)
	go h.read()
	go h.write()
	go h.handle()
	h.wg.Wait()
}

func (h *Hub) read() {
	h.log(h.ctx, "start read loop", nil)

	defer func() {
		h.log(h.ctx, "stop read loop", nil)
		h.conn.Close()
		h.cancel()
		h.wg.Done()
	}()

	h.conn.SetPongHandler(func(string) error {
		err := h.conn.SetReadDeadline(time.Now().Add(time.Second * 30))
		if err != nil {
			h.log(h.ctx, "", err)
			return err
		}
		return nil
	})

	err := h.conn.SetReadDeadline(time.Now().Add(time.Second * 30))
	if err != nil {
		h.log(h.ctx, "", err)
		return
	}

	for {
		select {
		case <-h.ctx.Done():
			return
		default:
			_, b, err := h.conn.ReadMessage()
			if err != nil {
				h.log(h.ctx, "", err)
				return
			}

			message, err := h.decoder(b)
			if err != nil {
				h.log(h.ctx, "", err)
				continue
			}

			if h.config.debug {
				h.log(h.ctx, fmt.Sprintf("read message: %s", string(b)), nil)
			}

			h.config.reading <- message
		}
	}
}

func (h *Hub) write() {
	h.log(h.ctx, "start write loop", nil)

	defer func() {
		h.log(h.ctx, "stop write loop", nil)
		h.conn.Close()
		h.cancel()
		h.wg.Done()
	}()

	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			err := h.conn.SetWriteDeadline(time.Now().Add(time.Second * 10))
			if err != nil {
				h.log(h.ctx, "", err)
				return
			}

			err = h.conn.WriteMessage(websocket.PingMessage, nil)
			if err != nil {
				h.log(h.ctx, "", err)
				return
			}
		case message := <-h.config.writing:
			err := h.conn.SetWriteDeadline(time.Now().Add(time.Second * 10))
			if err != nil {
				h.log(h.ctx, "", err)
				return
			}

			b, err := message.Marshal()
			if err != nil {
				h.log(h.ctx, "", err)
				continue
			}

			err = h.conn.WriteMessage(websocket.TextMessage, b)
			if err != nil {
				h.log(h.ctx, "", err)
				return
			}

			if h.config.debug {
				h.log(h.ctx, fmt.Sprintf("write message: %s", string(b)), nil)
			}
		}
	}
}

func (h *Hub) handle() {
	h.log(h.ctx, "start handle loop", nil)

	defer func() {
		h.log(h.ctx, "stop handle loop", nil)
		h.conn.Close()
		h.cancel()
		h.wg.Done()
	}()

	for {
		select {
		case <-h.ctx.Done():
			return
		case message := <-h.config.reading:
			handler, ok := h.handlers[message.GetType()]
			if !ok {
				h.log(h.ctx, "", fmt.Errorf("handler %s not found", message.GetType()))
				continue
			}

			go handler(h.ctx, message, h.config.writing)
		}
	}
}

func (h *Hub) log(ctx context.Context, message string, err error) {
	if h.config.logger == nil {
		return
	}

	if err != nil {
		h.config.logger.Error(ctx, err.Error(), nil)
		return
	}

	h.config.logger.Info(ctx, message, nil)
}
