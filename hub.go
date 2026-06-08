package wshub

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type MessageType string

type Message interface {
	GetType() MessageType
	Marshal() ([]byte, error)
}

type Decoder func([]byte) (Message, error)

type Logger func(ctx context.Context, err error, message string)

type Handler func(ctx context.Context, message Message, writing chan<- Message)

type Hub struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	conn *websocket.Conn

	decoder Decoder
	logger  Logger

	handlers map[MessageType]Handler

	reading chan Message
	writing chan Message

	debug bool
}

func New(conn *websocket.Conn, decoder Decoder, handlers map[MessageType]Handler, opts ...Option) *Hub {
	h := &Hub{
		conn:     conn,
		decoder:  decoder,
		handlers: handlers,
	}

	for _, opt := range opts {
		opt(h)
	}

	if h.logger == nil {
		h.logger = h.handleLog
	}

	if h.reading == nil {
		h.reading = make(chan Message, 100)
	}

	if h.writing == nil {
		h.writing = make(chan Message, 100)
	}

	return h
}

func (h *Hub) Writing() chan<- Message {
	return h.writing
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
	h.logger(h.ctx, nil, "start read loop")

	defer func() {
		h.logger(h.ctx, nil, "stop read loop")
		h.conn.Close()
		h.cancel()
		h.wg.Done()
	}()

	h.conn.SetPongHandler(func(string) error {
		err := h.conn.SetReadDeadline(time.Now().Add(time.Second * 30))
		if err != nil {
			h.logger(h.ctx, err, "")
			return err
		}
		return nil
	})

	err := h.conn.SetReadDeadline(time.Now().Add(time.Second * 30))
	if err != nil {
		h.logger(h.ctx, err, "")
		return
	}

	for {
		select {
		case <-h.ctx.Done():
			return
		default:
			_, b, err := h.conn.ReadMessage()
			if err != nil {
				h.logger(h.ctx, err, "")
				return
			}

			message, err := h.decoder(b)
			if err != nil {
				h.logger(h.ctx, err, "")
				continue
			}

			if h.debug {
				h.logger(h.ctx, nil, fmt.Sprintf("read message: %s", string(b)))
			}

			h.reading <- message
		}
	}
}

func (h *Hub) write() {
	h.logger(h.ctx, nil, "start write loop")

	defer func() {
		h.logger(h.ctx, nil, "stop write loop")
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
				h.logger(h.ctx, err, "")
				return
			}

			err = h.conn.WriteMessage(websocket.PingMessage, nil)
			if err != nil {
				h.logger(h.ctx, err, "")
				return
			}
		case message := <-h.writing:
			err := h.conn.SetWriteDeadline(time.Now().Add(time.Second * 10))
			if err != nil {
				h.logger(h.ctx, err, "")
				return
			}

			b, err := message.Marshal()
			if err != nil {
				h.logger(h.ctx, err, "")
				continue
			}

			err = h.conn.WriteMessage(websocket.TextMessage, b)
			if err != nil {
				h.logger(h.ctx, err, "")
				return
			}

			if h.debug {
				h.logger(h.ctx, nil, fmt.Sprintf("write message: %s", string(b)))
			}
		}
	}
}

func (h *Hub) handle() {
	h.logger(h.ctx, nil, "start handle loop")

	defer func() {
		h.logger(h.ctx, nil, "stop handle loop")
		h.conn.Close()
		h.cancel()
		h.wg.Done()
	}()

	for {
		select {
		case <-h.ctx.Done():
			return
		case message := <-h.reading:
			handler, ok := h.handlers[message.GetType()]
			if !ok {
				h.logger(h.ctx, fmt.Errorf("handler %s not found", message.GetType()), "")
				continue
			}

			go handler(h.ctx, message, h.writing)
		}
	}
}

func (h *Hub) handleLog(ctx context.Context, err error, message string) {
	if err != nil {
		log.Println(err)
		return
	}
	log.Println(message)
}
