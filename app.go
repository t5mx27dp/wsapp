package wsapp

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/t5mx27dp/app"
	"github.com/t5mx27dp/wsapp/message"
)

type Decoder func([]byte) (message.Message, error)

type Encoder func(message.Message) ([]byte, error)

type Handler func(ctx context.Context, message message.Message, writing chan<- message.Message)

type App struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	conn *websocket.Conn

	logger app.Logger

	decoder Decoder
	encoder Encoder

	handlers map[message.Type]Handler

	reading chan message.Message
	writing chan message.Message
}

func New(conn *websocket.Conn, logger app.Logger, decoder Decoder, encoder Encoder, handlers map[message.Type]Handler, opts ...Option) *App {
	a := &App{
		conn:     conn,
		logger:   logger,
		decoder:  decoder,
		encoder:  encoder,
		handlers: handlers,
	}

	for _, opt := range opts {
		opt(a)
	}

	if a.reading == nil {
		a.reading = make(chan message.Message, 100)
	}

	if a.writing == nil {
		a.writing = make(chan message.Message, 100)
	}

	return a
}

func (a *App) Writing() chan<- message.Message {
	return a.writing
}

func (a *App) Run(ctx context.Context) error {
	a.ctx, a.cancel = context.WithCancel(ctx)

	a.wg.Add(3)
	go a.read()
	go a.write()
	go a.handle()
	a.wg.Wait()

	return nil
}

func (a *App) read() {
	a.logger.Log(a.ctx, "start read loop", nil)

	defer func() {
		a.logger.Log(a.ctx, "stop read loop", nil)
		a.conn.Close()
		a.cancel()
		a.wg.Done()
	}()

	a.conn.SetPongHandler(func(string) error {
		err := a.conn.SetReadDeadline(time.Now().Add(time.Second * 30))
		if err != nil {
			a.logger.Error(a.ctx, err, nil)
			return err
		}
		return nil
	})

	err := a.conn.SetReadDeadline(time.Now().Add(time.Second * 30))
	if err != nil {
		a.logger.Error(a.ctx, err, nil)
		return
	}

	for {
		select {
		case <-a.ctx.Done():
			return
		default:
			_, b, err := a.conn.ReadMessage()
			if err != nil {
				a.logger.Error(a.ctx, err, nil)
				return
			}

			message, err := a.decoder(b)
			if err != nil {
				a.logger.Error(a.ctx, err, nil)
				continue
			}

			a.reading <- message
		}
	}
}

func (a *App) write() {
	a.logger.Log(a.ctx, "start write loop", nil)

	defer func() {
		a.logger.Log(a.ctx, "stop write loop", nil)
		a.conn.Close()
		a.cancel()
		a.wg.Done()
	}()

	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			err := a.conn.SetWriteDeadline(time.Now().Add(time.Second * 10))
			if err != nil {
				a.logger.Error(a.ctx, err, nil)
				return
			}

			err = a.conn.WriteMessage(websocket.PingMessage, nil)
			if err != nil {
				a.logger.Error(a.ctx, err, nil)
				return
			}
		case message := <-a.writing:
			err := a.conn.SetWriteDeadline(time.Now().Add(time.Second * 10))
			if err != nil {
				a.logger.Error(a.ctx, err, nil)
				return
			}

			b, err := a.encoder(message)
			if err != nil {
				a.logger.Error(a.ctx, err, nil)
				continue
			}

			err = a.conn.WriteMessage(websocket.TextMessage, b)
			if err != nil {
				a.logger.Error(a.ctx, err, nil)
				return
			}
		}
	}
}

func (a *App) handle() {
	a.logger.Log(a.ctx, "start handle loop", nil)

	defer func() {
		a.logger.Log(a.ctx, "stop handle loop", nil)
		a.conn.Close()
		a.cancel()
		a.wg.Done()
	}()

	for {
		select {
		case <-a.ctx.Done():
			return
		case message := <-a.reading:
			handler, ok := a.handlers[message.GetType()]
			if !ok {
				a.logger.Error(a.ctx, fmt.Errorf("handler %s not found", message.GetType()), nil)
				continue
			}

			go handler(a.ctx, message, a.writing)
		}
	}
}
