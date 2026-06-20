package wsapp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	appmock "github.com/t5mx27dp/app/mock"

	"github.com/t5mx27dp/wsapp"
	"github.com/t5mx27dp/wsapp/message"
)

type Message struct {
	Type message.Type `json:"type"`
	Body []byte       `json:"body"`
}

var _ (message.Message) = (*Message)(nil)

func (m *Message) GetType() message.Type {
	return m.Type
}

func (m *Message) GetBody() []byte {
	return m.Body
}

func Decode(b []byte) (message.Message, error) {
	m := &Message{}
	err := json.Unmarshal(b, m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func Encode(message message.Message) ([]byte, error) {
	return json.Marshal(message)
}

const (
	ServerToClientHello message.Type = "ServerToClientHello"
	ClientToServerHello message.Type = "ClientToServerHello"
)

func TestApp(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			upgrader := websocket.Upgrader{}

			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()

			logger := appmock.NewLogger(t)

			logger.On("Log", mock.Anything, mock.Anything, mock.Anything).Return()
			logger.On("Error", mock.Anything, mock.Anything, mock.Anything).Return()

			handlers := map[message.Type]wsapp.Handler{
				ClientToServerHello: func(ctx context.Context, message message.Message, writing chan<- message.Message) {
					require.Equal(t, ClientToServerHello, message.GetType())
					require.Equal(t, "ClientToServerHello", string(message.GetBody()))

					writing <- &Message{
						Type: ServerToClientHello,
						Body: []byte("ServerToClientHello"),
					}
				},
			}

			app := wsapp.New(r.Context(), conn, logger, Decode, Encode, handlers)

			err = app.Run()
			require.Nil(t, err)
		}))
		defer server.Close()

		wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		require.Nil(t, err)
		defer conn.Close()

		ctx := context.Background()
		logger := appmock.NewLogger(t)

		logger.On("Log", mock.Anything, mock.Anything, mock.Anything).Return()
		logger.On("Error", mock.Anything, mock.Anything, mock.Anything).Return()

		handlers := map[message.Type]wsapp.Handler{
			ServerToClientHello: func(ctx context.Context, message message.Message, writing chan<- message.Message) {
				require.Equal(t, ServerToClientHello, message.GetType())
				require.Equal(t, "ServerToClientHello", string(message.GetBody()))

				// 主动关闭
				conn.Close()
			},
		}

		app := wsapp.New(ctx, conn, logger, Decode, Encode, handlers)

		go func() {
			writing := app.Writing()

			writing <- &Message{
				Type: ClientToServerHello,
				Body: []byte("ClientToServerHello"),
			}
		}()

		err = app.Run()
		require.Nil(t, err)
	})
}
