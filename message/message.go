package message

type Type string

type Message interface {
	GetType() Type
	Marshal() ([]byte, error)
}
