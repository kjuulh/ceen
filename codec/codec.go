package codec

import "errors"

var (
	ErrCodecNotRegistered = errors.New("ceen: codec not registered")

	Default = JSON

	Codecs = map[string]Codec{
		JSON.Name():   JSON,
		Binary.Name(): Binary,
	}
)

type Codec interface {
	Name() string
	Marshal(any) ([]byte, error)
	Unmarshal([]byte, any) error
}
