package ceen

import (
	"fmt"
	"github.com/kjuulh/ceen/codec"
	"github.com/kjuulh/ceen/id"
	"github.com/kjuulh/ceen/types"
	"github.com/nats-io/nats.go"
)

type Ceen struct {
	nc    *nats.Conn
	js    nats.JetStreamContext
	types *types.Registry
	id    id.ID
}

type ceenOption func(o *Ceen) error

func (f ceenOption) addOption(o *Ceen) error {
	return f(o)
}

type CeenOption interface {
	addOption(o *Ceen) error
}

func TypeRegistry(types *types.Registry) CeenOption {
	return ceenOption(func(o *Ceen) error {
		o.types = types
		return nil
	})
}

func (c *Ceen) EventStore(name string) (*EventStore, error) {
	return &EventStore{name: name, c: c}, nil
}

func (c *Ceen) UnpackEvent(msg *nats.Msg) (*Event, error) {
	eventType := msg.Header.Get(eventTypeHdr)
	codecName := msg.Header.Get(eventCodecHdr)
	var (
		data any
		err  error
	)

	codc, ok := codec.Codecs[codecName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", codec.ErrCodecNotRegistered, codecName)
	}

	if c.types == nil {
		var b []byte
		err = codc.Unmarshal(msg.Data, &b)
		data = b
	} else {
		var v any
		v, err = c.types.Init(eventType)
		if err == nil {
			err = codc.Unmarshal(msg.Data, v)
			data = v
		}
	}
	if err != nil {
		return nil, err
	}

	var seq uint64
	if msg.Reply != "" {
		md, err := msg.Metadata()
		if err != nil {
			return nil, fmt.Errorf("unpack: failed to get metadata: %s", err)
		}
		seq = md.Sequence.Stream
	}

	return &Event{
		ID:       msg.Header.Get(nats.MsgIdHdr),
		Type:     msg.Header.Get(eventTypeHdr),
		Data:     data,
		Subject:  msg.Subject,
		Sequence: seq,
	}, nil
}

func New(nc *nats.Conn, options ...CeenOption) (*Ceen, error) {
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	c := &Ceen{
		nc: nc,
		js: js,
		id: id.NUID,
	}

	for _, o := range options {
		if err := o.addOption(c); err != nil {
			return nil, err
		}
	}

	return c, nil
}
