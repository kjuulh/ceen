package ceen

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kjuulh/ceen/codec"
	"github.com/nats-io/nats.go"
	"strings"
)

const (
	eventTypeHdr  = "ceen-type"
	eventCodecHdr = "ceen-codec"
)

type EventStore struct {
	c    *Ceen
	name string
}

func (es *EventStore) Create(conf *nats.StreamConfig) error {
	if conf == nil {
		conf = &nats.StreamConfig{}
	}
	conf.Name = es.name

	if len(conf.Subjects) == 0 {
		conf.Subjects = []string{fmt.Sprintf("%s.>", es.name)}
	}
	_, err := es.c.js.AddStream(conf)
	return err
}

func (es *EventStore) Append(ctx context.Context, subject string, events ...*Event) (uint64, error) {
	var ack *nats.PubAck

	for _, event := range events {
		popts := []nats.PubOpt{
			nats.Context(ctx),
			nats.ExpectStream(es.name),
		}

		e, err := es.wrapEvent(event)
		if err != nil {
			return 0, err
		}

		msg, err := es.packEvent(subject, e)
		if err != nil {
			return 0, err
		}

		ack, err = es.c.js.PublishMsg(msg, popts...)
		if err != nil {
			if strings.Contains(err.Error(), "wrong last sequence") {
				return 0, errors.New("wrong last sequence")
			}
			return 0, err
		}
	}

	return ack.Sequence, nil
}

func (es *EventStore) wrapEvent(event *Event) (*Event, error) {
	if event.Data == nil {
		return nil, errors.New("event data is required")
	}

	if es.c.types == nil {
		if event.Type == "" {
			return nil, errors.New("event type is required")
		}
	} else {
		t, err := es.c.types.Lookup(event.Data)
		if err != nil {
			return nil, err
		}

		if event.Type == "" {
			event.Type = t
		} else if event.Type != t {
			return nil, fmt.Errorf("wrong type for event data: %s", event.Data)
		}
	}
	if v, ok := event.Data.(validator); ok {
		if err := v.Validate(); err != nil {
			return nil, err
		}
	}
	if event.ID == "" {
		event.ID = es.c.id.New()
	}

	return event, nil
}

func (es *EventStore) packEvent(subject string, event *Event) (*nats.Msg, error) {
	var (
		data      []byte
		err       error
		codecName string
	)

	if es.c.types == nil {
		data, err = codec.Binary.Marshal(event.Data)
		codecName = codec.Binary.Name()
	} else {
		data, err = es.c.types.Marshal(event.Data)
		codecName = es.c.types.Codec().Name()
	}
	if err != nil {
		return nil, err
	}

	msg := nats.NewMsg(subject)
	msg.Data = data

	msg.Header.Set(nats.MsgIdHdr, event.ID)
	msg.Header.Set(eventTypeHdr, event.Type)
	msg.Header.Set(eventCodecHdr, codecName)

	return msg, nil
}

type loadOpts struct {
	afterSeq *uint64
}

type natsApiError struct {
	Code        int    `json:"code"`
	ErrCode     uint16 `json:"err_code"`
	Description string `json:"description"`
}

type natsStoredMsg struct {
	Sequence uint64 `json:"seq"`
}

type natsGetMsgRequest struct {
	LastBySubject string `json:"last_by_subj"`
}

type natsGetMsgResponse struct {
	Type    string         `json:"type"`
	Error   *natsApiError  `json:"error"`
	Message *natsStoredMsg `json:"message"`
}

func (es *EventStore) Load(ctx context.Context, subject string) ([]*Event, uint64, error) {
	lastMsg, err := es.lastMsgForSubject(ctx, subject)
	if err != nil {
		return nil, 0, err
	}

	if lastMsg.Sequence == 0 {
		return nil, 0, nil
	}

	sopts := []nats.SubOpt{
		nats.OrderedConsumer(),
		nats.DeliverAll(),
	}

	sub, err := es.c.js.SubscribeSync(subject, sopts...)
	if err != nil {
		return nil, 0, err
	}

	defer sub.Unsubscribe()

	var events []*Event

	for {
		msg, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			return nil, 0, err
		}

		event, err := es.c.UnpackEvent(msg)
		if err != nil {
			return nil, 0, err
		}

		events = append(events, event)
		if event.Sequence == lastMsg.Sequence {
			break
		}
	}

	return events, lastMsg.Sequence, nil
}

func (es *EventStore) lastMsgForSubject(ctx context.Context, subject string) (*natsStoredMsg, error) {
	rsubject := fmt.Sprintf("$JS.API.STREAM.MSG.GET.%s", es.name)

	data, _ := json.Marshal(&natsGetMsgRequest{
		LastBySubject: subject,
	})

	msg, err := es.c.nc.RequestWithContext(ctx, rsubject, data)
	if err != nil {
		return nil, err
	}

	var rep natsGetMsgResponse
	err = json.Unmarshal(msg.Data, &rep)
	if err != nil {
		return nil, err
	}

	if rep.Error != nil {
		if rep.Error.Code == 404 {
			return &natsStoredMsg{}, nil
		}

		return nil, fmt.Errorf("%s (%d)", rep.Error.Description, rep.Error.Code)
	}

	return rep.Message, nil
}

func (es *EventStore) Delete() error {
	return es.c.js.DeleteStream(es.name)
}
