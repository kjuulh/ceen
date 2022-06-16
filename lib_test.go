package ceen

import (
	"context"
	"fmt"
	"github.com/kjuulh/ceen/internal/testutil"
	"github.com/kjuulh/ceen/types"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLib(t *testing.T) {
	srv := testutil.NewNatsServer(-1)
	defer testutil.ShutdownNatsServer(srv)

	nc, _ := nats.Connect(srv.ClientURL())

	c, err := New(nc)
	require.NoError(t, err)

	testItems, err := c.EventStore("test_items")

	err = testItems.Create(&nats.StreamConfig{
		Storage: nats.MemoryStorage,
	})
	require.NoError(t, err)

	ctx := context.Background()

	seq, err := testItems.Append(ctx, "test_items.1", &Event{Type: "test_item", Data: []byte("first-item")})
	require.NoError(t, err)
	require.Equal(t, uint64(1), seq)

	events, _, err := testItems.Load(ctx, "test_items.1")
	require.NoError(t, err)
	require.Equal(t, "test_item", events[0].Type)
	require.Equal(t, any([]byte("first-item")), events[0].Data)
}

type TestEventCreated struct {
	ID string
}

func TestLibWithRegistry(t *testing.T) {
	tests := []struct {
		Name string
		Run  func(t *testing.T, es *EventStore, subject string)
	}{
		{
			Name: "append-load-no-occ",
			Run: func(t *testing.T, es *EventStore, subject string) {
				ctx := context.Background()
				testEvent := TestEventCreated{ID: "some-event-id"}
				seq, err := es.Append(ctx, subject, &Event{Data: &testEvent})
				require.NoError(t, err)
				require.Equal(t, uint64(1), seq)

				events, lseq, err := es.Load(ctx, subject)
				require.NoError(t, err)
				require.Equal(t, seq, lseq)

				require.True(t, events[0].ID != "")
				require.Equal(t, "test-event-created", events[0].Type)

				data, ok := events[0].Data.(*TestEventCreated)
				require.True(t, ok)
				require.Equal(t, testEvent, *data)
			},
		},
	}

	srv := testutil.NewNatsServer(-1)
	defer testutil.ShutdownNatsServer(srv)

	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)

	tr, err := types.NewRegistry(map[string]*types.Type{
		"test-event-created": {
			Init: func() any {
				return &TestEventCreated{}
			},
		},
	})
	require.NoError(t, err)

	c, err := New(nc, TypeRegistry(tr))
	require.NoError(t, err)

	for i, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			es, err := c.EventStore("testevents")
			require.NoError(t, err)

			_ = es.Delete()
			err = es.Create(&nats.StreamConfig{Storage: nats.MemoryStorage})
			require.NoError(t, err)

			subject := fmt.Sprintf("testevents.%d", i)

			test.Run(t, es, subject)
		})
	}
}
