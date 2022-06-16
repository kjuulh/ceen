package ceen

type Event struct {
	ID string

	Type string

	Data     any
	Subject  string
	Sequence uint64
}
