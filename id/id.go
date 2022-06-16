package id

import "github.com/nats-io/nuid"

var (
	NUID ID = &nuidGen{}
)

type ID interface {
	New() string
}

type nuidGen struct{}

func (i *nuidGen) New() string {
	return nuid.Next()
}
