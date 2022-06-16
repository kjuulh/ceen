package types

import (
	"errors"
	"fmt"
	"github.com/kjuulh/ceen/codec"
	"reflect"
	"regexp"
)

var (
	ErrTypeNotValid      = errors.New("ceen: type not valid")
	ErrTypeNotRegistered = errors.New("ceen: type not registered")
	ErrNoTypeForStruct   = errors.New("ceen: no type for struct")
	ErrMarshal           = errors.New("ceen: marshal error")
	ErrUnmarshal         = errors.New("ceen: unmarshal error")

	nameRegex = regexp.MustCompile(`^[\w-]+(\.[\w-]+)*$`)
)

type Type struct {
	Init func() any
}

type Registry struct {
	rtypes map[reflect.Type]string
	types  map[string]*Type
	codec  codec.Codec
}

func (r *Registry) Lookup(v any) (string, error) {
	ref := reflect.TypeOf(v)
	t, ok := r.rtypes[ref]
	if !ok {
		return "", fmt.Errorf("%w: %s", errors.New("no type for struct"), ref)
	}

	return t, nil
}

func (r *Registry) Init(eventType string) (any, error) {
	t, ok := r.types[eventType]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTypeNotRegistered, eventType)
	}

	v := t.Init()

	return v, nil
}

func (r *Registry) validate(name string, typeDec *Type) error {
	if name == "" {
		return fmt.Errorf("%w: missing name", ErrTypeNotValid)
	}

	if err := validateTypeName(name); err != nil {
		return err
	}

	if typeDec.Init == nil {
		return fmt.Errorf("%w: %s", ErrTypeNotValid, name)
	}

	v := typeDec.Init()
	if v == nil {
		return fmt.Errorf("%w: %s: init func returns nil", ErrTypeNotValid, name)
	}

	rt := reflect.TypeOf(v)

	if rt.Kind() != reflect.Ptr {
		return fmt.Errorf("%w: %s: init func must return a pointer value", ErrTypeNotValid, name)
	}

	if rt.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("%w: %s", ErrTypeNotValid, name)
	}

	b, err := r.codec.Marshal(v)
	if err != nil {
		return fmt.Errorf("%w: %s: failed to marshal with codec: %s", ErrTypeNotValid, name, err)
	}

	err = r.codec.Unmarshal(b, v)
	if err != nil {
		return fmt.Errorf("%w: %s: failed to unmarshal with codec: %s", ErrTypeNotValid, name, err)
	}

	return nil
}

func validateTypeName(name string) error {
	if !nameRegex.MatchString(name) {
		return fmt.Errorf("%w: name %q has invalid characters", ErrTypeNotValid, name)
	}
	return nil
}

func (r *Registry) addType(name string, typeDec *Type) {
	r.types[name] = typeDec

	v := typeDec.Init()
	rt := reflect.TypeOf(v)

	r.rtypes[rt] = name
	r.rtypes[rt.Elem()] = name
}

func (r *Registry) Marshal(data any) ([]byte, error) {
	_, err := r.Lookup(data)
	if err != nil {
		return nil, err
	}

	b, err := r.codec.Marshal(data)
	if err != nil {
		return b, fmt.Errorf("%T, marshal error: %w", data, err)
	}
	return b, nil
}

func (r *Registry) Codec() codec.Codec {
	return r.codec
}

func NewRegistry(typeDecs map[string]*Type) (*Registry, error) {
	r := &Registry{
		rtypes: make(map[reflect.Type]string),
		types:  make(map[string]*Type),
		codec:  codec.Default,
	}

	for n, typeDec := range typeDecs {
		err := r.validate(n, typeDec)
		if err != nil {
			return nil, err
		}
		r.addType(n, typeDec)
	}

	return r, nil
}
