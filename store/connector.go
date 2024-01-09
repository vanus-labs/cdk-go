package store

import (
	"context"
	"path"
)

func NewConnectorStore(connectorID string) KVStore {
	return &connectorStore{connectorID: connectorID}
}

type connectorStore struct {
	connectorID string
}

func (s *connectorStore) Set(ctx context.Context, key string, value []byte) error {
	if s == nil || kvStore == nil {
		return nil
	}
	key = path.Join(s.connectorID, key)
	return kvStore.Set(ctx, key, value)
}

func (s *connectorStore) Get(ctx context.Context, key string) ([]byte, error) {
	if s == nil || kvStore == nil {
		return nil, nil
	}
	key = path.Join(s.connectorID, key)
	return kvStore.Get(ctx, key)
}
func (s *connectorStore) Delete(ctx context.Context, key string) error {
	if s == nil || kvStore == nil {
		return nil
	}
	key = path.Join(s.connectorID, key)
	return kvStore.Delete(ctx, key)
}
func (s *connectorStore) Close() error {
	return nil
}

type storeKey struct{}

func WithKVStore(ctx context.Context, store KVStore) context.Context {
	return context.WithValue(ctx, storeKey{}, store)
}

func FromContext(ctx context.Context) KVStore {
	if store, ok := ctx.Value(storeKey{}).(KVStore); ok {
		return store
	}
	return kvStore
}
