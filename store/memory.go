// Copyright 2022 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"context"
	"sync"
)

type memoryStore struct {
	data map[string][]byte
	lock sync.RWMutex
}

func NewMemoryStore() KVStore {
	return &memoryStore{
		data: map[string][]byte{},
	}
}

func (s *memoryStore) Set(ctx context.Context, key string, value []byte) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.data[key] = value
	if err := s.Save(ctx); err != nil {
		return err
	}
	return nil
}

func (s *memoryStore) Get(_ context.Context, key string) ([]byte, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	value, ok := s.data[key]
	if !ok {
		return nil, ErrKeyNotExist
	}
	return value, nil
}

func (s *memoryStore) Delete(ctx context.Context, key string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	_, ok := s.data[key]
	if !ok {
		return nil
	}
	delete(s.data, key)
	if err := s.Save(ctx); err != nil {
		return err
	}
	return nil
}

func (s *memoryStore) Close() error {
	return nil
}

func (s *memoryStore) Save(_ context.Context) error {
	return nil
}
