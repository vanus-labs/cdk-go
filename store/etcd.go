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
	"path"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type etcdStore struct {
	client    *clientv3.Client
	keyPrefix string
}

func NewEtcdStore(endpoints []string, keyPrefix string) (KVStore, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:            endpoints,
		DialTimeout:          5 * time.Second,
		DialKeepAliveTime:    time.Second,
		DialKeepAliveTimeout: 3 * time.Second,
	})
	if err != nil {
		return nil, errors.Wrap(err, "init etcd client error")
	}
	return &etcdStore{client: client, keyPrefix: keyPrefix}, nil
}

func (s *etcdStore) Set(ctx context.Context, key string, value []byte) error {
	key = path.Join(s.keyPrefix, key)
	_, err := s.client.Put(ctx, key, string(value))
	return err
}

func (s *etcdStore) Get(ctx context.Context, key string) ([]byte, error) {
	key = path.Join(s.keyPrefix, key)
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, ErrKeyNotExist
	}
	return resp.Kvs[0].Value, nil
}

func (s *etcdStore) Delete(ctx context.Context, key string) error {
	key = path.Join(s.keyPrefix, key)
	_, err := s.client.Delete(ctx, key)
	if err != nil {
		return err
	}
	return nil
}

func (s *etcdStore) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.Close()
}
