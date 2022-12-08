/*
Copyright 2022-Present The Vance Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this fileStore except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

import "github.com/pkg/errors"

type StoreType string

const (
	MemoryStore StoreType = "memory"
	FileStore   StoreType = "file"
	EtcdStore   StoreType = "etcd"
)

type StoreConfig struct {
	StoreType StoreType `json:"v_store_type" yaml:"v_store_type"`
	// file store
	StoreFile string `json:"v_store_file" yaml:"v_store_file"`
	// etcd store
	Endpoints []string `json:"v_store_endpoints" yaml:"v_store_endpoints"`
	KeyPrefix string   `json:"v_store_key_prefix" yaml:"v_store_key_prefix"`
}

func (c *StoreConfig) Validate() error {
	if c == nil {
		return nil
	}
	switch c.StoreType {
	case FileStore:
		if c.StoreFile == "" {
			return errors.New("config storeType is file, but config storeFile is empty")
		}
	case EtcdStore:
		if len(c.Endpoints) == 0 {
			return errors.New("config storeType is etcd, but config endpoints is empty")
		}
	}
	return nil
}