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

package store

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/pkg/errors"
)

type fileStore struct {
	memoryStore
	fileName string
}

func NewFileStore(fileName string) (KVStore, error) {
	f := &fileStore{
		fileName: fileName,
	}
	if err := f.load(); err != nil {
		return nil, err
	}
	return f, nil
}

func (s *fileStore) load() error {
	data := make(map[string][]byte)
	b, err := ioutil.ReadFile(s.fileName)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return errors.Wrapf(err, "read file %s error", s.fileName)
		}
	} else {
		if err = json.Unmarshal(b, &data); err != nil {
			return errors.Wrapf(err, "json unmarshal error;[%s]", string(b))
		}
	}
	s.data = data
	return nil
}

func (s *fileStore) Save(_ context.Context) error {
	b, err := json.Marshal(s.data)
	if err != nil {
		return err
	}
	if err = ioutil.WriteFile(s.fileName, b, 0644); err != nil {
		return errors.Wrapf(err, "write file %s error", s.fileName)
	}
	return nil
}
