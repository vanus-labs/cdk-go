// Copyright 2023 Linkall Inc.
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

package config

import (
	"net/url"

	"github.com/pkg/errors"
)

type ProtocolType string

const (
	HTTPProtocol  ProtocolType = "http"
	VanusProtocol ProtocolType = "vanus"
)

type ProtocolConfigSink struct {
	Type ProtocolType `json:"type" yaml:"type" validate:"require"`
	Port int          `json:"port" yaml:"port" validate:"require"`
}

func (p *ProtocolConfigSink) Validate() error {
	exist := isProtocolExist(p.Type)
	if !exist {
		return errors.Errorf("unknown protocol type %s", p.Type)
	}
	return nil
}

type ProtocolConfigSinkList []ProtocolConfigSink

func (l ProtocolConfigSinkList) Validate() error {
	ports := make(map[int]struct{}, len(l))
	types := make(map[ProtocolType]struct{}, len(l))
	for _, p := range l {
		err := p.Validate()
		if err != nil {
			return err
		}
		// repeat protocol type.
		_, exist := types[p.Type]
		if exist {
			return errors.Errorf("config protocol type is repeat")
		}
		types[p.Type] = struct{}{}
		// check port repeat.
		_, exist = ports[p.Port]
		if exist {
			return errors.Errorf("config protocol port is used by other protocol")
		}
		ports[p.Port] = struct{}{}
	}
	return nil
}

type ProtocolConfigSource struct {
	Type ProtocolType `json:"type" yaml:"type"`

	Target string `json:"target" yaml:"target"`

	Eventbus string `json:"eventbus" yaml:"eventbus"`
}

func (p *ProtocolConfigSource) Validate() error {
	if p == nil {
		return nil
	}
	exist := isProtocolExist(p.Type)
	if !exist {
		return errors.Errorf("unknown protocol type %s", p.Type)
	}
	if p.Target == "" {
		return errors.New("target is empty")
	}
	_, err := url.Parse(p.Target)
	if err != nil {
		return errors.Wrapf(err, "parse target %s error", p.Target)
	}
	switch p.Type {
	case HTTPProtocol:
	case VanusProtocol:
		if p.Eventbus == "" {
			return errors.New("protocol type is vanus,but eventbus is empty")
		}
	}
	return nil
}

func isProtocolExist(t ProtocolType) bool {
	switch t {
	case HTTPProtocol, VanusProtocol:
		return true
	default:
		return false
	}
}
