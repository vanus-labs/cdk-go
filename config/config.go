/*
Copyright 2022-Present The Vance Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

import (
	"os"
	"reflect"

	"github.com/pkg/errors"

	"github.com/linkall-labs/cdk-go/util"
)

type Type string

const (
	SinkConnector   Type = "sink"
	SourceConnector Type = "source"
)

type SecretAccessor interface {
}

type ConfigAccessor interface {
	ConnectorType() Type
	Validate() error
	// GetSecret SecretAccessor implement type must be pointer
	GetSecret() SecretAccessor
}

const (
	EnvTarget     = "CONNECTOR_TARGET"
	EnvPort       = "CONNECTOR_PORT"
	EnvConfigFile = "CONNECTOR_CONFIG"
	EnvSecretFile = "CONNECTOR_SECRET"

	defaultPort     = 8080
	defaultAttempts = 3
)

func ParseConfig(cfg ConfigAccessor) error {
	err := parseSecret(cfg.GetSecret())
	if err != nil {
		return err
	}
	err = parseConfig(cfg)
	if err != nil {
		return err
	}
	return nil
}

func parseConfig(cfg ConfigAccessor) error {
	file := os.Getenv(EnvConfigFile)
	if file == "" {
		file = "config.yaml"
	}
	err := util.ParseConfig(file, cfg)
	if err != nil {
		return err
	}
	return nil
}

func parseSecret(secret SecretAccessor) error {
	if secret == nil {
		return nil
	}
	v := reflect.ValueOf(secret)
	if v.Kind() != reflect.Ptr {
		return errors.New("secret type must be pointer")
	}
	file := os.Getenv(EnvSecretFile)
	if file == "" {
		file = "secret.yaml"
	}
	err := util.ParseConfig(file, secret)
	if err != nil {
		return err
	}
	return nil
}
