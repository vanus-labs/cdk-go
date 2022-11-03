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

package connector

import (
	"fmt"
	cdkutil "github.com/linkall-labs/cdk-go/utils"
	"os"
	"strconv"
)

type StateStore string

const (
	targetEndpointEnv = "CONNECTOR_TARGET"
	portEnv           = "CONNECTOR_PORT"
	configFileEnv     = "CONNECTOR_CONFIG"
	secretFileEnv     = "CONNECTOR_SECRET"
)

var (
	fileStateStore = StateStore("file")
	etcdStateStore = StateStore("etcd")
)

type Config struct {
	Target    string     `json:"v_target" yaml:"v_target"`
	Port      int        `json:"v_port" yaml:"v_port"`
	StoreType StateStore `json:"v_store_type" yaml:"v_store_type"`
	StoreURI  string     `json:"v_store_uri" yaml:"v_store_uri"`
}

func initConnectorConfig() (*Config, error) {
	cfg := &Config{}
	if err := cdkutil.ParseConfig(os.Getenv(configFileEnv), cfg); err != nil {
		return nil, err
	}

	if cfg.Target == "" {
		cfg.Target = os.Getenv(targetEndpointEnv)
		if cfg.Target == "" {
			return nil, fmt.Errorf("the v_target can't be empty")
		}
	}

	if cfg.Port <= 0 {
		p, err := strconv.ParseInt(os.Getenv(portEnv), 10, 16)
		if err != nil {
			return nil, fmt.Errorf("the v_port is empty or invalid")
		}
		cfg.Port = int(p)
	}
	return cfg, nil
}
