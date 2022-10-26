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
	"os"
	"strconv"

	cdkutil "github.com/linkall-labs/cdk-go/utils"
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
	Target    string     `json:"v_target" yaml:"vTarget"`
	Port      int        `json:"v_port" yaml:"vPort"`
	StoreType StateStore `json:"v_store_type" yaml:"vStoreType"`
	StoreURI  string     `json:"v_store_uri" yaml:"vStoreURI"`
}

func initConnectorConfig() (*Config, error) {
	cfg := &Config{}
	configFile := os.Getenv(configFileEnv)
	if configFile == "" {
		// default path
		configFile = "./config.yaml"
	}

	if err := cdkutil.ParseConfig(os.Getenv(configFileEnv), cfg); err != nil {
		return nil, err
	}

	if cfg.Target == "" {
		cfg.Target = os.Getenv(targetEndpointEnv)
	}

	if cfg.Port <= 0 {
		portStr := os.Getenv(portEnv)
		if portStr != "" {
			p, err := strconv.ParseInt(portStr, 10, 16)
			if err != nil {
				return nil, fmt.Errorf("the v_port invalid")
			}
			cfg.Port = int(p)
		} else {
			// default port
			cfg.Port = 8080
		}
	}
	return cfg, nil
}
