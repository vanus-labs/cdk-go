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

package util

import (
	"encoding/json"
	"os"

	"gopkg.in/yaml.v3"
)

const (
	EnvConnectorConfigType = "CONNECTOR_CONFIG_TYPE"
	EnvConfigFile          = "CONNECTOR_CONFIG"
)

func getConfigFilePath() string {
	file := os.Getenv(EnvConfigFile)
	if file == "" {
		file = "config.yml"
	}
	return file
}

func ReadConfigFile() ([]byte, error) {
	return os.ReadFile(getConfigFilePath())
}

func ParseConfig(data []byte, v interface{}) error {
	configType := os.Getenv(EnvConnectorConfigType)
	if configType == "" {
		// config default use yaml
		configType = "yaml"
	}
	return parseConfig(configType, data, v)
}

func parseConfig(t string, data []byte, v interface{}) error {
	if t == "json" {
		return json.Unmarshal(data, v)
	}
	return yaml.Unmarshal(data, v)
}
