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

var _ SinkConfigAccessor = &SinkConfig{}

type SinkConfigAccessor interface {
	ConfigAccessor
	// GetProtocolConfig receive event server use protocol, default http and port is 8080
	GetProtocolConfig() ProtocolConfigSinkList
}

type SinkConfig struct {
	Config   `json:",inline" yaml:",inline"`
	Protocol []ProtocolConfigSink `json:"protocol" yaml:"protocol"`
}

func (c *SinkConfig) GetSecret() SecretAccessor {
	return nil
}

func (c *SinkConfig) Validate() error {
	err := c.GetProtocolConfig().Validate()
	if err != nil {
		return err
	}
	return c.Config.Validate()
}

func (c *SinkConfig) ConnectorType() Type {
	return SinkConnector
}

func (c *SinkConfig) GetProtocolConfig() ProtocolConfigSinkList {
	t := getProtocolType()
	port := getPort()
	if len(c.Protocol) > 0 {
		for i := range c.Protocol {
			if c.Protocol[i].Type == "" {
				c.Protocol[i].Type = t
			}
			if c.Protocol[i].Port == 0 {
				c.Protocol[i].Port = port
			}
		}
		return c.Protocol
	}
	return []ProtocolConfigSink{{Type: t, Port: port}}
}
