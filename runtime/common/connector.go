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

package common

import (
	"context"

	"github.com/go-playground/validator/v10"
	"github.com/pkg/errors"

	"github.com/vanus-labs/cdk-go/config"
	"github.com/vanus-labs/cdk-go/connector"
	"github.com/vanus-labs/cdk-go/util"
)

type Connector struct {
	Config    config.ConfigAccessor
	Connector connector.Connector
}

func (c *Connector) initConfig(ctx context.Context, config []byte) error {
	err := util.ParseConfig(config, c.Config)
	if err != nil {
		return errors.Wrap(err, "parse config error")
	}
	err = validator.New().StructCtx(ctx, c.Config)
	if err != nil {
		return errors.Wrap(err, "config tag validator has error")
	}
	err = c.Config.Validate()
	if err != nil {
		return errors.Wrap(err, "config validate error")
	}
	return nil
}

func (c *Connector) InitConnector(ctx context.Context, config []byte) error {
	err := c.initConfig(ctx, config)
	if err != nil {
		return errors.Wrap(err, "init config error")
	}
	err = c.Connector.Initialize(ctx, c.Config)
	if err != nil {
		return errors.Wrap(err, "sink Initialize error")
	}
	return nil
}
