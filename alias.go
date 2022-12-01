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

package cdkgo

import (
	"github.com/linkall-labs/cdk-go/config"
	"github.com/linkall-labs/cdk-go/connector"
	"github.com/linkall-labs/cdk-go/runtime"
)

type ConfigAccessor = config.ConfigAccessor
type SecretAccessor = config.SecretAccessor

type SourceConfig = config.SourceConfig
type Tuple = connector.Tuple

type SinkConfig = config.SinkConfig
type Result = connector.Result

var (
	NewTuple  = connector.NewTuple
	RunSource = runtime.RunSource

	SuccessResult = connector.Success
	NewResult     = connector.NewResult
	RunSink       = runtime.RunSink
)
