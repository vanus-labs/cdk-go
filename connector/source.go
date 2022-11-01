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
	"context"
	"os"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/cdk-go/log"
	cdkutil "github.com/linkall-labs/cdk-go/utils"
)

// Source is the interface a source connector expected to implement
type Source interface {
	Connector
	Run() error
	//Adapt transforms data into CloudEvents
	Adapt(args ...interface{}) cloudevents.Event
}

//RunSource method is used to run a source connector
func RunSource(source Source) {
	logger := log.NewLogger()
	logger.SetName(source.Name())
	source.SetLogger(logger)
	//cfg, err := initConnectorConfig()
	//if err != nil {
	//	panic("init global config file failed: " + err.Error())
	//}
	logger.Info("init global configuration success", nil)

	if err := source.Init(os.Getenv(configFileEnv), os.Getenv(secretFileEnv)); err != nil {
		panic("init config file failed: " + err.Error())
	}
	logger.Info("init connector configuration success", nil)

	var ctx = cdkutil.SetupSignalContext()
	ctx = context.WithValue(ctx, log.ConnectorName, source.Name())

	wait(ctx, source, source.Run)
	if err := source.Destroy(); err != nil {
		log.Warning("there was error when destroy sink", map[string]interface{}{
			log.KeyError: err,
		})
	} else {
		log.Info("the sink server has been shutdown gracefully", map[string]interface{}{
			log.ConnectorName: source.Name(),
		})
	}
}
