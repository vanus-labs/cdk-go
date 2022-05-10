package config

import (
	"github.com/go-logr/logr"
	"github.com/tidwall/gjson"
	"linkall.com/cdk-go/v1/log"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"strings"
)

const (
	VanceConfigPath   string = "vance_config_path"
	VanceConfigPathDv string = "/vance/config/config.json"
	VanceSink         string = "vance_sink"
	VanceSinkDv       string = "vance_sink"
	VancePort         string = "vance_port"
	VancePortDv       string = "8081"
)

// ConfigAccessor provides an easy way to obtain configs
type ConfigAccessor struct {
	DefaultValues map[string]string
	Logger        logr.Logger
}

var Accessor ConfigAccessor
var userConfig map[string]string

func init() {
	log.SetLogger(zap.New())
	Accessor = ConfigAccessor{
		DefaultValues: map[string]string{
			VanceConfigPath: VanceConfigPathDv,
			VanceSink:       VanceSinkDv,
			VancePort:       VancePortDv,
		},
		Logger: log.Log.WithName("ConfigAccessor"),
	}
	configPath, existed := os.LookupEnv(strings.ToUpper(VanceConfigPath))
	if !existed {
		configPath = VanceConfigPathDv
	}
	userConfig = make(map[string]string)
	content, err := os.ReadFile(configPath)

	if err != nil {
		Accessor.Logger.Info("read vance config failed")
		content, err = os.ReadFile("./config.json")
		if err != nil {
			Accessor.Logger.Info("read local config failed")
		}
	}
	if len(content) != 0 {
		conf := gjson.ParseBytes(content).Map()
		Accessor.Logger.Info("conf length", "len", len(conf))

		for k, v := range conf {
			userConfig[k] = v.Str
		}

	}
}

func (a *ConfigAccessor) Get(key string) string {
	var ret string
	ret, existed := os.LookupEnv(key)
	if !existed {
		a.Logger.Info("userConfig length", "len", len(userConfig))
		ret = userConfig[key]
	}
	return ret
}

func (a *ConfigAccessor) getOrDefault(key string) string {
	ret := a.Get(key)
	if ret == "" {
		ret = a.DefaultValues[key]
	}
	return ret
}

func (a *ConfigAccessor) VanceSink() string {
	return a.getOrDefault(VanceSink)
}
func (a *ConfigAccessor) VancePort() string {
	return a.getOrDefault(VancePort)
}
