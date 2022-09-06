package store

import (
	"context"
	"github.com/linkall-labs/cdk-go/connector"
	"github.com/linkall-labs/cdk-go/log"
	cdkutil "github.com/linkall-labs/cdk-go/utils"
	clientv3 "go.etcd.io/etcd/client/v3"
	"os"
	"time"
)

const (
	configFileEnv = "CONNECTOR_CONFIG"
	etcdUrlDv     = "localhost:2379"
)

type KVStoreAccessor interface {
	Put(key string, value string)
	Get(key string) string
	Delete(key string) error
}

type etcdAccessor struct {
	Cli *clientv3.Client
	ctx context.Context
}

var KVAccessor KVStoreAccessor

func init() {
	cfg := &connector.Config{}
	if err := cdkutil.ParseConfig(os.Getenv(configFileEnv), cfg); err != nil {
		log.Error(context.Background(), "Config load error", map[string]interface{}{
			"error": err,
		})
	}
	if cfg.StoreType == "etcd" {
		if etcdKVAccessor, err := initEtcdAccessor(cfg.StoreURI); err != nil {
			log.Error(context.Background(), "Etcd init error", map[string]interface{}{
				"error": err,
			})
		} else {
			KVAccessor = etcdKVAccessor
		}
	} else {
		log.Error(context.Background(), "Only etcd is supported for the time being", map[string]interface{}{})
	}
}

func initEtcdAccessor(etcdUrl string) (*etcdAccessor, error) {
	if etcdUrl == "" {
		etcdUrl = etcdUrlDv
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdUrl},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(err)
		return nil, err
	}
	EtcdKVAccessor := &etcdAccessor{
		Cli: cli,
		ctx: context.Background(),
	}
	log.Info(EtcdKVAccessor.ctx, "Etcd connection success", map[string]interface{}{
		"etcd_url": etcdUrl,
	})
	return EtcdKVAccessor, nil
}

func (EtcdKVAccessor *etcdAccessor) Put(key string, value string) {
	_, err := EtcdKVAccessor.Cli.Put(EtcdKVAccessor.ctx, key, value)
	if err != nil {
		log.Error(EtcdKVAccessor.ctx, "Etcd put error", map[string]interface{}{
			log.KeyError: err,
		})
	}
}

func (EtcdKVAccessor *etcdAccessor) Get(key string) string {
	if getResp, err := EtcdKVAccessor.Cli.Get(EtcdKVAccessor.ctx, key); err != nil {
		log.Error(EtcdKVAccessor.ctx, "Etcd get error", map[string]interface{}{
			log.KeyError: err,
		})
		return ""
	} else {
		if len(getResp.Kvs) == 0 {
			log.Info(EtcdKVAccessor.ctx, "Etcd key not exist", map[string]interface{}{
				"key_not_exist": key,
			})
			return ""
		}
		val := getResp.Kvs[0].Value
		return string(val)
	}
}

func (EtcdKVAccessor *etcdAccessor) Delete(key string) error {
	if _, err := EtcdKVAccessor.Cli.Delete(EtcdKVAccessor.ctx, key); err != nil {
		log.Error(EtcdKVAccessor.ctx, "Etcd delete error", map[string]interface{}{
			log.KeyError: err,
		})
	}
	return nil
}
