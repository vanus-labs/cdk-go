package store

import (
	"context"
	"github.com/linkall-labs/cdk-go/connector"
	"github.com/linkall-labs/cdk-go/log"
	cdkutil "github.com/linkall-labs/cdk-go/utils"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

const (
	configFileEnv = "CONNECTOR_CONFIG"
	etcdUrlDv     = "localhost:2379"
)

type KVStoreAccessor interface {
	Put(ctx context.Context, key string, value string)
	Get(ctx context.Context, key string) string
	Delete(ctx context.Context, key string) error
}

type etcdAccessor struct {
	Cli *clientv3.Client
}

func NewKVStore(cfgPath string) KVStoreAccessor {
	cfg := &connector.Config{}
	if err := cdkutil.ParseConfig(cfgPath, cfg); err != nil {
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
			return etcdKVAccessor
		}
	} else {
		log.Error(context.Background(), "Only etcd is supported for the time being", map[string]interface{}{})
	}
	return nil
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
	}
	log.Info(context.Background(), "Etcd connection success", map[string]interface{}{
		"etcd_url": etcdUrl,
	})
	return EtcdKVAccessor, nil
}

func (EtcdKVAccessor *etcdAccessor) Put(ctx context.Context, key string, value string) {
	_, err := EtcdKVAccessor.Cli.Put(ctx, key, value)
	if err != nil {
		log.Error(ctx, "Etcd put error", map[string]interface{}{
			log.KeyError: err,
		})
	}
}

func (EtcdKVAccessor *etcdAccessor) Get(ctx context.Context, key string) string {
	if getResp, err := EtcdKVAccessor.Cli.Get(ctx, key); err != nil {
		log.Error(ctx, "Etcd get error", map[string]interface{}{
			log.KeyError: err,
		})
		return ""
	} else {
		if len(getResp.Kvs) == 0 {
			log.Info(ctx, "Etcd key not exist", map[string]interface{}{
				"key_not_exist": key,
			})
			return ""
		}
		val := getResp.Kvs[0].Value
		return string(val)
	}
}

func (EtcdKVAccessor *etcdAccessor) Delete(ctx context.Context, key string) error {
	if _, err := EtcdKVAccessor.Cli.Delete(ctx, key); err != nil {
		log.Error(ctx, "Etcd delete error", map[string]interface{}{
			log.KeyError: err,
		})
	}
	return nil
}
