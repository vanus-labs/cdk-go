package store

import (
	"context"
	"github.com/linkall-labs/cdk-go/config"
	"github.com/linkall-labs/cdk-go/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
)

type KvStoreAccessor struct {
	Cli *clientv3.Client
	ctx context.Context
}

var Accessor KvStoreAccessor

func init() {
	etcdUrl := config.Accessor.EtcdUrl()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdUrl},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	Accessor = KvStoreAccessor{
		Cli: cli,
		ctx: context.Background(),
	}
	log.Info(Accessor.ctx, "Etcd connection success", map[string]interface{}{
		"etcd_url": etcdUrl,
	})
}

func (Accessor *KvStoreAccessor) Put(key string, value string) {
	_, err := Accessor.Cli.Put(Accessor.ctx, key, value)
	if err != nil {
		log.Error(Accessor.ctx, "Etcd put error", map[string]interface{}{
			log.KeyError: err,
		})
	}
}

func (Accessor *KvStoreAccessor) Get(key string) string {
	if getResp, err := Accessor.Cli.Get(Accessor.ctx, key); err != nil {
		log.Error(Accessor.ctx, "Etcd get error", map[string]interface{}{
			log.KeyError: err,
		})
		return ""
	} else {
		if len(getResp.Kvs) == 0 {
			log.Info(Accessor.ctx, "Etcd key not exist", map[string]interface{}{
				"key_not_exist": key,
			})
			return ""
		}
		val := getResp.Kvs[0].Value
		return string(val)
	}
}

func (Accessor *KvStoreAccessor) Delete(key string) error {
	if _, err := Accessor.Cli.Delete(Accessor.ctx, key); err != nil {
		log.Error(Accessor.ctx, "Etcd delete error", map[string]interface{}{
			log.KeyError: err,
		})
	}
	return nil
}
