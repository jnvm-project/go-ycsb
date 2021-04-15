package hpredis

import (
	"context"
	"encoding/json"
	"fmt"

	"gitlab.inf.telecom-sudparis.eu/YohanPipereau/go-redis-pmem/redis"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type hpredis struct {
	op redis.Operations
}

func (r *hpredis) Close() error {
	fmt.Printf("Closing Server\n");
	return nil
}

func (r *hpredis) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (r *hpredis) CleanupThread(_ context.Context) {
}

func (r *hpredis) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	data := make(map[string][]byte, len(fields))

	res, err := r.op.Hget(table + "/" + key)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(res, &data)
	if err != nil {
		return nil, err
	}

	// TODO: filter by fields

	return data, err
}

func (r *hpredis) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (r *hpredis) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	var data []byte
	var err error

	curVal := map[string][]byte{}
	for k, v := range values {
		curVal[k] = v
	}
	data, err = json.Marshal(curVal)
	if err != nil {
		return err
	}

	return r.op.Hset(table+"/"+key, string(data), 0)
}

func (r *hpredis) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	data, err := json.Marshal(values)
	if err != nil {
		fmt.Errorf("marshalling failed\n")
		return err
	}

	return r.op.Hset(table+"/"+key, string(data), 0)
}

func (r *hpredis) Delete(ctx context.Context, table string, key string) error {
	return r.op.Del(table + "/" + key)
}

type hpredisCreator struct{}

func (r hpredisCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	prds := &hpredis{}

	prds.op = redis.CreateOperations("/pmem0/coucou")

	return prds, nil
}

func init() {
	ycsb.RegisterDBCreator("hpredis", hpredisCreator{})
}
