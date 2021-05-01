package predis

import (
	"context"
	"encoding/json"
	"fmt"

	"gitlab.inf.telecom-sudparis.eu/YohanPipereau/go-redis-pmem/redis"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/pingcap/go-ycsb/pkg/prop"
)

//type predisServer interface {
//	Get(key string)
//	Scan(cursor uint64, match string, count int64)
//	Set(key string, value interface{}, expiration time.Duration)
//	Del(keys ...string)
//	FlushDB()
//	Close() error
//}

type predis struct {
	op redis.Operations
}

func (r *predis) Close() error {
	fmt.Printf("Closing Server\n");
	return nil
}

func (r *predis) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (r *predis) CleanupThread(_ context.Context) {
}

func (r *predis) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	data := make(map[string][]byte, len(fields))

	res, err := r.op.Get(table + "/" + key)

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

func (r *predis) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (r *predis) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	d, err := r.op.Get(table + "/" + key)
	if err != nil {
		return err
	}

	curVal := map[string][]byte{}
	err = json.Unmarshal([]byte(d), &curVal)
	if err != nil {
		return err
	}
	for k, v := range values {
		curVal[k] = v
	}
	var data []byte
	data, err = json.Marshal(curVal)
	if err != nil {
		return err
	}

	return r.op.Set(table+"/"+key, string(data), 0)
}

func (r *predis) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	data, err := json.Marshal(values)
	if err != nil {
		fmt.Errorf("marshalling failed\n")
		return err
	}

	return r.op.Set(table+"/"+key, string(data), 0)
}

func (r *predis) Delete(ctx context.Context, table string, key string) error {
	return r.op.Del(table + "/" + key)
}

type predisCreator struct{}

func (r predisCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	prds := &predis{}

	prds.op = redis.CreateOperations("/pmem0/coucou", int(p.GetInt64(prop.RecordCount, 0)))

	return prds, nil
}

func init() {
	ycsb.RegisterDBCreator("predis", predisCreator{})
}
