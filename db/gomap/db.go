package gomap

import (
	"context"
	"fmt"

	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type contextKey string
const stateKey = contextKey("mapDB")

type mapState struct {
}

/*
 * Implement mapDB interface (pkg/ycsb/db.go)
 */

type mapDB struct {
	m	map[string][]byte
}

func (db *mapDB) Close() error {
	return nil
}

func (db *mapDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	state := new(mapState)

	return context.WithValue(ctx, stateKey, state)
}

func (db *mapDB) CleanupThread(_ context.Context) {
}

func (db *mapDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	if len(fields) == 0 { //
		fmt.Printf("Read fields=NULL")
		ret := map[string][]byte {
			"key": db.m[key],
		}
		return ret, nil
	} else {
		fmt.Printf("Read fields=%s\n", fields)
		ret := map[string][]byte {
			"key": db.m[key],
		}
		return ret, nil
	}
}

func (db *mapDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	fmt.Printf("Scan table=%s count=%d\n", table, count)
	return nil, nil
}

func (db *mapDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	pairs := util.NewFieldPairs(values)
	for _, p := range pairs {
		fmt.Printf("Update table=%s, key=%s field=%s\n", table, key, p.Field)
		db.m[key]=p.Value
	}

	return nil
}

func (db *mapDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	pairs := util.NewFieldPairs(values)
	for _, p := range pairs {
		fmt.Printf("Insert table=%s, key=%s, field=%s\n", table, key, p.Field)
		db.m[key]=p.Value
	}

	return nil
}

func (db *mapDB) Delete(ctx context.Context, table string, key string) error {
	fmt.Printf("Delete table=%s key=%s\n", table, key)
	delete(db.m, key)

	return nil
}

/*
 * Implement DBCreator interface (pkg/ycsb/db.go)
 */

type mapDBCreator struct{}

func (mapDBCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	db := new(mapDB)

	/* make([]T, length): alloc and initialize the map + return value
	 * new : alloc + return ptr */
	db.m = make(map[string][]byte)

	return db, nil
}

func init() {
	ycsb.RegisterDBCreator("map", mapDBCreator{})
}
