package snowflake

import (
	"context"
	"errors"
	"github.com/sony/sonyflake"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sort"
)

var (
	NoSuchNodeErr          = errors.New("not such node")
	SonyflakeHasNotInitErr = errors.New("sonyflake not init")
)

type Generator struct {
	etcd      *clientv3.Client
	addr      string
	sonyFlake *sonyflake.Sonyflake
}

// NewGenerator create a new generator
func NewGenerator(etcd *clientv3.Client, addr string) (*Generator, error) {
	gen := &Generator{
		etcd: etcd,
		addr: addr,
	}

	machineID, err := gen.setMachineID(addr)
	if err != nil {
		return nil, err
	}

	var st sonyflake.Settings
	st.MachineID = func() (uint16, error) {
		return machineID, nil
	}

	return &Generator{
		etcd:      etcd,
		addr:      addr,
		sonyFlake: sonyflake.NewSonyflake(st),
	}, nil
}

// GetID get a snowflake id
func (gen *Generator) GetID() (uint64, error) {
	if gen.sonyFlake == nil {
		return 0, SonyflakeHasNotInitErr
	}

	id, err := gen.sonyFlake.NextID()
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (gen *Generator) setMachineID(addr string) (uint16, error) {
	if len(addr) == 0 {
		return 0, errors.New("address is empty")
	}

	ctx := context.Background()
	key := "machine_id_" + addr
	if _, err := gen.etcd.Put(ctx, key, "0"); err != nil {
		return 0, err
	}

	// get all machine_id
	resp, err := gen.etcd.Get(ctx, "machine_id", clientv3.WithPrefix())
	if err != nil {
		return 0, err
	}

	kvList := resp.Kvs

	// sort by create revision
	sort.Slice(kvList, func(i, j int) bool {
		return kvList[i].CreateRevision > kvList[j].CreateRevision // new machine_id is in front
	})

	// find the machine_id
	for i, kv := range kvList {
		if string(kv.Key) == key {
			return uint16(i), nil
		}
	}
	return 0, NoSuchNodeErr
}
