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

var (
	sonyFlake     *sonyflake.Sonyflake
	sonyMachineID uint16
)

// Init init sonyflake and set machine id
func Init(etcd *clientv3.Client, addr string) error {
	err := setMachineID(etcd, addr)
	if err != nil {
		return err
	}

	var st sonyflake.Settings
	st.MachineID = func() (uint16, error) {
		return sonyMachineID, nil
	}
	sonyFlake = sonyflake.NewSonyflake(st)
	return nil
}

// GetID get a snowflake id
func GetID() (uint64, error) {
	if sonyFlake == nil {
		return 0, SonyflakeHasNotInitErr
	}

	id, err := sonyFlake.NextID()
	if err != nil {
		return 0, err
	}
	return id, nil
}

func setMachineID(etcd *clientv3.Client, addr string) error {
	if len(addr) == 0 {
		return errors.New("address is empty")
	}

	ctx := context.Background()
	key := "machine_id_" + addr
	if _, err := etcd.Put(ctx, key, "0"); err != nil {
		return err
	}

	// get all machine_id
	resp, err := etcd.Get(ctx, "machine_id", clientv3.WithPrefix())
	if err != nil {
		return err
	}

	kvList := resp.Kvs

	// sort by create revision
	sort.Slice(kvList, func(i, j int) bool {
		return kvList[i].CreateRevision > kvList[j].CreateRevision // new machine_id is in front
	})

	// find the machine_id
	for i, kv := range kvList {
		if string(kv.Key) == key {
			sonyMachineID = uint16(i)
			return nil
		}
	}
	return NoSuchNodeErr
}
