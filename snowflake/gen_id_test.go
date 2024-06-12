package snowflake

import (
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
	"testing"
	"time"
)

func createEtcdClient() (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{
			"http://192.168.185.128:20000",
			"http://192.168.185.128:20002",
			"http://192.168.185.128:20004",
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return cli, nil
}

func TestGetID(t *testing.T) {
	cli, err := createEtcdClient()
	assert.Nil(t, err)

	gennerator, err := NewGenerator(cli, "8080")
	assert.Nil(t, err)

	id, err := gennerator.GetID()
	assert.Nil(t, err)
	t.Logf("id: %d", id)
}
