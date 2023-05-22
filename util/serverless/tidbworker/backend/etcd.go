// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"context"
	"path"
	"strconv"
	"time"

	"github.com/pingcap/tidb/ddl/util"
	"github.com/tikv/client-go/v2/tikv"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	rootPath   = "/tidb/remote/worker/"
	retryCount = 1
	// This is the value of the key, which is currently not used; it may be used to specify the total number of workers
	// requested in the future. For compatibility, it's currently set to "1".
	keyVal = "1"
)

var _ Backend = &etcdBackend{}

// etcdBackend is the tidb worker backend implementation using etcd.
type etcdBackend struct {
	keyspaceIDStr string
	cli           *clientv3.Client
}

// NewEtcdBackend creates a new etcd backend.
func NewEtcdBackend(etcdClient *clientv3.Client, keyspaceID tikv.KeyspaceID) Backend {
	return &etcdBackend{
		keyspaceIDStr: strconv.FormatInt(int64(keyspaceID), 10),
		cli:           etcdClient,
	}
}

// getEtcdKeyPath is a helper function that assembles the complete etcd key from the information given.
// e.g. /tidb/remote/worker/{keyType}/{keyspaceID}/{timestamp}
func getEtcdKeyPath(keyType string, keyspaceIDStr string, timestamp time.Time) string {
	return path.Join(rootPath, keyType, keyspaceIDStr, strconv.FormatInt(timestamp.Unix(), 10))
}

// Set writes the composed key into etcd.
func (e *etcdBackend) Set(ctx context.Context, KeyType string, timestamp time.Time) error {
	keyPath := getEtcdKeyPath(KeyType, e.keyspaceIDStr, timestamp)
	return util.PutKVToEtcd(ctx, e.cli, retryCount, keyPath, keyVal)
}

// DelBefore compose left and right bound using the given timestamp and delete all keys in the range.
func (e *etcdBackend) DelBefore(ctx context.Context, KeyType string, timestamp time.Time) error {
	leftBound := getEtcdKeyPath(KeyType, e.keyspaceIDStr, time.Unix(0, 0))
	rightBound := getEtcdKeyPath(KeyType, e.keyspaceIDStr, timestamp)
	ctx, cancel := context.WithTimeout(ctx, util.KeyOpDefaultTimeout)
	defer cancel()
	_, err := e.cli.Delete(ctx, leftBound, clientv3.WithRange(rightBound))
	return err
}

// Empty checks whether the key type is empty via the etcd key with keyspaceID prefix.
func (e *etcdBackend) Empty(ctx context.Context, KeyType string) (bool, error) {
	pathPrefix := path.Join(rootPath, KeyType, e.keyspaceIDStr) + "/"
	ctx, cancel := context.WithTimeout(ctx, util.KeyOpDefaultTimeout)
	resp, err := e.cli.Get(ctx, pathPrefix, clientv3.WithPrefix())
	cancel()
	if err != nil {
		return false, err
	}
	return len(resp.Kvs) == 0, nil
}

// Close closes the etcd client.
func (e *etcdBackend) Close() error {
	return e.cli.Close()
}
