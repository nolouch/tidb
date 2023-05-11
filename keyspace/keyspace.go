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

package keyspace

import (
	"fmt"
	"os"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/tikv"
)

const (
	// tidbKeyspaceEtcdPathPrefix is the keyspace prefix for etcd namespace
	tidbKeyspaceEtcdPathPrefix = "/keyspaces/tidb/"

	// EnvVarKeyspaceName is the system env name for keyspace name.
	EnvVarKeyspaceName = "KEYSPACE_NAME"
)

// CodecV1 represents api v1 codec.
var CodecV1 = tikv.NewCodecV1(tikv.ModeTxn)

// MakeKeyspaceEtcdNamespace return the keyspace prefix path for etcd namespace
func MakeKeyspaceEtcdNamespace(c tikv.Codec) string {
	if c.GetAPIVersion() == kvrpcpb.APIVersion_V1 {
		return ""
	}
	return fmt.Sprintf(tidbKeyspaceEtcdPathPrefix+"%d", c.GetKeyspaceID())
}

// GetKeyspaceNameBySettings is used to get Keyspace name setting.
func GetKeyspaceNameBySettings() (keyspaceName string) {
	keyspaceName = config.GetGlobalConfig().KeyspaceName
	if !IsKeyspaceNameEmpty(keyspaceName) {
		return keyspaceName
	}

	keyspaceName = os.Getenv(EnvVarKeyspaceName)
	config.UpdateGlobal(func(c *config.Config) {
		c.KeyspaceName = keyspaceName
	})
	return keyspaceName
}

// IsKeyspaceNameEmpty is used to determine whether keyspaceName is set.
func IsKeyspaceNameEmpty(keyspaceName string) bool {
	return keyspaceName == ""
}

// IsKvStorageKeyspaceSet return true if you get keyspace meta successes.
func IsKvStorageKeyspaceSet(store kv.Storage) bool {
	return store.GetCodec().GetKeyspace() != nil
}

// CheckKeyspaceName checks whether the keyspace name is equal to the name in the configuration.
func CheckKeyspaceName(keyspaceName string) error {
	configKeyspaceName := GetKeyspaceNameBySettings()
	// If the keyspace name is not set in the configuration, it is not checked.
	if keyspaceName == configKeyspaceName {
		return nil
	}
	return errors.Errorf("keyspace name: %s is not equal setting: %s", keyspaceName, configKeyspaceName)
}
