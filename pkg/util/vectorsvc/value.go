// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vectorsvc

import (
	"fmt"

	vectorsvcproto "github.com/pingcap/tidb/pkg/util/vectorsvc/proto/v1"
)

// MapValueToProto converts a Go value to a proto Value.
func MapValueToProto(value interface{}) *vectorsvcproto.Value {
	switch v := value.(type) {
	case string:
		return &vectorsvcproto.Value{Kind: &vectorsvcproto.Value_StringVal{StringVal: v}}
	case int:
		return &vectorsvcproto.Value{Kind: &vectorsvcproto.Value_Int64Val{Int64Val: int64(v)}}
	case int64:
		return &vectorsvcproto.Value{Kind: &vectorsvcproto.Value_Int64Val{Int64Val: v}}
	case uint:
		return &vectorsvcproto.Value{Kind: &vectorsvcproto.Value_Uint64Val{Uint64Val: uint64(v)}}
	case uint32:
		return &vectorsvcproto.Value{Kind: &vectorsvcproto.Value_Uint64Val{Uint64Val: uint64(v)}}
	case uint64:
		return &vectorsvcproto.Value{Kind: &vectorsvcproto.Value_Uint64Val{Uint64Val: v}}
	case float64:
		return &vectorsvcproto.Value{Kind: &vectorsvcproto.Value_Float64Val{Float64Val: v}}
	case bool:
		return &vectorsvcproto.Value{Kind: &vectorsvcproto.Value_BoolVal{BoolVal: v}}
	default:
		return &vectorsvcproto.Value{Kind: &vectorsvcproto.Value_StringVal{StringVal: fmt.Sprintf("%v", v)}}
	}
}
