package errmsg

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/assert"
)

type testError struct {
	orgErr   error
	tagedErr error
	tag      errTag
}

// TestErrTag tests that the error tag is added to the error message correctly.
func TestErrTag(t *testing.T) {
	testErrs := make([]testError, 0, 20)

	for i := 0; i < 20; i++ {
		testErr := testError{
			orgErr: errors.Errorf("test error %d", i),
			tag:    errTag(rand.Intn(8)),
		}
		switch testErr.tag {
		case errUserPrefix:
			testErr.tagedErr = WithUserPrefixErrTag(testErr.orgErr)
		case errRequireSecureTransport:
			testErr.tagedErr = WithRequireSecureTransportErrTag(testErr.orgErr)
		case errResourceUnit:
			testErr.tagedErr = WithResourceUnitErrTag(testErr.orgErr)
		case errServerlessNotSupport:
			testErr.tagedErr = WithServerlessNotSupportErrTag(testErr.orgErr)
		case errInvisibleTable:
			testErr.tagedErr = WithInvisibleTableErrTag(testErr.orgErr)
		case errInvisibleSysVar:
			testErr.tagedErr = WithInvisibleSysVarErrTag(testErr.orgErr)
		default:
			testErr.tagedErr = testErr.orgErr
		}
		testErrs = append(testErrs, testErr)
	}

	for i := 0; i < 20; i++ {
		tag := testErrs[i].tag
		tagedErr := testErrs[i].tagedErr
		switch tag {
		case errUserPrefix:
			assert.True(t, strings.HasPrefix(tagedErr.Error(), fmt.Sprintf("WithErrTag:%d", errUserPrefix)))
		case errRequireSecureTransport:
			assert.True(t, strings.HasPrefix(tagedErr.Error(), fmt.Sprintf("WithErrTag:%d", errRequireSecureTransport)))
		case errResourceUnit:
			assert.True(t, strings.HasPrefix(tagedErr.Error(), fmt.Sprintf("WithErrTag:%d", errResourceUnit)))
		case errServerlessNotSupport:
			assert.True(t, strings.HasPrefix(tagedErr.Error(), fmt.Sprintf("WithErrTag:%d", errServerlessNotSupport)))
		case errInvisibleTable:
			assert.True(t, strings.HasPrefix(tagedErr.Error(), fmt.Sprintf("WithErrTag:%d", errInvisibleTable)))
		case errInvisibleSysVar:
			assert.True(t, strings.HasPrefix(tagedErr.Error(), fmt.Sprintf("WithErrTag:%d", errInvisibleSysVar)))
		default:
			assert.False(t, strings.HasPrefix(tagedErr.Error(), "WithErrTag:"))
		}
	}
}

// TestExtendErrorMessage tests the ExtendErrorMessage function that can extend the error message correctly.
func TestExtendErrorMessage(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.ExtendedErrorMsgs = map[string]string{
		"user-prefix-error":              "extend user prefix error message",
		"ratelimit-error":                "extend ratelimit error message",
		"require-secure-transport-error": "extend require secure transport error message",
		"resource-unit-error":            "extend resource unit error message",
		"serverless-not-support-error":   "extend serverless not support error message",
		"invisible-table-error":          "extend invisible table error message",
		"invisible-sysvar-error":         "extend invisible sysvar error message",
	}
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
	}()

	for i := 0; i < 20; i++ {
		testErr := testError{
			orgErr: errors.Errorf("test error %d", i),
			tag:    errTag(rand.Intn(8)),
		}
		switch testErr.tag {
		case errUserPrefix:
			testErr.tagedErr = WithUserPrefixErrTag(testErr.orgErr)
		case errRequireSecureTransport:
			testErr.tagedErr = WithRequireSecureTransportErrTag(testErr.orgErr)
		case errResourceUnit:
			testErr.tagedErr = WithResourceUnitErrTag(testErr.orgErr)
		case errServerlessNotSupport:
			testErr.tagedErr = WithServerlessNotSupportErrTag(testErr.orgErr)
		case errInvisibleTable:
			testErr.tagedErr = WithInvisibleTableErrTag(testErr.orgErr)
		case errInvisibleSysVar:
			testErr.tagedErr = WithInvisibleSysVarErrTag(testErr.orgErr)
		default:
			testErr.tagedErr = testErr.orgErr
		}
		m := mysql.NewErrf(mysql.ErrUnknown, "%s", nil, testErr.orgErr.Error())
		ExtendErrorMessage(testErr.tagedErr, m)

		switch testErr.tag {
		case errUserPrefix:
			assert.True(t, strings.HasSuffix(m.Message, "extend user prefix error message."))
		case errRequireSecureTransport:
			assert.True(t, strings.HasSuffix(m.Message, "extend require secure transport error message."))
		case errResourceUnit:
			assert.True(t, strings.HasSuffix(m.Message, "extend resource unit error message."))
		case errServerlessNotSupport:
			assert.True(t, strings.HasSuffix(m.Message, "extend serverless not support error message."))
		case errInvisibleTable:
			assert.True(t, strings.HasSuffix(m.Message, "extend invisible table error message."))
		case errInvisibleSysVar:
			assert.True(t, strings.HasSuffix(m.Message, "extend invisible sysvar error message."))
		default:
			assert.Equal(t, m.Message, testErr.orgErr.Error())
		}
	}
}
