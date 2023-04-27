package errmsg

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pkg/errors"
)

type errTag int

const (
	unknownErrTag errTag = iota
	errUserPrefix
	errMaxSleepSeconds
	errRequireSecureTransport
	errResourceUnit
	errServerlessNotSupport
	errInvisibleTable
	errInvisibleSysVar
	errRatelimit
	errMinTiFlashReplica
)

// The tag name is used to find the extended error message from config file.
// For example, the tag name of errUserPrefix is "user-prefix-error",
// if we want to extend the error message of errUserPrefix,
// we can add the following config to the config file:
// ```
// [extended-error-messages]
// user-prefix-error = "see https://docs.pingcap.com/tidbcloud/select-cluster-tier#user-name-prefix for more details"
// ```
func (e errTag) errTagName() string {
	switch e {
	case errUserPrefix:
		return "user-prefix-error"
	case errMaxSleepSeconds:
		return "max-sleep-seconds-error"
	case errRequireSecureTransport:
		return "require-secure-transport-error"
	case errResourceUnit:
		return "resource-unit-error"
	case errServerlessNotSupport:
		return "serverless-not-support-error"
	case errInvisibleTable:
		return "invisible-table-error"
	case errInvisibleSysVar:
		return "invisible-sysvar-error"
	case errRatelimit:
		return "ratelimit-error"
	case errMinTiFlashReplica:
		return "min-tiflash-replica-error"
	default:
		return "unknown"
	}
}

func withErrTag(err error, tag errTag) error {
	return errors.WithMessagef(err, "WithErrTag:%d", tag)
}

// WithUserPrefixErrTag is used to add a tag to the user prefix error.
func WithUserPrefixErrTag(err error) error {
	return withErrTag(err, errUserPrefix)
}

// WithMaxSleepSecondsErrTag is used to add a tag to the max sleep seconds error.
func WithMaxSleepSecondsErrTag(err error) error {
	return withErrTag(err, errMaxSleepSeconds)
}

// WithRequireSecureTransportErrTag is used to add a tag to the require secure transport error.
func WithRequireSecureTransportErrTag(err error) error {
	return withErrTag(err, errRequireSecureTransport)
}

// WithResourceUnitErrTag is used to add a tag to the resource unit error.
func WithResourceUnitErrTag(err error) error {
	return withErrTag(err, errResourceUnit)
}

// WithServerlessNotSupportErrTag is used to add a tag to the serverless not support error.
func WithServerlessNotSupportErrTag(err error) error {
	return withErrTag(err, errServerlessNotSupport)
}

// WithInvisibleTableErrTag is used to add a tag to the invisible table error.
func WithInvisibleTableErrTag(err error) error {
	return withErrTag(err, errInvisibleTable)
}

// WithInvisibleSysVarErrTag is used to add a tag to the invisible sysvar error.
func WithInvisibleSysVarErrTag(err error) error {
	return withErrTag(err, errInvisibleSysVar)
}

// WithRatelimitErrTag is used to add a tag to the ratelimit error.
func WithRatelimitErrTag(err error) error {
	return withErrTag(err, errRatelimit)
}

// WithMinTiFlashReplicaErrTag is used to add a tag to the min tiflash replica error.
func WithMinTiFlashReplicaErrTag(err error) error {
	return withErrTag(err, errMinTiFlashReplica)
}

func getErrTag(err error) errTag {
	errStr := err.Error()
	errTag := unknownErrTag
	if strings.HasPrefix(errStr, "WithErrTag:") {
		fmt.Sscanf(errStr, "WithErrTag:%d", &errTag)
	}
	return errTag
}

// ExtendErrorMessage is used to extend the error message.
func ExtendErrorMessage(err error, m *mysql.SQLError) {
	eems := config.GetGlobalConfig().ExtendedErrorMsgs
	if len(eems) == 0 {
		return
	}
	et := getErrTag(err)
	if et == unknownErrTag {
		return
	}
	if eem, ok := eems[et.errTagName()]; ok {
		extendErrorMessage(m, eem)
	}
}

func extendErrorMessage(m *mysql.SQLError, msg string) {
	m.Message = fmt.Sprintf("%s, %s.", strings.TrimSuffix(m.Message, "."), msg)
}
