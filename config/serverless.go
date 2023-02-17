package config

// RatelimitConfig contains ratelimit configuration options.
type RatelimitConfig struct {
	FullSpeed           int   `toml:"full-speed" json:"full-speed"`
	FullSpeedCapacity   int   `toml:"full-speed-capacity" json:"full-speed-capacity"`
	LowSpeed            int   `toml:"low-speed" json:"low-speed"`
	LowSpeedCapacity    int   `toml:"low-speed-capacity" json:"low-speed-capacity"`
	LowSpeedWatermark   int64 `toml:"low-speed-watermark" json:"low-speed-watermark"`
	BlockWriteWatermark int64 `toml:"block-write-watermark" json:"block-write-watermark"`
}

// defaultRatelimitConfig creates a new RatelimitConfig.
func defaultRatelimitConfig() RatelimitConfig {
	return RatelimitConfig{
		FullSpeed:           1024 * 1024,            // 1MiB/s
		FullSpeedCapacity:   10 * 1024 * 1024,       // 10MiB
		LowSpeed:            1024 * 10,              // 10KiB/s
		LowSpeedCapacity:    1024 * 1024,            // 1MiB
		LowSpeedWatermark:   1024 * 1024 * 1024,     // 1GiB
		BlockWriteWatermark: 2 * 1024 * 1024 * 1024, // 2GiB
	}
}

// BootstrapControl contains ratelimit configuration options.
type BootstrapControl struct {
	SkipServerlessVariables bool `toml:"skip-serverless-variables" json:"skip-serverless-variables"`
	SkipRootPriv            bool `toml:"skip-root-priv" json:"skip-root-priv"`
	SkipCloudAdminPriv      bool `toml:"skip-cloud-admin-priv" json:"skip-cloud-admin-priv"`
	SkipRoleAdminPriv       bool `toml:"skip-role-admin-priv" json:"skip-role-admin-priv"`
	SkipPushdownBlacklist   bool `toml:"skip-pushdown-blacklist" json:"skip-pushdown-blacklist"`
}

// defaultBootstrapControl creates a new BootstrapControl.
func defaultBootstrapControl() BootstrapControl {
	return BootstrapControl{
		SkipServerlessVariables: false,
		SkipRootPriv:            false,
		SkipCloudAdminPriv:      false,
		SkipRoleAdminPriv:       false,
		SkipPushdownBlacklist:   false,
	}
}

// DefaultResourceGroup is the default resource group name for all txns and snapshots.
var DefaultResourceGroup string
