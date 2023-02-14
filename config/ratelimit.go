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
