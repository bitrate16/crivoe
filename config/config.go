package config

import "flag"

type Config struct {
	MemoryMode  bool
	StoragePath string
	Port        int
	Host        string
}

var config *Config

func parseConfig() {
	var args Config

	flag.IntVar(
		&args.Port,
		"port",
		8374,
		"server port",
	)

	flag.StringVar(
		&args.Host,
		"host",
		"0.0.0.0",
		"server host",
	)

	flag.StringVar(
		&args.StoragePath,
		"storage",
		"storage",
		"storage path",
	)

	flag.BoolVar(
		&args.MemoryMode,
		"memory",
		false,
		"memory mode",
	)

	flag.Parse()

	config = &args
}

func GetConfig() *Config {
	if config == nil {
		parseConfig()
	}

	return config
}
