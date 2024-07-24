package config

import "flag"

type Config struct {
	MaxWorkers int
	MaxTasks   int
	Port       int
	Host       string
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

	flag.IntVar(
		&args.MaxWorkers,
		"max-workers",
		1,
		"max allowed amount of parallel workers",
	)

	flag.IntVar(
		&args.MaxTasks,
		"max-tasks",
		65536,
		"max allowed amount of waiting tasks in scheduler",
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
