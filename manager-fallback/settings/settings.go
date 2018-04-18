package settings

import (
	"flag"

	log "github.com/golang/glog"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var settings *viper.Viper

// Init initializes the settings
func Init() error {
	settings = viper.New()
	setDefaults()
	settings.SetConfigName("worker")
	settings.AddConfigPath("/etc/heracles/")
	setOptions()

	if err := settings.ReadInConfig(); err != nil {
		log.Errorf("%v", err)
	}

	log.V(1).Info("Running with the following settings")
	for k, v := range settings.AllSettings() {
		log.V(1).Infof("\t%s: %+v\n", k, v)
	}

	return nil
}

func setDefaults() {
	settings.SetDefault("broker.queue_name", "heracles_tasks")
	settings.SetDefault("broker.address", "")
	settings.SetDefault("state.backend", "file")
	settings.SetDefault("state.location", "")
	settings.SetDefault("server.port", 8081)
	settings.SetDefault("scheduler.input_chunk_size", 64*1024*1024*1024)
	settings.SetDefault("scheduler.intermediate_data_location", "/tmp/heracles_intermediate")
}

func setOptions() {
	flag.String("broker.queue_name", "", "queue name")
	flag.String("broker.address", "", "address of the broker")
	flag.String("state.backend", "", "backend of the state store")
	flag.String("state.location", "", "path to the file store")
	flag.Int("scheduler.input_chunk_size", 64*1024*1024*1024, "chunk size")
	flag.String("scheduler.intermediate_data_location", "", "location of intermediate files")
	flag.Int("server.port", 8081, "server port")

	flag.Parse()

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()
	settings.BindPFlags(pflag.CommandLine)
}

// Get returns the value of a key.
func Get(key string) interface{} {
	return settings.Get(key)
}

// GetString setting
// DEPRECATED: Use `String`
func GetString(key string) string {
	return String(key)
}

// String settings
func String(key string) string {
	return settings.GetString(key)
}

// Int setting
func Int(key string) int {
	return settings.GetInt(key)
}

// Set a value in the settings
func Set(key string, value interface{}) {
	settings.Set(key, value)
}
