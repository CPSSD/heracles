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
}

func setOptions() {
	flag.String("broker.queue_name", "", "queue name")
	flag.String("broker.address", "", "address of the broker")
	flag.String("state.backend", "", "backend of the state store")
	flag.String("state.location", "", "path to the file store")

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
func GetString(key string) string {
	return settings.GetString(key)
}

// Set a value in the settings
func Set(key string, value interface{}) {
	settings.Set(key, value)
}
