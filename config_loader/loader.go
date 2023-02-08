//

package config_loader

import (
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
)

type Configuration interface {
	// DefaultValue returns the default value for the given field.
	DefaultValue(field string) interface{}

	// SetField is used to set the value of a field from environment variables.
	SetField(field string, value interface{})
}

type ConfigOption func(*LoaderOpts)

// LoaderOpts is a struct that contains options for the config loader.
type LoaderOpts struct {
	// configPath is the path to the config file with either .json or .yaml extension.
	configPath string

	// envPrefix is the prefix for environment variables.
	envPrefix string

	// errorIfNotFound is a flag that determines whether an error should be returned if the config file is not found.
	errorIfNotFound bool
}

// ConfigPath sets the path to the config file.
func ConfigPath(path string) ConfigOption {
	return func(o *LoaderOpts) {
		o.configPath = path
	}
}

// EnvPrefix sets the prefix for environment variables. Will capitalize the prefix.
func EnvPrefix(prefix string) ConfigOption {
	return func(o *LoaderOpts) {
		o.envPrefix = strings.ToUpper(prefix)
	}
}

// ErrorIfNotFound sets the flag that determines whether an error should be returned if the config file is not found.
func ErrorIfNotFound(errorIfNotFound bool) ConfigOption {
	return func(o *LoaderOpts) {
		o.errorIfNotFound = errorIfNotFound
	}
}

// Load loads the configuration from the given config file.
//
// Rules:
//
// - The config file must be either a JSON or YAML file.
//
// - All fields must be exported.
//
// - Fields without env tag will be uppercase and used as environment variable name.

func Load(config Configuration, options ...ConfigOption) error {
	opts := &LoaderOpts{}
	for _, option := range options {
		option(opts)
	}

	setDefaults(config)
	defer readEnvOverrides(opts.envPrefix, config)

	if _, err := os.Stat(opts.configPath); os.IsNotExist(err) {
		if opts.errorIfNotFound {
			return fmt.Errorf("config file not found: %s", opts.configPath)
		}

		return nil
	}

	readBytes, err := os.ReadFile(opts.configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	switch filepath.Ext(opts.configPath) {
	case ".json":
		err = loadJSON(readBytes, config)
	case ".yaml", ".yml":
		err = loadYAML(readBytes, config)
	default:
		err = fmt.Errorf("unsupported config file format: %s", opts.configPath)
	}

	return nil
}

func loadJSON(readBytes []byte, config Configuration) error {
	if err := json.Unmarshal(readBytes, &config); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	return nil
}

func loadYAML(readBytes []byte, config Configuration) error {
	if err := yaml.Unmarshal(readBytes, &config); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	return nil
}

func foreachField(config Configuration, it func(field reflect.StructField)) {
	rf := reflect.TypeOf(config).Elem()

	for i := 0; i < rf.NumField(); i++ {
		it(rf.Field(i))
	}
}

func setDefaults(config Configuration) {
	foreachField(config, func(field reflect.StructField) {
		fieldName := field.Name
		config.SetField(fieldName, config.DefaultValue(fieldName))
	})
}

func readEnvOverrides(prefix string, config Configuration) {
	// TODO: add .env file support, but commandline env vars should override .env file vars
	foreachField(config, func(field reflect.StructField) {
		fieldName := field.Name
		envName := field.Tag.Get("env")
		if envName == "" {
			envName = strings.ToUpper(fieldName)
		}

		if prefix != "" {
			envName = prefix + "_" + envName
		} else {
			envName = strings.ToUpper(envName)
		}

		envValue := os.Getenv(envName)

		if envValue == "" {
			return
		}

		switch field.Type.Kind() {
		case reflect.String:
			config.SetField(fieldName, envValue)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			num, _ := strconv.Atoi(envValue)
			config.SetField(fieldName, num)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			num, _ := strconv.ParseUint(envValue, 10, 64)
			config.SetField(fieldName, num)
		case reflect.Bool:
			b, _ := strconv.ParseBool(envValue)
			config.SetField(fieldName, b)
		default:
			panic(fmt.Sprintf("unsupported field type: %s", field.Type.Kind()))
		}
	})
}
