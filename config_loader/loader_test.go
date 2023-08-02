//

package config_loader

import (
	"os"
	"testing"
)

type BasicConfig struct {
	Username string `json:"username"`
	Act      string `default:"holo***"`
}

func TestLoadConfig(t *testing.T) {
	config := &BasicConfig{}
	err := Load(config, ConfigPath("basic_config.json"))
	if err != nil {
		t.Fatal(err)
	}

	if config.Username != "admin" {
		t.Fatal("username not set correctly")
	}
}

func TestEnvOverrides(t *testing.T) {
	os.Setenv("TEST_USERNAME", "test_admin")

	config := &BasicConfig{}
	err := Load(config, ConfigPath("basic_config.json"), EnvPrefix("TEST"))
	if err != nil {
		t.Fatal(err)
	}

	if config.Username != "test_admin" {
		t.Fatal("username not set correctly")
	}
}

func TestDefaultValues(t *testing.T) {
	config := &BasicConfig{}
	err := Load(config)
	if err != nil {
		t.Fatal(err)
	}

	if config.Act != "holo***" {
		t.Fatal("username not set correctly: " + config.Username)
	}
}
