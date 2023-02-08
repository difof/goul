//

package config_loader

import (
	"os"
	"testing"
)

type BasicConfig struct {
	Username string `json:"username" env:"USERNAME"`
}

func (c *BasicConfig) DefaultValue(field string) interface{} {
	switch field {
	case "Username":
		return "default"
	}

	return nil
}

func (c *BasicConfig) SetField(field string, value interface{}) {
	switch field {
	case "Username":
		c.Username = value.(string)
	}
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
