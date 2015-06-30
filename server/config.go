package server

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Addr     string `json:"addr" toml:"addr"`
	User     string `json:"user" toml:"user"`
	Password string `json:"password" toml:"password"`
	LogLevel string `json:"log_level" toml:"log_level"`
	SkipAuth bool   `json:"skip_auth" toml:"skip_auth"`
}

func ParseConfigJsonData(data []byte) (*Config, error) {
	var cfg Config
	if err := json.Unmarshal([]byte(data), &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func ParseConfigTomlData(data []byte) (*Config, error) {
	var cfg Config
	if _, err := toml.Decode(string(data), &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func ParseConfigFile(fileName string) (*Config, error) {
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}
	if strings.ToLower(filepath.Ext(fileName)) == ".toml" {
		return ParseConfigTomlData(data)
	} else {
		return ParseConfigJsonData(data)
	}
}
