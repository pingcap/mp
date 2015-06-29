package config

import (
	"encoding/json"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
)

type SchemaConfig struct {
	DB           string       `json:"db" toml:"db"`
	ShardIds     []string     `json:"shard_ids" toml:"shard_ids"`
	RouterConifg RouterConfig `json:"router" toml:"router"`
}

type RouterConfig struct {
	Default   []string    `json:"default_shards" toml:"default_shards"`
	TableRule []TableRule `json:"table_rules" toml:"table_rules"`
}

type TableRule struct {
	Table        string   `json:"table" toml:"table"`
	ShardingKey  string   `json:"key" toml:"key"`
	RowCacheType string   `json:"row_cache_type" toml:"row_cache_type"`
	MapToShards  []string `json:"map_to_shards" toml:"map_to_shards"` //shard ids
}

type ShardConfig struct {
	Id       string `json:"id" toml:"id"`
	User     string `json:"user" toml:"user"`
	Password string `json:"password" toml:"password"`

	Master string   `json:"master" toml:"master"`
	Slave  []string `json:"slave" toml:"slave"`
}

type Config struct {
	Addr     string         `json:"addr" toml:"addr"`
	User     string         `json:"user" toml:"user"`
	Password string         `json:"password" toml:"password"`
	LogLevel string         `json:"log_level" toml:"log_level"`
	SkipAuth bool           `json:"skip_auth" toml:"skip_auth"`
	Shards   []ShardConfig  `json:"shards" toml:"shards"`
	Schemas  []SchemaConfig `json:"schemas" toml:"schemas"`
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
