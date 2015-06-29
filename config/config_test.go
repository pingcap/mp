package config

import (
	"testing"

	"github.com/BurntSushi/toml"
)

var tomlData = `
addr = "0.0.0.0:4000"
log_level = "debug"
skip_auth = true
password = ""

[rowcache_conf]
binary = "/usr/bin/memcached"
connections = 1024
lock_paged = false
mem = 128
port = 11222
threads = -1
socket = ""

[[schemas]]
db = "fusion"
shard_ids = ["shard1"]

[schemas.router]
    default_shards = ["shard1"]

    [[schemas.router.table_rules]]
    table = "Test1"
    row_cache_type = "RW"

    [[schemas.router.table_rules]]
    table = "Test2"
    row_cache_type = "RW"


[[shards]]
id = "shard1"
down_after_noalive = 0
idle_conns = 100
master =  "127.0.0.1:3306"
password = ""
rw_split = false
slave = []
user = "root"

`

func TestTomlConfig(t *testing.T) {
	var conf Config
	if _, err := toml.Decode(tomlData, &conf); err != nil {
		// handle error
		if err != nil {
			t.Error(err)
		}
	}
	if conf.Shards[0].Id != "shard1" {
		t.Error("shard[0] is error")
	}

	if len(conf.Schemas[0].RouterConifg.TableRule) != 2 {
		t.Error("Schemas is error")
	}
}
