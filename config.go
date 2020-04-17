package main

import (
	"sort"

	"github.com/BurntSushi/toml"
)

type BotConfig struct {
	APIKey string `toml:"api-key"`

	UserWhitelist []string `toml:"user-whitelist"`

	// Constraints
	MaxFeedsPerChat      int `toml:"max-feeds-per-chat"`
	MaxTotalFeedsByUser  int `toml:"max-total-feeds-by-user"`
	MaxActiveFeedsByUser int `toml:"max-active-feeds-by-user"`
}

type DBConfig struct {
	Driver string `toml:"driver"`
	Source string `toml:"src"`
}

type Config struct {
	Bot BotConfig `toml:"bot"`
	DB  DBConfig  `toml:"db"`
}

func loadConfigFile(path string) (*Config, error) {
	cfg := new(Config)

	if _, err := toml.DecodeFile(configfilePath, &cfg); err != nil {
		return nil, err
	}

	sort.Strings(cfg.Bot.UserWhitelist)

	return cfg, nil
}

func (c *Config) IsWhitelisted(username string) bool {
	if len(c.Bot.UserWhitelist) == 0 {
		return true
	}

	i := sort.SearchStrings(c.Bot.UserWhitelist, username)
	return i != len(c.Bot.UserWhitelist) && c.Bot.UserWhitelist[i] == username
}
