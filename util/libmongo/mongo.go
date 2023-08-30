package libmongo

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type CollConfig struct {
	Tbl string `json:"tbl"`
	*DBConfig
}

type DBConfig struct {
	DbName      string   `json:"dbName"`
	Hosts       []string `json:"hosts"`
	UserName    string   `json:"username"`
	Password    string   `json:"password"`
	HeartbeatMs int      `json:"heartbeatMs"`
	MaxPoolSize uint64   `json:"maxPoolSize"`
}

func NewDB(cfg *DBConfig) (*mongo.Database, error) {
	if cfg.DbName == "" {
		return nil, errors.New("invalid db name")
	}
	var auth string
	if cfg.UserName != "" && cfg.Password != "" {
		auth = cfg.UserName + ":" + cfg.Password + "@"
	}
	if len(cfg.Hosts) == 0 {
		return nil, errors.New("invalid hosts")
	}
	if cfg.MaxPoolSize == 0 {
		cfg.MaxPoolSize = 100
	}
	uri := "mongodb://" + auth + strings.Join(cfg.Hosts, ",") + "/" + cfg.DbName
	opts := options.Client().ApplyURI(uri).SetMaxPoolSize(cfg.MaxPoolSize).SetHeartbeatInterval(time.Millisecond * time.Duration(cfg.HeartbeatMs))
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.HeartbeatMs)*time.Millisecond*3)
	defer cancel()
	cli, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}
	return cli.Database(cfg.DbName), nil
}

func NewCollection(cfg *CollConfig) (*mongo.Collection, error) {
	if cfg.Tbl == "" {
		return nil, errors.New("invalid table name")
	}
	db, err := NewDB(cfg.DBConfig)
	if err != nil {
		return nil, err
	}
	return db.Collection(cfg.Tbl), nil
}
