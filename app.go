package gomongo

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const Version = "1.0.4"

type Client struct {
	Db             *mongo.Client // 驱动
	Dns            string        // 连接地址
	DatabaseName   string        // 库名
	collectionName string        // 表名
}

// NewClient 实例化并链接数据库
func NewClient(dns string, databaseName string) *Client {
	// 连接到MongoDB
	db, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(dns))
	if err != nil {
		panic(fmt.Sprintf("数据库【mongo】连接失败：%v", err))
	}

	// 检查连接
	err = db.Ping(context.TODO(), nil)
	if err != nil {
		panic(fmt.Sprintf("数据库【mongo】连接服务器失败：%v", err))
	}

	if databaseName == "" {
		return &Client{Db: db, Dns: dns}
	}
	return &Client{Db: db, Dns: dns, DatabaseName: databaseName}
}

// NewDb 实例化并传入链接
func NewDb(db *mongo.Client, databaseName string) *Client {
	if databaseName == "" {
		return &Client{Db: db}
	}
	return &Client{Db: db, DatabaseName: databaseName}
}

// Close 关闭
func (c *Client) Close() {
	err := c.Db.Disconnect(context.TODO())
	if err != nil {
		panic(errors.New(fmt.Sprintf("数据库【mongo】关闭失败：%v", err)))
	}
	return
}
