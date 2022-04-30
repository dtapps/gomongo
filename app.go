package gomongo

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const Version = "1.0.0"

type app struct {
	db             *mongo.Client // 驱动
	Dns            string        // 连接地址
	databaseName   string        // 库名
	collectionName string        // 表名
}

// NewApp 实例化并链接数据库
func NewApp(dns string) *app {
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

	return &app{db: db, Dns: dns}
}

// NewDb 实例化并传入链接
func NewDb(db *mongo.Client) *app {
	return &app{db: db}
}

// Close 关闭
func (app *app) Close() {
	err := app.db.Disconnect(context.TODO())
	if err != nil {
		panic(errors.New(fmt.Sprintf("数据库【mongo】关闭失败：%v", err)))
	}
	return
}
