package gomongo

import (
	"github.com/dtapps/gotime"
	"time"
)

// BsonTime 时间类型
func (c *Client) BsonTime(value time.Time) string {
	return gotime.SetCurrent(value).Bson()
}
