package mq

import (
	"github.com/ryozero0120/mq/channel"
	"github.com/ryozero0120/mq/connection"
)

type MqConfig struct {
	Connection connection.ConnectionConfig
	Pool       channel.PoolConfig
}
