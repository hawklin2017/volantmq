package systree

import (
	"encoding/json"

	"github.com/VolantMQ/volantmq/packet"
	"github.com/VolantMQ/volantmq/types"
)

type server struct {
	version      string
	upTime       *dynamicValueUpTime
	currTime     *dynamicValueCurrentTime
	capabilities struct {
		SupportedVersions             []packet.ProtocolVersion
		MaxQoS                        string
		MaxConnections                uint64
		MaximumPacketSize             uint32
		ServerKeepAlive               uint16
		ReceiveMaximum                uint16
		RetainAvailable               bool
		WildcardSubscriptionAvailable bool
		SubscriptionIDAvailable       bool
		SharedSubscriptionAvailable   bool
	}
}

//C 语法，修改传递参数
//uptime、datetime动态值，继承dynamicValue（Retained/Publish/Topics）生成retained、publish时候，通过getValue获取实际值
//version、capabilities静态值
func newServer(topicPrefix string, dynRetains, staticRetains *[]types.RetainObject) server {
	b := server{
		upTime:   newDynamicValueUpTime(topicPrefix + "/uptime"),
		currTime: newDynamicValueCurrentTime(topicPrefix + "/datetime"),
		version:  "1.0.0",
	}

	m, _ := packet.New(packet.ProtocolV311, packet.PUBLISH)
	msg, _ := m.(*packet.Publish)
	msg.SetQoS(packet.QoS0)                // nolint: errcheck
	msg.SetTopic(topicPrefix + "/version") // nolint: errcheck
	msg.SetPayload([]byte(b.version))

	*dynRetains = append(*dynRetains, b.upTime)
	*dynRetains = append(*dynRetains, b.currTime)
	*staticRetains = append(*staticRetains, msg)

	m, _ = packet.New(packet.ProtocolV311, packet.PUBLISH)
	msg, _ = m.(*packet.Publish)
	msg.SetQoS(packet.QoS0)                     // nolint: errcheck
	msg.SetTopic(topicPrefix + "/capabilities") // nolint: errcheck

	if data, err := json.Marshal(&b.capabilities); err == nil {
		msg.SetPayload(data)
	} else {
		msg.SetPayload([]byte(err.Error()))
	}

	*staticRetains = append(*staticRetains, msg)

	return b
}
