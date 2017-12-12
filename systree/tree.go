package systree

import (
	"github.com/VolantMQ/volantmq/types"
)

type impl struct {
	server        server
	metrics       metric
	topics        topicStat
	subscriptions subscriptionsStat
	clients       clients
	sessions      sessions
}

// NewTree allocate systree provider
func NewTree(base string) (Provider, []types.RetainObject, []DynamicValue, error) {
	var retains []types.RetainObject
	var staticRetains []types.RetainObject

	tr := &impl{
		//系统状态值，例如$SYS/servers/${nodename}/uptime
		//更新方法为Retained、Publish
		newServer(base, &retains, &staticRetains),
		//系统状态值，例如$SYS/servers/${nodename}/metrics/packets/connect
		//更新方法为Sent、Received
		newMetric(base, &retains),
		//系统状态值，例如$SYS/servers/${nodename}/stats/topics/max
		//更新方法为Added、Removed
		newStatTopic(base+"/stats", &retains),
		//系统状态值，例如$SYS/servers/${nodename}/stats/subscriptions/max
		//更新方法为Subscribed、UnSubscribed
		newStatSubscription(base+"/stats", &retains),
		//系统状态值，例如$SYS/servers/${nodename}/client/${id}/connected
		//更新方法为Connected、Disconnected
		newClients(base, &retains),
		//系统状态值，例如$SYS/servers/${nodename}/sessions/${id}/removed
		//更新方法为Created、Removed
		newSessions(base, &retains),
	}

	var dynUpdates []DynamicValue
	for _, d := range retains {
		v := d.(DynamicValue)
		dynUpdates = append(dynUpdates, v)
	}

	retains = append(retains, staticRetains...)
	return tr, retains, dynUpdates, nil
}

// SetCallbacks
func (t *impl) SetCallbacks(cb types.TopicMessenger) {
	t.clients.topicsManager = cb
	t.sessions.topicsManager = cb
}

// Sessions get sessions stat provider
func (t *impl) Sessions() Sessions {
	return &t.sessions
}

// Clients get clients stat provider
func (t *impl) Clients() Clients {
	return &t.clients
}

// Topics get topics stat provider
func (t *impl) Topics() TopicsStat {
	return &t.topics
}

// Metric get metric provider
func (t *impl) Metric() Metric {
	return &t.metrics
}

// Subscriptions get subscriptions stat provider
func (t *impl) Subscriptions() SubscriptionsStat {
	return &t.subscriptions
}
