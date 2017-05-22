package session

import (
	"container/list"
	"github.com/troian/surgemq/message"
	persistTypes "github.com/troian/surgemq/persistence/types"
	"sync/atomic"
)

func (s *Type) onClose(will bool) {
	defer func() {
		atomic.StoreInt64(&s.running, 0)

		var persist *persistTypes.SessionMessages
		shutdown := true

		if !s.clean {
			persist = &persistTypes.SessionMessages{}

			var next *list.Element

			for elem := s.publisher.messages.Front(); elem != nil; elem = next {
				next = elem.Next()

				if m, ok := s.publisher.messages.Remove(elem).(message.Provider); ok {
					persist.Out.Messages = append(persist.Out.Messages, m)
				}
			}

			for _, m := range s.ack.pubOut.get() {
				persist.Out.Messages = append(persist.Out.Messages, m)
			}
			s.ack.pubOut.wipe()

			for _, m := range s.ack.pubIn.get() {
				persist.Out.Messages = append(persist.In.Messages, m)
			}

			s.ack.pubIn.wipe()

			// if non clean session check if it has any active subscriptions
			// if not tell manager to shut it down
			if len(s.config.subscriptions) > 0 {
				shutdown = false
			}
		}

		s.config.callbacks.onDisconnect(s.config.id, persist, shutdown)

		s.wgSessionStopped.Done()
	}()

	// just in case make sure session has been started
	s.wgSessionStarted.Wait()

	// [MQTT-3.1.3.3]
	if will && s.will != nil {
		appLog.Debugf("connection unexpectedly closed [%s]. Sending Will", s.config.id)
		s.publishToTopic(s.will) // nolint: errcheck
	}

	unSub := func(t string, q message.QosType) {
		appLog.Debugf("[%s]: unsubscribing topic %s", s.config.id, t)
		if err := s.config.topicsMgr.UnSubscribe(t, &s.subscriber); err != nil {
			appLog.Errorf("[%s]: error unsubscribing topic %q: %v", s.config.id, t, err)
		}
	}

	for t, q := range s.config.subscriptions {
		// if this is clean session unsubscribe from all topics
		if s.clean {
			unSub(t, q)
			delete(s.config.subscriptions, t)
		} else if q == message.QosAtMostOnce {
			// if session is non clean unsubscribe only from topics with Fire and Forget QoS
			unSub(t, q)
			delete(s.config.subscriptions, t)
		}
	}

	// Make sure all of publishes to subscriber finished before continue
	s.subscriber.WgWriters.Wait()

	close(s.publisher.quit)
	s.publisher.cond.Broadcast()

	// Wait writer to finish it's job
	s.publisher.stopped.Wait()

	s.conn = nil

	// [MQTT-3.3.1-7]
	// Discard retained messages with QoS 0
	s.retained.lock.Lock()
	for _, m := range s.retained.list {
		s.config.topicsMgr.Retain(m) // nolint: errcheck
	}
	s.retained.list = []*message.PublishMessage{}
	s.retained.lock.Unlock()
}

// onPublish invoked when server receives PUBLISH message from remote
// On QoS == 0, we should just take the next step, no ack required
// On QoS == 1, send back PUBACK, then take the next step
// On QoS == 2, we need to put it in the ack queue, send back PUBREC
func (s *Type) onPublish(msg *message.PublishMessage) error {
	// check for topic access
	var err error

	switch msg.QoS() {
	case message.QosExactlyOnce:
		resp := message.NewPubRecMessage()
		resp.SetPacketID(msg.PacketID())

		if _, err = s.conn.writeMessage(resp); err == nil {
			s.ack.pubIn.put(msg)
		}
	case message.QosAtLeastOnce:
		resp := message.NewPubAckMessage()
		resp.SetPacketID(msg.PacketID())

		// We publish QoS even if error during ack happened.
		// Remote then will send same message with DUP flag set
		s.conn.writeMessage(resp) // nolint: errcheck
		fallthrough
	case message.QosAtMostOnce: // QoS 0
		err = s.publishToTopic(msg)
	}

	return err
}

// onAck server received acknowledgment from remote
func (s *Type) onAck(msg message.Provider) error {
	var err error

	switch msg.(type) {
	case *message.PubAckMessage:
		// remote acknowledged PUBLISH QoS 1 message sent by this server
		s.ack.pubOut.ack(msg) // nolint: errcheck
	case *message.PubRecMessage:
		// remote received PUBLISH message sent by this server
		s.ack.pubOut.ack(msg) // nolint: errcheck

		resp := message.NewPubRelMessage()
		resp.SetPacketID(msg.PacketID())

		// 2. Put PUBREL into ack queue
		// Do it before writing into network as theoretically response may come
		// faster than put into queue
		s.ack.pubOut.put(resp)

		// 2. Try send PUBREL reply
		if _, err = s.conn.writeMessage(resp); err == nil {
			// 3. PUBREL delivered to remote. Wait to PUBCOMP
		} else {
			appLog.Errorf("[%s] Couldn't deliver PUBREL. Requeue publish", s.config.id)
			// Couldn't deliver message. Remove it from ack queue and put into publish queue
			s.ack.pubOut.ack(resp) // nolint: errcheck
			s.publisher.lock.Lock()
			s.publisher.messages.PushBack(resp)
			s.publisher.lock.Unlock()
			s.publisher.cond.Signal()
		}
	case *message.PubRelMessage:
		// Message sent by remote has been released
		// send corresponding PUBCOMP
		resp := message.NewPubCompMessage()
		resp.SetPacketID(msg.PacketID())

		if _, err = s.conn.writeMessage(resp); err == nil {
			s.ack.pubIn.ack(msg) // nolint: errcheck
		} else {
			appLog.Errorf("[%s] Couldn't deliver PUBCOMP", s.config.id)
		}
	case *message.PubCompMessage:
		// PUBREL message has been acknowledged, release from queue
		s.ack.pubOut.ack(msg) // nolint: errcheck
	default:
		appLog.Errorf("[%s] Unsupported ack message type: %s", s.config.id, msg.Type().String())
	}

	return err
}

func (s *Type) onSubscribe(msg *message.SubscribeMessage) error {
	resp := message.NewSubAckMessage()
	resp.SetPacketID(msg.PacketID())

	// Subscribe to the different topics
	var retCodes []message.QosType

	topics := msg.Topics()

	var retainedMessages []*message.PublishMessage

	for _, t := range topics {
		// Let topic manager know we want to listen to given topic
		qos := msg.TopicQos(t)
		appLog.Tracef("Subscribing [%s] to [%s:%d]", s.config.id, t, qos)
		rQoS, err := s.config.topicsMgr.Subscribe(t, qos, &s.subscriber)
		if err != nil {
			return err
		}
		s.addTopic(t, qos) // nolint: errcheck

		retCodes = append(retCodes, rQoS)

		// yeah I am not checking errors here. If there's an error we don't want the
		// subscription to stop, just let it go.
		s.config.topicsMgr.Retained(t, &retainedMessages) // nolint: errcheck
	}

	if err := resp.AddReturnCodes(retCodes); err != nil {
		return err
	}

	if _, err := s.conn.writeMessage(resp); err != nil {
		// TODO: Unsubscribe
		appLog.Errorf("[%s] couldn't send SUBACK: %s", s.config.id, err.Error())
		return err
	}

	// Now put retained messages into publish queue
	for _, rm := range retainedMessages {
		m := message.NewPublishMessage()
		// [MQTT-3.3.1-8]
		m.SetRetain(true)
		m.SetQoS(rm.QoS()) // nolint: errcheck
		m.SetPayload(rm.Payload())
		m.SetTopic(rm.Topic()) // nolint: errcheck
		if m.PacketID() == 0 && (m.QoS() == message.QosAtLeastOnce || m.QoS() == message.QosExactlyOnce) {
			m.SetPacketID(s.newPacketID())
		}

		s.publisher.lock.Lock()
		s.publisher.messages.PushBack(m)
		s.publisher.lock.Unlock()
		s.publisher.cond.Signal()
	}

	return nil
}

func (s *Type) onUnSubscribe(msg *message.UnSubscribeMessage) (*message.UnSubAckMessage, error) {
	for _, t := range msg.Topics() {
		s.config.topicsMgr.UnSubscribe(t, &s.subscriber) // nolint: errcheck
		s.removeTopic(t)                                 // nolint: errcheck
	}

	resp := message.NewUnSubAckMessage()
	resp.SetPacketID(msg.PacketID())

	return resp, nil
}
