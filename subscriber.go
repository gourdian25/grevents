// File: subscriber.go

package grevents

import "sync"

// subscription is one registered (topic, handler) pair.
type subscription struct {
	id      uint64
	topic   string
	handler HandlerFunc
}

// SubscribeOption is an extension point for per-subscription
// configuration, passed variadically to Bus.Subscribe.
//
// Notes:
//   - No concrete option is defined in v1
//
// Use case: reserved for future per-subscription knobs (e.g. a
// subscription-specific retry override) without a breaking change to
// Bus.Subscribe's signature.
type SubscribeOption func(*subscription)

// registry is the topic -> subscriptions index shared by sync and async
// delivery. All access is guarded by mu; snapshot returns a copy so
// delivery never holds the registry lock while invoking handlers.
type registry struct {
	mu   sync.RWMutex
	subs map[string][]*subscription
	next uint64
}

func newRegistry() *registry {
	return &registry{subs: make(map[string][]*subscription)}
}

func (r *registry) subscribe(topic string, handler HandlerFunc, opts ...SubscribeOption) Unsubscribe {
	r.mu.Lock()
	r.next++
	sub := &subscription{id: r.next, topic: topic, handler: handler}
	for _, opt := range opts {
		opt(sub)
	}
	r.subs[topic] = append(r.subs[topic], sub)
	r.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			list := r.subs[topic]
			for i, s := range list {
				if s.id == sub.id {
					r.subs[topic] = append(list[:i:i], list[i+1:]...)
					return
				}
			}
		})
	}
}

// snapshot returns a copy of the current subscriber list for topic, safe
// to range over without holding the registry lock during delivery.
func (r *registry) snapshot(topic string) []*subscription {
	r.mu.RLock()
	defer r.mu.RUnlock()
	list := r.subs[topic]
	if len(list) == 0 {
		return nil
	}
	out := make([]*subscription, len(list))
	copy(out, list)
	return out
}
