package etcd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"google.golang.org/grpc/resolver"

	"github.com/openimsdk/tools/log"
)

type resolverBuilder struct {
	client *clientv3.Client
	pool   *connPool
}

func (builder resolverBuilder) Scheme() string { return "etcd" }

func (builder resolverBuilder) Build(target resolver.Target, conn resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	if target.Endpoint() == "" {
		return nil, errors.New("etcd resolver: empty target")
	}

	prefix := target.Endpoint()
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	if builder.pool != nil {
		return builder.pool.buildSharedResolver(prefix, conn)
	}

	ctx, cancel := context.WithCancel(builder.client.Ctx())
	watcher := &endpointWatcher{
		client: builder.client,
		prefix: prefix,
		conn:   conn,
		cancel: cancel,
		done:   make(chan struct{}),
		ready:  make(chan struct{}),
	}
	go watcher.run(ctx)
	return watcher, nil
}

type endpointWatcher struct {
	client *clientv3.Client
	prefix string
	conn   resolver.ClientConn
	cancel context.CancelFunc
	done   chan struct{}

	mu        sync.RWMutex
	listeners map[uint64]func([]resolver.Address)
	lastAddrs []resolver.Address
	nextSubID uint64
	ready     chan struct{}
	readyOnce sync.Once
}

func (ew *endpointWatcher) ResolveNow(resolver.ResolveNowOptions) {}

func (ew *endpointWatcher) Close() {
	if ew.cancel != nil {
		ew.cancel()
	}
	<-ew.done
}

func (ew *endpointWatcher) addListener(fn func([]resolver.Address)) (uint64, []resolver.Address) {
	ew.mu.Lock()
	defer ew.mu.Unlock()
	if ew.listeners == nil {
		ew.listeners = make(map[uint64]func([]resolver.Address))
	}
	ew.nextSubID++
	id := ew.nextSubID
	ew.listeners[id] = fn
	var current []resolver.Address
	if ew.lastAddrs != nil {
		current = append([]resolver.Address(nil), ew.lastAddrs...)
	}
	return id, current
}

func (ew *endpointWatcher) getAddresses() []resolver.Address {
	ew.mu.RLock()
	defer ew.mu.RUnlock()
	if ew.lastAddrs == nil {
		return nil
	}
	return append([]resolver.Address(nil), ew.lastAddrs...)
}

func (ew *endpointWatcher) removeListener(id uint64) {
	ew.mu.Lock()
	delete(ew.listeners, id)
	ew.mu.Unlock()
}

func (ew *endpointWatcher) waitReady(ctx context.Context) error {
	if ew.ready == nil {
		return nil
	}
	select {
	case <-ew.ready:
		return nil
	case <-ew.done:
		return errors.New("etcd watcher: closed")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (ew *endpointWatcher) run(ctx context.Context) {
	defer close(ew.done)
	delay := 100 * time.Millisecond
	for ctx.Err() == nil {
		err := ew.watchSnapshot(ctx, func() { delay = 100 * time.Millisecond })
		if ctx.Err() != nil {
			return
		}
		if ew.conn != nil {
			ew.conn.ReportError(err)
		}
		log.ZWarn(ctx, "etcd watcher failed, retrying", err,
			"prefix", ew.prefix,
			"retryDelay", delay,
		)
		if !sleepWithContext(ctx, delay) {
			return
		}
		delay = min(delay*2, 3*time.Second)
	}
}

func (ew *endpointWatcher) watchSnapshot(parent context.Context, recovered func()) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	getCtx, getCancel := context.WithTimeout(ctx, 5*time.Second)
	snapshot, err := ew.client.Get(getCtx, ew.prefix, clientv3.WithPrefix())
	getCancel()
	if err != nil {
		return err
	}
	addresses := make(map[string]resolver.Address, len(snapshot.Kvs))
	for _, entry := range snapshot.Kvs {
		ew.setAddress(addresses, string(entry.Key), entry.Value)
	}
	ew.publish(addresses)
	updates := ew.client.Watch(ctx, ew.prefix, clientv3.WithPrefix(), clientv3.WithRev(snapshot.Header.Revision+1), clientv3.WithProgressNotify())
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case update, open := <-updates:
			if !open {
				return errors.New("etcd watcher: watch closed")
			}
			if err := update.Err(); err != nil {
				return err
			}
			if len(update.Events) > 0 || update.IsProgressNotify() {
				recovered()
			}
			for _, event := range update.Events {
				switch event.Type {
				case clientv3.EventTypePut:
					if event.Kv != nil {
						ew.setAddress(addresses, string(event.Kv.Key), event.Kv.Value)
					}
				case clientv3.EventTypeDelete:
					if event.Kv != nil {
						delete(addresses, string(event.Kv.Key))
					} else if event.PrevKv != nil {
						delete(addresses, string(event.PrevKv.Key))
					}
				}
			}
			if len(update.Events) > 0 {
				ew.publish(addresses)
			}
		}
	}
}

func (ew *endpointWatcher) setAddress(addresses map[string]resolver.Address, key string, value []byte) {
	var endpoint endpoints.Endpoint
	if err := json.Unmarshal(value, &endpoint); err == nil && isValidHostPort(endpoint.Addr) {
		addresses[key] = resolver.Address{Addr: endpoint.Addr, Metadata: endpoint.Metadata}
		return
	}
	if lastSlash := strings.LastIndex(key, "/"); lastSlash != -1 && lastSlash < len(key)-1 {
		addr := key[lastSlash+1:]
		if isValidHostPort(addr) {
			addresses[key] = resolver.Address{Addr: addr}
			return
		}
	}
	if ew.conn != nil {
		err := fmt.Errorf("invalid endpoint value at %s: %s", key, string(value))
		log.ZWarn(context.Background(), "invalid endpoint in etcd", err,
			"key", key,
			"prefix", ew.prefix,
		)
		ew.conn.ReportError(err)
	}
}

func isValidHostPort(addr string) bool {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return false
	}
	port, err := strconv.Atoi(portStr)
	return err == nil && port > 0 && port <= 65535
}

func (ew *endpointWatcher) publish(addresses map[string]resolver.Address) {
	keys := make([]string, 0, len(addresses))
	for key := range addresses {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	addrs := make([]resolver.Address, 0, len(keys))
	for _, key := range keys {
		addrs = append(addrs, addresses[key])
	}
	state := resolver.State{Addresses: addrs}
	if ew.conn != nil {
		if err := ew.conn.UpdateState(state); err != nil {
			log.ZWarn(context.Background(), "etcd resolver update state failed", err,
				"prefix", ew.prefix,
				"addrCount", len(addrs),
			)
			ew.conn.ReportError(err)
		}
	}

	ew.mu.Lock()
	ew.lastAddrs = addrs
	var fns []func([]resolver.Address)
	for _, fn := range ew.listeners {
		fns = append(fns, fn)
	}
	ew.mu.Unlock()

	for _, fn := range fns {
		fn(addrs)
	}

	ew.readyOnce.Do(func() {
		if ew.ready != nil {
			close(ew.ready)
		}
	})
}

type sharedResolverHandle struct {
	watcher *endpointWatcher
	subID   uint64
}

func (h *sharedResolverHandle) ResolveNow(resolver.ResolveNowOptions) {}

func (h *sharedResolverHandle) Close() {
	h.watcher.removeListener(h.subID)
}
