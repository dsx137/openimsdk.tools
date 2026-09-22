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
	watcher := &endpointResolver{
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

type endpointResolver struct {
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

func (watcher *endpointResolver) ResolveNow(resolver.ResolveNowOptions) {}

func (watcher *endpointResolver) Close() {
	if watcher.cancel != nil {
		watcher.cancel()
	}
	<-watcher.done
}

func (watcher *endpointResolver) addListener(fn func([]resolver.Address)) (uint64, []resolver.Address) {
	watcher.mu.Lock()
	defer watcher.mu.Unlock()
	if watcher.listeners == nil {
		watcher.listeners = make(map[uint64]func([]resolver.Address))
	}
	watcher.nextSubID++
	id := watcher.nextSubID
	watcher.listeners[id] = fn
	var current []resolver.Address
	if watcher.lastAddrs != nil {
		current = append([]resolver.Address(nil), watcher.lastAddrs...)
	}
	return id, current
}

func (watcher *endpointResolver) getAddresses() []resolver.Address {
	watcher.mu.RLock()
	defer watcher.mu.RUnlock()
	if watcher.lastAddrs == nil {
		return nil
	}
	return append([]resolver.Address(nil), watcher.lastAddrs...)
}

func (watcher *endpointResolver) removeListener(id uint64) {
	watcher.mu.Lock()
	delete(watcher.listeners, id)
	watcher.mu.Unlock()
}

func (watcher *endpointResolver) waitReady(ctx context.Context) error {
	if watcher.ready == nil {
		return nil
	}
	select {
	case <-watcher.ready:
		return nil
	case <-watcher.done:
		return errors.New("etcd resolver: closed")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (watcher *endpointResolver) run(ctx context.Context) {
	defer close(watcher.done)
	delay := 100 * time.Millisecond
	for ctx.Err() == nil {
		err := watcher.watchSnapshot(ctx, func() { delay = 100 * time.Millisecond })
		if ctx.Err() != nil {
			return
		}
		if watcher.conn != nil {
			watcher.conn.ReportError(err)
		}
		log.ZWarn(ctx, "etcd resolver watch failed, retrying", err,
			"prefix", watcher.prefix,
			"retryDelay", delay,
		)
		if !sleepWithContext(ctx, delay) {
			return
		}
		delay = min(delay*2, 3*time.Second)
	}
}

func (watcher *endpointResolver) watchSnapshot(parent context.Context, recovered func()) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	getCtx, getCancel := context.WithTimeout(ctx, 5*time.Second)
	snapshot, err := watcher.client.Get(getCtx, watcher.prefix, clientv3.WithPrefix())
	getCancel()
	if err != nil {
		return err
	}
	addresses := make(map[string]resolver.Address, len(snapshot.Kvs))
	for _, entry := range snapshot.Kvs {
		watcher.setAddress(addresses, string(entry.Key), entry.Value)
	}
	watcher.publish(addresses)
	updates := watcher.client.Watch(ctx, watcher.prefix, clientv3.WithPrefix(), clientv3.WithRev(snapshot.Header.Revision+1), clientv3.WithProgressNotify())
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case update, open := <-updates:
			if !open {
				return errors.New("etcd resolver: watch closed")
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
						watcher.setAddress(addresses, string(event.Kv.Key), event.Kv.Value)
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
				watcher.publish(addresses)
			}
		}
	}
}

func (watcher *endpointResolver) setAddress(addresses map[string]resolver.Address, key string, value []byte) {
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
	if watcher.conn != nil {
		err := fmt.Errorf("invalid endpoint value at %s: %s", key, string(value))
		log.ZWarn(context.Background(), "invalid endpoint in etcd", err,
			"key", key,
			"prefix", watcher.prefix,
		)
		watcher.conn.ReportError(err)
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

func (watcher *endpointResolver) publish(addresses map[string]resolver.Address) {
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
	if watcher.conn != nil {
		if err := watcher.conn.UpdateState(state); err != nil {
			log.ZWarn(context.Background(), "etcd resolver update state failed", err,
				"prefix", watcher.prefix,
				"addrCount", len(addrs),
			)
			watcher.conn.ReportError(err)
		}
	}

	watcher.mu.Lock()
	watcher.lastAddrs = addrs
	var fns []func([]resolver.Address)
	for _, fn := range watcher.listeners {
		fns = append(fns, fn)
	}
	watcher.mu.Unlock()

	for _, fn := range fns {
		fn(addrs)
	}

	watcher.readyOnce.Do(func() {
		if watcher.ready != nil {
			close(watcher.ready)
		}
	})
}

type sharedResolverHandle struct {
	watcher *endpointResolver
	subID   uint64
}

func (h *sharedResolverHandle) ResolveNow(resolver.ResolveNowOptions) {}

func (h *sharedResolverHandle) Close() {
	h.watcher.removeListener(h.subID)
}
