package etcd

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"

	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/utils/datautil"
)

type connPool struct {
	client        *clientv3.Client
	rootDirectory string
	resolver      resolver.Builder

	mu                 sync.RWMutex
	connMap            map[string][]*grpc.ClientConn // fullServiceKey -> []*ClientConn
	dialOptions        []grpc.DialOption
	serviceDialOptions map[string][]grpc.DialOption

	resolversMu sync.Mutex
	resolvers   map[string]*endpointResolver // prefix -> *endpointResolver
	watchNames  []string
}

func newConnPool(client *clientv3.Client, rootDirectory string, watchNames []string) *connPool {
	cp := &connPool{
		client:             client,
		rootDirectory:      rootDirectory,
		connMap:            make(map[string][]*grpc.ClientConn),
		serviceDialOptions: make(map[string][]grpc.DialOption),
		resolvers:          make(map[string]*endpointResolver),
		watchNames:         watchNames,
	}
	cp.resolver = resolverBuilder{client: client, pool: cp}
	cp.watchServiceChanges()
	return cp
}

func (cp *connPool) combineKeyWithPrefix(key string) string {
	return fmt.Sprintf("%s/%s", cp.rootDirectory, key)
}

func (cp *connPool) getOrCreateResolver(prefix string) (*endpointResolver, error) {
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	cp.resolversMu.Lock()
	defer cp.resolversMu.Unlock()

	if rsv, ok := cp.resolvers[prefix]; ok {
		return rsv, nil
	}
	if cp.client == nil {
		return nil, fmt.Errorf("etcd client closed")
	}

	ctx, cancel := context.WithCancel(cp.client.Ctx())
	rsv := &endpointResolver{
		client: cp.client,
		prefix: prefix,
		cancel: cancel,
		done:   make(chan struct{}),
		ready:  make(chan struct{}),
	}

	serviceName := strings.TrimPrefix(prefix, cp.rootDirectory+"/")
	serviceName = strings.TrimSuffix(serviceName, "/")
	rsv.addListener(func(addrs []resolver.Address) {
		cp.syncConnMap(serviceName, addrs)
	})

	cp.resolvers[prefix] = rsv
	go rsv.run(ctx)
	return rsv, nil
}

func (cp *connPool) ensureServiceResolver(serviceName string) (*endpointResolver, error) {
	prefix := cp.combineKeyWithPrefix(serviceName)
	return cp.getOrCreateResolver(prefix)
}

func (cp *connPool) buildSharedResolver(prefix string, conn resolver.ClientConn) (resolver.Resolver, error) {
	rsv, err := cp.getOrCreateResolver(prefix)
	if err != nil {
		return nil, err
	}

	subID, initialAddrs := rsv.addListener(func(addrs []resolver.Address) {
		if err := conn.UpdateState(resolver.State{Addresses: addrs}); err != nil {
			conn.ReportError(err)
		}
	})

	if len(initialAddrs) > 0 {
		_ = conn.UpdateState(resolver.State{Addresses: initialAddrs})
	}

	return &sharedResolverHandle{
		resolver: rsv,
		subID:    subID,
	}, nil
}

func (cp *connPool) watchServiceChanges() {
	for _, s := range cp.watchNames {
		if _, err := cp.ensureServiceResolver(s); err != nil {
			log.ZWarn(context.Background(), "ensure service watcher err", err, zap.String("service", s))
		}
	}
}

func (cp *connPool) stopServiceResolvers() {
	cp.resolversMu.Lock()
	rsvs := make([]*endpointResolver, 0, len(cp.resolvers))
	for _, w := range cp.resolvers {
		rsvs = append(rsvs, w)
	}
	cp.resolvers = make(map[string]*endpointResolver)
	cp.resolversMu.Unlock()

	for _, w := range rsvs {
		w.Close()
	}
}

func (cp *connPool) syncConnMap(serviceName string, addresses []resolver.Address) {
	fullServiceKey := cp.combineKeyWithPrefix(serviceName)

	newAddrMap := make(map[string]struct{}, len(addresses))
	for _, a := range addresses {
		if a.Addr != "" {
			newAddrMap[a.Addr] = struct{}{}
		}
	}

	cp.mu.Lock()
	if cp.client == nil {
		cp.mu.Unlock()
		return
	}

	oldList := cp.connMap[fullServiceKey]
	oldAddresses := datautil.SliceToMapAny(oldList, func(e *grpc.ClientConn) (string, *grpc.ClientConn) { return e.Target(), e })

	var toKeep []*grpc.ClientConn
	var toAdd []string
	var toClose []*grpc.ClientConn

	seen := make(map[string]struct{}, len(addresses))
	for _, a := range addresses {
		addr := a.Addr
		if addr == "" {
			continue
		}
		if _, exists := seen[addr]; exists {
			continue
		}
		seen[addr] = struct{}{}
		if conn, ok := oldAddresses[addr]; ok {
			toKeep = append(toKeep, conn)
		} else {
			toAdd = append(toAdd, addr)
		}
	}

	for _, conn := range oldList {
		if _, ok := newAddrMap[conn.Target()]; !ok {
			toClose = append(toClose, conn)
		}
	}

	cp.connMap[fullServiceKey] = toKeep

	dialOpts := append([]grpc.DialOption{}, cp.dialOptions...)
	if storedOpts, ok := cp.serviceDialOptions[fullServiceKey]; ok && len(storedOpts) > 0 {
		dialOpts = append(dialOpts, storedOpts...)
	}
	dialOpts = append(dialOpts, grpc.WithResolvers(cp.resolver))
	cp.mu.Unlock()

	ctx := context.Background()
	for _, conn := range toClose {
		if err := conn.Close(); err != nil {
			log.ZWarn(ctx, "failed to close stale conn", err, zap.String("service", serviceName), zap.String("addr", conn.Target()))
		}
	}

	if len(toAdd) > 0 {
		var newlyCreated []*grpc.ClientConn
		for _, addr := range toAdd {
			conn, err := grpc.NewClient(addr, dialOpts...)
			if err != nil {
				log.ZWarn(ctx, "failed to dial new endpoint", err, zap.String("service", serviceName), zap.String("addr", addr))
				continue
			}
			newlyCreated = append(newlyCreated, conn)
		}

		if len(newlyCreated) > 0 {
			cp.mu.Lock()
			if currentList, ok := cp.connMap[fullServiceKey]; ok {
				cp.connMap[fullServiceKey] = append(currentList, newlyCreated...)
			} else {
				for _, c := range newlyCreated {
					_ = c.Close()
				}
			}
			cp.mu.Unlock()
		}
	}
}

// GetConns returns gRPC client connections for a given service name
func (cp *connPool) GetConns(ctx context.Context, serviceName string, opts ...grpc.DialOption) ([]grpc.ClientConnInterface, error) {
	rsv, err := cp.ensureServiceResolver(serviceName)
	if err != nil {
		return nil, err
	}

	fullServiceKey := cp.combineKeyWithPrefix(serviceName)

	if len(opts) > 0 {
		cp.mu.Lock()
		cp.serviceDialOptions[fullServiceKey] = append([]grpc.DialOption(nil), opts...)
		cp.mu.Unlock()
	}

	waitCtx, cancel := withTimeout(ctx, 3*time.Second)
	defer cancel()
	_ = rsv.waitReady(waitCtx)

	cp.mu.RLock()
	conns := cp.connMap[fullServiceKey]
	res := datautil.Slice(conns, func(e *grpc.ClientConn) grpc.ClientConnInterface { return e })
	cp.mu.RUnlock()

	if len(res) == 0 {
		if addrs := rsv.getAddresses(); len(addrs) > 0 {
			cp.syncConnMap(serviceName, addrs)
			cp.mu.RLock()
			conns = cp.connMap[fullServiceKey]
			res = datautil.Slice(conns, func(e *grpc.ClientConn) grpc.ClientConnInterface { return e })
			cp.mu.RUnlock()
		}
	}

	return res, nil
}

// GetConn returns a single gRPC client connection for a given service name
func (cp *connPool) GetConn(ctx context.Context, serviceName string, opts ...grpc.DialOption) (grpc.ClientConnInterface, error) {
	target := fmt.Sprintf("etcd:///%s", cp.combineKeyWithPrefix(serviceName))

	dialOpts := append(append(cp.dialOptions, opts...), grpc.WithResolvers(cp.resolver))

	return grpc.NewClient(target, dialOpts...)
}

// AddOption appends gRPC dial options to the existing options
func (cp *connPool) AddOption(opts ...grpc.DialOption) {
	cp.mu.Lock()
	toClose := cp.resetConnMapLocked()
	cp.dialOptions = append(cp.dialOptions, opts...)
	cp.mu.Unlock()

	ctx := context.Background()
	for _, c := range toClose {
		if err := c.Close(); err != nil {
			log.ZWarn(ctx, "failed to close conn", err)
		}
	}

	cp.resolversMu.Lock()
	type syncItem struct {
		serviceName string
		addrs       []resolver.Address
	}
	var toSync []syncItem
	for prefix, w := range cp.resolvers {
		serviceName := strings.TrimPrefix(prefix, cp.rootDirectory+"/")
		serviceName = strings.TrimSuffix(serviceName, "/")
		if addrs := w.getAddresses(); len(addrs) > 0 {
			toSync = append(toSync, syncItem{serviceName: serviceName, addrs: addrs})
		}
	}
	cp.resolversMu.Unlock()

	for _, item := range toSync {
		cp.syncConnMap(item.serviceName, item.addrs)
	}
}

func (cp *connPool) resetConnMapLocked() []*grpc.ClientConn {
	var toClose []*grpc.ClientConn
	for _, conns := range cp.connMap {
		toClose = append(toClose, conns...)
	}
	cp.connMap = make(map[string][]*grpc.ClientConn)
	return toClose
}

func (cp *connPool) close() {
	cp.stopServiceResolvers()
	cp.mu.Lock()
	toClose := cp.resetConnMapLocked()
	cp.mu.Unlock()

	for _, c := range toClose {
		_ = c.Close()
	}
}
