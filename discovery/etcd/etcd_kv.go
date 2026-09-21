package etcd

import (
	"context"
	"fmt"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/log"
	"github.com/openimsdk/tools/utils/datautil"
)

type kvHub struct {
	client        *clientv3.Client
	rootDirectory string

	watchMu      sync.Mutex
	watchEntries map[string]*watchKeyEntry // key -> *watchKeyEntry

	keepAliveMu      sync.Mutex
	keepAliveCancels []context.CancelFunc
}

func newKVHub(client *clientv3.Client, rootDirectory string) *kvHub {
	return &kvHub{
		client:        client,
		rootDirectory: rootDirectory,
		watchEntries:  make(map[string]*watchKeyEntry),
	}
}

func (h *kvHub) combineKeyWithPrefix(key string) string {
	return fmt.Sprintf("%s/%s", h.rootDirectory, key)
}

// keepAliveLease maintains the lease alive by sending keep-alive requests
func (h *kvHub) keepAliveLease(ctx context.Context, leaseID clientv3.LeaseID) {
	ch, err := h.client.KeepAlive(ctx, leaseID)
	if err != nil {
		return
	}
	for ka := range ch {
		if ka == nil {
			return
		}
	}
}

func (h *kvHub) newKVKeepAliveContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	h.keepAliveMu.Lock()
	h.keepAliveCancels = append(h.keepAliveCancels, cancel)
	h.keepAliveMu.Unlock()
	return ctx
}

func (h *kvHub) stopKVKeepAlives() {
	h.keepAliveMu.Lock()
	cancels := h.keepAliveCancels
	h.keepAliveCancels = nil
	h.keepAliveMu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
}

func (h *kvHub) SetKey(ctx context.Context, key string, data []byte) error {
	if _, err := h.client.Put(ctx, h.combineKeyWithPrefix(key), string(data)); err != nil {
		return errs.WrapMsg(err, "etcd put err")
	}
	return nil
}

func (h *kvHub) setKeyWithLease(ctx context.Context, key string, val []byte, ttl int64) (clientv3.LeaseID, error) {
	leaseResp, err := h.client.Grant(ctx, ttl)
	if err != nil {
		return 0, errs.Wrap(err)
	}

	_, err = h.client.Put(ctx, h.combineKeyWithPrefix(key), string(val), clientv3.WithLease(leaseResp.ID))
	if err != nil {
		_, _ = h.client.Revoke(ctx, leaseResp.ID)
		return 0, errs.Wrap(err)
	}

	return leaseResp.ID, nil
}

func (h *kvHub) SetWithLease(ctx context.Context, key string, val []byte, ttl int64) error {
	id, err := h.setKeyWithLease(ctx, key, val, ttl)
	if err != nil {
		return errs.Wrap(err)
	}
	keepCtx := h.newKVKeepAliveContext()

	go func() {
		for {
			h.keepAliveLease(keepCtx, id)
			if keepCtx.Err() != nil {
				return
			}

			log.ZWarn(
				context.Background(),
				"etcd lease keepalive stopped, resetting key with lease",
				nil,
				zap.String("key", key),
				zap.Int64("leaseID", int64(id)),
			)

			if !sleepWithContext(keepCtx, keepAliveRetryDelay) {
				return
			}

			retryCtx, cancel := withTimeout(keepCtx, defaultRegisterTimeout)
			newID, err := h.setKeyWithLease(retryCtx, key, val, ttl)
			cancel()
			if err != nil {
				log.ZWarn(
					context.Background(),
					"reset etcd key with lease failed",
					err,
					zap.String("key", key),
					zap.Int64("leaseID", int64(id)),
				)
				continue
			}
			id = newID
		}
	}()

	return nil
}

func (h *kvHub) GetKey(ctx context.Context, key string) ([]byte, error) {
	resp, err := h.client.Get(ctx, h.combineKeyWithPrefix(key))
	if err != nil {
		return nil, errs.WrapMsg(err, "etcd get err")
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	return resp.Kvs[0].Value, nil
}

func (h *kvHub) GetKeyWithPrefix(ctx context.Context, key string) ([][]byte, error) {
	resp, err := h.client.Get(ctx, h.combineKeyWithPrefix(key), clientv3.WithPrefix())
	if err != nil {
		return nil, errs.WrapMsg(err, "etcd get err")
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	return datautil.Batch(func(kv *mvccpb.KeyValue) []byte { return kv.Value }, resp.Kvs), nil
}

func (h *kvHub) DelData(ctx context.Context, key string) error {
	if _, err := h.client.Delete(ctx, h.combineKeyWithPrefix(key)); err != nil {
		return errs.WrapMsg(err, "etcd delete err")
	}
	return nil
}
