package etcd

import (
	"context"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type reconnectingWatcher struct {
	client *clientv3.Client

	mu      sync.Mutex
	watcher clientv3.Watcher
	closed  bool
}

func newReconnectingWatcher(client *clientv3.Client, watch clientv3.Watcher) *reconnectingWatcher {
	return &reconnectingWatcher{client: client, watcher: watch}
}

func (w *reconnectingWatcher) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	out := make(chan clientv3.WatchResponse)
	go func() {
		defer close(out)
		watch := w.current()
		if watch == nil {
			return
		}
		updates := watch.Watch(ctx, key, opts...)
		for {
			select {
			case <-ctx.Done():
				return
			case update, ok := <-updates:
				if !ok {
					w.replace(watch)
					return
				}
				if err := update.Err(); err != nil {
					w.replace(watch)
				}
				select {
				case out <- update:
				case <-ctx.Done():
					return
				}
				if update.Err() != nil {
					return
				}
			}
		}
	}()
	return out
}

func (w *reconnectingWatcher) RequestProgress(ctx context.Context) error {
	watch := w.current()
	if watch == nil {
		return context.Canceled
	}
	return watch.RequestProgress(ctx)
}

func (w *reconnectingWatcher) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	watch := w.watcher
	w.watcher = nil
	w.mu.Unlock()
	if watch == nil {
		return nil
	}
	return watch.Close()
}

func (w *reconnectingWatcher) current() clientv3.Watcher {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	return w.watcher
}

func (w *reconnectingWatcher) replace(old clientv3.Watcher) {
	w.mu.Lock()
	if w.closed || w.watcher != old {
		w.mu.Unlock()
		return
	}
	w.watcher = clientv3.NewWatcher(w.client)
	w.mu.Unlock()
	_ = old.Close()
}
