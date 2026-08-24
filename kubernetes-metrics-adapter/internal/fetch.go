package internal

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

var ctx = context.Background()

const SERVICE_STAGE_HASH = "service-stage"
const SERVICE_STAGE_RUNNING = "2"

func FetchQueueLengths(volatile *redis.Client, persistent *redis.Client) (*QueueSnapshot, error) {
	// Make sure we get every enabled service, even if their queue is empty
	status, err := volatile.HGetAll(ctx, SERVICE_STAGE_HASH).Result()

	if err != nil {
		return nil, err
	}

	// Fetch the queue lengths
	services := make(map[string]int64)
	for name, stage := range status {
		lower_name := strings.ToLower(name)
		if stage == SERVICE_STAGE_RUNNING {
			count, err := volatile.ZCard(ctx, "service-queue-"+name).Result()
			if err != nil {
				return nil, err
			}

			services[lower_name] = count
		}
	}

	snapshot := &QueueSnapshot{
		lengths:   services,
		timestamp: metav1.Now(),
	}

	return snapshot, nil
}

type QueueSnapshot struct {
	timestamp metav1.Time
	lengths   map[string]int64
}

type QueueMonitor struct {
	valuesLock sync.Mutex
	values     *QueueSnapshot
}

func (qm *QueueMonitor) set(snapshot *QueueSnapshot) {
	qm.valuesLock.Lock()
	defer qm.valuesLock.Unlock()
	qm.values = snapshot
}

func (qm *QueueMonitor) read() *QueueSnapshot {
	qm.valuesLock.Lock()
	defer qm.valuesLock.Unlock()
	return qm.values
}

func StartQueueMonitor(volatile *redis.Client, persistent *redis.Client) (*QueueMonitor, error) {

	initial, err := FetchQueueLengths(volatile, persistent)
	if err != nil {
		return nil, err
	}

	monitor := &QueueMonitor{
		values: initial,
	}

	go monitor.queueMonitor(volatile, persistent)

	return monitor, nil
}

func (qm *QueueMonitor) queueMonitor(volatile *redis.Client, persistent *redis.Client) {
	for {
		// Wait the read interval, 10 seconds for the first pass
		time.Sleep(10 * time.Second)

		// Try to reload the info
		new, err := FetchQueueLengths(volatile, persistent)
		if err != nil {
			klog.Errorf("Could not load queue info: %s\n", err)
			continue
		}

		qm.set(new)
	}
}
