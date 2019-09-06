package redistorage

import (
	"time"

	"github.com/go-redis/redis"

	log "github.com/sirupsen/logrus"
)

type Queue struct {
	config        QueueConfig
	storage       *Storage
	queueKey      string
	processingKey string
	lastItem      string
	items         chan string
	errors        chan error
}

// Start the blocking redis queue reader
func (q *Queue) Start() {
	// stuck items checker
	go doEvery(q.config.ProcessingTimeout, q.check)
	for {
		i, err := q.storage.ListBlockingPopAndPush(q.queueKey, q.processingKey, q.config.BlockingDuration)
		if err != nil {
			// timeout reached
			q.errors <- err
			continue
		}

		// got something to work on
		q.items <- i
	}
}

// Done removes the item from the processing list
func (q *Queue) Done(i string) error {
	_, err := q.storage.ListRemove(q.processingKey, 0, i)
	return err
}

// Items feeds queue items through a channel
func (q *Queue) Items() <-chan string {
	return q.items
}

// Errors feeds any errors happening while listening for items through a channel
func (q *Queue) Errors() <-chan error {
	return q.errors
}

func (q *Queue) check() {
	log.Debug("checking for stuck items")
	// if last element of the list is always the same, it s stuck
	i, err := q.storage.ListGet(q.processingKey, -1)
	if err == redis.Nil {
		// nothing in the processing queue
		return
	} else if err != nil {
		log.Errorf("check list get %s", err)
		return
	}
	if q.lastItem == i {
		log.Infof("%s stuck in queue, retrying", i)
		err := q.storage.ListUnshift(q.queueKey, i)
		if err != nil {
			log.Errorf("check list unshift %s", err)
			return
		}
		// remove from processing list
		q.Done(i)
		// update to last item in processing list
		i, _ = q.storage.ListGet(q.processingKey, -1)
	}
	// set for next check
	q.lastItem = i
}

// NewQueue creates a new redis reliable queue instance
func NewQueue(s *Storage, qk string, pk string, c QueueConfig) *Queue {

	return &Queue{
		config:        c,
		storage:       s,
		queueKey:      qk,
		processingKey: pk,
		lastItem:      "",
		items:         make(chan string),
		errors:        make(chan error),
	}
}

func doEvery(d time.Duration, f func()) {
	for range time.Tick(d) {
		f()
	}
}
