package redistorage

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
	log "github.com/sirupsen/logrus"
)

// Storage facilitates streaming to redis
type Storage struct {
	Client   *redis.Client
	Receiver chan redis.Message
}

// New create a new redis cache connection
func New(host, password string) *Storage {
	return &Storage{
		Client:   createRedisClient(host, password),
		Receiver: make(chan redis.Message, 0),
	}
}

// Exists checks if keys exist and returns count of existing
func (s *Storage) Exists(keys ...string) (int64, error) {
	return s.Client.Exists(keys...).Result()
}

// Duplicate creates a copy of a key
func (s *Storage) Duplicate(key string, newKey string) error {
	return s.Client.Eval(
		`return redis.call('SET', KEYS[2], redis.call('GET', KEYS[1]))`,
		[]string{key, newKey},
	).Err()
}

// Count counts elements following a pattern
func (s *Storage) Count(pattern string) (int64, error) {
	r, err := s.Client.Eval(
		`return #redis.pcall('keys', KEYS[1])`,
		[]string{pattern},
	).Result()
	if err != nil {
		return 0, err
	}
	return r.(int64), nil
}

// KeyExists checks if single key exists
func (s *Storage) KeyExists(key string) (bool, error) {
	var count int64
	count, err := s.Client.Exists(key).Result()
	return count == 1, err
}

// Expire expires a key after duration
func (s *Storage) Expire(key string, after time.Duration) error {
	return s.Client.Expire(key, after).Err()
}

// GetKeys gets all keys for pattern
func (s *Storage) GetKeys(pattern string) ([]string, error) {
	return s.Client.Keys(pattern).Result()
}

// Set a value at key
func (s *Storage) Set(key string, value interface{}, expiration time.Duration) error {
	return s.Client.Set(key, value, expiration).Err()
}

// Get a value at key
func (s *Storage) Get(key string) (string, error) {
	return s.Client.Get(key).Result()
}

// GetStruct gets a value at key as struct
// obj needs to follow UnmarshalBinary interface
func (s *Storage) GetStruct(key string, obj interface{}) error {
	return s.Client.Get(key).Scan(obj)
}

// Remove deletes keys and values
func (s *Storage) Remove(keys ...string) (int64, error) {
	return s.Client.Del(keys...).Result()
}

// Rename renames a key to another
func (s *Storage) Rename(key, newKey string) error {
	return s.Client.Rename(key, newKey).Err()
}

// SetIfNotExists only sets if it doesn't exist
func (s *Storage) SetIfNotExists(key string, value interface{}, expiration time.Duration) (bool, error) {
	return s.Client.SetNX(key, value, expiration).Result()
}

// GetInt gets value as int
func (s *Storage) GetInt(key string) (int64, error) {
	return s.Client.Get(key).Int64()
}

// Hashmaps

// SetMap sets fields of a redis hashmap
func (s *Storage) SetMap(key string, fields map[string]interface{}) error {
	return s.Client.HMSet(key, fields).Err()
}

// SetMapField sets a field of a redis hashmap
func (s *Storage) SetMapField(key string, field string, value string) error {
	return s.Client.HSet(key, field, value).Err()
}

// GetMap gets all fields of a redis hashmap
func (s *Storage) GetMap(key string) (map[string]string, error) {
	return s.Client.HGetAll(key).Result()
}

// GetField gets field of a redis hashmap
func (s *Storage) GetField(key string, field string) (string, error) {
	return s.Client.HGet(key, field).Result()
}

// GetMapFields gets fields from map
func (s *Storage) GetMapFields(key string, fields ...string) ([]interface{}, error) {
	return s.Client.HMGet(key, fields...).Result()
}

// IncrementMapField Increments a hash maps field by value
func (s *Storage) IncrementMapField(key string, field string, inc int) error {
	return s.Client.HIncrBy(key, field, int64(inc)).Err()
}

// Lists
// ListLen returns a list length
func (s *Storage) ListLen(key string) (int64, error) {
	return s.Client.LLen(key).Result()
}

// ListGet returns list element at index
func (s *Storage) ListGet(key string, index int64) (string, error) {
	return s.Client.LIndex(key, index).Result()
}

// ListPush appends to list at key
func (s *Storage) ListPush(key string, values ...interface{}) error {
	return s.Client.RPush(key, values...).Err()
}

// ListUnshift prepends to list
func (s *Storage) ListUnshift(key string, values ...interface{}) error {
	return s.Client.LPush(key, values...).Err()
}

// ListShift removes first list element and returns it
func (s *Storage) ListShift(key string) (string, error) {
	return s.Client.LPop(key).Result()
}

// ListBlockingPopAndPush Blocks until an item is available, pops it from a list and pushes it to another
// times out after 10 minutes
func (s *Storage) ListBlockingPopAndPush(from, to string, wait time.Duration) (string, error) {
	return s.Client.BRPopLPush(from, to, wait).Result()
}

// ListRemove removes count of item from list
func (s *Storage) ListRemove(key string, count int64, value string) (int64, error) {
	return s.Client.LRem(key, count, value).Result()
}

// Sets

// SetAdd adds member to a set at key
func (s *Storage) SetAdd(key string, member string) error {
	return s.Client.SAdd(key, member).Err()
}

// GetSet gets set at key
func (s *Storage) SetGet(key string) ([]string, error) {
	return s.Client.SMembers(key).Result()
}

func (s *Storage) SetHasMember(key string, member string) (bool, error) {
	return s.Client.SIsMember(key, member).Result()
}

// SortedSetCount adds a member with score to sorted set
func (s *Storage) SortedSetCount(key, min, max string) (int64, error) {
	return s.Client.ZCount(key, min, max).Result()
}

// SortedSetAdd adds a member with score to sorted set
func (s *Storage) SortedSetAdd(key string, member string, score float64) error {
	return s.Client.ZAdd(key, redis.Z{Score: score, Member: member}).Err()
}

// SortedSetRemove adds a member with score to sorted set
func (s *Storage) SortedSetRemove(key string, member ...interface{}) (int64, error) {
	return s.Client.ZRem(key, member...).Result()
}

// SortedSetUpdate only sets score if it exists
func (s *Storage) SortedSetUpdate(key string, member string, score float64) error {
	return s.Client.ZAddXX(key, redis.Z{Score: score, Member: member}).Err()
}

// SortedSetGetByScore
func (s *Storage) SortedSetGetByScore(key string, min string, max string, limit int64) ([]string, error) {
	r := redis.ZRangeBy{Min: min, Max: max, Count: limit}
	return s.Client.ZRangeByScore(key, r).Result()
}

// SortedSetRemoveByScore
func (s *Storage) SortedSetRemoveByScore(key string, min string, max string) (int64, error) {
	return s.Client.ZRemRangeByScore(key, min, max).Result()
}

// GetSortedSetLTH gets sorted set at key low to high
func (s *Storage) GetSortedSetLTH(key string, start int64, stop int64) ([]string, error) {
	return s.Client.ZRange(key, start, stop).Result()
}

// GetSortedSetHTL gets sorted set at key high to low
func (s *Storage) GetSortedSetHTL(key string, start int64, stop int64) ([]string, error) {
	return s.Client.ZRevRange(key, start, stop).Result()
}

// IncrementSortedSetMemberScore Increments the score of a sorted set member
func (s *Storage) IncrementSortedSetMemberScore(key string, member string, inc float64) error {
	return s.Client.ZIncrBy(key, inc, member).Err()
}

// MoveSortedSetMemberUpdating moves a sorted set member between sets and updates score
func (s *Storage) MoveSortedSetMemberUpdating(fromKey string, toKey string, member string, newScore float64) error {
	pipe := s.Client.TxPipeline()
	pipe.ZAdd(toKey, redis.Z{Member: member, Score: newScore})
	pipe.ZRem(fromKey, member)
	_, err := pipe.Exec()
	return err
}

// KeysPrefixCount returns the number of keys with a given prefix
func (s *Storage) KeysPrefixCount(prefix string) (int64, error) {
	script := fmt.Sprintf("return #redis.pcall('keys', '%s*')", prefix)
	result, err := s.Client.Eval(script, []string{}).Result()
	count, _ := result.(int64)
	return count, err
}

// SortedSetsCardSum returns the total number of elements in a list of sets
func (s *Storage) SortedSetsCardSum(prefix string) (int64, error) {
	script := fmt.Sprintf(
		`local sum = 0
		 local matches = redis.pcall('KEYS', '%s*')
		
		 for _,key in ipairs(matches) do
		 	local val = redis.pcall('ZCARD', key)
		 	sum = sum + tonumber(val)
		 end
		
		 return sum`, prefix)
	result, err := s.Client.Eval(script, []string{}).Result()
	count, _ := result.(int64)
	return count, err
}

// GetScore gets core of member from set
func (s *Storage) GetScore(key string, member string) (float64, error) {
	return s.Client.ZScore(key, member).Result()
}

// Events

// Publish pushes a message to a channel
func (s *Storage) Publish(channel string, message string) error {
	return s.Client.Publish(channel, message).Err()
}

// Start connects to redis and starts listening for events
func (s *Storage) Start(subs ...string) {
	s.subscribe(subs...)
}

func (s *Storage) subscribe(subs ...string) {
	pubsub := s.Client.Subscribe(subs...)
	defer pubsub.Close()
	log.Debug("subscribed")
	for {
		msg, err := pubsub.ReceiveMessage()
		if err != nil {
			log.Error("failed to receive", err)
			break
		}

		s.Receiver <- *msg
	}

}

func createRedisClient(host, password string) *redis.Client {
	for {
		// Connect to redis
		// Create connection to redis
		redisClient := redis.NewClient(&redis.Options{
			Addr:     host,
			Password: password, // no password set
			DB:       0,        // TODO use default DB, should not be hardcoded
		})

		_, err := redisClient.Ping().Result()
		if err != nil {
			log.Println("error connecting to redis", err)
			time.Sleep(time.Second)
			continue
		}
		log.Info(`Connected to redis`)
		return redisClient
	}
}
