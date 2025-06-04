package database

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"
)

// PersistableEntry is a helper struct for RDB saving.
type PersistableEntry struct {
	Value      interface{}  // Actual value: string for RDBStringValueType, map[string]string for RDBHashValueType
	Type       RDBValueType // Type of the value (RDBStringValueType, RDBHashValueType, etc.)
	ExpiryTime time.Time    // Zero value if no expiry
}

// DataStore holds the key-value store, including strings, streams, hashes, and indexes
type DataStore struct {
	data     map[string]interface{}    // For simple GET/SET string values primarily
	Streams  map[string]*Stream        // Stores all stream data
	Hashes   map[string]map[string]string // Stores all hash data
	IndexMgr *IndexManager             // Manages secondary indexes
	expiries map[string]time.Time      // Stores expiry times for keys
	mu       sync.Mutex                // Protects concurrent access to data, Streams, Hashes, expiries maps
}

// NewDataStore creates and returns a new DataStore
func NewDataStore() *DataStore {
	ds := &DataStore{
		data:     make(map[string]interface{}),
		Streams:  make(map[string]*Stream),
		Hashes:   make(map[string]map[string]string),
		expiries: make(map[string]time.Time),
	}
	ds.IndexMgr = InitializeDefaultIndexes()
	return ds
}

// isExpired checks if a key is expired.
// IMPORTANT: This method assumes the caller (like Get, TTL etc.) already holds the ds.mu lock.
func (ds *DataStore) isExpired(key string) bool {
	expiryTime, ok := ds.expiries[key]
	if !ok {
		return false
	}
	if time.Now().After(expiryTime) {
		actualHashData, hashExisted := ds.Hashes[key]

		delete(ds.data, key)
		delete(ds.Hashes, key)
		delete(ds.Streams, key)
		delete(ds.expiries, key)

		if hashExisted && ds.IndexMgr != nil {
			ds.IndexMgr.RemoveFromIndexOnDel(key, actualHashData)
		}
		return true
	}
	return false
}

// SetOptions holds options for the Set command.
type SetOptions struct {
	NX bool
	XX bool
}

// SetResult holds the result of a Set operation.
type SetResult struct {
	OldValue interface{}
	Existed  bool
	DidSet   bool
}

// setInternal is the core logic for Set, assuming the lock is already held.
// It takes `existedBefore` and `oldValueBefore` as arguments because `ds.data[key]` might have been
// modified by `isExpired` if the key was fetched and found expired before calling setInternal.
func (ds *DataStore) setInternal(key string, value interface{}, opts SetOptions, existedBeforeClearExpiry bool, oldValueBeforeClearExpiry interface{}) SetResult {
	// Note: existedBeforeClearExpiry and oldValueBeforeClearExpiry represent the state *before* any potential
	// deletion by a preceding isExpired() call in the calling function (like Incr).
	// However, for a simple Set, the caller (public Set) would fetch current `existed` and `oldValue` state.

	// If called from public Set, existedBefore and oldValueBefore are current state.
	// If called from Incr, existedBefore and oldValueBefore are state *before* Incr's isExpired check.

	// Re-check existence here if critical, but public Set passes current state.
	// For Incr, it passes the state it observed.

	if opts.NX && existedBeforeClearExpiry { // Use the passed-in 'existed' status
		return SetResult{OldValue: oldValueBeforeClearExpiry, Existed: existedBeforeClearExpiry, DidSet: false}
	}
	if opts.XX && !existedBeforeClearExpiry { // Use the passed-in 'existed' status
		return SetResult{OldValue: nil, Existed: existedBeforeClearExpiry, DidSet: false}
	}

	ds.data[key] = value
	delete(ds.expiries, key) // SET clears expiry (no need for time.Time{} diagnostic anymore)

	// If the key previously existed, oldValueBeforeClearExpiry is its value.
	// If it was a new key, oldValueBeforeClearExpiry would be nil (or zero value from map access).
	return SetResult{OldValue: oldValueBeforeClearExpiry, Existed: existedBeforeClearExpiry, DidSet: true}
}


// Set adds or updates a key-value pair in the DataStore (for simple string values).
func (ds *DataStore) Set(key string, value interface{}, opts SetOptions) SetResult {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	// Get current state *after* lock acquisition
	oldValue, existed := ds.data[key]
	// If key was expired, Get/isExpired would delete it.
	// But Set itself doesn't care if it was expired before this call, only if it *currently* exists for NX/XX.
	// The isExpired check is primarily for read operations or TTL/Expire.
	// SET operation overwrites or creates. If it was expired, it's like it didn't exist for SET's purpose.
	// However, if an isExpired call *within another command* (like Incr) deleted it,
	// then `existed` here will be false, which is correct for NX/XX logic at this point.
	return ds.setInternal(key, value, opts, existed, oldValue)
}


// Get retrieves a value by its key from the DataStore (for simple string values).
func (ds *DataStore) Get(key string) (interface{}, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.isExpired(key) {
		return nil, false
	}
	val, ok := ds.data[key]
	if !ok {
		return nil, false
	}
	return val, ok
}

// GetAllPersistableData collects all data from the DataStore for RDB saving.
func (ds *DataStore) GetAllPersistableData() map[string]PersistableEntry {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	allData := make(map[string]PersistableEntry)

	for key, value := range ds.data {
		expiryTime, _ := ds.expiries[key]
		allData[key] = PersistableEntry{
			Value:      value,
			Type:       RDBStringValueType,
			ExpiryTime: expiryTime,
		}
	}
	for key, hashValue := range ds.Hashes {
		if _, exists := allData[key]; exists {
			fmt.Printf("Warning: Key %s exists as multiple types. Hash will overwrite previous entry in RDB dump data collection.\n", key)
		}
		expiryTime, _ := ds.expiries[key]
		allData[key] = PersistableEntry{
			Value:      hashValue,
			Type:       RDBHashValueType,
			ExpiryTime: expiryTime,
		}
	}
	return allData
}

// HSet sets the string value of a hash field.
func (ds *DataStore) HSet(key string, field string, value string) (int, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.isExpired(key) {}

	hash, ok := ds.Hashes[key]
	if !ok {
		hash = make(map[string]string)
		ds.Hashes[key] = hash
	}

	oldValue, fieldExisted := hash[field]
	hash[field] = value
	ds.expiries[key] = time.Time{}
	delete(ds.expiries, key)

	if ds.IndexMgr != nil {
		if fieldExisted {
			ds.IndexMgr.UpdateIndexOnSet(key, field, oldValue, value)
		} else {
			ds.IndexMgr.UpdateIndexOnSet(key, field, "", value)
		}
	}
	if fieldExisted { return 0, nil }
	return 1, nil
}

// HGet retrieves the value of a hash field.
func (ds *DataStore) HGet(key string, field string) (string, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if ds.isExpired(key) { return "", false }
	hash, ok := ds.Hashes[key]
	if !ok { return "", false }
	value, found := hash[field]
	return value, found
}

// HDel deletes one or more hash fields.
func (ds *DataStore) HDel(key string, fields ...string) (int, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if ds.isExpired(key) { return 0, nil }
	hash, ok := ds.Hashes[key]
	if !ok { return 0, nil }

	deletedCount := 0
	modified := false
	if len(fields) > 0 {
		for _, fieldName := range fields {
			oldValue, fieldExisted := hash[fieldName]
			if fieldExisted {
				delete(hash, fieldName)
				deletedCount++
				modified = true
				if ds.IndexMgr != nil {
					ds.IndexMgr.RemoveFieldFromIndex(key, fieldName, oldValue)
				}
			}
		}
		if modified {
			ds.expiries[key] = time.Time{}
			delete(ds.expiries, key)
		}
	}
	if len(hash) == 0 { delete(ds.Hashes, key) }
	return deletedCount, nil
}

// internalDel is called by Expire or isExpired when a key is confirmed to be deleted.
// Assumes lock is already held.
func (ds *DataStore) internalDel(key string) {
	hashData, hashExisted := ds.Hashes[key]
	delete(ds.data, key)
	delete(ds.Hashes, key)
	delete(ds.Streams, key)
	delete(ds.expiries, key)
	if hashExisted && ds.IndexMgr != nil {
		dataCopy := make(map[string]string, len(hashData))
		for k, v := range hashData { dataCopy[k] = v }
		ds.IndexMgr.RemoveFromIndexOnDel(key, dataCopy)
	}
}

// Del deletes one or more keys.
func (ds *DataStore) Del(keys ...string) int {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	deletedCount := 0
	for _, key := range keys {
		_, existsInData := ds.data[key]
		_, existsInHashes := ds.Hashes[key]
		_, existsInStreams := ds.Streams[key]
		if existsInData || existsInHashes || existsInStreams {
			ds.internalDel(key)
			deletedCount++
		}
	}
	return deletedCount
}

// Expire sets an expiry for a key.
func (ds *DataStore) Expire(key string, seconds int64) (bool, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	_, existsInData := ds.data[key]
	_, existsInHashes := ds.Hashes[key]
	_, existsInStreams := ds.Streams[key]
	keyExists := existsInData || existsInHashes || existsInStreams
	if !keyExists { return false, nil }
	if ds.isExpired(key) { return false, nil }
	if seconds <= 0 {
		ds.internalDel(key)
		return true, nil
	}
	expiryTime := time.Now().Add(time.Duration(seconds) * time.Second)
	ds.expiries[key] = expiryTime
	return true, nil
}

// TTL returns the remaining time to live of a key that has a timeout.
func (ds *DataStore) TTL(key string) (int64, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if ds.isExpired(key) { return -2, nil }
	_, existsInData := ds.data[key]
	_, existsInHashes := ds.Hashes[key]
	_, existsInStreams := ds.Streams[key]
	keyActuallyExists := existsInData || existsInHashes || existsInStreams
	if !keyActuallyExists { return -2, nil }
	expiryTime, ok := ds.expiries[key]
	if !ok { return -1, nil }
	remainingDuration := time.Until(expiryTime)
	if remainingDuration.Nanoseconds() <= 0 { return -2, nil }
	remainingSeconds := int64(remainingDuration.Seconds())
	if remainingDuration.Nanoseconds() > 0 && remainingSeconds == 0 { return 1, nil }
	return remainingSeconds, nil
}

// Pexpire sets an expiry for a key in milliseconds.
func (ds *DataStore) Pexpire(key string, milliseconds int64) (bool, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	_, existsInData := ds.data[key]
	_, existsInHashes := ds.Hashes[key]
	_, existsInStreams := ds.Streams[key]
	keyExists := existsInData || existsInHashes || existsInStreams
	if !keyExists { return false, nil }
	if ds.isExpired(key) { return false, nil }
	if milliseconds <= 0 {
		ds.internalDel(key)
		return true, nil
	}
	expiryTime := time.Now().Add(time.Duration(milliseconds) * time.Millisecond)
	ds.expiries[key] = expiryTime
	return true, nil
}

// PTTL returns the remaining time to live of a key that has a timeout in milliseconds.
func (ds *DataStore) PTTL(key string) (int64, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if ds.isExpired(key) { return -2, nil }
	_, existsInData := ds.data[key]
	_, existsInHashes := ds.Hashes[key]
	_, existsInStreams := ds.Streams[key]
	keyActuallyExists := existsInData || existsInHashes || existsInStreams
	if !keyActuallyExists { return -2, nil }
	expiryTime, ok := ds.expiries[key]
	if !ok { return -1, nil }
	remainingMilliseconds := time.Until(expiryTime).Milliseconds()
	if remainingMilliseconds < 0 { return -2, nil }
	return remainingMilliseconds, nil
}

// Incr increments the integer value of a key by the given amount.
func (ds *DataStore) Incr(key string, increment int64) (int64, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.isExpired(key) { /* Key acts as non-existent */ }

	currentValue, existed := ds.data[key]
	var newValueInt64 int64

	if !existed {
		newValueInt64 = increment
	} else {
		currentValueStr, ok := currentValue.(string)
		if !ok {
			byteVal, okByte := currentValue.([]byte)
			if okByte {
				currentValueStr = string(byteVal)
			} else {
				return 0, fmt.Errorf("ERR value is not an integer or out of range (wrong type %T)", currentValue)
			}
		}
		parsedInt, err := strconv.ParseInt(currentValueStr, 10, 64)
		if err != nil {
			return 0, errors.New("ERR value is not an integer or out of range")
		}
		newValueInt64 = parsedInt + increment
	}
	// Call setInternal directly, passing the state observed by Incr
	// existed is true if key was in ds.data *before* Incr's isExpired check might have removed it.
	// currentValue is the value from ds.data *before* Incr's isExpired check.
	// This means setInternal's NX/XX will operate on the state Incr initially saw.
	ds.setInternal(key, strconv.FormatInt(newValueInt64, 10), SetOptions{}, existed, currentValue)
	return newValueInt64, nil
}

// RDBValueType is defined in rdb.go and accessible within the database package.
// Type returns the string representation of the type of value stored at key.
// Possible return values are "string", "hash", "stream", "list", "set", "zset", or "none" if key does not exist.
// It handles expired keys by treating them as non-existent.
func (ds *DataStore) Type(key string) (string, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.isExpired(key) { // This will delete the key if it's expired
		return "none", nil
	}

	if _, ok := ds.data[key]; ok {
		return "string", nil
	}
	if _, ok := ds.Hashes[key]; ok {
		return "hash", nil
	}
	if _, ok := ds.Streams[key]; ok {
		return "stream", nil
	}
	// TODO: Add checks for List, Set, ZSet when implemented.

	return "none", nil // Key not found in any type-specific store
}

// type RDBValueType byte
// const (
// 	RDBStringValueType RDBValueType = 0
// 	RDBHashValueType   RDBValueType = 1
// 	RDBStreamValueType RDBValueType = 2
// )
