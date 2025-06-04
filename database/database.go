package database

import "fmt"

import "sync"

// DataStore holds the key-value store, including strings, streams, hashes, and indexes
type DataStore struct {
	data     map[string]interface{}    // For simple GET/SET string values primarily
	Streams  map[string]*Stream        // Stores all stream data
	Hashes   map[string]map[string]string // Stores all hash data
	IndexMgr *IndexManager             // Manages secondary indexes
	mu       sync.Mutex                // Protects concurrent access to data, Streams, Hashes maps
}

// NewDataStore creates and returns a new DataStore
func NewDataStore() *DataStore {
	ds := &DataStore{
		data:    make(map[string]interface{}),
		Streams: make(map[string]*Stream),
		Hashes:  make(map[string]map[string]string),
	}
	ds.IndexMgr = InitializeDefaultIndexes() // Initialize default indexes
	return ds
}

// Set adds or updates a key-value pair in the DataStore
func (ds *DataStore) Set(key string, value interface{}) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.data[key] = value
}

// Get retrieves a value by its key from the DataStore
// It returns the value and a boolean indicating if the key was found.
func (ds *DataStore) Get(key string) (interface{}, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	val, ok := ds.data[key]
	return val, ok
}

// GetAllData returns a copy of all key-value pairs in the DataStore (string values only for now).
// This is used for persistence operations like saving to RDB.
func (ds *DataStore) GetAllData() map[string]interface{} {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	dataCopy := make(map[string]interface{}, len(ds.data))
	for k, v := range ds.data {
		dataCopy[k] = v // Assuming RDB save will handle type assertion to string
	}
	return dataCopy
}

// HSet sets the string value of a hash field.
// Returns 1 if field is a new field in the hash and value was set.
// Returns 0 if field already exists in the hash and the value was updated.
func (ds *DataStore) HSet(key string, field string, value string) (int, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	hash, ok := ds.Hashes[key]
	if !ok {
		hash = make(map[string]string)
		ds.Hashes[key] = hash
	}

	oldValue, fieldExisted := hash[field]
	hash[field] = value

	// Update indexes
	if ds.IndexMgr != nil {
		if fieldExisted {
			ds.IndexMgr.UpdateIndexOnSet(key, field, oldValue, value)
		} else {
			ds.IndexMgr.UpdateIndexOnSet(key, field, "", value) // No old value
		}
	}

	if fieldExisted {
		return 0, nil // Field updated
	}
	return 1, nil // New field created
}

// HGet retrieves the value of a hash field.
func (ds *DataStore) HGet(key string, field string) (string, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	hash, ok := ds.Hashes[key]
	if !ok {
		return "", false // Key not found
	}

	value, found := hash[field]
	return value, found
}

// HDel deletes one or more hash fields.
// Returns the number of fields that were removed from the hash, not including specified but non existing fields.
func (ds *DataStore) HDel(key string, fields ...string) (int, error) {
    ds.mu.Lock()
    defer ds.mu.Unlock()

    hash, ok := ds.Hashes[key]
    if !ok {
        return 0, nil // Key not found
    }

    deletedCount := 0
    for _, field := range fields {
        oldValue, fieldExisted := hash[field]
        if fieldExisted {
            delete(hash, field)
            deletedCount++
            if ds.IndexMgr != nil {
                ds.IndexMgr.RemoveFieldFromIndex(key, field, oldValue)
            }
        }
    }

    if len(hash) == 0 { // If hash becomes empty after HDEL, delete the key itself
        delete(ds.Hashes, key)
    }
    return deletedCount, nil
}


// Del deletes a key. It needs to handle different types of keys (string, hash, stream etc.)
// and also update indexes if a hash key is deleted.
// For now, it only handles string keys from `ds.data` and hash keys from `ds.Hashes`.
func (ds *DataStore) Del(keys ...string) int {
    ds.mu.Lock()
    defer ds.mu.Unlock()

    deletedCount := 0
    for _, key := range keys {
        // Check simple string keys
        if _, ok := ds.data[key]; ok {
            delete(ds.data, key)
            deletedCount++
            continue // Move to next key
        }

        // Check hash keys
        if hashData, ok := ds.Hashes[key]; ok {
            delete(ds.Hashes, key)
            deletedCount++
            // Update indexes for the deleted hash
            if ds.IndexMgr != nil {
                // Pass a copy of hashData as it might be modified by RemoveFromIndexOnDel if not careful,
                // though current RemoveFromIndexOnDel doesn't modify its input map.
                // Making a copy is safer if an index implementation might want to iterate/modify.
                dataCopy := make(map[string]string, len(hashData))
                for k,v := range hashData { dataCopy[k] = v }
                ds.IndexMgr.RemoveFromIndexOnDel(key, dataCopy)
            }
            continue
        }

        // TODO: Check stream keys, list keys, set keys etc. when they are implemented
    }
    return deletedCount
}


func Connect() {
	fmt.Println("Connected to the database.")
}

// Placeholder types for other data structures
type List []interface{}
// type Hash map[string]interface{} // This was a placeholder, now we implement Hashes in DataStore
type Set map[interface{}]struct{}
type SortedSet struct{} // Placeholder, will need a more complex structure
