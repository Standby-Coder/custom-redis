package database

import (
	"path/filepath" // For KeyPattern matching (filepath.Match)
	"sync"
	// "fmt" // For logging if needed
)

// Index represents a secondary index definition.
type Index struct {
	Name       string                            // e.g., 'users_by_city'
	KeyPattern string                            // e.g., 'user:*' (glob pattern for keys to index)
	FieldName  string                            // The field within the hash to index, e.g., 'city'
	IndexData  map[string]map[string]struct{}    // value -> set of main_keys, e.g. 'london' -> {'user:1', 'user:2'}
	mu         sync.RWMutex                      // To protect IndexData
}

// NewIndex creates a new index.
func NewIndex(name, keyPattern, fieldName string) *Index {
	return &Index{
		Name:       name,
		KeyPattern: keyPattern,
		FieldName:  fieldName,
		IndexData:  make(map[string]map[string]struct{}),
	}
}

// Add adds a key to the index for a given value.
// value: The field value being indexed (e.g., "london").
// mainKey: The Redis key of the hash (e.g., "user:1").
func (idx *Index) Add(value string, mainKey string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if _, ok := idx.IndexData[value]; !ok {
		idx.IndexData[value] = make(map[string]struct{})
	}
	idx.IndexData[value][mainKey] = struct{}{}
}

// Remove removes a key from the index for a given value.
// This is needed if a field's value changes or the item is deleted.
func (idx *Index) Remove(value string, mainKey string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if keys, ok := idx.IndexData[value]; ok {
		delete(keys, mainKey)
		if len(keys) == 0 {
			delete(idx.IndexData, value)
		}
	}
}

// Get returns the set of main keys associated with an indexed value.
func (idx *Index) Get(value string) map[string]struct{} {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if keys, ok := idx.IndexData[value]; ok {
		// Return a copy to prevent external modification
		result := make(map[string]struct{}, len(keys))
		for k := range keys {
			result[k] = struct{}{}
		}
		return result
	}
	return nil
}

// IndexManager manages all defined indexes.
type IndexManager struct {
	mu      sync.RWMutex
	Indexes map[string]*Index // Name -> Index definition
	// Store   *DataStore      // Reference to the main DataStore, if needed for re-indexing or complex ops
}

// NewIndexManager creates a new IndexManager.
func NewIndexManager() *IndexManager {
	return &IndexManager{
		Indexes: make(map[string]*Index),
	}
}

// AddIndex adds a new index definition to the manager.
func (im *IndexManager) AddIndex(index *Index) {
	im.mu.Lock()
	defer im.mu.Unlock()
	im.Indexes[index.Name] = index
}

// GetIndex retrieves an index by its name.
func (im *IndexManager) GetIndex(name string) (*Index, bool) {
	im.mu.RLock()
	defer im.mu.RUnlock()
	index, ok := im.Indexes[name]
	return index, ok
}

// InitializeDefaultIndexes creates and configures default indexes.
// In a real system, these would be loaded from config or created via commands.
func InitializeDefaultIndexes() *IndexManager {
	idxManager := NewIndexManager()

	// Example: Index users by their city
	// This assumes user data is stored in hashes like "user:1", "user:2", etc.
	// And each hash has a "city" field.
	usersByCityIndex := NewIndex("users_by_city", "user:*", "city")
	idxManager.AddIndex(usersByCityIndex)

	// Example: Index products by their category
	productsByCategoryIndex := NewIndex("products_by_category", "product:*", "category")
	idxManager.AddIndex(productsByCategoryIndex)

	// fmt.Println("Default indexes initialized.")
	return idxManager
}

// UpdateIndexOnSet is called when HSET (or similar) modifies a hash field.
// It checks if the modified field is part of any index and updates the index accordingly.
// oldValue is the value of the field *before* the HSET operation. Can be empty if field is new.
// newValue is the new value being set.
func (im *IndexManager) UpdateIndexOnSet(mainKey, fieldName, oldValue, newValue string) {
	im.mu.RLock() // Lock for reading Indexes map
	defer im.mu.RUnlock()

	for _, index := range im.Indexes {
		// Check if the field being set is the one this index cares about
		if fieldName != index.FieldName {
			continue
		}

		// Check if the key pattern matches
		matched, _ := filepath.Match(index.KeyPattern, mainKey)
		if !matched {
			continue
		}

		// If the old value was set and is different from new value, remove from old index entry
		if oldValue != "" && oldValue != newValue {
			index.Remove(oldValue, mainKey)
		}

		// If the new value is not empty, add to new index entry
		if newValue != "" {
			index.Add(newValue, mainKey)
		}
		// fmt.Printf("Updated index '%s' for key '%s', field '%s': oldV='%s', newV='%s'\n", index.Name, mainKey, fieldName, oldValue, newValue)
	}
}

// RemoveFromIndexOnDel is called when a key is deleted (DEL command).
// It removes the key from all relevant indexes.
// hashData would be the complete data of the hash *before* deletion.
func (im *IndexManager) RemoveFromIndexOnDel(mainKey string, hashData map[string]string) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	for _, index := range im.Indexes {
		matched, _ := filepath.Match(index.KeyPattern, mainKey)
		if !matched {
			continue
		}

		// Check if the indexed field exists in the hash data
		if indexedValue, ok := hashData[index.FieldName]; ok {
			index.Remove(indexedValue, mainKey)
			// fmt.Printf("Removed key '%s' from index '%s' for value '%s' due to DEL.\n", mainKey, index.Name, indexedValue)
		}
	}
}

// RemoveFieldFromIndex is called when HDEL removes a field from a hash.
func (im *IndexManager) RemoveFieldFromIndex(mainKey, fieldName, fieldValue string) {
    im.mu.RLock()
    defer im.mu.RUnlock()

    for _, index := range im.Indexes {
        if fieldName == index.FieldName {
            matched, _ := filepath.Match(index.KeyPattern, mainKey)
            if matched {
                index.Remove(fieldValue, mainKey)
                // fmt.Printf("Removed field '%s' for key '%s' from index '%s'.\n", fieldName, mainKey, index.Name)
            }
        }
    }
}
