package database

import (
	"reflect"
	"testing"
	// "path/filepath" // For filepath.Match if testing KeyPattern matching directly
)

func TestIndex_Add_Get_Remove(t *testing.T) {
	idx := NewIndex("test_idx", "object:*", "city")

	// Test Add
	idx.Add("london", "object:1")
	idx.Add("london", "object:2")
	idx.Add("paris", "object:3")

	// Test Get
	londonKeys := idx.Get("london")
	wantLondonKeys := map[string]struct{}{"object:1": {}, "object:2": {}}
	if !reflect.DeepEqual(londonKeys, wantLondonKeys) {
		t.Errorf("Index Get(london) = %v, want %v", londonKeys, wantLondonKeys)
	}

	parisKeys := idx.Get("paris")
	wantParisKeys := map[string]struct{}{"object:3": {}}
	if !reflect.DeepEqual(parisKeys, wantParisKeys) {
		t.Errorf("Index Get(paris) = %v, want %v", parisKeys, wantParisKeys)
	}

	nilKeys := idx.Get("tokyo")
	if nilKeys != nil {
		t.Errorf("Index Get(tokyo) = %v, want nil", nilKeys)
	}

	// Test Remove
	idx.Remove("london", "object:1")
	londonKeysAfterRemove1 := idx.Get("london")
	wantLondonKeysAfterRemove1 := map[string]struct{}{"object:2": {}}
	if !reflect.DeepEqual(londonKeysAfterRemove1, wantLondonKeysAfterRemove1) {
		t.Errorf("Index Get(london) after removing object:1 = %v, want %v", londonKeysAfterRemove1, wantLondonKeysAfterRemove1)
	}

	// Test Remove last key for a value (should delete the value from IndexData)
	idx.Remove("london", "object:2")
	londonKeysAfterRemove2 := idx.Get("london")
	if londonKeysAfterRemove2 != nil {
		t.Errorf("Index Get(london) after removing all keys = %v, want nil", londonKeysAfterRemove2)
	}
	idx.mu.RLock() // Need to lock for direct inspection
	if _, exists := idx.IndexData["london"]; exists {
		t.Errorf("IndexData should not contain 'london' after all its keys are removed")
	}
	idx.mu.RUnlock()


	// Test Remove non-existent key for a value
	idx.Remove("paris", "object:nonexistent")
	parisKeysAfterRemoveNonExistent := idx.Get("paris")
	if !reflect.DeepEqual(parisKeysAfterRemoveNonExistent, wantParisKeys) { // Should be unchanged
		t.Errorf("Index Get(paris) after removing non-existent key = %v, want %v", parisKeysAfterRemoveNonExistent, wantParisKeys)
	}

	// Test Remove from non-existent value
	idx.Remove("tokyo", "object:1") // Should be a no-op, no error
	if len(idx.IndexData) != 1 { // Should only contain paris now
		t.Errorf("IndexData size after removing from non-existent value, got %d, want 1", len(idx.IndexData))
	}
}

func TestIndexManager_GetIndex(t *testing.T) {
	im := NewIndexManager()
	idx1 := NewIndex("idx1", "*", "f1")
	im.AddIndex(idx1)

	retrievedIdx, found := im.GetIndex("idx1")
	if !found {
		t.Fatalf("GetIndex('idx1') not found")
	}
	if retrievedIdx.Name != "idx1" {
		t.Errorf("GetIndex('idx1') got name %s, want idx1", retrievedIdx.Name)
	}

	_, found = im.GetIndex("nonexistent_idx")
	if found {
		t.Errorf("GetIndex('nonexistent_idx') found, but should not")
	}
}

func TestIndexManager_UpdateIndexOnSet(t *testing.T) {
	im := NewIndexManager()
	// KeyPattern "user:*", FieldName "city"
	cityIndex := NewIndex("users_by_city", "user:*", "city")
	im.AddIndex(cityIndex)
	// KeyPattern "item:*", FieldName "color"
	colorIndex := NewIndex("items_by_color", "item:*", "color")
	im.AddIndex(colorIndex)

	// Case 1: Add new value to city index
	im.UpdateIndexOnSet("user:1", "city", "", "london")
	londonUsers := cityIndex.Get("london")
	if _, ok := londonUsers["user:1"]; !ok {
		t.Errorf("user:1 not added to 'london' city index")
	}

	// Case 2: Update existing value in city index
	im.UpdateIndexOnSet("user:1", "city", "london", "paris")
	londonUsers = cityIndex.Get("london")
	if _, ok := londonUsers["user:1"]; ok {
		t.Errorf("user:1 should have been removed from 'london' city index")
	}
	parisUsers := cityIndex.Get("paris")
	if _, ok := parisUsers["user:1"]; !ok {
		t.Errorf("user:1 not added to 'paris' city index after update")
	}

	// Case 3: Non-matching field name
	im.UpdateIndexOnSet("user:1", "country", "", "uk")
	if len(cityIndex.IndexData) != 1 || len(cityIndex.IndexData["paris"]) != 1 { // Only paris should have user:1
		t.Errorf("City index should not be affected by 'country' field update")
	}

	// Case 4: Non-matching key pattern
	im.UpdateIndexOnSet("order:1", "city", "", "tokyo")
	tokyoUsers := cityIndex.Get("tokyo")
	if tokyoUsers != nil {
		t.Errorf("City index should not contain 'tokyo' from 'order:1' key")
	}

	// Case 5: Matching key, different field (for color index)
	im.UpdateIndexOnSet("item:1", "name", "", "widget")
	if len(colorIndex.IndexData) != 0 {
		t.Errorf("Color index should not be affected by 'name' field update")
	}
	im.UpdateIndexOnSet("item:1", "color", "", "red")
	redItems := colorIndex.Get("red")
	if _, ok := redItems["item:1"]; !ok {
		t.Errorf("item:1 not added to 'red' color index")
	}

	// Case 6: Setting field to empty string (effectively removing it from index for that value)
	im.UpdateIndexOnSet("user:1", "city", "paris", "")
	parisUsers = cityIndex.Get("paris")
	if parisUsers != nil { // paris should be empty or nil now
		t.Errorf("user:1 should have been removed from 'paris' city index when set to empty string")
	}
	// Check if 'paris' entry itself is removed from IndexData if empty
	cityIndex.mu.RLock()
	if _, exists := cityIndex.IndexData["paris"]; exists && len(cityIndex.IndexData["paris"])==0 {
		// This behavior (removing value entry if set of keys becomes empty) is handled by Index.Remove
		// UpdateIndexOnSet calls Index.Remove for oldValue. If newValue is empty, it doesn't call Index.Add.
	} else if exists && len(cityIndex.IndexData["paris"]) !=0 {
		t.Errorf("'paris' entry in IndexData should be empty or removed, but has keys")
	}
	cityIndex.mu.RUnlock()


	// Case 7: Value not changed
	im.UpdateIndexOnSet("item:1", "color", "red", "red")
	redItems = colorIndex.Get("red")
	if _, ok := redItems["item:1"]; !ok || len(redItems) != 1 {
		t.Errorf("item:1 should still be in 'red' color index and only once")
	}
}

func TestIndexManager_RemoveFieldFromIndex(t *testing.T) {
	im := NewIndexManager()
	cityIndex := NewIndex("users_by_city", "user:*", "city")
	im.AddIndex(cityIndex)

	im.UpdateIndexOnSet("user:1", "city", "", "london")
	im.UpdateIndexOnSet("user:2", "city", "", "london")

	// Remove field that is indexed
	im.RemoveFieldFromIndex("user:1", "city", "london")
	londonUsers := cityIndex.Get("london")
	if _, ok := londonUsers["user:1"]; ok {
		t.Errorf("user:1 should be removed from 'london' index after RemoveFieldFromIndex")
	}
	if _, ok := londonUsers["user:2"]; !ok {
		t.Errorf("user:2 should still be in 'london' index")
	}

	// Remove field that is not indexed by this index
	im.RemoveFieldFromIndex("user:2", "country", "uk") // No change expected
	londonUsers = cityIndex.Get("london")
	if _, ok := londonUsers["user:2"]; !ok {
		t.Errorf("user:2 should still be in 'london' index after removing non-indexed field")
	}

	// Remove last user for "london"
	im.RemoveFieldFromIndex("user:2", "city", "london")
	londonUsers = cityIndex.Get("london")
	if londonUsers != nil {
		t.Errorf("'london' should be nil in index after all users removed")
	}
}

func TestIndexManager_RemoveFromIndexOnDel(t *testing.T) {
	im := NewIndexManager()
	cityIndex := NewIndex("users_by_city", "user:*", "city")
	im.AddIndex(cityIndex)

	im.UpdateIndexOnSet("user:1", "city", "", "london")
	im.UpdateIndexOnSet("user:2", "city", "", "london")
	im.UpdateIndexOnSet("user:3", "city", "", "paris")

	// Data before DEL for user:1
	hashDataUser1 := map[string]string{"city": "london", "name": "Alice"}
	im.RemoveFromIndexOnDel("user:1", hashDataUser1)

	londonUsers := cityIndex.Get("london")
	if _, ok := londonUsers["user:1"]; ok {
		t.Errorf("user:1 should be removed from 'london' index after RemoveFromIndexOnDel")
	}
	if _, ok := londonUsers["user:2"]; !ok {
		t.Errorf("user:2 should still be in 'london' index")
	}

	// Data before DEL for user:3
	hashDataUser3 := map[string]string{"city": "paris", "name": "Bob"}
	im.RemoveFromIndexOnDel("user:3", hashDataUser3)
	parisUsers := cityIndex.Get("paris")
	if parisUsers != nil {
		t.Errorf("'paris' should be nil in index after user:3 removed")
	}

	// Try removing a key that doesn't match pattern
	hashDataOrder1 := map[string]string{"city": "london"}
	im.RemoveFromIndexOnDel("order:1", hashDataOrder1) // Should not affect users_by_city
	londonUsers = cityIndex.Get("london") // Should still contain user:2
	if _, ok := londonUsers["user:2"]; !ok {
		t.Errorf("user:2 should still be in 'london' index after deleting non-matching key pattern")
	}

	// Try removing a key that matches pattern but indexed field not in hashData
	hashDataUser2NoCity := map[string]string{"name": "Charlie"}
	im.RemoveFromIndexOnDel("user:2", hashDataUser2NoCity) // Should not remove user:2 from london
	londonUsers = cityIndex.Get("london")
	if _, ok := londonUsers["user:2"]; !ok {
		t.Errorf("user:2 should still be in 'london' index if deleted hashData didn't contain 'city'")
	}
}
