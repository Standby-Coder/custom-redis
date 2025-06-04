package database

import (
	"strings"
	"testing"
	"time"
)

// Helper to create a new DataStore for testing
func newTestDS(t *testing.T) *DataStore {
	return NewDataStore()
}

// resetKey is a local helper for tests in this file
func resetKey(ds *DataStore, key string) {
	ds.Del(key)
}

func TestDataStore_HDel(t *testing.T) {
	ds := newTestDS(t)
	ds.HSet("myhash", "field1", "value1")
	ds.HSet("myhash", "field2", "value2")
	ds.HSet("myhash", "field3", "value3")

	deleted, err := ds.HDel("myhash", "field1", "field3", "nonexistent")
	if err != nil {
		t.Fatalf("HDel() error = %v", err)
	}
	if deleted != 2 {
		t.Errorf("HDel() deleted = %d, want 2", deleted)
	}

	_, found := ds.HGet("myhash", "field1")
	if found {
		t.Errorf("HDel() field1 should be deleted, but found")
	}
	val2, found2 := ds.HGet("myhash", "field2")
	if !found2 || val2 != "value2" {
		t.Errorf("HDel() field2 should still exist and be 'value2', got %s, %t", val2, found2)
	}
	_, found3 := ds.HGet("myhash", "field3")
	if found3 {
		t.Errorf("HDel() field3 should be deleted, but found")
	}

	ds.HSet("anotherhash", "f1", "v1")
	deletedAll, _ := ds.HDel("anotherhash", "f1")
	if deletedAll != 1 {
		t.Errorf("HDel() deleting last field: deleted = %d, want 1", deletedAll)
	}
	ds.mu.Lock()
	if _, hashStillExists := ds.Hashes["anotherhash"]; hashStillExists {
		t.Errorf("HDel() deleting last field should remove the hash key 'anotherhash'")
	}
	ds.mu.Unlock()

	deletedNonExistent, _ := ds.HDel("nonhash", "f1")
	if deletedNonExistent != 0 {
		t.Errorf("HDel() on non-existent key: deleted = %d, want 0", deletedNonExistent)
	}
}

func TestDataStore_Del(t *testing.T) {
	ds := newTestDS(t)
	ds.Set("mystr", "value", SetOptions{})
	ds.HSet("myhash", "field", "value")
	ds.Expire("myhash", 60)

	deleted := ds.Del("mystr", "myhash", "nonexistent")
	if deleted != 2 {
		t.Errorf("Del() deleted = %d, want 2", deleted)
	}

	_, foundStr := ds.Get("mystr")
	if foundStr {
		t.Errorf("Del() mystr should be deleted, but found")
	}

	ds.mu.Lock()
	if _, hashExists := ds.Hashes["myhash"]; hashExists {
		t.Errorf("Del() myhash should be deleted, but found in Hashes map")
	}
	if _, expiryExists := ds.expiries["myhash"]; expiryExists {
		t.Errorf("Del() myhash should have its expiry removed, but found in expiries map")
	}
	ds.mu.Unlock()

	deletedAgain := ds.Del("mystr", "nonexistent2")
	if deletedAgain != 0 {
		t.Errorf("Del() on already deleted keys: deleted = %d, want 0", deletedAgain)
	}
}

func TestDataStore_PexpirePttl(t *testing.T) {
	ds := newTestDS(t)
	key := "pkey"

	ok, _ := ds.Pexpire(key, 1000)
	if ok {
		t.Errorf("Pexpire on non-existent key: got %t, want false", ok)
	}
	pttl, _ := ds.PTTL(key)
	if pttl != -2 {
		t.Errorf("PTTL on non-existent key (after Pexpire attempt): got %d, want -2", pttl)
	}

	ds.Set(key, "value", SetOptions{})
	ok, _ = ds.Pexpire(key, 200)
	if !ok {
		t.Errorf("Pexpire on existing key: got %t, want true", ok)
	}

	pttl, _ = ds.PTTL(key)
	if pttl <= 0 || pttl > 200 {
		t.Errorf("PTTL after Pexpire: got %d, want positive value <= 200", pttl)
	}

	time.Sleep(100 * time.Millisecond)
	pttlAfterSleep, _ := ds.PTTL(key)
	if pttlAfterSleep >= pttl || pttlAfterSleep <= 0 {
		t.Errorf("PTTL after sleep: got %d, should be less than %d and positive", pttlAfterSleep, pttl)
	}

	time.Sleep(150 * time.Millisecond)
	pttlExpired, _ := ds.PTTL(key)
	if pttlExpired != -2 {
		t.Errorf("PTTL after expiry: got %d, want -2", pttlExpired)
	}
	_, foundGet := ds.Get(key)
	if foundGet {
		t.Errorf("Get after PTTL expiry: key should not be found")
	}

	ds.Set(key, "value_again", SetOptions{})
	okDel, _ := ds.Pexpire(key, 0)
	if !okDel {
		t.Errorf("Pexpire with ms=0: got %t, want true (key deleted)", okDel)
	}
	pttlDel, _ := ds.PTTL(key)
	if pttlDel != -2 {
		t.Errorf("PTTL after Pexpire with ms=0: got %d, want -2", pttlDel)
	}
}

func TestDataStore_Incr(t *testing.T) {
	ds := newTestDS(t)
	key := "counter"

	resetKey(ds, key)
	val, err := ds.Incr(key, 1)
	if err != nil { t.Fatalf("Incr non-existent error: %v", err) }
	if val != 1 { t.Errorf("Incr non-existent: got %d, want 1", val) }

	resetKey(ds, key)
	ds.Set(key, "10", SetOptions{})
	val, err = ds.Incr(key, 5)
	if err != nil { t.Fatalf("Incr existing error: %v", err) }
	if val != 15 { t.Errorf("Incr existing: got %d, want 15", val) }

	resetKey(ds, key)
	ds.Set(key, "4", SetOptions{})
	val, err = ds.Incr(key, -2)
	if err != nil { t.Fatalf("Incr negative error: %v", err) }
	if val != 2 { t.Errorf("Incr negative: got %d, want 2", val) }

	resetKey(ds, "notint")
	ds.Set("notint", "hello", SetOptions{})
	_, err = ds.Incr("notint", 1)
	if err == nil || !strings.Contains(err.Error(), "value is not an integer") {
		t.Errorf("Incr on non-integer: expected error, got %v", err)
	}

	resetKey(ds, key)
	ds.Set(key, "100", SetOptions{})
	ds.Expire(key, 60)
	ds.Incr(key, 1)
	ttl, _ := ds.TTL(key)
	if ttl != -1 {
		t.Errorf("Incr should clear expiry, TTL: got %d, want -1", ttl)
	}
	getVal, _ := ds.Get(key)
	strVal, _ := getVal.(string)
	if strVal != "101" {
		t.Errorf("Incr value check: got %s, want 101", strVal)
	}

	resetKey(ds, key)
	ds.Set(key, "50", SetOptions{})
	ds.Expire(key, 1)
	time.Sleep(1100 * time.Millisecond)
	val, err = ds.Incr(key, 1)
	if err != nil { t.Fatalf("Incr on key just expired error: %v", err) }
	if val != 1 { t.Errorf("Incr on key just expired: got %d, want 1", val) }

	resetKey(ds, key)
	ds.mu.Lock()
	ds.data[key] = []byte("20")
	ds.mu.Unlock()
	val, err = ds.Incr(key, 3)
	if err != nil { t.Fatalf("Incr on []byte value error: %v", err) }
	if val != 23 { t.Errorf("Incr on []byte value: got %d, want 23", val) }

	finalVal, _ := ds.Get(key)
	if _, ok := finalVal.(string); !ok {
		t.Errorf("Incr on []byte value should store result as string, got %T", finalVal)
	}
}

func TestDataStore_Type(t *testing.T) {
	ds := newTestDS(t)

	ds.Set("strkey", "strval", SetOptions{})
	ds.HSet("hashkey", "f1", "v1")
	ds.mu.Lock()
	ds.Streams["streamkey"] = &Stream{}
	ds.mu.Unlock()

	tests := []struct {key  string; want string}{
		{"strkey", "string"}, {"hashkey", "hash"},
		{"streamkey", "stream"}, {"nonexistentkey", "none"},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			got, err := ds.Type(tt.key)
			if err != nil { t.Fatalf("Type(%q) error = %v", tt.key, err) }
			if got != tt.want { t.Errorf("Type(%q) = %s, want %s", tt.key, got, tt.want) }
		})
	}

	ds.Set("expiringkey", "willvanish", SetOptions{})
	ds.Pexpire("expiringkey", 20)
	time.Sleep(100 * time.Millisecond)

	gotExpired, _ := ds.Type("expiringkey")
	if gotExpired != "none" {
		t.Errorf("Type for expired key: got %s, want none", gotExpired)
	}
}

func TestDataStore_setInternal_NX_XX(t *testing.T) {
	ds := newTestDS(t)
	key := "nxxtestkey"

	res1 := ds.setInternal(key, "val1", SetOptions{NX: true}, false, nil)
	if !res1.DidSet { t.Errorf("setInternal NX on new key: DidSet=false, want true") }
	val1, _ := ds.Get(key); if val1.(string) != "val1" {t.Errorf("setInternal NX on new key: value not set")}

	res2 := ds.setInternal(key, "val2", SetOptions{NX: true}, true, "val1")
	if res2.DidSet { t.Errorf("setInternal NX on existing key: DidSet=true, want false") }
	val2, _ := ds.Get(key); if val2.(string) != "val1" {t.Errorf("setInternal NX on existing key: value changed to %s", val2)}

	resetKey(ds, key)

	res3 := ds.setInternal(key, "val3", SetOptions{XX: true}, false, nil)
	if res3.DidSet { t.Errorf("setInternal XX on new key: DidSet=true, want false") }
	_, keyExists3 := ds.Get(key); if keyExists3 {t.Errorf("setInternal XX on new key: key was created")}

	ds.setInternal(key, "val4", SetOptions{}, false, nil)

	res4 := ds.setInternal(key, "val5", SetOptions{XX: true}, true, "val4")
	if !res4.DidSet { t.Errorf("setInternal XX on existing key: DidSet=false, want true") }
	val5, _ := ds.Get(key); if val5.(string) != "val5" {t.Errorf("setInternal XX on existing key: value not updated to val5, got %s", val5)}
}

func TestDataStore_Expire_Branches(t *testing.T) {
	ds := newTestDS(t)
	key := "expirebranchkey"

	ok, _ := ds.Expire(key, 10)
	if ok { t.Errorf("Expire on non-existent key: got true, want false") }

	ds.Set(key, "val", SetOptions{})
	ds.mu.Lock()
	ds.expiries[key] = time.Now().Add(-1 * time.Second)
	ds.mu.Unlock()

	ok, _ = ds.Expire(key, 10)
	if ok { t.Errorf("Expire on already (just now) expired key: got true, want false") }
	if _, exists := ds.Get(key); exists {t.Errorf("Key should be deleted after Expire on past-expiry")}
}

func TestDataStore_TTL_Branches(t *testing.T) {
	ds := newTestDS(t)
	keyNoExist := "ttl_no_exist"; keyNoExp := "ttl_no_exp"
	keyWithExp := "ttl_with_exp"; keyExpired := "ttl_expired"

	ttl, _ := ds.TTL(keyNoExist)
	if ttl != -2 { t.Errorf("TTL for non-existent key: got %d, want -2", ttl) }

	ds.Set(keyNoExp, "val", SetOptions{})
	ttl, _ = ds.TTL(keyNoExp)
	if ttl != -1 { t.Errorf("TTL for key with no expiry: got %d, want -1", ttl) }

	ds.Set(keyWithExp, "val", SetOptions{})
	ds.Expire(keyWithExp, 10)
	ttl, _ = ds.TTL(keyWithExp)
	if !(ttl > 0 && ttl <= 10) { t.Errorf("TTL for key with expiry: got %d, want >0 and <=10", ttl) }

	ds.Set(keyExpired, "val", SetOptions{})
	ds.mu.Lock()
	ds.expiries[keyExpired] = time.Now().Add(-1 * time.Second)
	ds.mu.Unlock()
	ttl, _ = ds.TTL(keyExpired)
	if ttl != -2 { t.Errorf("TTL for key that was expired: got %d, want -2", ttl) }
	if _, exists := ds.Get(keyExpired); exists {t.Errorf("Key should be deleted by TTL call after expiry")}

	ds.Set("ttl_subsecond", "val", SetOptions{})
	ds.mu.Lock()
	ds.expiries["ttl_subsecond"] = time.Now().Add(500 * time.Millisecond)
	ds.mu.Unlock()
	ttl, _ = ds.TTL("ttl_subsecond")
	if ttl != 1 {t.Errorf("TTL for sub-second expiry: got %d, want 1", ttl)}
}

func TestDataStore_HSet_Branches(t *testing.T) {
	ds := newTestDS(t)
	key := "hset_branch_key"
	// var err error // Not needed if errHSet is used locally

	originalIndexMgr := ds.IndexMgr
	ds.IndexMgr = nil

	count, errHSet := ds.HSet(key, "f1", "v1")
	if errHSet != nil {t.Fatalf("HSet with nil IndexMgr error: %v", errHSet)}
	if count != 1 {t.Errorf("HSet with nil IndexMgr (new field): count=%d, want 1", count)}

	count, errHSet = ds.HSet(key, "f1", "v1_updated")
	if errHSet != nil {t.Fatalf("HSet with nil IndexMgr (update field) error: %v", errHSet)}
	if count != 0 {t.Errorf("HSet with nil IndexMgr (update field): count=%d, want 0", count)}

	ds.IndexMgr = originalIndexMgr
}

// TestRDBSave_TypeErrors was here
// ... (rest of RDB, Stream, Indexing tests from their respective files)
// It's better to keep tests in their specific files (rdb_test.go, stream_test.go, indexing_test.go)
// This file (database_test.go) should primarily test methods in database.go.
// For the purpose of this tool run, I'll assume `go test ./...` from `cd /app/database`
// will correctly find and run all *_test.go files in the database package.
// The overwrite ensures this file is clean.

// Test for RDB Save error path (unknown type)
// This requires specific setup for ds.GetAllPersistableData to return a custom type
// which is hard without modifying GetAllPersistableData or Save's signature.
// For now, we assume GetAllPersistableData is well-behaved.
// The default case in Save's switch for entry.Type is the target.

// Test for RDB Load error paths for corrupted data
// (e.g. premature EOF at different stages, invalid counts)
// TestRDLoad_CorruptedOrInvalidContent in rdb_test.go covers some of these.
