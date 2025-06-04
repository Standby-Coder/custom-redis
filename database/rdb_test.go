package database

import (
	"bufio"           // For TestRDBVersionHandling, TestLoadEmptyRDBFile
	"encoding/binary" // For TestRDBVersionHandling
	"os"
	"path/filepath"
	"reflect" // For compareHashes helper
	"strings" // For error checking in TestRDBVersionHandling, TestLoadEmptyRDBFile
	"testing"
	"time"
	"io"      // For io.EOF
)

// Helper to create a temporary RDB file path for tests
func tempRDBFile(t *testing.T) string {
	t.Helper()
	// Use a subdirectory to avoid clutter and ensure it's cleaned up if possible
	dir := t.TempDir() // Creates a temporary directory that's automatically cleaned up
	return filepath.Join(dir, "test_dump.rdb")
}

func TestRDBSaveLoad_Strings(t *testing.T) {
	rdbFile := tempRDBFile(t)
	defer os.Remove(rdbFile) // Clean up

	dsSave := NewDataStore()
	dsSave.Set("key1", "value1", SetOptions{})
	dsSave.Set("key2", "value2", SetOptions{})
	dsSave.Expire("key2", 2) // Expires in 2 seconds

	dsSave.Set("key_expired_in_rdb", "value_expired", SetOptions{})
	// Set expiry in the past directly for testing RDB loading of pre-expired key
	dsSave.mu.Lock()
	dsSave.expiries["key_expired_in_rdb"] = time.Now().Add(-1 * time.Hour)
	dsSave.mu.Unlock()

	dsSave.Set("key_no_expiry_val", "persistent_value", SetOptions{})


	err := Save(dsSave, rdbFile)
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	// Introduce a small delay to ensure key2 can expire if Save is slow
	// Or, more reliably, control time for expiry tests. For now, sleep.
	// time.Sleep(50 * time.Millisecond) // Let's test PTTL before it fully expires

	dsLoad, err := Load(rdbFile)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Test key1 (no expiry)
	val1, ok1 := dsLoad.Get("key1")
	if !ok1 || val1.(string) != "value1" {
		t.Errorf("Load() for key1: got %v, %t; want value1, true", val1, ok1)
	}
	ttl1, _ := dsLoad.TTL("key1")
	if ttl1 != -1 {
		t.Errorf("TTL for key1: got %d, want -1", ttl1)
	}

	// Test key2 (with expiry)
	val2, ok2 := dsLoad.Get("key2")
	if !ok2 || val2.(string) != "value2" {
		t.Errorf("Load() for key2: got %v, %t; want value2, true", val2, ok2)
	}
	pttl2, _ := dsLoad.PTTL("key2")
	if pttl2 <= 0 || pttl2 > 2000 { // Should be positive and <= 2000ms
		t.Errorf("PTTL for key2: got %d, want positive value <= 2000", pttl2)
	}

	// Test key_expired_in_rdb (should not be loaded or be immediately expired)
	_, okExpired := dsLoad.Get("key_expired_in_rdb")
	if okExpired {
		t.Errorf("Load() for key_expired_in_rdb: got value, want nil (should be expired)")
	}
	ttlExpired, _ := dsLoad.TTL("key_expired_in_rdb")
	if ttlExpired != -2 {
		t.Errorf("TTL for key_expired_in_rdb: got %d, want -2", ttlExpired)
	}

	// Test key_no_expiry_val
	valNoExp, okNoExp := dsLoad.Get("key_no_expiry_val")
	if !okNoExp || valNoExp.(string) != "persistent_value" {
		t.Errorf("Load() for key_no_expiry_val: got %v, %t; want persistent_value, true", valNoExp, okNoExp)
	}
	ttlNoExp, _ := dsLoad.TTL("key_no_expiry_val")
	if ttlNoExp != -1 {
		t.Errorf("TTL for key_no_expiry_val: got %d, want -1", ttlNoExp)
	}

	// Test that key2 expires after some time
	time.Sleep(2100 * time.Millisecond) // Wait for key2 to expire
	_, ok2AfterExpiry := dsLoad.Get("key2")
	if ok2AfterExpiry {
		t.Errorf("Get for key2 after expiry: got value, want nil")
	}
	ttl2AfterExpiry, _ := dsLoad.TTL("key2")
	if ttl2AfterExpiry != -2 {
		t.Errorf("TTL for key2 after expiry: got %d, want -2", ttl2AfterExpiry)
	}
}

func TestRDBSaveLoad_Hashes(t *testing.T) {
	rdbFile := tempRDBFile(t)
	defer os.Remove(rdbFile)

	dsSave := NewDataStore()
	// Hash 1 (no expiry)
	dsSave.HSet("hashkey1", "field1", "val1")
	dsSave.HSet("hashkey1", "field2", "val2")

	// Hash 2 (with expiry)
	dsSave.HSet("hashkey2", "f1", "v1")
	dsSave.Expire("hashkey2", 3) // Expires in 3 seconds

	// Hash 3 (will be expired in RDB)
	dsSave.HSet("hashkey_expired", "f_exp", "v_exp")
	dsSave.mu.Lock()
	dsSave.expiries["hashkey_expired"] = time.Now().Add(-2 * time.Second)
	dsSave.mu.Unlock()


	err := Save(dsSave, rdbFile)
	if err != nil {
		t.Fatalf("Save() error for hashes = %v", err)
	}

	dsLoad, err := Load(rdbFile)
	if err != nil {
		t.Fatalf("Load() error for hashes = %v", err)
	}

	// Test hashkey1
	h1f1, ok_h1f1 := dsLoad.HGet("hashkey1", "field1")
	if !ok_h1f1 || h1f1 != "val1" {
		t.Errorf("Load() for hashkey1[field1]: got %s, %t; want val1, true", h1f1, ok_h1f1)
	}
	h1f2, ok_h1f2 := dsLoad.HGet("hashkey1", "field2")
	if !ok_h1f2 || h1f2 != "val2" {
		t.Errorf("Load() for hashkey1[field2]: got %s, %t; want val2, true", h1f2, ok_h1f2)
	}
	ttl_h1, _ := dsLoad.TTL("hashkey1")
	if ttl_h1 != -1 {
		t.Errorf("TTL for hashkey1: got %d, want -1", ttl_h1)
	}

	// Test hashkey2
	h2f1, ok_h2f1 := dsLoad.HGet("hashkey2", "f1")
	if !ok_h2f1 || h2f1 != "v1" {
		t.Errorf("Load() for hashkey2[f1]: got %s, %t; want v1, true", h2f1, ok_h2f1)
	}
	pttl_h2, _ := dsLoad.PTTL("hashkey2")
	if pttl_h2 <= 0 || pttl_h2 > 3000 {
		t.Errorf("PTTL for hashkey2: got %d, want positive value <= 3000ms", pttl_h2)
	}

	// Test hashkey_expired
	_, ok_hexp := dsLoad.HGet("hashkey_expired", "f_exp")
	if ok_hexp {
		t.Errorf("Load() for hashkey_expired: got value, want nil (should be expired)")
	}
	ttl_hexp, _ := dsLoad.TTL("hashkey_expired")
	if ttl_hexp != -2 {
		t.Errorf("TTL for hashkey_expired: got %d, want -2", ttl_hexp)
	}

	// Test hashkey2 expires
	time.Sleep(3100 * time.Millisecond)
	_, ok_h2f1_after := dsLoad.HGet("hashkey2", "f1")
	if ok_h2f1_after {
		t.Errorf("HGet for hashkey2 after expiry: got value, want nil")
	}
	ttl_h2_after, _ := dsLoad.TTL("hashkey2")
	if ttl_h2_after != -2 {
		t.Errorf("TTL for hashkey2 after expiry: got %d, want -2", ttl_h2_after)
	}
}

func TestRDBSaveLoad_MixedTypes(t *testing.T) {
	rdbFile := tempRDBFile(t)
	defer os.Remove(rdbFile)

	dsSave := NewDataStore()
	dsSave.Set("stringkey", "stringvalue", SetOptions{})
	dsSave.Expire("stringkey", 5) // 5s

	dsSave.HSet("hashkey", "hfield", "hvalue")
	// No expiry for hashkey

	err := Save(dsSave, rdbFile)
	if err != nil {
		t.Fatalf("Save() mixed types error = %v", err)
	}

	dsLoad, err := Load(rdbFile)
	if err != nil {
		t.Fatalf("Load() mixed types error = %v", err)
	}

	// Check stringkey
	valStr, okStr := dsLoad.Get("stringkey")
	if !okStr || valStr.(string) != "stringvalue" {
		t.Errorf("Load() mixed for stringkey: got %v, %t", valStr, okStr)
	}
	ttlStr, _ := dsLoad.TTL("stringkey")
	if !(ttlStr > 0 && ttlStr <= 5) {
		t.Errorf("TTL for stringkey: got %d, want between 1-5", ttlStr)
	}

	// Check hashkey
	valHash, okHash := dsLoad.HGet("hashkey", "hfield")
	if !okHash || valHash != "hvalue" {
		t.Errorf("Load() mixed for hashkey[hfield]: got %v, %t", valHash, okHash)
	}
	ttlHash, _ := dsLoad.TTL("hashkey")
	if ttlHash != -1 {
		t.Errorf("TTL for hashkey: got %d, want -1", ttlHash)
	}
}

// Test for RDB version compatibility (simplified)
func TestRDBVersionHandling(t *testing.T) {
	rdbFile := tempRDBFile(t)
	defer os.Remove(rdbFile)

	// Create a dummy RDB with a newer version
	file, _ := os.Create(rdbFile)
	writer := bufio.NewWriter(file)
	writer.WriteString(rdbMagicString)
	binary.Write(writer, binary.LittleEndian, uint32(rdbVersion+1)) // Future version
	writer.Flush()
	file.Close()

	_, err := Load(rdbFile)
	if err == nil || !strings.Contains(err.Error(), "unsupported RDB version") {
		t.Errorf("Load() with newer RDB version: expected unsupported version error, got %v", err)
	}

	// Create a dummy RDB with an older version (e.g. v1 if current is v2)
	// This test assumes current rdbVersion > 1
	if rdbVersion > 1 {
		fileV1, _ := os.Create(rdbFile)
		writerV1 := bufio.NewWriter(fileV1)
		writerV1.WriteString(rdbMagicString)
		binary.Write(writerV1, binary.LittleEndian, uint32(1)) // Old version 1
		// For v1, we might need some minimal valid v1 data, e.g. just EOF or an empty key
		// The current Load might fail if it expects v2 structure after version check.
		// This test is more about the version number check itself.
		// If Load is strict about v2 structure after reading v1, it might fail further down.
		// For now, let's just check if it *doesn't* error on the version number itself.
		writerV1.Flush()
		fileV1.Close()

		// This will likely fail because the rest of the RDB v1 format is not what v2 loader expects.
		// But it shouldn't fail with "unsupported RDB version" if we allow older versions.
		// The current Load function: `if version > rdbVersion` means it allows loading older or same version.
		// So, loading a v1 file with a v2 loader should pass the version check.
		// It will then likely fail on parsing the actual data if formats differ significantly.
		// For this test, we only ensure the version check itself doesn't deny older.
		dsLoadV1, errLoadV1 := Load(rdbFile)
		if errLoadV1 != nil && strings.Contains(errLoadV1.Error(), "unsupported RDB version") {
			t.Errorf("Load() with older RDB version (v1): got unsupported error %v, but should allow older", errLoadV1)
		} else if errLoadV1 != nil {
			// It's okay if it fails later due to format mismatch, just not on version number.
			t.Logf("Load() with older RDB (v1) failed as expected due to format changes: %v (this is okay for version check test)", errLoadV1)
		} else if dsLoadV1 == nil { // Should not happen if errLoadV1 is nil
            t.Errorf("Load() with older RDB (v1) returned nil DataStore without error.")
        }
	}
}

// Test for empty RDB file
func TestLoadEmptyRDBFile(t *testing.T) {
    rdbFile := tempRDBFile(t)
    defer os.Remove(rdbFile)

    // Create an empty file
    file, _ := os.Create(rdbFile)
    file.Close()

    // Current Load behavior: returns empty DataStore and nil error for empty/too-short file.
    ds, err := Load(rdbFile)
    if err != nil {
        t.Errorf("Load() with empty RDB file: expected nil error, got %v", err)
    }
    if ds == nil {
        t.Errorf("Load() with empty RDB file: expected non-nil DataStore, got nil")
    } else if len(ds.GetAllPersistableData()) != 0 { // Check if it's actually empty
		t.Errorf("Load() with empty RDB file: expected empty DataStore, got %d keys", len(ds.GetAllPersistableData()))
	}


	// Create a file with just magic string (incomplete) -> should error on version
	file, _ = os.Create(rdbFile)
	writer := bufio.NewWriter(file)
	writer.WriteString(rdbMagicString)
	writer.Flush()
	file.Close()
	_, err = Load(rdbFile)
    if err == nil || !strings.Contains(err.Error(), "failed to read RDB version") {
		t.Errorf("Load() with incomplete RDB (only magic): expected error about version or EOF, got %v", err)
	}
}


// Test saving then loading with a non-default RDB filename (via main.go config)
// This test is more of an integration test and relies on main.go's global rdbFilenameFlag.
// It's harder to do in isolation here in database_test.go without setting that flag.
// We can test Save/Load with a specific filename directly.
func TestRDBSaveLoad_SpecificFilename(t *testing.T) {
	// This test uses a filename different from default "dump.rdb"
	// but doesn't rely on command-line flags, just tests Save/Load functions.
	specificRDBFile := tempRDBFile(t) // Get a temp file path
	defer os.Remove(specificRDBFile)

	dsSave := NewDataStore()
	dsSave.Set("testfilekey", "testfilevalue", SetOptions{})
	err := Save(dsSave, specificRDBFile)
	if err != nil {
		t.Fatalf("Save() to specific file error = %v", err)
	}

	dsLoad, err := Load(specificRDBFile)
	if err != nil {
		t.Fatalf("Load() from specific file error = %v", err)
	}
	val, ok := dsLoad.Get("testfilekey")
	if !ok || val.(string) != "testfilevalue" {
		t.Errorf("Load() specific file: got %v, %t; want testfilevalue, true", val, ok)
	}
}

// Helper to compare reflect.DeepEqual for maps (hashes)
func compareHashes(t *testing.T, got, want map[string]string, keyName string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Loaded hash for key '%s' does not match saved hash. Got: %v, Want: %v", keyName, got, want)
	}
}

// Test to cover the case where a key exists as multiple types (should warn during save data collection)
// And ensure the loaded version is the one that was last written (e.g. hash overwrites string if same key)
func TestRDBSaveLoad_KeyTypeConflict(t *testing.T) {
    rdbFile := tempRDBFile(t)
    defer os.Remove(rdbFile)

    dsSave := NewDataStore()
    dsSave.Set("conflictkey", "string_version", SetOptions{})
    dsSave.HSet("conflictkey", "field", "hash_version") // This HSet should clear stringkey's expiry and effectively make it a hash

	// The GetAllPersistableData should prioritize or log this.
	// Current GetAllPersistableData: hash will overwrite string if same key.
	// So, we expect to load a hash.

    err := Save(dsSave, rdbFile)
    if err != nil {
        t.Fatalf("Save() with key type conflict error = %v", err)
    }

    dsLoad, err := Load(rdbFile)
    if err != nil {
        t.Fatalf("Load() with key type conflict error = %v", err)
    }

    // Check if it's a hash
    hVal, hOk := dsLoad.HGet("conflictkey", "field")
    if !hOk || hVal != "hash_version" {
        t.Errorf("Expected 'conflictkey' to be loaded as a hash with field='hash_version', got HGet: %s, %t", hVal, hOk)
    }

    // Check it's not a string
    sVal, sOk := dsLoad.Get("conflictkey")
    if sOk {
        t.Errorf("Expected 'conflictkey' not to be loaded as a string, but got string value: %v", sVal)
    }
}

func TestRDBSave_TypeErrors(t *testing.T) {
	rdbFile := tempRDBFile(t)
	defer os.Remove(rdbFile)

	dsSave := NewDataStore()

	// Mock GetAllPersistableData to return an entry with a bad value type
	// This requires a bit of a workaround as we can't easily modify DataStore's internal GetAllPersistableData behavior for one test.
	// Instead, we'll add a key with an unsupported type directly to ds.data and then call ds.GetAllPersistableData()
	// and then modify it, or we directly construct the map to pass to Save.
	// For simplicity, let's test the Save function's internal logic by providing a problematic entry.
	// This means we are not testing GetAllPersistableData directly here but the Save logic.

	// This approach doesn't work because Save calls ds.GetAllPersistableData() internally.
	// We need to ensure ds.GetAllPersistableData() would produce such an entry.
	// Let's try to add an integer to ds.data, which GetAllPersistableData will pick up as RDBStringValueType.

	dsSave.mu.Lock()
	dsSave.data["baddatatype"] = 12345 // Integer, not string or []byte
	dsSave.mu.Unlock()

	err := Save(dsSave, rdbFile)
	if err == nil {
		t.Errorf("Save() with bad string data type: expected error, got nil")
	} else if !strings.Contains(err.Error(), "invalid string value type for key 'baddatatype'") {
		t.Errorf("Save() with bad string data type: got error %q, want error containing 'invalid string value type'", err.Error())
	}
	// Clear the bad data
	dsSave.mu.Lock()
	delete(dsSave.data, "baddatatype")
	dsSave.mu.Unlock()

	// Now test for bad hash data type
	dsSave.mu.Lock()
	// Add a key to ds.Hashes that GetAllPersistableData will pick up, then replace its value with bad type
	// This is tricky. Instead, let's test the path where entry.Value for a hash is not map[string]string.
	// The current GetAllPersistableData returns map[string]string directly from ds.Hashes.
	// So, to test this, Save would need to be passed a map where one entry has a bad hash value type.
	// This means we would need a custom ds.GetAllPersistableData for this test, or make Save more public.

	// Let's refine: The type assertion is on entry.Value.
	// GetAllPersistableData for RDBStringValueType puts ds.data[key] into Value.
	// If ds.data[key] is an int, Save will find entry.Type = RDBStringValueType, entry.Value = int(12345)
	// Then `stringValue, ok := entry.Value.(string)` will have ok=false.
	// Then `byteValue, okByte := entry.Value.([]byte)` will have okByte=false.
	// Then it errors. This is what the above test covers.

	// For RDBHashValueType:
	// GetAllPersistableData puts ds.Hashes[key] (which is map[string]string) into Value.
	// So `hashValue, ok := entry.Value.(map[string]string)` should always be true if Type is RDBHashValueType.
	// To test this error path in Save, entry.Value would have to be something else *despite*
	// entry.Type being RDBHashValueType. This implies GetAllPersistableData is inconsistent.
	// So this specific error path in Save for hashes is harder to trigger if GetAllPersistableData is correct.
	// The current structure means GetAllPersistableData is responsible for providing correctly typed values.
	// The check in Save `if !ok` for hashValue is more of a safeguard.

	// We can test an unknown type during save:
	// Create a custom PersistableEntry map
	customData := make(map[string]PersistableEntry)
	customData["unknownTypeKey"] = PersistableEntry{
		Value: "some value",
		Type:  RDBValueType(99), // Unknown type
		ExpiryTime: time.Time{},
	}

	// To test Save with this customData, we'd need to modify Save or pass data directly.
	// This suggests unit testing the serialization of individual types might be better.
	// For now, the string type error is testable as shown.
	// The "unknown value type %d for key '%s' during save" can be tested if we can inject a custom entry.
	// Given current structure, this path is hard to reach without modifying GetAllPersistableData.
	// Let's skip testing this specific error path for now as it implies inconsistency from GetAllPersistableData.

	// Test unknown type by manually creating a problematic DataStore state for GetAllPersistableData to pick up.
	// This is difficult because GetAllPersistableData assigns specific types.
	// A direct test of Save's default case would require mocking GetAllPersistableData or passing a map.
	// However, if we add a new RDBValueType and don't handle it in Save, that would trigger it.
	// For now, this error condition in Save is hard to test cleanly without more significant refactoring.
}

func TestRDLoad_CorruptedOrInvalidContent(t *testing.T) {
	rdbFile := tempRDBFile(t)
	defer os.Remove(rdbFile)

	// Test 1: File with valid magic and version, but unknown value type
	file, _ := os.Create(rdbFile)
	writer := bufio.NewWriter(file)
	writer.WriteString(rdbMagicString)
	binary.Write(writer, binary.LittleEndian, uint32(rdbVersion))

	// Write an unknown value type
	unknownValueType := RDBValueType(99)
	writer.WriteByte(byte(unknownValueType))

	// Write a dummy key to make it attempt to process the unknown type
	dummyKey := "somekey"
	keyBytes := []byte(dummyKey)
	binary.Write(writer, binary.LittleEndian, uint32(len(keyBytes)))
	writer.Write(keyBytes)
	// No need to write value as it should fail on the type marker.

	writer.Flush()
	file.Close()

	_, err := Load(rdbFile)
	if err == nil {
		t.Errorf("Load() with unknown value type: expected error, got nil")
	} else if !strings.Contains(err.Error(), "unknown RDB value type 99 for key 'somekey'") {
		t.Errorf("Load() with unknown value type: got error %q, want specific message", err.Error())
	}

	// Test 2: File ends prematurely after ExpiryTimeMs opcode
	file, _ = os.Create(rdbFile)
	writer = bufio.NewWriter(file)
	writer.WriteString(rdbMagicString)
	binary.Write(writer, binary.LittleEndian, uint32(rdbVersion))
	writer.WriteByte(byte(rdbOpcodeExpireTimeMs)) // Write opcode
	// Do not write the 8-byte timestamp, simulate premature EOF
	writer.Flush()
	file.Close()

	_, err = Load(rdbFile)
	if err == nil {
		t.Errorf("Load() with premature EOF after ExpireTimeMs: expected error, got nil")
	} else if !strings.Contains(err.Error(), "failed to read expiry timestamp") && !strings.Contains(err.Error(), io.EOF.Error()){
		// Error might be io.EOF directly from binary.Read or wrapped.
		t.Errorf("Load() with premature EOF after ExpireTimeMs: got error %q, want error about reading timestamp or EOF", err.Error())
	}

	// Test 3: File ends prematurely after ValueType marker (before key length)
	file, _ = os.Create(rdbFile)
	writer = bufio.NewWriter(file)
	writer.WriteString(rdbMagicString)
	binary.Write(writer, binary.LittleEndian, uint32(rdbVersion))
	writer.WriteByte(byte(RDBStringValueType)) // Write type
	// Do not write key length or key data
	writer.Flush()
	file.Close()

	_, err = Load(rdbFile)
	if err == nil {
		t.Errorf("Load() with premature EOF after ValueType: expected error, got nil")
	} else if !strings.Contains(err.Error(), "failed to read key length") && !strings.Contains(err.Error(), io.EOF.Error()){
		t.Errorf("Load() with premature EOF after ValueType: got error %q, want error about reading key length or EOF", err.Error())
	}
}
