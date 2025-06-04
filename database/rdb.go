package database

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const (
	rdbMagicString = "GOREDIS"
	rdbVersion     = 1
)

// ValueType indicates the type of the value stored.
type ValueType byte

const (
	// StringType represents a string value.
	StringType ValueType = 0
	// ListType represents a list value (placeholder for now).
	ListType ValueType = 1
	// HashType represents a hash value (placeholder for now).
	HashType ValueType = 2
	// SetType represents a set value (placeholder for now).
	SetType ValueType = 3
	// SortedSetType represents a sorted set value (placeholder for now).
	SortedSetType ValueType = 4
)

// Save saves the DataStore to a file in RDB format.
// Currently, it only saves string values.
func Save(ds *DataStore, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create RDB file: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	// Write magic string and version
	if _, err := writer.WriteString(rdbMagicString); err != nil {
		return fmt.Errorf("failed to write RDB magic string: %w", err)
	}
	if err := binary.Write(writer, binary.LittleEndian, uint32(rdbVersion)); err != nil {
		return fmt.Errorf("failed to write RDB version: %w", err)
	}

	// Iterate over key-value pairs
	// ds.data is not directly accessible here if it's not exported or we don't have a method
	// For now, let's assume DataStore has a way to iterate or get all string data.
	// Modifying DataStore to add a method like GetAllStringData might be needed.
	// For this iteration, I will assume ds.data is accessible for simplicity of this step.
	// This will likely cause a compile error later, which we will fix.

	allData := ds.GetAllData() // Use the new method to get data

	for key, value := range allData {
		stringValue, ok := value.(string)
		if !ok {
			// Skip non-string values for now
			continue
		}

		// Write key length and key
		keyBytes := []byte(key)
		if err := binary.Write(writer, binary.LittleEndian, uint32(len(keyBytes))); err != nil {
			return fmt.Errorf("failed to write key length for key '%s': %w", key, err)
		}
		if _, err := writer.Write(keyBytes); err != nil {
			return fmt.Errorf("failed to write key '%s': %w", key, err)
		}

		// Write value type (string)
		if err := writer.WriteByte(byte(StringType)); err != nil {
			return fmt.Errorf("failed to write value type for key '%s': %w", key, err)
		}

		// Write value length and value
		valueBytes := []byte(stringValue)
		if err := binary.Write(writer, binary.LittleEndian, uint32(len(valueBytes))); err != nil {
			return fmt.Errorf("failed to write value length for key '%s': %w", key, err)
		}
		if _, err := writer.Write(valueBytes); err != nil {
			return fmt.Errorf("failed to write value for key '%s': %w", key, err)
		}
	}

	return writer.Flush()
}

// Load loads data from an RDB file into a new DataStore.
// Currently, it only loads string values.
func Load(filename string) (*DataStore, error) {
	ds := NewDataStore()

	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return ds, nil // Return empty DataStore if file doesn't exist
		}
		return nil, fmt.Errorf("failed to open RDB file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	// Read and verify magic string
	magicBuf := make([]byte, len(rdbMagicString))
	if _, err := io.ReadFull(reader, magicBuf); err != nil {
		return nil, fmt.Errorf("failed to read RDB magic string: %w", err)
	}
	if string(magicBuf) != rdbMagicString {
		return nil, fmt.Errorf("invalid RDB magic string")
	}

	// Read and verify version
	var version uint32
	if err := binary.Read(reader, binary.LittleEndian, &version); err != nil {
		return nil, fmt.Errorf("failed to read RDB version: %w", err)
	}
	if version != rdbVersion {
		return nil, fmt.Errorf("unsupported RDB version: got %d, want %d", version, rdbVersion)
	}

	// Read key-value pairs
	for {
		var keyLength uint32
		err := binary.Read(reader, binary.LittleEndian, &keyLength)
		if err != nil {
			if err == io.EOF {
				break // End of file, successfully loaded
			}
			return nil, fmt.Errorf("failed to read key length: %w", err)
		}

		keyBytes := make([]byte, keyLength)
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			return nil, fmt.Errorf("failed to read key: %w", err)
		}
		key := string(keyBytes)

		valueTypeByte, err := reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("failed to read value type for key '%s': %w", key, err)
		}
		valueType := ValueType(valueTypeByte)

		if valueType != StringType {
			// For now, skip non-string types. We need to read and discard the value.
			var valueLength uint32
			if err := binary.Read(reader, binary.LittleEndian, &valueLength); err != nil {
				return nil, fmt.Errorf("failed to read value length for skipped type for key '%s': %w", key, err)
			}
			if _, err := io.CopyN(io.Discard, reader, int64(valueLength)); err != nil {
				return nil, fmt.Errorf("failed to discard value for skipped type for key '%s': %w", key, err)
			}
			continue
		}

		var valueLength uint32
		if err := binary.Read(reader, binary.LittleEndian, &valueLength); err != nil {
			return nil, fmt.Errorf("failed to read value length for key '%s': %w", key, err)
		}

		valueBytes := make([]byte, valueLength)
		if _, err := io.ReadFull(reader, valueBytes); err != nil {
			return nil, fmt.Errorf("failed to read value for key '%s': %w", key, err)
		}
		ds.Set(key, string(valueBytes)) // Assuming Set is available and works
	}

	return ds, nil
}
