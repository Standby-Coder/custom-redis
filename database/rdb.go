package database

import (
	"bufio"
	"bytes" // For Load function, if still needed for some RESP parsing parts within RDB
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"
	// "path/filepath" // Not currently used in rdb.go
	// "sync" // DataStore handles its own locking
)

const (
	rdbMagicString        = "GOREDIS"
	rdbVersion            = 2    // Format with types and expiry
	rdbOpcodeExpireTimeMs = 0xFC // Opcode for upcoming expiry in milliseconds
	// rdbOpcodeSelectDB  = 0xFE // Not implemented
	// rdbOpcodeEOF       = 0xFF // End of RDB file
)

// RDBValueType indicates the type of the value stored in RDB.
// This definition is now canonical for the 'database' package.
type RDBValueType byte

const (
	RDBStringValueType RDBValueType = 0 // Standard string value
	RDBHashValueType   RDBValueType = 1 // Hash object
	RDBStreamValueType RDBValueType = 2 // Placeholder for now
)

// Save saves the DataStore to a file in RDB format.
// It now handles strings and hashes, and includes expiry times.
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

	allPersistableData := ds.GetAllPersistableData()

	for key, entry := range allPersistableData {
		if !entry.ExpiryTime.IsZero() && time.Now().After(entry.ExpiryTime) {
			continue // Don't save expired keys
		}

		// Write Expiry Opcode and Timestamp if expiry is set
		if !entry.ExpiryTime.IsZero() {
			if err := writer.WriteByte(byte(rdbOpcodeExpireTimeMs)); err != nil {
				return fmt.Errorf("failed to write ExpiryTimeMs opcode for key '%s': %w", key, err)
			}
			expiryMs := entry.ExpiryTime.UnixNano() / int64(time.Millisecond)
			if err := binary.Write(writer, binary.LittleEndian, expiryMs); err != nil {
				return fmt.Errorf("failed to write expiry timestamp for key '%s': %w", key, err)
			}
		}

		// Write Value Type Marker
		if err := writer.WriteByte(byte(entry.Type)); err != nil {
			return fmt.Errorf("failed to write value type for key '%s': %w", key, err)
		}

		// Write Key (always as a length-prefixed string)
		keyBytes := []byte(key)
		if err := binary.Write(writer, binary.LittleEndian, uint32(len(keyBytes))); err != nil {
			return fmt.Errorf("failed to write key length for key '%s': %w", key, err)
		}
		if _, err := writer.Write(keyBytes); err != nil {
			return fmt.Errorf("failed to write key '%s': %w", key, err)
		}

		// Write Value based on type
		switch entry.Type {
		case RDBStringValueType:
			stringValue, ok := entry.Value.(string)
			if !ok {
				byteValue, okByte := entry.Value.([]byte)
				if !okByte {
					return fmt.Errorf("invalid string value type for key '%s': expected string or []byte, got %T", key, entry.Value)
				}
				stringValue = string(byteValue)
			}
			valueBytes := []byte(stringValue)
			if err := binary.Write(writer, binary.LittleEndian, uint32(len(valueBytes))); err != nil {
				return fmt.Errorf("failed to write string value length for key '%s': %w", key, err)
			}
			if _, err := writer.Write(valueBytes); err != nil {
				return fmt.Errorf("failed to write string value for key '%s': %w", key, err)
			}
		case RDBHashValueType:
			hashValue, ok := entry.Value.(map[string]string)
			if !ok {
				return fmt.Errorf("invalid hash value type for key '%s': expected map[string]string, got %T", key, entry.Value)
			}
			if err := binary.Write(writer, binary.LittleEndian, uint32(len(hashValue))); err != nil {
				return fmt.Errorf("failed to write hash field count for key '%s': %w", key, err)
			}
			for field, val := range hashValue {
				fieldBytes := []byte(field)
				valBytes := []byte(val)
				if err := binary.Write(writer, binary.LittleEndian, uint32(len(fieldBytes))); err != nil { return fmt.Errorf("failed to write hash field length for key '%s', field '%s': %w", key, field, err) }
				if _, err := writer.Write(fieldBytes); err != nil { return fmt.Errorf("failed to write hash field for key '%s', field '%s': %w", key, field, err) }
				if err := binary.Write(writer, binary.LittleEndian, uint32(len(valBytes))); err != nil { return fmt.Errorf("failed to write hash value length for key '%s', field '%s': %w", key, field, err) }
				if _, err := writer.Write(valBytes); err != nil { return fmt.Errorf("failed to write hash value for key '%s', field '%s': %w", key, field, err) }
			}
		// case RDBStreamValueType: // TODO
		default:
			return fmt.Errorf("unknown value type %d for key '%s' during save", entry.Type, key)
		}
	}
	return writer.Flush()
}

// Load loads data from an RDB file into a new DataStore.
func Load(filename string) (*DataStore, error) {
	ds := NewDataStore()

	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) { return ds, nil } // Return empty DataStore if file doesn't exist
		return nil, fmt.Errorf("failed to open RDB file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	magicBuf := make([]byte, len(rdbMagicString))
	if _, err := io.ReadFull(reader, magicBuf); err != nil {
		// If EOF and no bytes read, it's an empty file. If some bytes read, it's corrupt/too short.
		if err == io.EOF && bytes.Equal(magicBuf, make([]byte, len(rdbMagicString))) { // Truly empty or too short for magic
			return ds, nil // Treat as empty RDB if it's too short to even contain magic string
		}
		return nil, fmt.Errorf("failed to read RDB magic string: %w", err)
	}
	if string(magicBuf) != rdbMagicString {
		return nil, fmt.Errorf("invalid RDB magic string")
	}

	var version uint32
	if err := binary.Read(reader, binary.LittleEndian, &version); err != nil {
		return nil, fmt.Errorf("failed to read RDB version: %w", err)
	}
	if version > rdbVersion {
		return nil, fmt.Errorf("unsupported RDB version: got %d, want <= %d", version, rdbVersion)
	}

	nextKeyExpiryTime := time.Time{}

	for {
		firstByte, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF { break }
			return nil, fmt.Errorf("failed to read RDB opcode or value type: %w", err)
		}

		if firstByte == byte(rdbOpcodeExpireTimeMs) {
			var expiryMs int64
			if err := binary.Read(reader, binary.LittleEndian, &expiryMs); err != nil {
				return nil, fmt.Errorf("failed to read expiry timestamp: %w", err)
			}
			nextKeyExpiryTime = time.Unix(0, expiryMs*int64(time.Millisecond))
			continue
		}

		valueType := RDBValueType(firstByte)

		var keyLength uint32
		if err := binary.Read(reader, binary.LittleEndian, &keyLength); err != nil { return nil, fmt.Errorf("failed to read key length: %w", err) }
		keyBytes := make([]byte, keyLength)
		if _, err := io.ReadFull(reader, keyBytes); err != nil { return nil, fmt.Errorf("failed to read key data: %w", err) }
		key := string(keyBytes)

		// Apply expiry if one was pending for *this* key (before loading its value)
		// Check if the loaded key is already past its expiry time
		if !nextKeyExpiryTime.IsZero() && time.Now().After(nextKeyExpiryTime) {
			// Key is expired, skip loading its value by reading and discarding based on type
			// This is complex as value structure varies.
			// Simpler: load it, then let isExpired handle it on first access or rely on TTL.
			// For now, we load it and then the Set/HSet will apply the expiry.
			// If we load and then immediately set expiry, an already past expiry will be handled by isExpired.
		}

		switch valueType {
		case RDBStringValueType:
			var valLength uint32
			if err := binary.Read(reader, binary.LittleEndian, &valLength); err != nil { return nil, fmt.Errorf("failed to read string value length for key '%s': %w", key, err) }
			valBytes := make([]byte, valLength)
			if _, err := io.ReadFull(reader, valBytes); err != nil { return nil, fmt.Errorf("failed to read string value data for key '%s': %w", key, err) }
			ds.Set(key, string(valBytes), SetOptions{})

		case RDBHashValueType:
			var fieldCount uint32
			if err := binary.Read(reader, binary.LittleEndian, &fieldCount); err != nil { return nil, fmt.Errorf("failed to read hash field count for key '%s': %w", key, err) }

			// Create the hash or ensure it's clean if HSet is used repeatedly
			// ds.Hashes[key] = make(map[string]string) // Ensures a clean start if key somehow existed

			for i := uint32(0); i < fieldCount; i++ {
				var fieldLen uint32
				if err := binary.Read(reader, binary.LittleEndian, &fieldLen); err != nil { return nil, fmt.Errorf("RDB load: hash field len read error for key %s: %w", key, err) }
				fieldBytes := make([]byte, fieldLen)
				if _, err := io.ReadFull(reader, fieldBytes); err != nil { return nil, fmt.Errorf("RDB load: hash field data read error for key %s: %w", key, err) }
				field := string(fieldBytes)

				var valLen uint32
				if err := binary.Read(reader, binary.LittleEndian, &valLen); err != nil { return nil, fmt.Errorf("RDB load: hash value len read error for key %s, field %s: %w", key, field, err) }
				valBytes := make([]byte, valLen)
				if _, err := io.ReadFull(reader, valBytes); err != nil { return nil, fmt.Errorf("RDB load: hash value data read error for key %s, field %s: %w", key, field, err) }
				value := string(valBytes)

				// HSet will also update indexes, which is good.
				// HSet also clears the main key's expiry, which we will re-apply if nextKeyExpiryTime is set.
				_, errHSet := ds.HSet(key, field, value)
				if errHSet != nil { return nil, fmt.Errorf("failed to HSet field '%s' for key '%s' during RDB load: %w", field, key, errHSet) }
			}
		// case RDBStreamValueType: // TODO
		default:
			return nil, fmt.Errorf("unknown RDB value type %d for key '%s' during load", valueType, key)
		}

		if !nextKeyExpiryTime.IsZero() {
			// If already expired, ds.Expire will handle deletion.
			// This is slightly inefficient as Set/HSet might have cleared expiry, then we set it, then Expire might delete.
			// A more direct way:
			if time.Now().After(nextKeyExpiryTime) {
				ds.Del(key) // Ensure it's gone if expired
			} else {
				// ds.expiries map is protected by ds.mu, which is acquired by Expire/Pexpire.
				// For direct setting:
				ds.mu.Lock()
				ds.expiries[key] = nextKeyExpiryTime
				ds.mu.Unlock()
			}
			nextKeyExpiryTime = time.Time{}
		}
	}
	return ds, nil
}
