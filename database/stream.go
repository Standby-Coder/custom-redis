package database

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// StreamID represents a unique ID for a stream entry.
type StreamID struct {
	Timestamp int64
	Sequence  int64
}

// String formats the StreamID as "timestamp-sequence".
func (sid StreamID) String() string {
	return fmt.Sprintf("%d-%d", sid.Timestamp, sid.Sequence)
}

// GenerateStreamID generates a new StreamID.
// - `previousID`: The last ID generated for the stream.
// - `customTimestamp`: Optional. If nil, current time is used. If non-nil, this timestamp is used.
// - `customSequence`: Optional. If nil, sequence is auto-generated. If non-nil, this sequence is used.
// This function needs to handle cases like "0-0" and "*", "ts-*".
// For simplicity, this initial version will be less strict than Redis's full ID generation.
func GenerateStreamID(previousID StreamID, customTimestamp *int64, customSequence *int64) (StreamID, error) {
	newID := StreamID{}

	if customTimestamp == nil { // Auto-generate timestamp (like "*")
		newID.Timestamp = time.Now().UnixNano() / int64(time.Millisecond)
	} else {
		newID.Timestamp = *customTimestamp
	}

	if customSequence == nil { // Auto-generate sequence (like "ts-*")
		if newID.Timestamp == previousID.Timestamp {
			newID.Sequence = previousID.Sequence + 1
		} else {
			newID.Sequence = 0
		}
	} else {
		newID.Sequence = *customSequence
	}

	// Basic validation: new ID must be greater than previousID
	// unless previousID is zero and newID is 0-0 (special case for first entry sometimes, though Redis is specific)
	if previousID.Timestamp == 0 && previousID.Sequence == 0 {
		// Allow any valid new ID if stream was empty or started with 0-0
	} else if newID.Timestamp < previousID.Timestamp {
		return StreamID{}, fmt.Errorf("ERR The new Stream ID specified is smaller than the newest ID already stored in the stream")
	} else if newID.Timestamp == previousID.Timestamp && newID.Sequence <= previousID.Sequence {
		return StreamID{}, fmt.Errorf("ERR The new Stream ID specified is not greater than the newest ID already stored in the stream")
	}

	// Specific Redis rule: if new ID is 0-0, it's only valid if lastGeneratedID is also 0-0 (empty stream effectively)
	// This is simplified here. More advanced check:
	if newID.Timestamp == 0 && newID.Sequence == 0 && !(previousID.Timestamp == 0 && previousID.Sequence == 0) {
         // Allow 0-0 only if stream is empty (previousID is 0-0)
        // However, Redis XADD with explicit 0-0 is an error unless stream is empty.
        // Our GenerateStreamID is more for internal use or when '*' is used.
        // Let's refine this: XADD with specific "0-0" has its own rules.
        // This generator should probably prevent generating 0-0 unless explicitly asked for and previous is also 0-0.
        // If auto-generating and previousID is 0-0, next should be current_time-0, not 0-1.
        if customTimestamp == nil && previousID.Timestamp == 0 && previousID.Sequence == 0 { // Auto-generating timestamp for empty stream
             if newID.Timestamp == 0 { // Very unlikely current time is 0, but to be safe
                 newID.Sequence = 1 // As per Redis: "0-0 is not a valid ID. an ID must be greater than 0-0"
                                  // Actually, redis allows 0-0 if stream empty, then next is ms-0
                                  // "The first entry ID generated is <ms>-0"
                                  // So if prev is 0-0, and current time is used, sequence should be 0.
                 newID.Sequence = 0
             }
        } else if newID.Timestamp == 0 && newID.Sequence == 0 && (previousID.Timestamp != 0 || previousID.Sequence != 0) {
             return StreamID{}, fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
        }
	}


	return newID, nil
}

// StreamEntry represents a single entry in a stream.
type StreamEntry struct {
	ID     StreamID
	Fields map[string]string
}

// Stream holds all entries for a given stream key.
type Stream struct {
	Entries         []StreamEntry
	LastGeneratedID StreamID
	MaxDeletedID    StreamID // For XTRIM, not used yet
	mu              sync.Mutex
}

// NewStream creates a new Stream object.
func NewStream() *Stream {
	return &Stream{
		Entries: make([]StreamEntry, 0),
		// LastGeneratedID is implicitly 0-0 initially
	}
}

// XAdd adds an entry to the stream.
// ds: The DataStore instance.
// key: The key of the stream.
// idStr: The ID string from the command (e.g., "*", "timestamp-seq", "timestamp-*").
// fields: The field-value pairs for the stream entry.
// Returns the actual StreamID used and an error if any.
func XAdd(ds *DataStore, streamKey string, idStr string, fields map[string]string) (StreamID, error) {
	ds.mu.Lock() // Lock DataStore for stream creation/access
	stream, ok := ds.Streams[streamKey]
	if !ok {
		stream = NewStream()
		ds.Streams[streamKey] = stream
	}
	ds.mu.Unlock()

	stream.mu.Lock()
	defer stream.mu.Unlock()

	var newID StreamID
	var err error

	parts := strings.Split(idStr, "-")
	timestampStr := parts[0]
	var sequenceStr string
	if len(parts) > 1 {
		sequenceStr = parts[1]
	}

	if timestampStr == "*" { // Fully automatic ID generation
		if len(parts) > 1 { // "*" cannot have a sequence part like "*-1"
			return StreamID{}, fmt.Errorf("ERR Invalid stream ID specified: %s - '*' cannot be followed by a sequence number", idStr)
		}
		newID, err = GenerateStreamID(stream.LastGeneratedID, nil, nil)
		if err != nil {
			return StreamID{}, err
		}
	} else { // Timestamp is specified or partially specified
		ts, errParseTs := strconv.ParseInt(timestampStr, 10, 64)
		if errParseTs != nil {
			return StreamID{}, fmt.Errorf("ERR Invalid stream ID specified: %s - invalid timestamp part", idStr)
		}

		if len(parts) == 1 { // e.g. "12345" - Redis XADD doesn't allow this, needs "ts-seq" or "ts-*" or "*"
		    return StreamID{}, fmt.Errorf("ERR Invalid stream ID specified: %s - must be '*', 'ts-*' or 'ts-seq'", idStr)
        }


		if sequenceStr == "*" { // "timestamp-*"
			newID, err = GenerateStreamID(stream.LastGeneratedID, &ts, nil)
			if err != nil {
				return StreamID{}, err
			}
		} else { // "timestamp-sequence"
			seq, errParseSeq := strconv.ParseInt(sequenceStr, 10, 64)
			if errParseSeq != nil {
				return StreamID{}, fmt.Errorf("ERR Invalid stream ID specified: %s - invalid sequence part", idStr)
			}

			// Explicit ID validation
			if ts == 0 && seq == 0 {
				if len(stream.Entries) > 0 { // Or check stream.LastGeneratedID != (0-0)
					return StreamID{}, fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
				}
				// If stream is empty, 0-0 is allowed by some interpretations, but then next ID must be > 0-0
				// For simplicity, let's align with: if explicit 0-0, and stream not empty, error.
				// If stream empty, 0-0 is fine. LastGeneratedID would be 0-0.
				newID = StreamID{Timestamp: 0, Sequence: 0}

			} else { // Not "0-0"
				if ts < stream.LastGeneratedID.Timestamp {
					return StreamID{}, fmt.Errorf("ERR The new Stream ID specified is smaller than the newest ID already stored in the stream")
				}
				if ts == stream.LastGeneratedID.Timestamp && seq <= stream.LastGeneratedID.Sequence {
					return StreamID{}, fmt.Errorf("ERR The new Stream ID specified is not greater than the newest ID already stored in the stream")
				}
				newID = StreamID{Timestamp: ts, Sequence: seq}
			}
		}
	}

	// Final check to prevent timestamp going backwards or sequence not incrementing on same timestamp
    // This is partially covered by GenerateStreamID and explicit checks above.
    // But GenerateStreamID also needs to be robust.
    // If GenerateStreamID was called (idStr contained '*'), it should have produced a valid ID > LastGeneratedID
    // or errored. If ID was explicit, checks above apply.

	entry := StreamEntry{
		ID:     newID,
		Fields: fields,
	}

	// Entries should be sorted by ID. XADD appends if ID is greater.
	// If an explicit ID is given that's not greater (but not 0-0 for empty stream), it's an error handled above.
	stream.Entries = append(stream.Entries, entry)
	stream.LastGeneratedID = newID

	return newID, nil
}
