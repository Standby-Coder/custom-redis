package database

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Error constants for stream operations
var (
	ErrInvalidStreamIDFormat  = errors.New("ERR Invalid stream ID format: Use 'ms-seq', '*', or 'ms-*'")
	ErrStreamIDTooSmall       = errors.New("ERR The ID specified in XADD is not greater than the last entry ID")
	ErrStreamIDZeroNotAllowed = errors.New("ERR The ID 0-0 is not a valid ID. An ID must be greater than 0-0 if it's an explicit ID for XADD, or if generated, it must be greater than previous 0-0.")
	ErrTimestampTooSmall      = errors.New("ERR The ID specified in XADD has a timestamp smaller than the last entry")
	ErrSequenceOverflow       = errors.New("ERR Sequence number overflow for the given timestamp")
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
// GenerateStreamID generates a new StreamID based on the previous ID and custom inputs.
// - `previousID`: The last ID in the stream.
// - `requestedTimestamp`: Optional pointer to a millisecond timestamp. If nil, current time is used.
// - `requestedSequence`: Optional pointer to a sequence number. If nil, sequence is auto-generated.
// This function is used by XADD when '*' or 'ts-*' ID format is provided.
func GenerateStreamID(previousID StreamID, requestedTimestamp *int64, requestedSequence *int64) (StreamID, error) {
	var newTimestamp int64
	var newSequence int64

	if requestedTimestamp == nil { // For '*' or when XADD provides nil for auto-timestamp
		newTimestamp = time.Now().UnixMilli()
	} else {
		newTimestamp = *requestedTimestamp
	}

	if newTimestamp < previousID.Timestamp {
		return StreamID{}, ErrTimestampTooSmall
	}

	if newTimestamp == previousID.Timestamp {
		if requestedSequence == nil { // Auto-generate sequence for same timestamp (e.g. "ts-*" or "*" if time didn't change)
			// If previous ID was 0-0 and the new timestamp is also 0 (e.g. '0-*' on empty stream),
			// the sequence should be 0, to form 0-0.
			if newTimestamp == 0 && previousID.Timestamp == 0 && previousID.Sequence == 0 {
				newSequence = 0
			} else if previousID.Sequence == math.MaxInt64 {
				return StreamID{}, ErrSequenceOverflow
			} else {
				newSequence = previousID.Sequence + 1
			}
		} else { // Explicit sequence provided for the same timestamp
			newSequence = *requestedSequence
			// XADD will validate if this explicit sequence is > previousID.Sequence
		}
	} else { // newTimestamp > previousID.Timestamp
		if requestedSequence == nil { // Auto-generate sequence for a new, greater timestamp
			newSequence = 0
		} else { // Explicit sequence provided for a new, greater timestamp
			newSequence = *requestedSequence
		}
	}

	// Note: This function does not enforce that the generated ID is strictly greater than previousID
	// if an explicit requestedSequence is provided for the same timestamp.
	// That specific validation (new_ts-seq > prev_ts-seq) is the responsibility of the XADD command
	// when handling fully explicit IDs or when using a generated ID.
	// This generator's main job is to correctly increment sequences or set them to 0 for new timestamps
	// when auto-generation is requested.

	return StreamID{Timestamp: newTimestamp, Sequence: newSequence}, nil
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
	// Get or create the stream (DataStore level lock)
	ds.mu.Lock()
	stream, ok := ds.Streams[streamKey]
	if !ok {
		stream = NewStream() // LastGeneratedID will be 0-0
		ds.Streams[streamKey] = stream
	}
	ds.mu.Unlock()

	// Lock the specific stream for the XADD operation
	stream.mu.Lock()
	defer stream.mu.Unlock()

	var newID StreamID
	var err error

	if idStr == "*" { // Fully auto-generate ID ("*")
		newID, err = GenerateStreamID(stream.LastGeneratedID, nil, nil)
		if err != nil {
			return StreamID{}, err // Could be ErrTimestampTooSmall or ErrSequenceOverflow
		}
		// Additional check for '*' based generation: newID must be > LastGeneratedID
		// This is implicitly handled by GenerateStreamID if prevID is not 0-0.
		// If prevID is 0-0, any valid current time based ID (e.g. ms-0) is fine.
		// If prevID is maxed out, GenerateStreamID should error.
		if newID.Timestamp == 0 && newID.Sequence == 0 && !(stream.LastGeneratedID.Timestamp == 0 && stream.LastGeneratedID.Sequence == 0) {
             // This should ideally not be hit if GenerateStreamID is correct.
             // It implies '*' generated 0-0 for a non-empty stream, which is wrong.
            return StreamID{}, ErrStreamIDZeroNotAllowed
        }

	} else { // Specific timestamp or timestamp-sequence ("ts-seq" or "ts-*")
		parts := strings.Split(idStr, "-")
		if len(parts) > 2 { // e.g. "1-2-3"
			return StreamID{}, ErrInvalidStreamIDFormat
		}

		timestamp, errParseTs := strconv.ParseInt(parts[0], 10, 64)
		if errParseTs != nil {
			return StreamID{}, ErrInvalidStreamIDFormat // Invalid timestamp part
		}

		if len(parts) == 1 { // Only timestamp provided, e.g., "12345". Invalid for XADD.
			return StreamID{}, ErrInvalidStreamIDFormat
		}

		// Case "ts-*" (auto-generate sequence for given timestamp)
		if parts[1] == "*" {
			newID, err = GenerateStreamID(stream.LastGeneratedID, &timestamp, nil)
			if err != nil {
				return StreamID{}, err // e.g. ErrTimestampTooSmall, ErrSequenceOverflow
			}
			// GenerateStreamID already ensures ts >= previousID.Timestamp.
			// And if ts == previousID.Timestamp, seq will be previousID.Sequence + 1.
			// This means newID will always be > previousID.LastGeneratedID unless sequence overflows.
			// The only edge case is if requested timestamp is 0 for a non-empty stream (prevID != 0-0).
            // GenerateStreamID returns ErrTimestampTooSmall if timestamp < previousID.Timestamp.
            // If timestamp == 0 and previousID.Timestamp == 0 and previousID.Sequence > 0,
            // then newID.Sequence will be prev.Sequence+1. This must be checked against stream.LastGeneratedID.
            // The general check `newID must be > stream.LastGeneratedID` is below.

		} else { // Explicit ID "ts-seq"
			sequence, errParseSeq := strconv.ParseInt(parts[1], 10, 64)
			if errParseSeq != nil || sequence < 0 { // Sequence cannot be negative
				return StreamID{}, ErrInvalidStreamIDFormat // Invalid sequence part
			}

			if timestamp == 0 && sequence == 0 {
				return StreamID{}, ErrStreamIDZeroNotAllowed
			}

			newID = StreamID{Timestamp: timestamp, Sequence: sequence}

			// Validate explicit ID against last generated ID
			if newID.Timestamp < stream.LastGeneratedID.Timestamp {
				return StreamID{}, ErrTimestampTooSmall
			}
			if newID.Timestamp == stream.LastGeneratedID.Timestamp && newID.Sequence <= stream.LastGeneratedID.Sequence {
				return StreamID{}, ErrStreamIDTooSmall // Not strictly greater sequence for same timestamp
			}
		}
	}

	// Final validation for all ID generation/specification paths:
	// The new ID must be strictly greater than the stream's LastGeneratedID.
	// This is implicitly handled by GenerateStreamID for '*' and 'ts-*' (except for exact same ID if sequence was provided to GenerateStreamID)
	// and by explicit checks for "ts-seq".
	// Let's ensure this one more time for safety, especially for the path where GenerateStreamID might have
	// been called with an explicit sequence (though it's not its primary use case from XADD).
	if !(newID.Timestamp > stream.LastGeneratedID.Timestamp || (newID.Timestamp == stream.LastGeneratedID.Timestamp && newID.Sequence > stream.LastGeneratedID.Sequence)) {
		// This check is particularly for:
		// 1. Explicit "ts-seq" (already covered above, but good safeguard).
		// 2. "ts-*" if GenerateStreamID somehow produced an invalid ID (e.g., if requested ts was same as last, but generated seq was not greater).
		//    GenerateStreamID for "ts-*" with same ts *should* produce seq+1.
		// 3. "*" if it somehow generated an ID not greater (e.g., time didn't advance and sequence maxed out - ErrSequenceOverflow should catch this).
		// The only exception is an empty stream (LastGeneratedID is 0-0), where any valid ID (not 0-0 explicitly) is fine.
		if !(stream.LastGeneratedID.Timestamp == 0 && stream.LastGeneratedID.Sequence == 0 && (newID.Timestamp > 0 || newID.Sequence > 0) ) {
             // If stream was not empty OR if it was empty but newID is also 0-0 (which is disallowed for explicit XADD)
            if !(newID.Timestamp == 0 && newID.Sequence == 0) { // if newID is not 0-0, it must be greater
                 return StreamID{}, ErrStreamIDTooSmall
            }
            // If newID is 0-0, it's disallowed by explicit check for "ts-seq", and GenerateStreamID for '*' would not make 0-0 unless time is at epoch.
        }
	}


	entry := StreamEntry{
		ID:     newID,
		Fields: fields,
	}

	stream.Entries = append(stream.Entries, entry)
	stream.LastGeneratedID = newID // Update the last ID in the stream

	return newID, nil
}
