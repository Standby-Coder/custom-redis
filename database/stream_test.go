package database

import (
	"reflect"
	"testing"
	"time"
	// "fmt" // For debugging
	"math"
	// "strings" // For new tests - apparently not used directly in this file's tests
)

// Helper to create a new DataStore with an empty stream for testing XADD
func newTestStream(t *testing.T) (*DataStore, *Stream) {
	t.Helper()
	ds := NewDataStore()
	stream := NewStream()
	ds.Streams["teststream"] = stream
	return ds, stream
}

func TestGenerateStreamID(t *testing.T) {
	// Test cases for GenerateStreamID
	// Basic increment
	t.Run("basic_increment_seq", func(t *testing.T) {
		prevID := StreamID{Timestamp: 1000, Sequence: 0}
		ts := int64(1000)
		newID, err := GenerateStreamID(prevID, &ts, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if newID.Timestamp != 1000 || newID.Sequence != 1 {
			t.Errorf("got %s, want 1000-1", newID.String())
		}
	})

	// Timestamp increase, sequence reset
	t.Run("timestamp_increase_reset_seq", func(t *testing.T) {
		prevID := StreamID{Timestamp: 1000, Sequence: 5}
		ts := int64(1001)
		newID, err := GenerateStreamID(prevID, &ts, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if newID.Timestamp != 1001 || newID.Sequence != 0 {
			t.Errorf("got %s, want 1001-0", newID.String())
		}
	})

	// Auto timestamp '*'
	t.Run("auto_timestamp", func(t *testing.T) {
		prevID := StreamID{Timestamp: time.Now().UnixMilli() - 1000, Sequence: 0} // Ensure prev time is in past
		newID, err := GenerateStreamID(prevID, nil, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if newID.Timestamp <= prevID.Timestamp {
			t.Errorf("new timestamp %d not greater than previous %d", newID.Timestamp, prevID.Timestamp)
		}
		if newID.Timestamp == prevID.Timestamp && newID.Sequence <= prevID.Sequence {
			 t.Errorf("new ID %s not greater than previous %s", newID.String(), prevID.String())
		}
	})

	// Timestamp too small
	t.Run("timestamp_too_small", func(t *testing.T) {
		prevID := StreamID{Timestamp: 1000, Sequence: 0}
		ts := int64(999)
		_, err := GenerateStreamID(prevID, &ts, nil)
		if err != ErrTimestampTooSmall {
			t.Errorf("expected ErrTimestampTooSmall, got %v", err)
		}
	})

	// Sequence overflow
	t.Run("sequence_overflow", func(t *testing.T) {
		prevID := StreamID{Timestamp: 1000, Sequence: math.MaxInt64}
		ts := int64(1000)
		_, err := GenerateStreamID(prevID, &ts, nil) // Request next sequence for same timestamp
		if err != ErrSequenceOverflow {
			t.Errorf("expected ErrSequenceOverflow, got %v", err)
		}
	})

	// From 0-0 with '*'
	t.Run("from_0-0_auto_timestamp", func(t *testing.T) {
		prevID := StreamID{Timestamp: 0, Sequence: 0}
		newID, err := GenerateStreamID(prevID, nil, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if newID.Timestamp == 0 && newID.Sequence == 0 { // Highly unlikely current time is 0
			t.Errorf("expected new ID to be > 0-0, got %s", newID.String())
		}
		if newID.Timestamp > 0 && newID.Sequence != 0 {
			t.Errorf("expected sequence to be 0 for new timestamp, got %s", newID.String())
		}
	})

	t.Run("from_0-0_with_0-star", func(t *testing.T) {
		prevID := StreamID{Timestamp: 0, Sequence: 0}
		ts := int64(0)
		newID, err := GenerateStreamID(prevID, &ts, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if newID.Timestamp != 0 || newID.Sequence != 0 {
			t.Errorf("expected 0-0 from GenerateStreamID(0-0, 0, nil), got %s", newID.String())
		}
	})

	// Test sequence overflow for same timestamp
	t.Run("sequence_overflow_same_timestamp", func(t *testing.T) {
		prevID := StreamID{Timestamp: 1000, Sequence: math.MaxInt64}
		ts := int64(1000) // Same timestamp
		_, err := GenerateStreamID(prevID, &ts, nil) // Auto-increment sequence
		if err != ErrSequenceOverflow {
			t.Errorf("GenerateStreamID expected ErrSequenceOverflow for same timestamp, got %v", err)
		}
	})
}


func TestXAdd_IDValidation(t *testing.T) {
	fields := map[string]string{"field": "value"}

	t.Run("XADD_*_empty_stream", func(t *testing.T) {
		ds, _ := newTestStream(t)
		id, err := XAdd(ds, "teststream", "*", fields)
		if err != nil {
			t.Fatalf("XADD * on empty stream failed: %v", err)
		}
		if id.Timestamp == 0 && id.Sequence == 0 {
			t.Errorf("XADD * generated 0-0, expected something greater")
		}
	})

	t.Run("XADD_*_existing_entries", func(t *testing.T) {
		ds, stream := newTestStream(t)
		stream.LastGeneratedID = StreamID{Timestamp: 1000, Sequence: 0}

		id, err := XAdd(ds, "teststream", "*", fields)
		if err != nil {
			t.Fatalf("XADD * on existing stream failed: %v", err)
		}
		if !(id.Timestamp > 1000 || (id.Timestamp == 1000 && id.Sequence > 0)) {
			t.Errorf("XADD * did not generate ID greater than 1000-0, got %s", id.String())
		}
	})

	t.Run("XADD_ts-star_ts_greater", func(t *testing.T) {
		ds, stream := newTestStream(t)
		stream.LastGeneratedID = StreamID{Timestamp: 1000, Sequence: 5}

		id, err := XAdd(ds, "teststream", "1001-*", fields)
		if err != nil {
			t.Fatalf("XADD 1001-* failed: %v", err)
		}
		if id.Timestamp != 1001 || id.Sequence != 0 {
			t.Errorf("XADD 1001-* expected 1001-0, got %s", id.String())
		}
	})

	t.Run("XADD_ts-star_ts_equal", func(t *testing.T) {
		ds, stream := newTestStream(t)
		stream.LastGeneratedID = StreamID{Timestamp: 1000, Sequence: 5}

		id, err := XAdd(ds, "teststream", "1000-*", fields)
		if err != nil {
			t.Fatalf("XADD 1000-* failed: %v", err)
		}
		if id.Timestamp != 1000 || id.Sequence != 6 {
			t.Errorf("XADD 1000-* expected 1000-6, got %s", id.String())
		}
	})

	t.Run("XADD_ts-star_ts_less_fail", func(t *testing.T) {
		ds, stream := newTestStream(t)
		stream.LastGeneratedID = StreamID{Timestamp: 1000, Sequence: 5}

		_, err := XAdd(ds, "teststream", "999-*", fields)
		if err != ErrTimestampTooSmall {
			t.Errorf("XADD 999-* expected ErrTimestampTooSmall, got %v", err)
		}
	})

	t.Run("XADD_ts-star_0-star_on_empty_stream", func(t *testing.T) {
		ds, _ := newTestStream(t)
		id, err := XAdd(ds, "teststream", "0-*", fields)
		if err != nil {
			t.Fatalf("XADD 0-* on empty stream failed: %v", err)
		}
		if id.Timestamp != 0 || id.Sequence != 0 {
			 t.Errorf("XADD 0-* on empty stream: expected 0-0, got %s.", id.String())
		}
	})


	t.Run("XADD_explicit_0-0_fail", func(t *testing.T) {
		ds, _ := newTestStream(t)
		_, err := XAdd(ds, "teststream", "0-0", fields)
		if err != ErrStreamIDZeroNotAllowed {
			t.Errorf("XADD 0-0 expected ErrStreamIDZeroNotAllowed, got %v", err)
		}
	})

	t.Run("XADD_explicit_ts_less_fail", func(t *testing.T) {
		ds, stream := newTestStream(t)
		stream.LastGeneratedID = StreamID{Timestamp: 1000, Sequence: 5}
		_, err := XAdd(ds, "teststream", "999-0", fields)
		if err != ErrTimestampTooSmall {
			t.Errorf("XADD 999-0 expected ErrTimestampTooSmall, got %v", err)
		}
	})

	t.Run("XADD_explicit_ts_equal_seq_less_equal_fail", func(t *testing.T) {
		ds, stream := newTestStream(t)
		stream.LastGeneratedID = StreamID{Timestamp: 1000, Sequence: 5}

		_, err := XAdd(ds, "teststream", "1000-5", fields)
		if err != ErrStreamIDTooSmall {
			t.Errorf("XADD 1000-5 expected ErrStreamIDTooSmall, got %v", err)
		}
		_, err = XAdd(ds, "teststream", "1000-4", fields)
		if err != ErrStreamIDTooSmall {
			t.Errorf("XADD 1000-4 expected ErrStreamIDTooSmall, got %v", err)
		}
	})

	t.Run("XADD_explicit_valid_greater", func(t *testing.T) {
		ds, stream := newTestStream(t)
		stream.LastGeneratedID = StreamID{Timestamp: 1000, Sequence: 5}

		id, err := XAdd(ds, "teststream", "1000-6", fields)
		if err != nil {
			t.Fatalf("XADD 1000-6 failed: %v", err)
		}
		if id.Timestamp != 1000 || id.Sequence != 6 {
			t.Errorf("XADD 1000-6 expected 1000-6, got %s", id.String())
		}

		id, err = XAdd(ds, "teststream", "1001-0", fields)
		if err != nil {
			t.Fatalf("XADD 1001-0 failed: %v", err)
		}
		if id.Timestamp != 1001 || id.Sequence != 0 {
			t.Errorf("XADD 1001-0 expected 1001-0, got %s", id.String())
		}
	})

	malformedTests := []struct{name string; idStr string}{
		{"malformed_no_dash_but_not_star", "12345"},
		{"malformed_too_many_dashes", "1-2-3"},
		{"malformed_ts_not_int", "abc-123"},
		{"malformed_seq_not_int", "123-abc"},
		{"malformed_star_with_stuff", "*-1"},
		{"malformed_ts_star_with_stuff", "123-*-1"},
		{"malformed_negative_seq", "123--1"},
	}
	for _, mt := range malformedTests {
		t.Run(mt.name, func(t *testing.T){
			ds, _ := newTestStream(t)
			_, err := XAdd(ds, "teststream", mt.idStr, fields)
			if err != ErrInvalidStreamIDFormat {
				t.Errorf("XADD with malformed ID %q expected ErrInvalidStreamIDFormat, got %v", mt.idStr, err)
			}
		})
	}

	// Additional malformed ID tests for XAdd specifically
	t.Run("XADD_malformed_id_incomplete_dash", func(t *testing.T) {
		ds, _ := newTestStream(t)
		_, err := XAdd(ds, "teststream", "123-", fields)
		if err != ErrInvalidStreamIDFormat {
			t.Errorf("XADD with ID '123-' expected ErrInvalidStreamIDFormat, got %v", err)
		}
	})

	t.Run("XADD_malformed_id_star_prefixed_timestamp", func(t *testing.T) {
		ds, _ := newTestStream(t)
		_, err := XAdd(ds, "teststream", "*123-0", fields) // '*' should be alone or 'ts-*'
		if err != ErrInvalidStreamIDFormat { // This should fail because parts[0] is "*123" which is not "*" and not a valid int
			t.Errorf("XADD with ID '*123-0' expected ErrInvalidStreamIDFormat, got %v", err)
		}
	})
}

func TestStreamID_String(t *testing.T) {
	tests := []struct {
		name string
		sid  StreamID
		want string
	}{
		{"zero_id", StreamID{Timestamp: 0, Sequence: 0}, "0-0"},
		{"simple_id", StreamID{Timestamp: 1234567890123, Sequence: 42}, "1234567890123-42"},
		{"zero_seq", StreamID{Timestamp: 999, Sequence: 0}, "999-0"},
		{"zero_ts", StreamID{Timestamp: 0, Sequence: 10}, "0-10"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.sid.String(); got != tt.want {
				t.Errorf("StreamID.String() = %v, want %v", got, tt.want)
			}
		})
	}
}
// TestXAdd_Content checks if XAdd correctly adds entries to the stream
func TestXAdd_Content(t *testing.T) {
	ds, stream := newTestStream(t)

	fields1 := map[string]string{"f1": "v1", "f2": "v2"}
	id1, err1 := XAdd(ds, "teststream", "*", fields1)
	if err1 != nil { t.Fatalf("XADD 1 failed: %v", err1) }

	fields2 := map[string]string{"f3": "v3"}
	id2, err2 := XAdd(ds, "teststream", "*", fields2)
	if err2 != nil { t.Fatalf("XADD 2 failed: %v", err2) }

	if len(stream.Entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(stream.Entries))
	}
	if !reflect.DeepEqual(stream.Entries[0].ID, id1) || !reflect.DeepEqual(stream.Entries[0].Fields, fields1) {
		t.Errorf("Entry 1 mismatch: got ID %s, fields %v", stream.Entries[0].ID, stream.Entries[0].Fields)
	}
	if !reflect.DeepEqual(stream.Entries[1].ID, id2) || !reflect.DeepEqual(stream.Entries[1].Fields, fields2) {
		t.Errorf("Entry 2 mismatch: got ID %s, fields %v", stream.Entries[1].ID, stream.Entries[1].Fields)
	}
	if !reflect.DeepEqual(stream.LastGeneratedID, id2) {
		t.Errorf("LastGeneratedID mismatch: got %s, want %s", stream.LastGeneratedID, id2)
	}
}
