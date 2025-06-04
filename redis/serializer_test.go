package redis

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

func TestSerializeResponse(t *testing.T) {
	// Helper for comparing byte slices and printing them nicely on error
	assertEqualBytes := func(t *testing.T, got, want []byte, msg string) {
		t.Helper()
		if !bytes.Equal(got, want) {
			t.Errorf("%s: SerializeResponse() = %q (%v), want %q (%v)", msg, string(got), got, string(want), want)
		}
	}

	var nilSlice []interface{} = nil // Explicitly nil slice for Null Array test

	tests := []struct {
		name     string
		response interface{}
		want     []byte
	}{
		// Simple Strings
		{"simple_string_ok", "OK", []byte("+OK\r\n")},
		{"simple_string_hello_world", "hello world", []byte("+hello world\r\n")},
		{"simple_string_empty", "", []byte("+\r\n")},

		// Errors
		{"error_message", errors.New("Error message"), []byte("-Error message\r\n")},
		{"error_empty_message", errors.New(""), []byte("-\r\n")},

		// Integers
		{"integer_123", int64(123), []byte(":123\r\n")},
		{"integer_0", int(0), []byte(":0\r\n")}, // Test with int too
		{"integer_negative_1", int64(-1), []byte(":-1\r\n")},
		{"integer_uint", uint64(42), []byte(":42\r\n")},


		// Bulk Strings (input as []byte for clarity, or nil for Null Bulk String)
		{"bulk_string_hello", []byte("hello"), []byte("$5\r\nhello\r\n")},
		{"bulk_string_empty", []byte(""), []byte("$0\r\n\r\n")},
		{"null_bulk_string", nil, []byte("$-1\r\n")}, // This tests the `case nil:`

		// Arrays
		{
			name:     "array_set_command_style",
			response: []interface{}{[]byte("SET"), []byte("key"), []byte("value")}, // Simulating command parts as []byte
			want:     []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"),
		},
		{
			name:     "array_get_command_style",
			response: []interface{}{[]byte("GET"), []byte("key")},
			want:     []byte("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"),
		},
		{
			name:     "array_empty",
			response: []interface{}{},
			want:     []byte("*0\r\n"),
		},
		{
			name:     "array_mixed_types (nil becomes NullBulkString)",
			response: []interface{}{nil, "OK", int64(10), []byte("data")},
			want:     []byte("*4\r\n$-1\r\n+OK\r\n:10\r\n$4\r\ndata\r\n"),
		},
		{
			name:     "null_array (nil slice passed as interface)",
			response: nilSlice, // var nilSlice []interface{} = nil
			// The current SerializeResponse has `case nil:` which would catch a truly nil interface.
			// A nil slice passed as interface{} is not itself nil. It's an interface containing a nil slice.
			// This should hit the `case []interface{}:` and then the `arr == nil` check inside it.
			want:     []byte("*-1\r\n"),
		},
		{
			name: "array_nested",
			response: []interface{}{
				[]byte("ARRAY"),
				[]interface{}{
					int64(1),
					[]byte("TWO"),
					nil, // Null Bulk String
				},
			},
			want: []byte("*2\r\n$5\r\nARRAY\r\n*3\r\n:1\r\n$3\r\nTWO\r\n$-1\r\n"),
		},
		{
			name: "array_with_error_element", // Errors in arrays are also serialized
			response: []interface{}{
				[]byte("CMD"),
				errors.New("FAILED"),
			},
			want: []byte("*2\r\n$3\r\nCMD\r\n-FAILED\r\n"),
		},

		// Unknown Type
		{
			name:     "unknown_type_struct",
			response: struct{}{},
			want:     []byte(fmt.Sprintf("-ERR unhandled response type %T\r\n", struct{}{})),
		},
		{
			name:     "unknown_type_chan",
			response: make(chan int),
			want:     []byte(fmt.Sprintf("-ERR unhandled response type %T\r\n", make(chan int))),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SerializeResponse(tt.response)
			assertEqualBytes(t, got, tt.want, tt.name)
		})
	}
}
