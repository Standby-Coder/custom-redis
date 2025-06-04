package redis

import (
	"errors"
	"reflect"
	"testing"
)

func TestParseCommand(t *testing.T) {
	tests := []struct {
		name        string
		request     []byte
		wantCmdName string
		wantArgs    []string
		wantErr     error
	}{
		// Valid Commands (Arrays of Bulk Strings)
		{
			name:        "GET command",
			request:     []byte("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"),
			wantCmdName: "GET",
			wantArgs:    []string{"key"},
			wantErr:     nil,
		},
		{
			name:        "SET command",
			request:     []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"),
			wantCmdName: "SET",
			wantArgs:    []string{"key", "value"},
			wantErr:     nil,
		},
		{
			name:        "Command with no arguments",
			request:     []byte("*1\r\n$4\r\nPING\r\n"),
			wantCmdName: "PING",
			wantArgs:    []string{},
			wantErr:     nil,
		},
		{
			name:        "Command with empty string argument",
			request:     []byte("*2\r\n$3\r\nSET\r\n$0\r\n\r\n"),
			wantCmdName: "SET",
			wantArgs:    []string{""},
			wantErr:     nil,
		},
		{
            name:        "Command with multiple arguments",
            request:     []byte("*4\r\n$4\r\nLREM\r\n$5\r\nmykey\r\n$1\r\n0\r\n$5\r\nvalue\r\n"),
            wantCmdName: "LREM",
            wantArgs:    []string{"mykey", "0", "value"},
            wantErr:     nil,
        },

		// Invalid Inputs or Malformed Commands
		{
			name:        "Empty input",
			request:     []byte{},
			wantErr:     errors.New("ERR failed to read type marker"),
		},
		{
			name:        "Non-array type marker (Simple String)",
			request:     []byte("+OK\r\n"),
			wantErr:     errors.New("ERR expected array marker '*', got '+'"),
		},
		{
			name:        "Non-array type marker (Error)",
			request:     []byte("-Error message\r\n"),
			wantErr:     errors.New("ERR expected array marker '*', got '-'"),
		},
		{
			name:        "Non-array type marker (Integer)",
			request:     []byte(":123\r\n"),
			wantErr:     errors.New("ERR expected array marker '*', got ':'"),
		},
		{
			name:        "Non-array type marker (Bulk String)",
			request:     []byte("$5\r\nhello\r\n"),
			wantErr:     errors.New("ERR expected array marker '*', got '$'"),
		},
		{
			name:        "Null array",
			request:     []byte("*-1\r\n"),
			wantErr:     errors.New("ERR invalid number of arguments"),
		},
		{
			name:        "Empty array",
			request:     []byte("*0\r\n"),
			wantErr:     errors.New("ERR invalid number of arguments"),
		},
		{
			name:        "Array with non-bulk string element (integer)",
			request:     []byte("*2\r\n$3\r\nGET\r\n:123\r\n"),
			wantErr:     errors.New("ERR expected bulk string marker '$', got ':'"),
		},
		{
			name:        "Incomplete array length (missing CRLF)",
			request:     []byte("*2"),
			wantErr:     errors.New("ERR failed to read array length"),
		},
		{
			name:        "Invalid array length (not a number)",
			request:     []byte("*foo\r\n"),
			wantErr:     errors.New("ERR invalid number of arguments"),
		},
		{
			name:        "Array length too short for elements (extra elements ignored)",
			request:     []byte("*1\r\n$3\r\nGET\r\n$3\r\nkey\r\n"),
			wantCmdName: "GET",
            wantArgs:    []string{},
            wantErr:     nil,
		},
		{
            name:        "Array elements not fully provided",
            request:     []byte("*2\r\n$3\r\nGET\r\n"),
            wantErr:     errors.New("ERR failed to read element type marker"),
        },
		{
			name:        "Bulk string with incorrect length (data shorter than declared, stream ends)",
			request:     []byte("*1\r\n$5\r\nhel"), // Stream ends after "hel"
			wantErr:     errors.New("ERR failed to read bulk string data"),
		},
		{
			name:        "Bulk string missing CRLF after data (stream ends)",
			request:     []byte("*1\r\n$5\r\nhello"), // Data is "hello", stream ends before final CRLF
			wantErr:     errors.New("ERR failed to read bulk string trailing CR"),
		},
		{
			name:        "Bulk string length negative (for command name)",
			request:     []byte("*1\r\n$-1\r\n"),
			wantErr:     errors.New("ERR invalid bulk string length"),
		},
		{
            name:        "Bulk string length for arg is negative (null)",
            request:     []byte("*2\r\n$3\r\nCMD\r\n$-1\r\n"),
            wantErr:     errors.New("ERR invalid bulk string length"),
        },
		{
			name:        "Nested array (not supported for commands)",
			request:     []byte("*2\r\n$3\r\nCMD\r\n*1\r\n$3\r\nARG\r\n"),
			wantErr:     errors.New("ERR expected bulk string marker '$', got '*'"),
		},
		{
			name:    "CRLF only",
			request: []byte("\r\n"),
			wantErr: errors.New("ERR expected array marker '*', got '\r'"), // Adjusted
		},
		{
			name:    "Incomplete bulk string length",
			request: []byte("*1\r\n$5"),
			wantErr: errors.New("ERR failed to read bulk string length"),
		},
		{
			name:    "Incomplete bulk string data (stream ends before data fully read)",
			request: []byte("*1\r\n$2\r\na"),
			wantErr: errors.New("ERR failed to read bulk string data"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCmdName, gotArgs, err := ParseCommand(tt.request)

			if tt.wantErr != nil {
				if err == nil {
					t.Errorf("ParseCommand() error = nil, wantErr %v", tt.wantErr)
					return
				}
				if err.Error() != tt.wantErr.Error() {
					t.Errorf("ParseCommand() error = %q, wantErr %q", err.Error(), tt.wantErr.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("ParseCommand() unexpected error = %v", err)
				return
			}

			if gotCmdName != tt.wantCmdName {
				t.Errorf("ParseCommand() gotCmdName = %q, want %q", gotCmdName, tt.wantCmdName)
			}

			// Handle wantArgs being nil vs empty for tests
			// Both are treated as empty for DeepEqual if one is nil and other is empty slice.
			// For stricter checking:
			if tt.wantArgs == nil && gotArgs != nil && len(gotArgs) == 0 {
				// If wantArgs is explicitly nil, but gotArgs is empty slice, treat as equal for this test.
				// Or, ensure wantArgs is always []string{} for no args.
			} else if len(tt.wantArgs) == 0 && gotArgs == nil {
				// If wantArgs is empty slice, but gotArgs is nil, treat as equal.
			} else if !reflect.DeepEqual(gotArgs, tt.wantArgs) {
				t.Errorf("ParseCommand() gotArgs = %#v, want %#v", gotArgs, tt.wantArgs)
			}
		})
	}
}
