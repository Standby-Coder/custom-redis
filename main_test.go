package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	// "os" // Removed unused import
	"strings"
	"testing"
	"time"
	"strconv" // Added for parsing integer responses in tests

	"goredis/database" // Added database import
)

// testServer represents a running server instance for testing.
type testServer struct {
	listener net.Listener
	errCh    chan error
	port     string
}

// startTestServer starts the main GoRedis server on a random available port.
// It returns the server instance or an error.
func startTestServer(tb testing.TB) *testServer {
	tb.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0") // Port 0 requests a system-allocated port
	if err != nil {
		tb.Fatalf("Failed to listen on a random port: %v", err)
	}

	port := fmt.Sprintf("%d", listener.Addr().(*net.TCPAddr).Port)
	// tb.Logf("Test server listening on port %s", port)

	// Override os.Args for flag parsing if your main() uses flags for port
	// For this test, we are manually setting the listener, so main's flag parsing for port
	// isn't strictly what's being tested for port selection, but the server logic itself.
	// If main() function *must* parse flags, this needs more complex setup (e.g. running main in a goroutine).
	// For simplicity, we'll assume the server logic can run with a listener we provide.
	// The actual main() is not run directly here; we re-create its core listening loop.

	// Initialize a new DataStore for each test server run
	// This avoids test interference.
	// Note: If your main() initializes global state that affects request handling,
	// ensure that state is also reset or managed for tests.
	// Our current main.go uses global `serverState` and `db`.
	// For true parallel tests, this global state would be problematic.
	// For sequential tests or careful management, it might be okay.
	// Let's re-initialize db for this test server.
	db := database.NewDataStore() // Fresh DataStore

	// Reset global serverState for cleaner tests if needed, though this is tricky.
	// For now, tests should be aware they might share/mutate this if run in parallel
	// without proper isolation (which `go test` does by default for different packages,
	// but not for tests within the same package unless -parallel flag is used and tests are designed for it).
	// serverState = ServerState{...} // Reset if absolutely necessary and understood


	errCh := make(chan error, 1)
	go func() {
		for {
			conn, errAccept := listener.Accept()
			if errAccept != nil {
				// Send error or simply return if listener is closed
				if strings.Contains(errAccept.Error(), "use of closed network listener") {
					errCh <- nil // Normal closure
				} else {
					errCh <- errAccept
				}
				return
			}
			go handleConnection(conn, db) // Use the fresh db
		}
	}()

	return &testServer{listener: listener, errCh: errCh, port: port}
}

// stop stops the test server.
func (s *testServer) stop(tb testing.TB) {
	tb.Helper()
	if err := s.listener.Close(); err != nil {
		tb.Logf("Error closing listener: %v", err) // Log instead of Fatal for cleanup
	}
	// Wait for the accept loop to return an error or nil (on clean shutdown)
	select {
	case err := <-s.errCh:
		if err != nil {
			tb.Logf("Error from server accept loop: %v", err)
		}
	case <-time.After(1 * time.Second): // Timeout if server doesn't stop
		tb.Logf("Timeout waiting for server to stop")
	}
}

// sendRequest connects to the test server, sends a command, and returns the raw response.
func sendRequest(tb testing.TB, addr string, rawCommand string) string {
	tb.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		tb.Fatalf("Failed to connect to test server at %s: %v", addr, err)
	}
	defer conn.Close()

	_, err = conn.Write([]byte(rawCommand))
	if err != nil {
		tb.Fatalf("Failed to send command %q: %v", rawCommand, err)
	}

	// Simple way to read response, assumes response is not excessively large or fragmented.
	// For more robust client, use RESP parser.
	reader := bufio.NewReader(conn)
	responseBytes := make([]byte, 0, 128) // Initial buffer

	// Read until CRLF for simple responses or first part of complex ones.
	// This is a very basic way to read, real client needs full RESP parsing.
	// For PING, responses are simple.

	// Set a read deadline to avoid test hanging
    conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	line, err := reader.ReadString('\n')
	if err != nil {
		if err == io.EOF && line != "" { // EOF but got some data
			responseBytes = append(responseBytes, []byte(line)...)
		} else if err != io.EOF {
			tb.Fatalf("Failed to read response line: %v (got: %q)", err, line)
		} else { // Just EOF
			if len(responseBytes) == 0 { // Truly empty response and EOF
				// tb.Logf("EOF with no data, command: %s", rawCommand)
				return "" // Or handle as error depending on test
			}
		}
	}
	if line != "" {
		responseBytes = append(responseBytes, []byte(line)...)
	}


	// If it's a bulk string, read the data part
	if strings.HasPrefix(string(responseBytes), "$") {
		parts := strings.SplitN(strings.TrimRight(string(responseBytes), "\r\n"), "\r\n", 2)
		if len(parts) > 0 {
			lenStr := parts[0][1:] // Remove $
			var length int // Declare length variable
			_, errScan := fmt.Sscanf(lenStr, "%d", &length) // Use Sscanf for better error handling potential
			if errScan != nil {
				tb.Logf("Failed to parse bulk string length from %q: %v", lenStr, errScan)
                // Decide how to handle this error, maybe return an error from sendRequest or Fatalf
			}

			if length > 0 { // Only read more if there's data
				// Read the data itself + final CRLF
				// This is simplified. `reader.ReadString('\n')` might not get everything if data contains \n
				// A proper client reads `length` bytes then CRLF.
				dataLine, err := reader.ReadString('\n')
				if err != nil && err != io.EOF {
					tb.Fatalf("Failed to read bulk string data part: %v", err)
				}
				responseBytes = append(responseBytes, []byte(dataLine)...)
			} else if length == 0 { // $0\r\n\r\n
				// The second CRLF for empty bulk string
				 crlfLine, err := reader.ReadString('\n')
				 if err != nil && err != io.EOF {
					 tb.Fatalf("Failed to read trailing CRLF for empty bulk string: %v", err)
				 }
				 responseBytes = append(responseBytes, []byte(crlfLine)...)
			}
			// For $-1 (null bulk string), responseBytes already has "$-1\r\n"
		}
	}
	return string(responseBytes)
}

func TestPingCommand(t *testing.T) {
	// Start the server
	// Note: This test structure assumes main() is not directly invoked but its core logic (handleConnection) is used.
	// The global `serverState` and `db` in main.go might be an issue for parallel tests if not careful.
	// For `go test`, tests in the same package run sequentially by default.

	// Reset global server state before tests if it matters for PING (it generally shouldn't)
	// serverState = ServerState{...} // As defined in main.go, with maps initialized
	// db = database.NewDataStore() // ensure fresh db state for other tests if they run after this one

	s := startTestServer(t)
	defer s.stop(t)

	serverAddr := "127.0.0.1:" + s.port

	t.Run("PING_no_argument", func(t *testing.T) {
		// Command in RESP format: *1\r\n$4\r\nPING\r\n
		rawCommand := "*1\r\n$4\r\nPING\r\n"
		want := "+PONG\r\n"
		got := sendRequest(t, serverAddr, rawCommand)
		if got != want {
			t.Errorf("PING (no arg) got %q, want %q", got, want)
		}
	})

	t.Run("PING_with_argument", func(t *testing.T) {
		// Command: PING "hello" -> *2\r\n$4\r\nPING\r\n$5\r\nhello\r\n
		rawCommand := "*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n"
		want := "$5\r\nhello\r\n"
		got := sendRequest(t, serverAddr, rawCommand)
		if got != want {
			t.Errorf("PING (with arg) got %q, want %q", got, want)
		}
	})

	t.Run("PING_with_empty_argument", func(t *testing.T) {
        rawCommand := "*2\r\n$4\r\nPING\r\n$0\r\n\r\n"
        want := "$0\r\n\r\n"
        got := sendRequest(t, serverAddr, rawCommand)
        if got != want {
            t.Errorf("PING (with empty arg) got %q, want %q", got, want)
        }
    })

	t.Run("PING_with_too_many_arguments", func(t *testing.T) {
		// Command: PING "hello" "world" -> *3\r\n$4\r\nPING\r\n$5\r\nhello\r\n$5\r\nworld\r\n
		rawCommand := "*3\r\n$4\r\nPING\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
		wantPrefix := "-ERR wrong number of arguments" // Error messages end with \r\n
		got := sendRequest(t, serverAddr, rawCommand)
		if !strings.HasPrefix(got, wantPrefix) {
			t.Errorf("PING (too many args) got %q, want prefix %q", got, wantPrefix)
		}
		if !strings.HasSuffix(got, "\r\n") {
             t.Errorf("PING (too many args) response %q missing CRLF suffix", got)
        }
	})
}


func TestIncrDecrByCommands(t *testing.T) {
	s := startTestServer(t)
	defer s.stop(t)
	serverAddr := "127.0.0.1:" + s.port

	key := "incdecbykey"

	resetKey := func() {
		delCmd := fmt.Sprintf("*2\r\n$3\r\nDEL\r\n$%d\r\n%s\r\n", len(key), key)
		sendRequest(t, serverAddr, delCmd)
	}

	// --- DECR Tests ---
	t.Run("DECR_non_existing_key", func(t *testing.T) {
		resetKey()
		rawCmd := fmt.Sprintf("*2\r\n$4\r\nDECR\r\n$%d\r\n%s\r\n", len(key), key)
		got := sendRequest(t, serverAddr, rawCmd)
		want := ":-1\r\n"
		if got != want {
			t.Errorf("DECR non-existing key: got %q, want %q", got, want)
		}
	})

	t.Run("DECR_existing_integer_key", func(t *testing.T) {
		resetKey()
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$2\r\n10\r\n", len(key), key))
		rawCmd := fmt.Sprintf("*2\r\n$4\r\nDECR\r\n$%d\r\n%s\r\n", len(key), key)
		got := sendRequest(t, serverAddr, rawCmd)
		want := ":9\r\n"
		if got != want {
			t.Errorf("DECR existing integer key: got %q, want %q", got, want)
		}
	})

	t.Run("DECR_non_integer_string_value", func(t *testing.T) {
		resetKey()
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$5\r\nhello\r\n", len(key), key))
		rawCmd := fmt.Sprintf("*2\r\n$4\r\nDECR\r\n$%d\r\n%s\r\n", len(key), key)
		got := sendRequest(t, serverAddr, rawCmd)
		if !strings.HasPrefix(got, "-ERR value is not an integer or out of range") {
			t.Errorf("DECR non-integer: got %q, want error", got)
		}
	})

	t.Run("DECR_clears_expiry", func(t *testing.T) {
		resetKey()
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$1\r\n5\r\n", len(key), key))
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$6\r\nEXPIRE\r\n$%d\r\n%s\r\n$2\r\n60\r\n", len(key), key))
		sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$4\r\nDECR\r\n$%d\r\n%s\r\n", len(key), key))
		got := sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$3\r\nTTL\r\n$%d\r\n%s\r\n", len(key), key))
		if got != ":-1\r\n" { // Check if the known SET/expiry issue affects this
			t.Logf("DECR_clears_expiry: TTL got %q, want -1. Known issue with SET/expiry might apply.", got)
			if got != ":-1\r\n" { // Still assert for failure if it's not the known issue value.
				t.Errorf("DECR_clears_expiry: TTL got %q, want -1", got)
			}
		}
	})

	// --- INCRBY Tests ---
	t.Run("INCRBY_non_existing_key", func(t *testing.T) {
		resetKey()
		rawCmd := fmt.Sprintf("*3\r\n$6\r\nINCRBY\r\n$%d\r\n%s\r\n$1\r\n5\r\n", len(key), key)
		got := sendRequest(t, serverAddr, rawCmd)
		want := ":5\r\n"
		if got != want {
			t.Errorf("INCRBY non-existing: got %q, want %q", got, want)
		}
	})

	t.Run("INCRBY_existing_integer_key", func(t *testing.T) {
		resetKey()
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$2\r\n10\r\n", len(key), key))
		rawCmd := fmt.Sprintf("*3\r\n$6\r\nINCRBY\r\n$%d\r\n%s\r\n$1\r\n5\r\n", len(key), key)
		got := sendRequest(t, serverAddr, rawCmd)
		want := ":15\r\n"
		if got != want {
			t.Errorf("INCRBY existing: got %q, want %q", got, want)
		}
	})

	t.Run("INCRBY_non_integer_increment", func(t *testing.T) {
		resetKey()
		rawCmd := fmt.Sprintf("*3\r\n$6\r\nINCRBY\r\n$%d\r\n%s\r\n$3\r\nfoo\r\n", len(key), key)
		got := sendRequest(t, serverAddr, rawCmd)
		if !strings.HasPrefix(got, "-ERR value is not an integer or out of range") {
			t.Errorf("INCRBY non-integer increment: got %q, want error", got)
		}
	})

	// --- DECRBY Tests ---
	t.Run("DECRBY_non_existing_key", func(t *testing.T) {
		resetKey()
		rawCmd := fmt.Sprintf("*3\r\n$6\r\nDECRBY\r\n$%d\r\n%s\r\n$1\r\n5\r\n", len(key), key)
		got := sendRequest(t, serverAddr, rawCmd)
		want := ":-5\r\n"
		if got != want {
			t.Errorf("DECRBY non-existing: got %q, want %q", got, want)
		}
	})

	t.Run("DECRBY_existing_integer_key", func(t *testing.T) {
		resetKey()
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$2\r\n10\r\n", len(key), key))
		rawCmd := fmt.Sprintf("*3\r\n$6\r\nDECRBY\r\n$%d\r\n%s\r\n$1\r\n3\r\n", len(key), key)
		got := sendRequest(t, serverAddr, rawCmd)
		want := ":7\r\n"
		if got != want {
			t.Errorf("DECRBY existing: got %q, want %q", got, want)
		}
	})

	t.Run("DECRBY_non_integer_decrement", func(t *testing.T) {
		resetKey()
		rawCmd := fmt.Sprintf("*3\r\n$6\r\nDECRBY\r\n$%d\r\n%s\r\n$3\r\nbar\r\n", len(key), key)
		got := sendRequest(t, serverAddr, rawCmd)
		if !strings.HasPrefix(got, "-ERR value is not an integer or out of range") {
			t.Errorf("DECRBY non-integer decrement: got %q, want error", got)
		}
	})
}


func TestIncrCommand(t *testing.T) {
	s := startTestServer(t)
	defer s.stop(t)
	serverAddr := "127.0.0.1:" + s.port

	key := "incrkey"

	resetKey := func() {
		delCmd := fmt.Sprintf("*2\r\n$3\r\nDEL\r\n$%d\r\n%s\r\n", len(key), key)
		sendRequest(t, serverAddr, delCmd)
	}

	t.Run("INCR_non_existing_key", func(t *testing.T) {
		resetKey()
		rawCmd := fmt.Sprintf("*2\r\n$4\r\nINCR\r\n$%d\r\n%s\r\n", len(key), key)
		got := sendRequest(t, serverAddr, rawCmd)
		want := ":1\r\n"
		if got != want {
			t.Errorf("INCR non-existing key: got %q, want %q", got, want)
		}
	})

	t.Run("INCR_existing_integer_key", func(t *testing.T) {
		resetKey()
		// SET key 10
		setCmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$2\r\n10\r\n", len(key), key)
		sendRequest(t, serverAddr, setCmd)

		rawCmd := fmt.Sprintf("*2\r\n$4\r\nINCR\r\n$%d\r\n%s\r\n", len(key), key)
		got := sendRequest(t, serverAddr, rawCmd)
		want := ":11\r\n"
		if got != want {
			t.Errorf("INCR existing integer key: got %q, want %q", got, want)
		}
	})

	t.Run("INCR_multiple_times", func(t *testing.T) {
		resetKey()
		rawCmd := fmt.Sprintf("*2\r\n$4\r\nINCR\r\n$%d\r\n%s\r\n", len(key), key)

		got1 := sendRequest(t, serverAddr, rawCmd)
		if got1 != ":1\r\n" {
			t.Errorf("INCR multiple (1st): got %q, want :1\r\n", got1)
		}
		got2 := sendRequest(t, serverAddr, rawCmd)
		if got2 != ":2\r\n" {
			t.Errorf("INCR multiple (2nd): got %q, want :2\r\n", got2)
		}
		got3 := sendRequest(t, serverAddr, rawCmd)
		if got3 != ":3\r\n" {
			t.Errorf("INCR multiple (3rd): got %q, want :3\r\n", got3)
		}
	})

	t.Run("INCR_non_integer_string_value", func(t *testing.T) {
		resetKey()
		// SET key "hello"
		setCmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$5\r\nhello\r\n", len(key), key)
		sendRequest(t, serverAddr, setCmd)

		rawCmd := fmt.Sprintf("*2\r\n$4\r\nINCR\r\n$%d\r\n%s\r\n", len(key), key)
		got := sendRequest(t, serverAddr, rawCmd)
		wantPrefix := "-ERR value is not an integer or out of range"
		if !strings.HasPrefix(got, wantPrefix) {
			t.Errorf("INCR non-integer string: got %q, want prefix %q", got, wantPrefix)
		}
	})

	t.Run("INCR_clears_expiry", func(t *testing.T) {
		resetKey()
		// SET key 5
		setCmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$1\r\n5\r\n", len(key), key)
		sendRequest(t, serverAddr, setCmd)
		// EXPIRE key 60
		expireCmd := fmt.Sprintf("*3\r\n$6\r\nEXPIRE\r\n$%d\r\n%s\r\n$2\r\n60\r\n", len(key), key)
		sendRequest(t, serverAddr, expireCmd)

		// INCR key
		incrCmd := fmt.Sprintf("*2\r\n$4\r\nINCR\r\n$%d\r\n%s\r\n", len(key), key)
		sendRequest(t, serverAddr, incrCmd) // Should be 6 now, and expiry cleared

		// TTL key
		ttlCmd := fmt.Sprintf("*2\r\n$3\r\nTTL\r\n$%d\r\n%s\r\n", len(key), key)
		got := sendRequest(t, serverAddr, ttlCmd)
		want := ":-1\r\n" // No expiry
		if got != want {
			// This test might be affected by the same issue as TestExpireTTLCommands/SET_clears_expiry
			t.Logf("INCR_clears_expiry: TTL got %q, want %q. This might indicate the known SET/expiry issue.", got, want)
			if got != want {
				t.Errorf("INCR_clears_expiry: TTL got %q, want %q", got, want)
			}
		}
	})

	t.Run("INCR_wrong_number_of_arguments_none", func(t *testing.T) {
		resetKey()
		rawCmd := "*1\r\n$4\r\nINCR\r\n"
		got := sendRequest(t, serverAddr, rawCmd)
		wantPrefix := "-ERR wrong number of arguments for 'incr' command"
		if !strings.HasPrefix(got, wantPrefix) {
			t.Errorf("INCR no args: got %q, want prefix %q", got, wantPrefix)
		}
	})

	t.Run("INCR_wrong_number_of_arguments_too_many", func(t *testing.T) {
		resetKey()
		rawCmd := fmt.Sprintf("*3\r\n$4\r\nINCR\r\n$%d\r\n%s\r\n$3\r\nfoo\r\n", len(key), key)
		got := sendRequest(t, serverAddr, rawCmd)
		wantPrefix := "-ERR wrong number of arguments for 'incr' command"
		if !strings.HasPrefix(got, wantPrefix) {
			t.Errorf("INCR too many args: got %q, want prefix %q", got, wantPrefix)
		}
	})
}
func TestEchoCommand(t *testing.T) {
	s := startTestServer(t)
	defer s.stop(t)

	serverAddr := "127.0.0.1:" + s.port

	tests := []struct {
		name       string
		message    string
		rawCommand string
		want       string
		wantErr    bool
		errPrefix  string
	}{
		{
			name:       "ECHO hello world",
			message:    "hello world",
			rawCommand: "*2\r\n$4\r\nECHO\r\n$11\r\nhello world\r\n",
			want:       "$11\r\nhello world\r\n",
			wantErr:    false,
		},
		{
			name:       "ECHO empty string",
			message:    "",
			rawCommand: "*2\r\n$4\r\nECHO\r\n$0\r\n\r\n",
			want:       "$0\r\n\r\n",
			wantErr:    false,
		},
		{
			name:       "ECHO no arguments",
			rawCommand: "*1\r\n$4\r\nECHO\r\n",
			wantErr:    true,
			errPrefix:  "-ERR wrong number of arguments for 'echo' command",
		},
		{
			name:       "ECHO too many arguments",
			rawCommand: "*3\r\n$4\r\nECHO\r\n$4\r\narg1\r\n$4\r\narg2\r\n", // Corrected lengths for "arg1" and "arg2"
			wantErr:    true,
			errPrefix:  "-ERR wrong number of arguments for 'echo' command",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sendRequest(t, serverAddr, tt.rawCommand)
			if tt.wantErr {
				if !strings.HasPrefix(got, tt.errPrefix) {
					t.Errorf("ECHO command error: got %q, want prefix %q", got, tt.errPrefix)
				}
				if !strings.HasSuffix(got, "\r\n") {
                    t.Errorf("ECHO command error response %q missing CRLF suffix", got)
                }
			} else {
				if got != tt.want {
					t.Errorf("ECHO command: got %q, want %q", got, tt.want)
				}
			}
		})
	}
}

func TestExpireTTLCommands(t *testing.T) {
	s := startTestServer(t)
	defer s.stop(t)
	serverAddr := "127.0.0.1:" + s.port

	// Helper to reset server state for tests
	resetKey := func(key string) {
		delCmd := fmt.Sprintf("*2\r\n$3\r\nDEL\r\n$%d\r\n%s\r\n", len(key), key)
		sendRequest(t, serverAddr, delCmd)
	}

	// Test EXPIRE on an existing key
	t.Run("EXPIRE_existing_key", func(t *testing.T) {
		key := "expkey1"
		resetKey(key)
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$5\r\nvalue\r\n", len(key), key))

		got := sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$6\r\nEXPIRE\r\n$%d\r\n%s\r\n$2\r\n10\r\n", len(key), key))
		want := ":1\r\n"
		if got != want {
			t.Errorf("EXPIRE existing_key: got %q, want %q", got, want)
		}

		got = sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$3\r\nTTL\r\n$%d\r\n%s\r\n", len(key), key))
		if !strings.HasPrefix(got, ":") || strings.TrimSpace(got[1:]) == "-1" || strings.TrimSpace(got[1:]) == "-2" {
			t.Errorf("TTL after EXPIRE: got %q, want positive TTL", got)
		}
	})

	t.Run("EXPIRE_non_existing_key", func(t *testing.T) {
		key := "expkey2"
		resetKey(key)
		got := sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$6\r\nEXPIRE\r\n$%d\r\n%s\r\n$2\r\n10\r\n", len(key), key))
		want := ":0\r\n"
		if got != want {
			t.Errorf("EXPIRE non_existing_key: got %q, want %q", got, want)
		}
		got = sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$3\r\nTTL\r\n$%d\r\n%s\r\n", len(key), key))
		want = ":-2\r\n"
		if got != want {
			t.Errorf("TTL for non_existing_key (after EXPIRE attempt): got %q, want %q", got, want)
		}
	})

	t.Run("EXPIRE_key_with_negative_seconds", func(t *testing.T) {
		key := "expkey3"
		resetKey(key)
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$5\r\nvalue\r\n", len(key), key))

		got := sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$6\r\nEXPIRE\r\n$%d\r\n%s\r\n$2\r\n-5\r\n", len(key), key))
		want := ":1\r\n" // Key is deleted, so EXPIRE returns 1
		if got != want {
			t.Errorf("EXPIRE with negative seconds: got %q, want %q", got, want)
		}

		// Key should be deleted
		got = sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key))
		want = "$-1\r\n" // Null bulk string
		if got != want {
			t.Errorf("GET after EXPIRE with negative seconds: got %q, want %q", got, want)
		}
		got = sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$3\r\nTTL\r\n$%d\r\n%s\r\n", len(key), key))
		want = ":-2\r\n" // Key does not exist
		if got != want {
			t.Errorf("TTL after EXPIRE with negative seconds: got %q, want %q", got, want)
		}
	})

	t.Run("TTL_key_with_no_expiry", func(t *testing.T) {
		key := "expkey4"
		resetKey(key)
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$5\r\nvalue\r\n", len(key), key))
		got := sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$3\r\nTTL\r\n$%d\r\n%s\r\n", len(key), key))
		want := ":-1\r\n"
		if got != want {
			t.Errorf("TTL key_with_no_expiry: got %q, want %q", got, want)
		}
	})

	t.Run("TTL_passes_GET_and_TTL_reflect_expiry", func(t *testing.T) {
		key := "expkey5"
		resetKey(key)
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$5\r\nvalue\r\n", len(key), key))
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$6\r\nEXPIRE\r\n$%d\r\n%s\r\n$1\r\n1\r\n", len(key), key)) // Expire in 1 second

		// Check TTL is positive initially
		gotInitialTTL := sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$3\r\nTTL\r\n$%d\r\n%s\r\n", len(key), key))
		if gotInitialTTL != ":1\r\n" && gotInitialTTL != ":0\r\n" { // Can be 0 if check is very fast after 1s
			// Allow for slight timing variance, TTL could be 1 or 0 if checked immediately.
			// A more robust test might check for > -1. Here we expect 1 or 0 if sleep is > 1s.
			// For this test, we just need to ensure it's not -1 or -2 yet.
			if !strings.HasPrefix(gotInitialTTL, ":") || strings.Contains(gotInitialTTL, "-") {
                 t.Errorf("Initial TTL for expkey5: got %q, want positive value like :1 or :0", gotInitialTTL)
            }
		}

		time.Sleep(1500 * time.Millisecond) // Wait for key to expire (added buffer)

		got := sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key))
		want := "$-1\r\n" // Null bulk string
		if got != want {
			t.Errorf("GET after TTL passed: got %q, want %q", got, want)
		}

		got = sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$3\r\nTTL\r\n$%d\r\n%s\r\n", len(key), key))
		want = ":-2\r\n" // Key should not exist
		if got != want {
			t.Errorf("TTL after TTL passed: got %q, want %q", got, want)
		}
	})

	t.Run("SET_clears_expiry", func(t *testing.T) {
		key := "expkey6"
		resetKey(key)
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$5\r\nvalue\r\n", len(key), key))
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$6\r\nEXPIRE\r\n$%d\r\n%s\r\n$2\r\n60\r\n", len(key), key))

		// Overwrite key with SET
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$7\r\nnewval\r\n", len(key), key))

		got := sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$3\r\nTTL\r\n$%d\r\n%s\r\n", len(key), key))
		want := ":-1\r\n" // No expiry
		if got != want {
			t.Errorf("TTL after SET cleared expiry: got %q, want %q", got, want)
		}
	})

	t.Run("DEL_clears_expiry", func(t *testing.T) {
		key := "expkey7"
		resetKey(key) // Ensure key doesn't exist
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$5\r\nvalue\r\n", len(key), key))
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$6\r\nEXPIRE\r\n$%d\r\n%s\r\n$2\r\n60\r\n", len(key), key))

		// Delete the key
		sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$3\r\nDEL\r\n$%d\r\n%s\r\n", len(key), key))

		got := sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$3\r\nTTL\r\n$%d\r\n%s\r\n", len(key), key))
		want := ":-2\r\n" // Key does not exist
		if got != want {
			t.Errorf("TTL after DEL cleared expiry: got %q, want %q", got, want)
		}
	})
}

func TestPexpirePttlCommands(t *testing.T) {
	s := startTestServer(t)
	defer s.stop(t)
	serverAddr := "127.0.0.1:" + s.port

	resetKey := func(key string) {
		delCmd := fmt.Sprintf("*2\r\n$3\r\nDEL\r\n$%d\r\n%s\r\n", len(key), key)
		sendRequest(t, serverAddr, delCmd)
	}

	t.Run("PEXPIRE_existing_key", func(t *testing.T) {
		key := "pexpkey1"
		resetKey(key)
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$5\r\nvalue\r\n", len(key), key))

		// Corrected: "10000" is 5 chars, so $5
		got := sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$7\r\nPEXPIRE\r\n$%d\r\n%s\r\n$5\r\n10000\r\n", len(key), key))
		want := ":1\r\n"
		if got != want {
			t.Errorf("PEXPIRE existing_key: got %q, want %q", got, want)
		}

		got = sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$4\r\nPTTL\r\n$%d\r\n%s\r\n", len(key), key))
		if !strings.HasPrefix(got, ":") || strings.HasPrefix(got, ":-") { // Check it's a positive number
			pttlVal, _ := strconv.ParseInt(strings.TrimSpace(got[1:]), 10, 64)
			if pttlVal <= 0 || pttlVal > 10000 {
				t.Errorf("PTTL after PEXPIRE: got %q, want positive PTTL <= 10000", got)
			}
		}
	})

	t.Run("PEXPIRE_non_existing_key", func(t *testing.T) {
		key := "pexpkey2"
		resetKey(key)
		// Corrected: "10000" is 5 chars, so $5
		got := sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$7\r\nPEXPIRE\r\n$%d\r\n%s\r\n$5\r\n10000\r\n", len(key), key))
		want := ":0\r\n"
		if got != want {
			t.Errorf("PEXPIRE non_existing_key: got %q, want %q", got, want)
		}
		got = sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$4\r\nPTTL\r\n$%d\r\n%s\r\n", len(key), key))
		want = ":-2\r\n"
		if got != want {
			t.Errorf("PTTL for non_existing_key (after PEXPIRE attempt): got %q, want %q", got, want)
		}
	})

	t.Run("PEXPIRE_key_with_negative_milliseconds", func(t *testing.T) {
		key := "pexpkey3"
		resetKey(key)
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$5\r\nvalue\r\n", len(key), key))

		// Corrected: "-500" is 4 chars, so $4
		got := sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$7\r\nPEXPIRE\r\n$%d\r\n%s\r\n$4\r\n-500\r\n", len(key), key))
		want := ":1\r\n" // Key is deleted
		if got != want {
			t.Errorf("PEXPIRE with negative ms: got %q, want %q", got, want)
		}
		got = sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$4\r\nPTTL\r\n$%d\r\n%s\r\n", len(key), key))
		want = ":-2\r\n" // Key does not exist
		if got != want {
			t.Errorf("PTTL after PEXPIRE with negative ms: got %q, want %q", got, want)
		}
	})

	t.Run("PTTL_key_with_no_expiry", func(t *testing.T) {
		key := "pexpkey4"
		resetKey(key)
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$5\r\nvalue\r\n", len(key), key))
		got := sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$4\r\nPTTL\r\n$%d\r\n%s\r\n", len(key), key))
		want := ":-1\r\n"
		if got != want {
			t.Errorf("PTTL key_with_no_expiry: got %q, want %q", got, want)
		}
	})

	t.Run("PTTL_passes_GET_and_PTTL_reflect_expiry", func(t *testing.T) {
		key := "pexpkey5"
		resetKey(key)
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$5\r\nvalue\r\n", len(key), key))
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$7\r\nPEXPIRE\r\n$%d\r\n%s\r\n$3\r\n100\r\n", len(key), key)) // Expire in 100 ms

		time.Sleep(150 * time.Millisecond) // Wait for key to expire

		got := sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key))
		want := "$-1\r\n"
		if got != want {
			t.Errorf("GET after PTTL passed: got %q, want %q", got, want)
		}
		got = sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$4\r\nPTTL\r\n$%d\r\n%s\r\n", len(key), key))
		want = ":-2\r\n"
		if got != want {
			t.Errorf("PTTL after PTTL passed: got %q, want %q", got, want)
		}
	})

	t.Run("EXPIRE_then_PTTL", func(t *testing.T) {
		key := "exp_pttl_key"
		resetKey(key)
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$1\r\nv\r\n", len(key), key))
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$6\r\nEXPIRE\r\n$%d\r\n%s\r\n$1\r\n5\r\n", len(key), key)) // 5 seconds

		got := sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$4\r\nPTTL\r\n$%d\r\n%s\r\n", len(key), key))
		if !strings.HasPrefix(got, ":") {
			t.Fatalf("PTTL response %q not an integer", got)
		}
		pttlVal, err := strconv.ParseInt(strings.TrimSpace(got[1:]), 10, 64)
		if err != nil {
			t.Fatalf("Failed to parse PTTL response %q: %v", got, err)
		}
		if !(pttlVal > 4000 && pttlVal <= 5000) { // Between 4 and 5 seconds (in ms)
			t.Errorf("PTTL after EXPIRE 5: got %dms, want between 4001-5000ms", pttlVal)
		}
	})

	t.Run("PEXPIRE_then_TTL", func(t *testing.T) {
		key := "pexp_ttl_key"
		resetKey(key)
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$1\r\nv\r\n", len(key), key))
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$7\r\nPEXPIRE\r\n$%d\r\n%s\r\n$4\r\n3000\r\n", len(key), key)) // 3000 ms

		got := sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$3\r\nTTL\r\n$%d\r\n%s\r\n", len(key), key))
		// Expect TTL to be 2 or 3 (rounding up from 2.xxx)
		if got != ":3\r\n" && got != ":2\r\n" {
			t.Errorf("TTL after PEXPIRE 3000ms: got %q, want :3\\r\\n or :2\\r\\n", got)
		}
	})

	// SET_clears_expiry_for_PTTL: This test relies on the SET command correctly clearing expiries.
	// If the original TestExpireTTLCommands/SET_clears_expiry is failing due to environment, this might too.
	t.Run("SET_clears_expiry_for_PTTL", func(t *testing.T) {
		key := "pexpkey_set_clear"
		resetKey(key)
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$1\r\nv\r\n", len(key), key))
		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$7\r\nPEXPIRE\r\n$%d\r\n%s\r\n$5\r\n60000\r\n", len(key), key)) // 60 seconds in ms

		sendRequest(t, serverAddr, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$2\r\nv2\r\n", len(key), key)) // Overwrite

		got := sendRequest(t, serverAddr, fmt.Sprintf("*2\r\n$4\r\nPTTL\r\n$%d\r\n%s\r\n", len(key), key))
		want := ":-1\r\n" // No expiry
		if got != want {
			t.Errorf("PTTL after SET cleared PEXPIRE: got %q, want %q", got, want)
		}
	})

}


func TestGetSetCommands(t *testing.T) {
	s := startTestServer(t)
	defer s.stop(t)
	serverAddr := "127.0.0.1:" + s.port

	// Helper to reset server state for GET/SET tests (clears specific keys)
	resetKeys := func(keys ...string) {
		if len(keys) == 0 {
			return
		}
		args := strings.Join(keys, "\r\n$")
		if len(keys) > 0 {
			args = "$" + args
		}
		// Construct DEL command: * (numkeys+1) \r\n $3 \r\n DEL \r\n $len(key1) \r\n key1 ...
		delCmd := fmt.Sprintf("*%d\r\n$3\r\nDEL\r\n", len(keys)+1)
		for _, k := range keys {
			delCmd += fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)
		}
		sendRequest(t, serverAddr, delCmd) // Response is number of keys deleted, ignore for reset
	}

	// --- GET Tests ---
	t.Run("GET_existing_key", func(t *testing.T) {
		resetKeys("mygetkey")
		sendRequest(t, serverAddr, "*3\r\n$3\r\nSET\r\n$8\r\nmygetkey\r\n$5\r\nhello\r\n") // Set value
		got := sendRequest(t, serverAddr, "*2\r\n$3\r\nGET\r\n$8\r\nmygetkey\r\n")
		want := "$5\r\nhello\r\n"
		if got != want {
			t.Errorf("GET existing_key: got %q, want %q", got, want)
		}
	})

	t.Run("GET_non_existent_key", func(t *testing.T) {
		resetKeys("nonexistentkey")
		got := sendRequest(t, serverAddr, "*2\r\n$3\r\nGET\r\n$14\r\nnonexistentkey\r\n") // Corrected length: $14
		want := "$-1\r\n" // Null Bulk String
		if got != want {
			t.Errorf("GET non_existent_key: got %q, want %q", got, want)
		}
	})

	// --- SET Tests (no options) ---
	t.Run("SET_new_key", func(t *testing.T) {
		resetKeys("newkey")
		got := sendRequest(t, serverAddr, "*3\r\n$3\r\nSET\r\n$6\r\nnewkey\r\n$5\r\nworld\r\n")
		want := "+OK\r\n"
		if got != want {
			t.Errorf("SET new_key: got %q, want %q", got, want)
		}
		// Verify value
		val := sendRequest(t, serverAddr, "*2\r\n$3\r\nGET\r\n$6\r\nnewkey\r\n")
		if val != "$5\r\nworld\r\n" {
			t.Errorf("SET new_key verification: GET got %q, want $5\r\nworld\r\n", val)
		}
	})

	t.Run("SET_overwrite_key", func(t *testing.T) {
		resetKeys("overwritekey")
		sendRequest(t, serverAddr, "*3\r\n$3\r\nSET\r\n$12\r\noverwritekey\r\n$7\r\ninitial\r\n")
		got := sendRequest(t, serverAddr, "*3\r\n$3\r\nSET\r\n$12\r\noverwritekey\r\n$6\r\nnewval\r\n")
		want := "+OK\r\n"
		if got != want {
			t.Errorf("SET overwrite_key: got %q, want %q", got, want)
		}
		val := sendRequest(t, serverAddr, "*2\r\n$3\r\nGET\r\n$12\r\noverwritekey\r\n")
		if val != "$6\r\nnewval\r\n" {
			t.Errorf("SET overwrite_key verification: GET got %q, want $6\r\nnewval\r\n", val)
		}
	})

	// --- SET with NX Tests ---
	t.Run("SET_NX_key_does_not_exist", func(t *testing.T) {
		resetKeys("nxkey1")
		got := sendRequest(t, serverAddr, "*4\r\n$3\r\nSET\r\n$6\r\nnxkey1\r\n$5\r\nvalue\r\n$2\r\nNX\r\n")
		want := "+OK\r\n"
		if got != want {
			t.Errorf("SET NX (key doesn't exist): got %q, want %q", got, want)
		}
	})

	t.Run("SET_NX_key_exists", func(t *testing.T) {
		resetKeys("nxkey2")
		sendRequest(t, serverAddr, "*3\r\n$3\r\nSET\r\n$6\r\nnxkey2\r\n$5\r\nvalue\r\n") // Pre-set key
		got := sendRequest(t, serverAddr, "*4\r\n$3\r\nSET\r\n$6\r\nnxkey2\r\n$8\r\nnewvalue\r\n$2\r\nNX\r\n") // Corrected length: $8 for newvalue
		want := "$-1\r\n" // Nil Bulk String
		if got != want {
			t.Errorf("SET NX (key exists): got %q, want %q", got, want)
		}
	})

	// --- SET with XX Tests ---
	t.Run("SET_XX_key_exists", func(t *testing.T) {
		resetKeys("xxkey1")
		sendRequest(t, serverAddr, "*3\r\n$3\r\nSET\r\n$6\r\nxxkey1\r\n$5\r\nvalue\r\n")
		got := sendRequest(t, serverAddr, "*4\r\n$3\r\nSET\r\n$6\r\nxxkey1\r\n$8\r\nnewvalue\r\n$2\r\nXX\r\n") // Corrected length: $8 for newvalue
		want := "+OK\r\n"
		if got != want {
			t.Errorf("SET XX (key exists): got %q, want %q", got, want)
		}
	})

	t.Run("SET_XX_key_does_not_exist", func(t *testing.T) {
		resetKeys("xxkey2")
		got := sendRequest(t, serverAddr, "*4\r\n$3\r\nSET\r\n$6\r\nxxkey2\r\n$5\r\nvalue\r\n$2\r\nXX\r\n")
		want := "$-1\r\n" // Nil Bulk String
		if got != want {
			t.Errorf("SET XX (key doesn't exist): got %q, want %q", got, want)
		}
	})

	// --- SET with GET option Tests ---
	t.Run("SET_GET_key_does_not_exist", func(t *testing.T) {
		resetKeys("getkey1")
		got := sendRequest(t, serverAddr, "*4\r\n$3\r\nSET\r\n$7\r\ngetkey1\r\n$5\r\nvalue\r\n$3\r\nGET\r\n")
		want := "$-1\r\n" // Nil Bulk String (old value was nil)
		if got != want {
			t.Errorf("SET GET (key doesn't exist): got %q, want %q", got, want)
		}
		val := sendRequest(t, serverAddr, "*2\r\n$3\r\nGET\r\n$7\r\ngetkey1\r\n")
		if val != "$5\r\nvalue\r\n" {
			t.Errorf("SET GET (key doesn't exist) verification: GET got %q", val)
		}
	})

	t.Run("SET_GET_key_exists", func(t *testing.T) {
		resetKeys("getkey2")
		sendRequest(t, serverAddr, "*3\r\n$3\r\nSET\r\n$7\r\ngetkey2\r\n$8\r\noldvalue\r\n")
		got := sendRequest(t, serverAddr, "*4\r\n$3\r\nSET\r\n$7\r\ngetkey2\r\n$8\r\nnewvalue\r\n$3\r\nGET\r\n")
		want := "$8\r\noldvalue\r\n"
		if got != want {
			t.Errorf("SET GET (key exists): got %q, want %q", got, want)
		}
		val := sendRequest(t, serverAddr, "*2\r\n$3\r\nGET\r\n$7\r\ngetkey2\r\n")
		if val != "$8\r\nnewvalue\r\n" {
			t.Errorf("SET GET (key exists) verification: GET got %q", val)
		}
	})

	// --- SET with GET and NX/XX Tests ---
	t.Run("SET_NX_GET_key_does_not_exist", func(t *testing.T) {
		resetKeys("nxgetkey1")
		got := sendRequest(t, serverAddr, "*5\r\n$3\r\nSET\r\n$9\r\nnxgetkey1\r\n$5\r\nvalue\r\n$2\r\nNX\r\n$3\r\nGET\r\n")
		want := "$-1\r\n" // Nil Bulk String (old value was nil because key was just set)
		if got != want {
			t.Errorf("SET NX GET (key doesn't exist): got %q, want %q", got, want)
		}
	})

	t.Run("SET_NX_GET_key_exists", func(t *testing.T) {
		resetKeys("nxgetkey2")
		sendRequest(t, serverAddr, "*3\r\n$3\r\nSET\r\n$9\r\nnxgetkey2\r\n$5\r\nvalue\r\n")
		got := sendRequest(t, serverAddr, "*5\r\n$3\r\nSET\r\n$9\r\nnxgetkey2\r\n$8\r\nnewvalue\r\n$2\r\nNX\r\n$3\r\nGET\r\n") // Corrected length: $8 for newvalue
		want := "$-1\r\n" // Nil Bulk String (SET failed due to NX)
		if got != want {
			t.Errorf("SET NX GET (key exists): got %q, want %q", got, want)
		}
		val := sendRequest(t, serverAddr, "*2\r\n$3\r\nGET\r\n$9\r\nnxgetkey2\r\n") // Verify value not changed
		if val != "$5\r\nvalue\r\n" {
			t.Errorf("SET NX GET (key exists) verification: GET got %q", val)
		}
	})

	t.Run("SET_XX_GET_key_exists", func(t *testing.T) {
		resetKeys("xxgetkey1")
		sendRequest(t, serverAddr, "*3\r\n$3\r\nSET\r\n$9\r\nxxgetkey1\r\n$8\r\noldvalue\r\n")
		got := sendRequest(t, serverAddr, "*5\r\n$3\r\nSET\r\n$9\r\nxxgetkey1\r\n$8\r\nnewvalue\r\n$2\r\nXX\r\n$3\r\nGET\r\n")
		want := "$8\r\noldvalue\r\n"
		if got != want {
			t.Errorf("SET XX GET (key exists): got %q, want %q", got, want)
		}
	})

	t.Run("SET_XX_GET_key_does_not_exist", func(t *testing.T) {
		resetKeys("xxgetkey2")
		got := sendRequest(t, serverAddr, "*5\r\n$3\r\nSET\r\n$9\r\nxxgetkey2\r\n$5\r\nvalue\r\n$2\r\nXX\r\n$3\r\nGET\r\n")
		want := "$-1\r\n" // Nil Bulk String (SET failed due to XX)
		if got != want {
			t.Errorf("SET XX GET (key doesn't exist): got %q, want %q", got, want)
		}
	})

	t.Run("SET_NX_XX_mutually_exclusive", func(t *testing.T) {
		resetKeys("nx_xx_key")
		rawCmd := "*5\r\n$3\r\nSET\r\n$9\r\nnx_xx_key\r\n$5\r\nvalue\r\n$2\r\nNX\r\n$2\r\nXX\r\n"
		got := sendRequest(t, serverAddr, rawCmd)
		if !strings.HasPrefix(got, "-ERR SET NX and XX options are mutually exclusive") {
			t.Errorf("SET NX XX: got %q, want error about mutual exclusivity", got)
		}
	})
}
