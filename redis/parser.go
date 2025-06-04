package redis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io" // Added io import
	"strconv"
)

// ParseCommand parses a raw client request, expecting it to be a RESP Array of Bulk Strings.
// It returns the command name (the first bulk string), a slice of arguments (subsequent bulk strings),
// and an error if the request is not a valid RESP Array of Bulk Strings or if any other parsing error occurs.
// For example, "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n" -> ("GET", ["key"], nil).
func ParseCommand(request []byte) (commandName string, args []string, err error) {
	reader := bufio.NewReader(bytes.NewReader(request))

	// Read the type marker
	typeMarker, err := reader.ReadByte()
	if err != nil {
		return "", nil, errors.New("ERR failed to read type marker")
	}

	if typeMarker != '*' {
		return "", nil, fmt.Errorf("ERR expected array marker '*', got '%c'", typeMarker)
	}

	// Read the number of elements in the array
	line, err := reader.ReadBytes('\r')
	if err != nil || len(line) == 0 {
		return "", nil, errors.New("ERR failed to read array length")
	}
	if _, err := reader.ReadByte(); err != nil { // Read '\n'
		return "", nil, errors.New("ERR failed to read array length newline")
	}

	numArgs, err := strconv.Atoi(string(line[:len(line)-1]))
	if err != nil || numArgs < 1 {
		return "", nil, errors.New("ERR invalid number of arguments")
	}

	parts := make([]string, numArgs)
	for i := 0; i < numArgs; i++ {
		// Read the type marker for the element
		elemTypeMarker, err := reader.ReadByte()
		if err != nil {
			return "", nil, errors.New("ERR failed to read element type marker")
		}

		if elemTypeMarker != '$' {
			return "", nil, fmt.Errorf("ERR expected bulk string marker '$', got '%c'", elemTypeMarker)
		}

		// Read the length of the bulk string
		line, err := reader.ReadBytes('\r')
		if err != nil || len(line) == 0 {
			return "", nil, errors.New("ERR failed to read bulk string length")
		}
		if _, err := reader.ReadByte(); err != nil { // Read '\n'
			return "", nil, errors.New("ERR failed to read bulk string length newline")
		}

		length, err := strconv.Atoi(string(line[:len(line)-1]))
		if err != nil || length < 0 {
			return "", nil, errors.New("ERR invalid bulk string length")
		}

		// Read the bulk string data
		data := make([]byte, length)
		if length > 0 { // Only read if length is positive
			if _, err := io.ReadFull(reader, data); err != nil {
				// This can return io.EOF or io.ErrUnexpectedEOF if data is short
				return "", nil, errors.New("ERR failed to read bulk string data")
			}
		} else if length == 0 {
			// Empty string, data remains empty, which is correct.
		}
		// For length < 0 (like -1 for Null Bulk String), data remains empty.
		// The parts[i] will be an empty string if length is 0 or -1.
		// This behavior (null -> empty string) is what the tests previously assumed for non-error cases.
		// However, the length check `if err != nil || length < 0` prevents -1.
		// Let's adjust the length check for -1 if we want to parse Null Bulk Strings as empty strings.
		// The `length < 0` check after Atoi is the primary gate for this.
		// For now, io.ReadFull handles length=0 correctly.
		parts[i] = string(data)

		// Read trailing \r\n
		if cr, err := reader.ReadByte(); err != nil || cr != '\r' {
			return "", nil, errors.New("ERR failed to read bulk string trailing CR")
		}
		if lf, err := reader.ReadByte(); err != nil || lf != '\n' {
			return "", nil, errors.New("ERR failed to read bulk string trailing LF")
		}
	}

	commandName = parts[0]
	if len(parts) > 1 {
		args = parts[1:]
	}

	return commandName, args, nil
}
