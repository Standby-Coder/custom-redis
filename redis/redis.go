package redis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

func Connect() {
	fmt.Println("Connected to Redis.")
}

// ParseCommand parses a raw request and returns the command name, arguments, and an error.
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
		if _, err := reader.Read(data); err != nil {
			return "", nil, errors.New("ERR failed to read bulk string data")
		}
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

// SerializeResponse serializes a response into the Redis protocol format.
func SerializeResponse(response interface{}) []byte {
	switch v := response.(type) {
	case string:
		return []byte(fmt.Sprintf("+%s\r\n", v))
	case error:
		return []byte(fmt.Sprintf("-%s\r\n", v.Error()))
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return []byte(fmt.Sprintf(":%d\r\n", v))
	case []byte: // Used for bulk strings
		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
	case nil:
		return []byte("$-1\r\n") // Null bulk string
	default:
		return []byte("-ERR unhandled response type\r\n")
	}
}
