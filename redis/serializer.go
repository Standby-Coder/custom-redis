package redis

import (
	"fmt"
	// "strconv" // Not strictly needed for current SerializeResponse but good for general serialization
	// "errors" // Needed if we want to handle error types more specifically, though current handles generic error
)

// SerializeResponse converts a Go data type into its RESP (REdis Serialization Protocol) byte representation.
// It handles common types like strings (as RESP Simple Strings), errors, integers,
// byte slices (as RESP Bulk Strings), nil (as RESP Null Bulk String), and slices of interface{} (as RESP Arrays).
// Unknown types result in a RESP Error indicating an unhandled response type.
func SerializeResponse(response interface{}) []byte {
	switch v := response.(type) {
	case string: // Serializes as a RESP Simple String
		return []byte(fmt.Sprintf("+%s\r\n", v))
	case error:
		return []byte(fmt.Sprintf("-%s\r\n", v.Error()))
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64: // Serializes as a RESP Integer
		// Sprintf %d works for all standard integer types
		return []byte(fmt.Sprintf(":%d\r\n", v))
	case []byte: // Serializes as a RESP Bulk String
		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
	case nil: // Serializes as a RESP Null Bulk String by default
		return []byte("$-1\r\n")
	case []interface{}: // Serializes as a RESP Array
		// Check for nil array explicitly, which is different from empty array in some contexts (e.g. WATCH abort)
		// However, standard Redis often returns empty array *0\r\n for empty lists or sets.
		// A nil response from EXEC due to WATCH abort is specifically "*-1\r\n"
		// This case is for actual arrays of responses, e.g. from EXEC
		// A nil slice is different from an empty slice.
		// A nil slice should be serialized as "*-1\r\n" (Null Array).
		// An empty slice should be serialized as "*0\r\n".
		arr := v // v is the actual []interface{} slice
		if arr == nil {
			return []byte("*-1\r\n") // Null Array
		}
		resp := fmt.Sprintf("*%d\r\n", len(arr))
		for _, item := range arr {
			resp += string(SerializeResponse(item)) // Recursively serialize array elements
		}
		return []byte(resp)
	default:
		// Consider how to handle unknown types. Returning an error to the client is safest.
		return []byte(fmt.Sprintf("-ERR unhandled response type %T\r\n", v))
	}
}
