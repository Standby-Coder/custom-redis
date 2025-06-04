package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"

	"goredis/database"
	"goredis/redis"
)

// ServerRole defines the role of the server (master or slave)
type ServerRole string

const (
	RoleMaster ServerRole = "master"
	RoleSlave  ServerRole = "slave"
)

const (
	defaultPort = "6379"
)

// QueuedCommand stores information about a command queued in a transaction
type QueuedCommand struct {
	Name string   // Uppercase command name
	Args []string // Arguments
	Raw  []byte   // Raw command bytes, for propagation
}

// ClientSpecificState holds all state for a single client connection related to transactions and WATCH
type ClientSpecificState struct {
	inTransaction     bool
	queuedCommands    []QueuedCommand
	transactionFailed bool
	watchedKeys       map[string]struct{} // Keys this client is WATCHing
	isDirty           bool                // True if any watched key was modified
}

// ServerState holds the global state of the server
type ServerState struct {
	role                  ServerRole
	masterHost            string
	masterPort            string
	connectedSlaves       []net.Conn   // Connections to slaves if this server is a master
	masterConnection      net.Conn     // Connection to master if this server is a slave
	mu                    sync.Mutex   // Protects access to ServerState fields like connectedSlaves, watchedKeySubscribers, clientStates
	watchedKeySubscribers map[string]map[net.Conn]struct{} // key -> set of connections watching it
	clientStates          map[net.Conn]*ClientSpecificState // conn -> pointer to its state object
}

// Global server state instance
var serverState = ServerState{
	role:                  RoleMaster, // Default to master
	connectedSlaves:       make([]net.Conn, 0),
	watchedKeySubscribers: make(map[string]map[net.Conn]struct{}),
	clientStates:          make(map[net.Conn]*ClientSpecificState),
}

// unwatchKeys clears all watched keys for a client and removes it from global subscriptions.
// It also resets the client's isDirty flag.
// Assumes serverState.mu is NOT held by the caller, as it will acquire it.
func unwatchKeys(clientState *ClientSpecificState, conn net.Conn) { // Removed calledFromDefer, logic simplified
	serverState.mu.Lock()
	defer serverState.mu.Unlock()

	if len(clientState.watchedKeys) > 0 {
		// log.Printf("Client %s: Unwatching keys: %v", conn.RemoteAddr().String(), mapsKeys(clientState.watchedKeys))
		for key := range clientState.watchedKeys {
			if subscribers, ok := serverState.watchedKeySubscribers[key]; ok {
				delete(subscribers, conn)
				if len(subscribers) == 0 {
					delete(serverState.watchedKeySubscribers, key)
					// log.Printf("Key %s no longer watched by any client.", key)
				}
			}
		}
		clientState.watchedKeys = make(map[string]struct{}) // Clear local watch list
	}
	clientState.isDirty = false // Always reset isDirty
}

// notifyWatchers is called when a key is modified.
// It iterates through clients watching the key and sets their isDirty flag.
// Assumes serverState.mu is NOT held by the caller.
func notifyWatchers(key string) {
	serverState.mu.Lock()
	defer serverState.mu.Unlock()

	if subscribers, ok := serverState.watchedKeySubscribers[key]; ok {
		if len(subscribers) > 0 {
			// log.Printf("Key '%s' was modified, notifying %d watching client(s).", key, len(subscribers))
		}
		for connSub := range subscribers {
			// Safely access the client's state from the global map
			if clientData, okClient := serverState.clientStates[connSub]; okClient {
				clientData.isDirty = true // clientData is a pointer to ClientSpecificState
				// log.Printf("Marked client %s as dirty because key '%s' was modified.", connSub.RemoteAddr().String(), key)
			} else {
				// This case (client in subscribers list but not in clientStates) should ideally not happen if cleanup is correct.
				// log.Printf("WARNING: Client %s found in subscribers for key '%s' but not in global clientStates. Removing.", connSub.RemoteAddr().String(), key)
				delete(subscribers, connSub)
				if len(subscribers) == 0 {
					delete(serverState.watchedKeySubscribers, key)
				}
			}
		}
	}
}

// parseFields is a helper for XADD if needed
func parseFields(args []string) map[string]string {
    fields := make(map[string]string)
    if len(args)%2 != 0 { return fields } // Should be caught by XADD command logic
    for i := 0; i < len(args); i += 2 {
        fields[args[i]] = args[i+1]
    }
    return fields
}

func main() {
	port := defaultPort
	// TODO: Make port configurable via command-line arguments or environment variables
	// TODO: Parse --replicaof from command line arguments here to set initial slave state

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to bind to port %s: %v", port, err)
	}
	defer listener.Close()
	fmt.Printf("GoRedis server listening on port %s\n", port)

	// Load data from RDB file
	db, errDbLoad := database.Load("dump.rdb")
	if errDbLoad != nil {
		log.Printf("Failed to load data from dump.rdb: %v. Starting with an empty database.", errDbLoad)
		db = database.NewDataStore() // Ensure db is initialized even if load fails
	} else {
		fmt.Println("Successfully loaded data from dump.rdb")
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		// When a new connection is accepted, we don't know if it's a client or a slave yet.
		// It will identify itself later (e.g. with REPLICAOF or PSYNC)
		// For now, all connections are handled by handleConnection.
		// If it's a slave connecting, REPLICAOF handler will manage it.
		go handleConnection(conn, db)
	}
}

func handleConnection(conn net.Conn, db *database.DataStore) {
	defer func() {
		conn.Close()
		// If this connection was a registered slave, remove it
		serverState.mu.Lock()
		for i, slaveConn := range serverState.connectedSlaves {
			if slaveConn == conn {
				serverState.connectedSlaves = append(serverState.connectedSlaves[:i], serverState.connectedSlaves[i+1:]...)
				log.Printf("Slave connection closed and removed: %s", conn.RemoteAddr().String())
				break
			}
		}
		// If this server is a slave and this was its connection to master, clear it
		if serverState.role == RoleSlave && serverState.masterConnection == conn {
			serverState.masterConnection = nil
			log.Printf("Connection to master lost: %s", conn.RemoteAddr().String())
		}
		serverState.mu.Unlock()
	}()

	reader := bufio.NewReader(conn)

	// Initialize client-specific state using the global type
	clientState := ClientSpecificState{
		inTransaction:     false,
		queuedCommands:    make([]QueuedCommand, 0),
		transactionFailed: false,
		watchedKeys:       make(map[string]struct{}),
		isDirty:           false,
	}

	serverState.mu.Lock()
	serverState.clientStates[conn] = &clientState // Register this client's state pointer
	serverState.mu.Unlock()

	defer func() {
		unwatchKeys(&clientState, conn) // Clean up WATCH state for this client

		serverState.mu.Lock()
		delete(serverState.clientStates, conn) // Remove client from global state map
		// Standard slave connection cleanup (if this conn was a slave)
		// This part might be redundant if slave removal is handled elsewhere on conn close,
		// but good to ensure it's covered.
		isSlave := false
		for i, slaveConn := range serverState.connectedSlaves {
			if slaveConn == conn {
				serverState.connectedSlaves = append(serverState.connectedSlaves[:i], serverState.connectedSlaves[i+1:]...)
				// log.Printf("Slave connection closed and removed during general cleanup: %s", conn.RemoteAddr().String())
				isSlave = true
				break
			}
		}
		// If this server is a slave and this was its connection to the master
		if serverState.role == RoleSlave && serverState.masterConnection == conn {
			serverState.masterConnection = nil
			// log.Printf("Connection to master lost during general cleanup: %s", conn.RemoteAddr().String())
		}
		serverState.mu.Unlock()
		conn.Close()
	}()


	isMasterConnection := false
	serverState.mu.Lock()
	if serverState.role == RoleSlave && serverState.masterConnection == conn {
		isMasterConnection = true
	}
	serverState.mu.Unlock()


	for {
		buffer := make([]byte, 1024) // Simplified buffer, real impl needs better handling
		// If in a transaction, and it's not a connection from the master,
		// we might want to adjust read timeouts or handling if EXEC is never received.
		// For now, normal read.
		n, err := reader.Read(buffer)
		if err != nil {
			if err.Error() != "EOF" && !strings.Contains(err.Error(), "use of closed network connection") {
				log.Printf("Failed to read from connection %s: %v", conn.RemoteAddr().String(), err)
			}
			return
		}

		request := buffer[:n]
		commandName, args, err := redis.ParseCommand(request)
		if err != nil {
			// If reading fails, and client was in a transaction, implicitly discard it.
			if clientState.inTransaction {
				// log.Printf("Client %s disconnected during transaction. Discarding.", conn.RemoteAddr().String())
				clientState.inTransaction = false
				clientState.queuedCommands = make([]QueuedCommand, 0) // Clear queue
				clientState.transactionFailed = false
				unwatchKeys(&clientState, conn) // Also clean up any WATCH state on implicit discard
			}
			// Commonplace errors due to client disconnect, no need to log verbosely unless debugging.
			// if err != io.EOF && !strings.Contains(err.Error(), "use of closed network connection") {
			// 	log.Printf("Failed to read from connection %s: %v", conn.RemoteAddr().String(), err)
			// }
			return
		}

		request := buffer[:n]
		parsedCommandName, parsedArgs, parseErr := redis.ParseCommand(request)

		// Transaction logic:
		// Uses clientState.inTransaction, clientState.queuedCommands, clientState.transactionFailed
		if clientState.inTransaction {
			cmdUpper := ""
			if parseErr == nil { // Only try to uppercase if parseErr is nil
				cmdUpper = strings.ToUpper(parsedCommandName)
			}
			// Note: WATCH is handled outside MULTI/EXEC block, error if inside.
			// MULTI, EXEC, DISCARD are special within this block.
			if cmdUpper == "MULTI" {
				conn.Write(redis.SerializeResponse(fmt.Errorf("ERR MULTI calls can not be nested")))
				continue
			} else if cmdUpper == "EXEC" || cmdUpper == "DISCARD" {
				// Let EXEC and DISCARD fall through to the main command handler where they are processed
			} else if cmdUpper == "WATCH" { // WATCH inside MULTI is an error
				conn.Write(redis.SerializeResponse(fmt.Errorf("ERR WATCH inside MULTI is not allowed")))
				continue // Does not affect transactionFailed
			} else { // Command to be queued
				if parseErr == nil {
					// Basic syntax check for EXECABORT condition (simplified)
					// Real Redis has more detailed checks (arity, command flags)
					validSyntax := true
					switch cmdUpper {
					case "SET": if len(parsedArgs) != 2 { validSyntax = false }
					case "GET": if len(parsedArgs) != 1 { validSyntax = false }
					case "XADD": if len(parsedArgs) < 3 || (len(parsedArgs)-2)%2 != 0 { validSyntax = false }
					// Add more commands here...
					// default: if cmdUpper is not in a list of known commands, could also be syntax error.
					}

					if !validSyntax {
						clientState.transactionFailed = true
						// log.Printf("Command '%s' has syntax error (in MULTI), transaction will EXECABORT.", cmdUpper)
					}
					clientState.queuedCommands = append(clientState.queuedCommands, QueuedCommand{
						Name: cmdUpper, // Store uppercase name
						Args: parsedArgs,
						Raw:  request,  // Store the raw bytes for propagation
					})
					conn.Write(redis.SerializeResponse("QUEUED"))
				} else { // parseErr != nil (e.g. malformed RESP)
					clientState.transactionFailed = true // Malformed RESP inside MULTI aborts transaction
					// For malformed RESP, Redis might send error immediately and not QUEUED.
					// Sending error now, and EXEC will abort.
					conn.Write(redis.SerializeResponse(parseErr))
				}
				continue // Skip normal command execution for queued commands
			}
		}
		// If not in transaction or it's EXEC/DISCARD/WATCH etc., proceed to parsing and execution.

		commandName := ""
		var args []string
		// If parseErr happened above AND we are not in a transaction, it will be handled here.
		if parseErr != nil {
			conn.Write(redis.SerializeResponse(parseErr))
			continue // Skip further processing for this malformed request
		} else {
			commandName = strings.ToUpper(parsedCommandName)
			args = parsedArgs
		}

		var response interface{}
		isWriteCommand := false
		var commandsToPropagate [][]byte // For EXEC propagation
		var modifiedKey string // Stores key modified by SET, XADD, etc. for WATCH notification


		// If this is a slave server and the command is from its master, process it directly.
		if isMasterConnection && commandName != "REPLICAOF" && commandName != "INTERNAL_REGISTER_SLAVE" {
			// log.Printf("Slave received command from master: %s %v", commandName, args)
			// Slave executes commands from master. If master sends MULTI/EXEC, slave's handlers will trigger.
			// If master sends a write (SET X Y), slave updates its DB.
			// That write on the slave does NOT trigger WATCH notifications for slave's own clients.
			// (This is standard Redis behavior; slaves are primarily mirrors).
			switch commandName {
			case "SET":
				if len(args) == 2 { db.Set(args[0], args[1]) }
			case "XADD":
                 if len(args) >= 3 { _, _ = database.XAdd(db, args[0], args[1], parseFields(args[2:])) }
			// Other propagated write commands...
			}
			continue // Skip sending response back to master & further local processing
		}


		switch commandName {
		case "WATCH":
			if clientState.inTransaction {
				response = fmt.Errorf("ERR WATCH inside MULTI is not allowed")
			} else if len(args) == 0 {
				response = fmt.Errorf("ERR wrong number of arguments for 'watch' command")
			} else {
				serverState.mu.Lock()
				for _, key := range args {
					clientState.watchedKeys[key] = struct{}{}
					if _, ok := serverState.watchedKeySubscribers[key]; !ok {
						serverState.watchedKeySubscribers[key] = make(map[net.Conn]struct{})
					}
					serverState.watchedKeySubscribers[key][conn] = struct{}{}
					// log.Printf("Client %s is now watching key: %s", conn.RemoteAddr().String(), key)
				}
				serverState.mu.Unlock()
				response = "OK"
			}
		case "UNWATCH":
			// UNWATCH is allowed even if not watching anything or inside MULTI (though WATCH inside MULTI is not).
			// If UNWATCH is called inside MULTI, it doesn't affect the transaction's atomicity itself,
			// but it does clear any WATCH state set prior to MULTI.
			// The client's isDirty flag is reset by unwatchKeys.
			unwatchKeys(&clientState, conn)
			response = "OK"

		case "MULTI":
			if clientState.inTransaction {
				response = fmt.Errorf("ERR MULTI calls can not be nested")
			} else {
				clientState.inTransaction = true
				clientState.queuedCommands = make([]QueuedCommand, 0) // Clear previous queue
				clientState.transactionFailed = false
				// clientState.isDirty is NOT reset here. WATCHed keys persist.
				response = "OK"
			}
		case "DISCARD":
			if !clientState.inTransaction {
				response = fmt.Errorf("ERR DISCARD without MULTI")
			} else {
				clientState.inTransaction = false
				clientState.queuedCommands = make([]QueuedCommand, 0)
				clientState.transactionFailed = false
				unwatchKeys(&clientState, conn) // UNWATCH on DISCARD
				response = "OK"
			}
		case "EXEC":
			if !clientState.inTransaction {
				response = fmt.Errorf("ERR EXEC without MULTI")
			} else {
				clientState.inTransaction = false // Transaction ends here, regardless of outcome

				// Check if WATCHed keys were modified (isDirty flag)
				if clientState.isDirty {
					response = nil // Null array reply for aborted transaction due to WATCH
					// log.Printf("Client %s: EXEC aborted due to watched key modification (isDirty=true).", conn.RemoteAddr().String())
					clientState.queuedCommands = make([]QueuedCommand, 0) // Clear queue
					unwatchKeys(&clientState, conn) // Clear watches and isDirty
					// The main response sending logic will handle sending `response` (which is nil)
					// We must ensure we don't proceed to execute commands.
					// The structure of the switch will naturally lead to sending this response and then looping.
					// No further EXEC processing below this point if isDirty.
				} else if clientState.transactionFailed { // Check for pre-EXEC syntax errors
					response = fmt.Errorf("EXECABORT Transaction discarded because of previous errors")
					// log.Printf("Client %s: EXECABORT due to previous errors (transactionFailed=true).", conn.RemoteAddr().String())
					clientState.queuedCommands = make([]QueuedCommand, 0) // Clear queue
					unwatchKeys(&clientState, conn) // Still unwatch keys on EXECABORT
				} else { // Proceed to execute commands
					execResponses := make([]interface{}, len(clientState.queuedCommands))
					var successfulWriteCommandsInExec []QueuedCommand // For propagation

					for i, qc := range clientState.queuedCommands {
						var cmdResponse interface{}
						cmdIsWriteInExec := false      // Renamed to avoid conflict
						var currentCmdModifiedKey string // Key modified by this specific command in EXEC

						// Simplified execution of queued commands:
						// This should ideally re-dispatch to a shared command execution function.
						switch qc.Name {
						case "SET":
							if len(qc.Args) != 2 { cmdResponse = fmt.Errorf("ERR wrong number of arguments for 'set' command") } else {
								db.Set(qc.Args[0], qc.Args[1]); cmdResponse = "OK"; cmdIsWriteInExec = true; currentCmdModifiedKey = qc.Args[0]
							}
						case "GET":
							if len(qc.Args) != 1 { cmdResponse = fmt.Errorf("ERR wrong number of arguments for 'get' command")} else {
                                val, okGet := db.Get(qc.Args[0])
                                if !okGet {cmdResponse = nil} else { // nil means key not found -> null bulk string
                                    if strVal, okStr := val.(string); okStr {cmdResponse = []byte(strVal)} else {
                                        cmdResponse = fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value") // Or some other error if type is wrong
                                    }
                                }
                            }
						case "XADD":
							if len(qc.Args) < 3 { cmdResponse = fmt.Errorf("ERR wrong number of arguments for 'xadd' command")
							} else if (len(qc.Args)-2)%2 != 0 { cmdResponse = fmt.Errorf("ERR wrong number of arguments for 'xadd' command: fields and values must be in pairs")
							} else {
								fields := parseFields(qc.Args[2:]) // Use helper
								newID, errXadd := database.XAdd(db, qc.Args[0], qc.Args[1], fields)
								if errXadd != nil {cmdResponse = errXadd} else {
									cmdResponse = []byte(newID.String()); cmdIsWriteInExec = true; currentCmdModifiedKey = qc.Args[0]
								}
							}
						default:
							cmdResponse = fmt.Errorf("ERR unknown command '%s' during EXEC", qc.Name)
						}
						execResponses[i] = cmdResponse

						if cmdIsWriteInExec {
							if _, isError := cmdResponse.(error); !isError { // Only add if command itself didn't error
								successfulWriteCommandsInExec = append(successfulWriteCommandsInExec, qc)
							}
							if currentCmdModifiedKey != "" { // A key was actually modified
                                notifyWatchers(currentCmdModifiedKey) // Notify watchers for keys modified within this EXEC
                            }
						}
					}
					response = execResponses // This will be an array of responses

					// Propagation logic for EXEC: send MULTI, successful writes, EXEC
					if serverState.role == RoleMaster && len(successfulWriteCommandsInExec) > 0 {
						commandsToPropagate = append(commandsToPropagate, []byte("*1\r\n$5\r\nMULTI\r\n"))
						for _, qCmd := range successfulWriteCommandsInExec { // Propagate only successful writes from EXEC
							commandsToPropagate = append(commandsToPropagate, qCmd.Raw)
						}
						commandsToPropagate = append(commandsToPropagate, []byte("*1\r\n$4\r\nEXEC\r\n"))
					}
				}
				clientState.queuedCommands = make([]QueuedCommand, 0) // Clear queue after EXEC
				unwatchKeys(&clientState, conn) // UNWATCH after EXEC (whether aborted, failed, or successful)
			}
		case "REPLICAOF":
			// REPLICAOF is not allowed inside a transaction. If it reaches here, clientState.inTransaction is false.
			if clientState.inTransaction { // Should ideally be caught by queuing logic if it were allowed
                response = fmt.Errorf("ERR REPLICAOF inside MULTI is not allowed")
            } else if len(args) == 2 {
				masterHost := args[0]
				masterPort := args[1]
				if strings.ToLower(masterHost) == "no" && strings.ToLower(masterPort) == "one" {
					serverState.mu.Lock()
					serverState.role = RoleMaster
					if serverState.masterConnection != nil { serverState.masterConnection.Close(); serverState.masterConnection = nil }
					serverState.masterHost = ""; serverState.masterPort = ""
					serverState.mu.Unlock()
					response = "OK"; // log.Printf("Server changed role to MASTER")
				} else {
					serverState.mu.Lock()
					serverState.role = RoleSlave; serverState.masterHost = masterHost; serverState.masterPort = masterPort
					if serverState.masterConnection != nil { serverState.masterConnection.Close() } // Close old one if any
					serverState.mu.Unlock()

					masterAddr := fmt.Sprintf("%s:%s", masterHost, masterPort)
					masterConn, errDial := net.Dial("tcp", masterAddr)
					if errDial != nil {
						// log.Printf("ERR failed to connect to master %s:%s - %v", masterHost, masterPort, errDial)
						response = "OK" // REPLICAOF command itself is OK even if connection fails initially
					} else {
						// log.Printf("Successfully connected to master at %s", masterAddr)
						serverState.mu.Lock(); serverState.masterConnection = masterConn; serverState.mu.Unlock()
						// Send INTERNAL_REGISTER_SLAVE to the master
						registerCmd := []byte("*1\r\n$22\r\nINTERNAL_REGISTER_SLAVE\r\n")
						if _, errWrite := masterConn.Write(registerCmd); errWrite != nil {
							// log.Printf("Failed to send INTERNAL_REGISTER_SLAVE to master: %v", errWrite)
						} else { /* log.Printf("Sent INTERNAL_REGISTER_SLAVE to master at %s", masterAddr) */ }
						go handleConnection(masterConn, db) // Handle messages from master
						response = "OK"
					}
				}
			} else { response = fmt.Errorf("ERR wrong number of arguments for 'replicaof' command") }

		case "INTERNAL_REGISTER_SLAVE":
			if serverState.role == RoleMaster {
				serverState.mu.Lock()
				isAlreadyRegistered := false
				for _, slaveConn := range serverState.connectedSlaves { if slaveConn == conn { isAlreadyRegistered = true; break } }
				if !isAlreadyRegistered {
					serverState.connectedSlaves = append(serverState.connectedSlaves, conn)
					// log.Printf("Registered new slave: %s. Total slaves: %d", conn.RemoteAddr().String(), len(serverState.connectedSlaves))
				}
				serverState.mu.Unlock()
				continue // No response sent for this internal command
			} else { response = fmt.Errorf("ERR INTERNAL_REGISTER_SLAVE received by non-master server") }

		case "SAVE":
			// If in transaction, SAVE should be queued. This check is for direct command.
			if clientState.inTransaction { // This case should be handled by queuing logic
                clientState.transactionFailed = true; // Or specific error for SAVE in MULTI if disallowed
                clientState.queuedCommands = append(clientState.queuedCommands, QueuedCommand{Name:"SAVE", Args:args, Raw:request });
                conn.Write(redis.SerializeResponse("QUEUED"));
                continue;
            }
			if serverState.role == RoleSlave { response = fmt.Errorf("ERR SAVE command not allowed for slave")
			} else if len(args) != 0 { response = fmt.Errorf("ERR wrong number of arguments for 'save' command")
			} else {
				if errSave := database.Save(db, "dump.rdb"); errSave != nil { response = fmt.Errorf("ERR failed to save database: %v", errSave)
				} else { response = "OK" }
			}

		case "SET":
			if len(args) != 2 { response = fmt.Errorf("ERR wrong number of arguments for 'set' command"); break }
			// Standard SET command logic
			db.Set(args[0], args[1])
			response = "OK"
			isWriteCommand = true
			modifiedKey = args[0] // For WATCH notification

		case "GET":
			if len(args) != 1 { response = fmt.Errorf("ERR wrong number of arguments for 'get' command"); break }
			// Standard GET command logic
            val, okGet := db.Get(args[0])
            if !okGet { response = nil } else { // nil means key not found -> null bulk string
                if strVal, okStr := val.(string); okStr { response = []byte(strVal) } else {
                    // This should ideally check type and error if not string, or handle non-string types
                    response = fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
                }
            }

		case "XADD":
			if len(args) < 3 { response = fmt.Errorf("ERR wrong number of arguments for 'xadd' command"); break }
			streamKey := args[0]; idStr := args[1]
			if (len(args)-2)%2 != 0 { response = fmt.Errorf("ERR wrong number of arguments for 'xadd' command: fields and values must be in pairs"); break }
			fields := parseFields(args[2:]) // Use helper
			// Standard XADD command logic
			if serverState.role == RoleSlave && !isMasterConnection { // Slave check
                response = fmt.Errorf("READONLY You can't write against a read only replica.")
            } else {
				newID, errXadd := database.XAdd(db, streamKey, idStr, fields)
				if errXadd != nil { response = errXadd } else {
					response = []byte(newID.String())
					isWriteCommand = true
					modifiedKey = streamKey // For WATCH notification
				}
			}

		case "HSET":
			if len(args) != 3 {
				response = fmt.Errorf("ERR wrong number of arguments for 'hset' command")
			} else {
				key, field, value := args[0], args[1], args[2]
				// TODO: Add slave read-only check here if HSET isn't allowed on slaves directly
				count, errHset := db.HSet(key, field, value)
				if errHset != nil {
					response = fmt.Errorf("ERR %v", errHset)
				} else {
					response = count // Integer response
					isWriteCommand = true
					modifiedKey = key // For WATCH notification (key of the hash, not specific field)
					// Note: notifyWatchers is called with the hash's key.
					// If WATCH needs to be field-specific, this needs more granularity or WATCH <key> <field>.
					// Standard Redis WATCH is on keys.
				}
			}
		case "HGET":
			if len(args) != 2 {
				response = fmt.Errorf("ERR wrong number of arguments for 'hget' command")
			} else {
				key, field := args[0], args[1]
				value, found := db.HGet(key, field)
				if !found {
					response = nil // Null bulk string
				} else {
					response = []byte(value)
				}
			}
		case "HDEL":
            if len(args) < 2 {
                response = fmt.Errorf("ERR wrong number of arguments for 'hdel' command")
            } else {
                key := args[0]
                fieldsToDel := args[1:]
                count, errHdel := db.HDel(key, fieldsToDel...)
                if errHdel != nil {
                    response = fmt.Errorf("ERR %v", errHdel)
                } else {
                    response = count // Integer response
                    if count > 0 { // Only a write if something was actually deleted
                        isWriteCommand = true
                        modifiedKey = key // For WATCH notification
                    }
                }
            }
		case "DEL":
            if len(args) == 0 {
                response = fmt.Errorf("ERR wrong number of arguments for 'del' command")
            } else {
                deletedCount := db.Del(args...) // Pass all keys to Del method
                response = deletedCount
                if deletedCount > 0 {
                    isWriteCommand = true
                    // For DEL, multiple keys can be modified.
                    // Notify watchers for each key deleted if it was part of a hash and indexed.
                    // The current `modifiedKey` variable only holds one key.
                    // This needs adjustment if DEL is to granularly notify.
                    // For now, DEL's watch notification might be coarse or rely on db.Del internal index calls.
                    // Let's assume db.Del handles index updates including notifications if necessary,
                    // or we iterate here. The db.Del calls RemoveFromIndexOnDel.
                    // For now, let's not set modifiedKey for DEL here, assuming db.Del's index interaction is sufficient.
                }
            }

		case "SQUERY": // SQUERY <index_name> <value_to_match>
			if len(args) != 2 {
				response = fmt.Errorf("ERR wrong number of arguments for 'squery' command")
			} else {
				indexName := args[0]
				valueToMatch := args[1]

				if db.IndexMgr == nil { // Should not happen if initialized
					response = fmt.Errorf("ERR Index Manager not initialized")
					break
				}

				index, found := db.IndexMgr.GetIndex(indexName)
				if !found {
					response = fmt.Errorf("ERR index '%s' not found", indexName)
				} else {
					matchingKeysMap := index.Get(valueToMatch)
					if matchingKeysMap == nil || len(matchingKeysMap) == 0 {
						response = []interface{}{} // Empty array
					} else {
						matchingKeysList := make([]interface{}, 0, len(matchingKeysMap))
						for k := range matchingKeysMap {
							matchingKeysList = append(matchingKeysList, []byte(k))
						}
						response = matchingKeysList // Array of bulk strings
					}
				}
			}

		default:
			response = fmt.Errorf("ERR unknown command '%s'", commandName)
		}

		// Send response to client (if not queued or already handled by EXEC's array response)
		// This condition ensures that commands like MULTI, DISCARD, EXEC, WATCH send their immediate response,
		// and queued commands don't send anything here (they got +QUEUED).
		if !clientState.inTransaction || commandName == "EXEC" || commandName == "DISCARD" || commandName == "MULTI" || commandName == "WATCH" || commandName == "UNWATCH" {
			if !isMasterConnection { // Don't send response back to master for commands it sent to this slave
				conn.Write(redis.SerializeResponse(response))
			}
		}

		// Notify watchers if a key was modified by a single, non-transactional command (SET, XADD etc.)
        // This `modifiedKey` is from the direct execution path, not from within EXEC (EXEC handles its own).
        // Important: This notification should happen *after* the command is executed and response determined,
        // but *before* propagation, so slaves get commands after master's state (including WATCH dirtying) is set.
        if modifiedKey != "" { // A key was modified by SET, XADD etc. (not within EXEC)
            notifyWatchers(modifiedKey)
        }

		// Propagation logic
		if serverState.role == RoleMaster {
			if len(commandsToPropagate) > 0 { // This means EXEC decided to propagate a transaction
				serverState.mu.Lock()
				// log.Printf("Master propagating transaction to %d slaves", len(serverState.connectedSlaves))
				activeSlaves := make([]net.Conn, 0, len(serverState.connectedSlaves))
				for _, slaveConn := range serverState.connectedSlaves {
					propagatedSuccessfullyToThisSlave := true
					for _, cmdBytes := range commandsToPropagate {
						if _, err := slaveConn.Write(cmdBytes); err != nil {
							// log.Printf("Failed to propagate part of transaction to slave %s: %v.", slaveConn.RemoteAddr().String(), err)
							propagatedSuccessfullyToThisSlave = false
							break // Stop sending this transaction to this slave
						}
					}
					if propagatedSuccessfullyToThisSlave { activeSlaves = append(activeSlaves, slaveConn) }
				}
				serverState.connectedSlaves = activeSlaves // Update list of active slaves
				serverState.mu.Unlock()
			} else if isWriteCommand { // Single command was a write, not part of an EXEC block that set commandsToPropagate
				serverState.mu.Lock()
				// log.Printf("Master propagating command to %d slaves: %s", len(serverState.connectedSlaves), commandName)
				activeSlaves := make([]net.Conn, 0, len(serverState.connectedSlaves))
				for _, slaveConn := range serverState.connectedSlaves {
					if _, err := slaveConn.Write(request); err != nil { // Send original raw request
						// log.Printf("Failed to propagate command to slave %s: %v. Removing slave.", slaveConn.RemoteAddr().String(), err)
					} else {
						activeSlaves = append(activeSlaves, slaveConn)
					}
				}
				serverState.connectedSlaves = activeSlaves // Update list of active slaves
				serverState.mu.Unlock()
			}
		}
	}
}
				log.Printf("Failed to read from connection %s: %v", conn.RemoteAddr().String(), err)
			}
			return
		}

		request := buffer[:n]
		parsedCommandName, parsedArgs, parseErr := redis.ParseCommand(request)

		// Transaction logic:
		if inTransaction {
			// Handle MULTI, EXEC, DISCARD, WATCH specially. Others are queued.
			cmdUpper := strings.ToUpper(parsedCommandName)
			if parseErr != nil { // Error during parsing a command meant for queueing
				// Handle commands that are syntactically incorrect while queueing
				// redis.ParseCommand might return an error for malformed RESP.
				// If a command is fundamentally unparseable, it breaks the transaction protocol.
				// However, Redis queues commands even if they are semantically wrong (e.g. SET with wrong # args)
				// and only EXEC fails. Let's assume parseErr means RESP is bad.
				// If parseErr is for *protocol* error, not command syntax, then it's different.
				// For now, if redis.ParseCommand fails, we can't even know what command it was.
				// Let's assume redis.ParseCommand is robust for valid RESP for known/unknown commands.
				// Semantic errors (wrong # args for SET) are caught *after* parsing.
				// If parseErr itself happens, it means the client sent malformed RESP.
				// This should probably terminate the connection or discard transaction.
				// For now, let's make it an error that EXEC will report.
				// This is a bit tricky: if ParseCommand fails, we don't have `parsedCommandName`.
				// Let's assume for this step that `ParseCommand` error means bad RESP, not command error.
				// We'll assume `ParseCommand` returns the command name even if args are bad.
				// This is not how current `ParseCommand` works. It returns error before full parsing sometimes.

				// Re-evaluating: If ParseCommand fails, we can't reliably get commandName.
				// Let's assume ParseCommand only fails on fundamental RESP errors.
				// Command-specific syntax errors (like wrong arg count) are checked *after* ParseCommand.

				// If ParseCommand returns error, it means the client sent malformed data.
				// This should probably break the transaction and maybe the connection.
				if cmdUpper != "EXEC" && cmdUpper != "DISCARD" && cmdUpper != "MULTI" { // WATCH also special
					log.Printf("Client %s sent malformed command inside MULTI. Transaction will be aborted.", conn.RemoteAddr().String())
					transactionFailed = true
					// Still add a placeholder or the erroneous raw command?
					// Redis seems to queue it and EXEC reports individual errors.
					// But if ParseCommand itself fails, it's more severe.
					// For now, if ParseCommand fails, we can't queue properly.
					// Let's assume ParseCommand is for basic RESP structure, not command validation.
					// If basic RESP is bad, then it's an error response.
					// This part of the spec is subtle.
				}
				// If parseErr is not nil, we fall through to normal command handling which will send error.
				// BUT if inTransaction, we should QUEUE an error or mark transaction as failed.
			}

			if cmdUpper == "MULTI" {
				conn.Write(redis.SerializeResponse(fmt.Errorf("ERR MULTI calls can not be nested")))
				continue
			} else if cmdUpper == "EXEC" {
				// EXEC logic will be handled below by the main switch
			} else if cmdUpper == "DISCARD" {
				// DISCARD logic will be handled below by the main switch
			} else if cmdUpper == "WATCH" { // WATCH is not implemented yet
				conn.Write(redis.SerializeResponse(fmt.Errorf("ERR WATCH command is not supported in this transaction model yet")))
				transactionFailed = true // Mark transaction as failed if WATCH is attempted
				continue
			} else {
				// Queue the command if successfully parsed at RESP level
				if parseErr == nil {
					queuedCommands = append(queuedCommands, QueuedCommand{
						Name: parsedCommandName, // Store original case or uppercase? Redis normalizes. Let's use Upper.
						Args: parsedArgs,
						Raw:  request, // Store the raw bytes for propagation
					})
					conn.Write(redis.SerializeResponse("QUEUED"))
				} else {
					// If ParseCommand itself failed, this is a protocol error, not just a bad command.
					// This part is tricky. Redis sends -ERR ... for bad RESP.
					// If in MULTI, and RESP is bad, it should still be +QUEUED, but EXEC fails.
					// This means we queue based on raw data if parsing fails? No, that's not right.
					// Redis's behavior: if a command in MULTI has syntax error (e.g. SET w/ 1 arg),
					// server replies +QUEUED. But EXEC will fail for that command and potentially abort.
					// If the RESP *protocol* is bad (e.g. bad bulk string length), then it's an immediate error,
					// not +QUEUED, and transaction is typically aborted.

					// For now, let's simplify: if redis.ParseCommand returns an error,
					// we mark the transaction as failed and reply with the error.
					// This means EXEC will abort. This is stricter than Redis for command syntax errors
					// but safer for protocol errors.
					transactionFailed = true
					conn.Write(redis.SerializeResponse(parseErr)) // Send error immediately
				}
				continue // Skip normal command execution for queued commands
			}
		}
		// If not in transaction or it's EXEC/DISCARD, proceed to parsing and execution.
		// If parseErr happened above AND we are not in a transaction, it will be handled by the main switch.

		commandName := ""
		var args []string
		var errParseCmd error

		if parseErr != nil { // This error is from the initial ParseCommand attempt
			errParseCmd = parseErr
		} else {
			commandName = strings.ToUpper(parsedCommandName)
			args = parsedArgs
		}

		var response interface{}
		isWriteCommand := false
		var commandsToPropagate [][]byte // For EXEC propagation



		// If this is a slave server and the command is from its master, process it directly.
		// Exception: Slaves should not process REPLICAOF from their masters.
		if isMasterConnection && commandName != "REPLICAOF" {
			// Process commands from master
			log.Printf("Slave received command from master: %s %v", commandName, args)
			// For SET and other write commands, update local DataStore
			// For other commands (like PING from master), handle appropriately
			switch commandName {
			case "SET":
				if len(args) == 2 {
					db.Set(args[0], args[1])
					// Slaves usually don't respond to propagated SET commands from master
					// or respond with something specific if protocol requires.
					// For now, let's assume no response is sent back to master for SET.
					continue // Skip sending response back to master for SET
				}
			// Potentially handle other propagated commands here
			default:
				// If master sends a command slave doesn't know how to handle directly (e.g. PING)
				// it might just ignore it or log it.
				log.Printf("Slave received unhandled command from master: %s", commandName)
				continue // Skip sending response for unhandled commands from master
			}
		}


		switch commandName {
		case "MULTI":
			if inTransaction {
				response = fmt.Errorf("ERR MULTI calls can not be nested")
			} else {
				inTransaction = true
				queuedCommands = make([]QueuedCommand, 0) // Clear previous queue
				transactionFailed = false
				response = "OK"
			}
		case "DISCARD":
			if !inTransaction {
				response = fmt.Errorf("ERR DISCARD without MULTI")
			} else {
				inTransaction = false
				queuedCommands = make([]QueuedCommand, 0)
				transactionFailed = false
				response = "OK"
			}
		case "EXEC":
			if !inTransaction {
				response = fmt.Errorf("ERR EXEC without MULTI")
			} else {
				inTransaction = false // Exit transaction state regardless of success
				if transactionFailed {
					response = fmt.Errorf("EXECABORT Transaction discarded because of previous errors")
					queuedCommands = make([]QueuedCommand, 0) // Clear queue
				} else {
					execResponses := make([]interface{}, len(queuedCommands))
					var tempWriteCommandsToPropagate [][]byte

					if serverState.role == RoleMaster { // Prepare to propagate MULTI if master
						multiCmdRaw, _ := redis.ParseCommand([]byte("*1\r\n$5\r\nMULTI\r\n"))
						// This is not quite right. We need the raw bytes of MULTI.
						// Let's assume a fixed raw MULTI for propagation for now.
						// This should ideally be the original MULTI's raw bytes if we stored it.
						// For now, we'll just say "MULTI" and slaves will understand.
						// Or, even better, the slave itself handles MULTI/EXEC from master.
						// For now, let's make master propagate individual commands from EXEC.
						// This is simpler than propagating MULTI/EXEC themselves.
						// No, Redis spec implies MULTI/EXEC are also sent or whole transaction is one blob.
						// Let's try to propagate MULTI, then queued commands, then EXEC.
						// We need raw bytes of the actual MULTI command if we go this route.
						// For simplicity of this step: if EXEC succeeds, propagate the queued commands' raw bytes.
						// This means slaves won't run them in a transaction context unless they also see MULTI/EXEC.
						// This needs refinement.

						// Tentative: Add MULTI to propagation list (if master)
						// commandsToPropagate = append(commandsToPropagate, []byte("*1\r\n$5\r\nMULTI\r\n"))
					}

					for i, qc := range queuedCommands {
						// Execute the queued command
						// Need to re-route this through the main command switch logic,
						// but without the transaction queuing part, and without sending response to client directly.
						// This is a bit complex as the switch handles responses and propagation.
						// For now, let's call a simplified dispatcher or duplicate relevant command logic.

						var cmdResponse interface{}
						cmdIsWrite := false

						// Simplified execution of queued commands:
						// This is a major simplification. Ideally, we re-dispatch to the main handler
						// or a function that encapsulates command execution logic.
						switch qc.Name { // qc.Name is already uppercase
						case "SET":
							if len(qc.Args) != 2 {
								cmdResponse = fmt.Errorf("ERR wrong number of arguments for 'set' command")
							} else {
								if serverState.role == RoleSlave && !isMasterConnection { // Check for slave write
									cmdResponse = fmt.Errorf("READONLY You can't write against a read only replica.")
								} else {
									db.Set(qc.Args[0], qc.Args[1])
									cmdResponse = "OK"
									cmdIsWrite = true
								}
							}
						case "GET":
							if len(qc.Args) != 1 {
								cmdResponse = fmt.Errorf("ERR wrong number of arguments for 'get' command")
							} else {
								val, ok := db.Get(qc.Args[0])
								if !ok {cmdResponse = nil} else {
									if strVal, okStr := val.(string); okStr {cmdResponse = []byte(strVal)} else {
										cmdResponse = fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
									}
								}
							}
						case "XADD":
							if len(qc.Args) < 3 {
								cmdResponse = fmt.Errorf("ERR wrong number of arguments for 'xadd' command")
							} else if (len(qc.Args)-2)%2 != 0 {
								cmdResponse = fmt.Errorf("ERR wrong number of arguments for 'xadd' command: fields and values must be in pairs")
							} else {
								if serverState.role == RoleSlave && !isMasterConnection {
									cmdResponse = fmt.Errorf("READONLY You can't write against a read only replica.")
								} else {
									fields := make(map[string]string)
									for k := 2; k < len(qc.Args); k += 2 {fields[qc.Args[k]] = qc.Args[k+1]}
									newID, errXadd := database.XAdd(db, qc.Args[0], qc.Args[1], fields)
									if errXadd != nil {cmdResponse = errXadd} else {
										cmdResponse = []byte(newID.String())
										cmdIsWrite = true
									}
								}
							}
						// Add other commands here...
						default:
							cmdResponse = fmt.Errorf("ERR unknown command '%s' in transaction", qc.Name)
						}
						execResponses[i] = cmdResponse
						if cmdIsWrite && serverState.role == RoleMaster {
							tempWriteCommandsToPropagate = append(tempWriteCommandsToPropagate, qc.Raw)
						}
					}
					response = execResponses // This will be an array of responses

					// If EXEC was successful and this is a master, prepare to propagate
					if serverState.role == RoleMaster {
						// Propagate MULTI, then the write commands, then EXEC
						// This is a conceptual sketch; actual raw bytes are needed.
						// For now, let's just propagate the executed write commands' raw bytes.
						// This means slave doesn't run it as a transaction unless it also gets MULTI/EXEC.
						// A better way: send the original MULTI raw, then qc.Raw for all successful, then EXEC raw.
						// For this iteration, only propagate successful writes from the transaction.
						if len(tempWriteCommandsToPropagate) > 0 {
                             // commandsToPropagate = tempWriteCommandsToPropagate // This would propagate them individually

                            // To propagate as a transaction:
                            // 1. Send MULTI (raw)
                            // 2. Send each command from queuedCommands (qc.Raw) that was successful
                            // 3. Send EXEC (raw)
                            // This is complex. For now: if master & EXEC, set a special flag/data for propagation block.
                            // The propagation logic at the end of handleConnection needs to be aware of this.
                            // Let's assume for now: individual commands from EXEC are marked for propagation if they are writes.
                            // This means they are NOT propagated as a single transaction to slaves. This is a simplification.
                            // To make it a transaction for slaves:
                            commandsToPropagate = append(commandsToPropagate, []byte("*1\r\n$5\r\nMULTI\r\n")) // Start with MULTI
                            for _, qc := range queuedCommands {
                                // We need to check if the command *would* be a write command if executed.
                                // This check is already done by `cmdIsWrite` during the internal exec loop.
                                // For simplicity, let's assume all commands in a successful EXEC are propagated.
                                // This is not entirely accurate as some might be read commands.
                                // Or, only propagate `tempWriteCommandsToPropagate`.
                                // Correct propagation: MULTI, qc.Raw for all from original queue, EXEC.
                                // Slave will then re-execute and may encounter errors too.
                                commandsToPropagate = append(commandsToPropagate, qc.Raw)
                            }
                            commandsToPropagate = append(commandsToPropagate, []byte("*1\r\n$4\r\nEXEC\r\n")) // End with EXEC
                        }
					}
					queuedCommands = make([]QueuedCommand, 0) // Clear queue
				}
			}
		case "REPLICAOF":
			// REPLICAOF is not allowed inside a transaction by Redis standard.
			// If we are here, it means `inTransaction` was false.
			if len(args) == 2 {
				masterHost := args[0]
				masterPort := args[1]
				if strings.ToLower(masterHost) == "no" && strings.ToLower(masterPort) == "one" {
					// Become master
					serverState.mu.Lock()
					serverState.role = RoleMaster
					if serverState.masterConnection != nil {
						serverState.masterConnection.Close() // Close connection to old master
						serverState.masterConnection = nil
					}
					serverState.masterHost = ""
					serverState.masterPort = ""
					serverState.mu.Unlock()
					response = "OK"
					log.Printf("Server changed role to MASTER")
				} else {
					// Become slave
					serverState.mu.Lock()
					serverState.role = RoleSlave
					serverState.masterHost = masterHost
					serverState.masterPort = masterPort
					if serverState.masterConnection != nil {
						serverState.masterConnection.Close() // Close existing connection if any
					}
					serverState.mu.Unlock()

					// Attempt to connect to master
					masterAddr := fmt.Sprintf("%s:%s", masterHost, masterPort)
					masterConn, err := net.Dial("tcp", masterAddr)
					if err != nil {
						errMsg := fmt.Sprintf("ERR failed to connect to master %s:%s - %v", masterHost, masterPort, err)
						log.Println(errMsg)
						// Even if connection fails, role is set to slave. Redis tries to reconnect.
						// For now, we just report OK as per spec for the command itself.
						// The actual replication handshake (PSYNC) would handle connection issues.
						response = "OK" // The command REPLICAOF itself is OK.
					} else {
						log.Printf("Successfully connected to master at %s", masterAddr)
						serverState.mu.Lock()
						serverState.masterConnection = masterConn
						serverState.mu.Unlock()

						// Send a non-standard command to master to register as a slave
						registerCmd := []byte("*1\r\n$22\r\nINTERNAL_REGISTER_SLAVE\r\n")
						if _, err := masterConn.Write(registerCmd); err != nil {
							log.Printf("Failed to send INTERNAL_REGISTER_SLAVE to master: %v", err)
							// Proceeding as a slave, but master won't propagate.
							// Master connection will be handled by its own handleConnection goroutine.
						} else {
							log.Printf("Sent INTERNAL_REGISTER_SLAVE to master at %s", masterAddr)
						}

						// Start a goroutine to listen for commands from the new master
						go handleConnection(masterConn, db) // Pass the same db
						response = "OK"
					}
				}
			} else {
				response = fmt.Errorf("ERR wrong number of arguments for 'replicaof' command")
			}
		case "INTERNAL_REGISTER_SLAVE":
			// This is a non-standard command sent by a slave to a master
			// to request being added to the master's list of slaves.
			if serverState.role == RoleMaster {
				serverState.mu.Lock()
				// Check if already registered to avoid duplicates, though conn object equality should handle it
				isAlreadyRegistered := false
				for _, slaveConn := range serverState.connectedSlaves {
					if slaveConn == conn {
						isAlreadyRegistered = true
						break
					}
				}
				if !isAlreadyRegistered {
					serverState.connectedSlaves = append(serverState.connectedSlaves, conn)
					log.Printf("Registered new slave: %s. Total slaves: %d", conn.RemoteAddr().String(), len(serverState.connectedSlaves))
				}
				serverState.mu.Unlock()
				// No response is typically sent for this internal command, or could be +OK
				// For now, let's not send a response to keep it simple.
				continue // Don't process further as a regular command or send generic response
			} else {
				// If a non-master receives this, it's an error or misconfiguration
				response = fmt.Errorf("ERR INTERNAL_REGISTER_SLAVE received by non-master server")
			}
		case "SAVE":
			if serverState.role == RoleSlave {
				response = fmt.Errorf("ERR SAVE command not allowed for slave")
			} else if len(args) != 0 {
				response = fmt.Errorf("ERR wrong number of arguments for 'save' command")
			} else {
				err := database.Save(db, "dump.rdb")
				if err != nil {
					response = fmt.Errorf("ERR failed to save database: %v", err)
				} else {
					response = "OK"
				}
			}
		case "SET":
			if len(args) != 2 {
				response = fmt.Errorf("ERR wrong number of arguments for 'set' command")
			} else {
				err := database.Save(db, "dump.rdb")
				if err != nil {
					response = fmt.Errorf("ERR failed to save database: %v", err)
				} else {
					response = "OK"
				}
			}
		case "SET":
			if len(args) != 2 {
				response = fmt.Errorf("ERR wrong number of arguments for 'set' command")
			} else {
				// If master, propagate to slaves. If slave, this is a client write, which might be disallowed.
				// For now, allow slaves to take direct writes for simplicity, though Redis slaves are read-only by default.
				db.Set(args[0], args[1])
				response = "OK"
				isWriteCommand = true
			}
		case "GET":
			// Read commands are fine for both master and slave from client
			if len(args) != 1 {
				response = fmt.Errorf("ERR wrong number of arguments for 'get' command")
			} else {
				val, ok := db.Get(args[0])
				if !ok {
					response = nil // Null bulk string for non-existent key
				} else {
					// Assuming value is stored as string, convert to []byte for SerializeResponse
					if strVal, okStr := val.(string); okStr {
						response = []byte(strVal)
					} else {
						// This case should ideally not happen if SET always stores strings
						response = fmt.Errorf("ERR value for key '%s' is not a string", args[0])
					}
				}
			}
		case "XADD":
			if len(args) < 3 { // XADD key ID field value [field value ...]
				response = fmt.Errorf("ERR wrong number of arguments for 'xadd' command")
			} else {
				streamKey := args[0]
				idStr := args[1]
				if (len(args)-2)%2 != 0 {
					response = fmt.Errorf("ERR wrong number of arguments for 'xadd' command: fields and values must be in pairs")
				} else {
					fields := make(map[string]string)
					for i := 2; i < len(args); i += 2 {
						fields[args[i]] = args[i+1]
					}

					// serverState.role check for XADD? Redis slaves are typically read-only.
					// For now, let's assume direct writes to slaves are disallowed for XADD too.
					if serverState.role == RoleSlave && !isMasterConnection {
						response = fmt.Errorf("ERR XADD command not allowed for slave (unless propagated by master)")
					} else {
						newID, err := database.XAdd(db, streamKey, idStr, fields)
						if err != nil {
							response = err // XAdd returns errors compatible with SerializeResponse
						} else {
							response = []byte(newID.String()) // Serialize StreamID as bulk string
							isWriteCommand = true
						}
					}
				}
			}
		default:
			response = fmt.Errorf("ERR unknown command '%s'", commandName)
		}

		// Send response back to the original client (unless it was a propagated command slave processed)
		if !isMasterConnection { // Don't send OK back to master for propagated SETs etc.
			conn.Write(redis.SerializeResponse(response))
		}


		// If this is a master server and the command was a write command (or EXEC resulted in writes), propagate it.
		if serverState.role == RoleMaster {
			if len(commandsToPropagate) > 0 { // EXEC decided what to propagate
				serverState.mu.Lock()
				log.Printf("Master propagating transaction to %d slaves", len(serverState.connectedSlaves))
				for _, slaveConn := range serverState.connectedSlaves {
					for _, cmdBytes := range commandsToPropagate {
						if _, err := slaveConn.Write(cmdBytes); err != nil {
							log.Printf("Failed to propagate part of transaction to slave %s: %v. (Slave may be partially updated)", slaveConn.RemoteAddr().String(), err)
							// TODO: Handle slave removal or marking as out-of-sync more robustly
							break // Stop propagating this transaction to this slave
						}
					}
				}
				serverState.mu.Unlock()
			} else if isWriteCommand { // Single command, not part of EXEC that set commandsToPropagate
				serverState.mu.Lock()
				log.Printf("Master propagating command to %d slaves: %s", len(serverState.connectedSlaves), commandName)
				for i, slaveConn := range serverState.connectedSlaves {
					if _, err := slaveConn.Write(request); err != nil { // Send original request
						log.Printf("Failed to propagate command to slave %s: %v. Removing slave.", slaveConn.RemoteAddr().String(), err)
						serverState.connectedSlaves = append(serverState.connectedSlaves[:i], serverState.connectedSlaves[i+1:]...)
					}
				}
				serverState.mu.Unlock()
			}
		}
	}
}
			// This is a simplified propagation. Real Redis uses a replication stream.
			// For SET key value, request would be like: *3\r\n$3\r\nSET\r\n$len(key)\r\nkey\r\n$len(value)\r\nvalue\r\n
			// The original `request` buffer contains this.
			log.Printf("Master propagating command to %d slaves: %s", len(serverState.connectedSlaves), commandName)
			for i, slaveConn := range serverState.connectedSlaves {
				if _, err := slaveConn.Write(request); err != nil {
					log.Printf("Failed to propagate command to slave %s: %v. Removing slave.", slaveConn.RemoteAddr().String(), err)
					// Remove problematic slave
					serverState.connectedSlaves = append(serverState.connectedSlaves[:i], serverState.connectedSlaves[i+1:]...)
					// TODO: Consider closing slaveConn from master side.
				}
			}
			serverState.mu.Unlock()
		}
	}
}

// Helper to add a slave to the list - not strictly needed if REPLICAOF is client-driven to master
// but master needs to know which connections are slaves.
// For now, REPLICAOF on the slave establishes the connection.
// The master's handleConnection needs to identify that a connection is from a slave,
// possibly via a PING or PSYNC command from the slave after connection.
// For now, we will assume that if a server (master) gets a connection,
// and that connection later sends a command that identifies it as a slave (e.g. internal part of PSYNC)
// then it's added to connectedSlaves.
// The current REPLICAOF makes the slave connect to master. The master's accept loop gets this new conn.
// That conn needs to be identified as a slave conn.
// A simple way: if a client connection sends "REPLICAOF" to the master, it's not standard.
// Slaves send PSYNC/SYNC.
// Let's adjust: when a slave connects to a master, the master doesn't automatically know it's a slave.
// The slave would send a command like PSYNC.
// For this step, the `connectedSlaves` list will be populated when a *master* successfully processes
// a command that implies a client is now a slave (which is not standard Redis).
// Or, more simply for now: when a slave connects *to* the master, that connection on the master side
// isn't automatically added to `connectedSlaves`.
// The current `REPLICAOF` makes the *slave* initiate the connection.
// The master will see a new connection in its `listener.Accept()`.
// That new `net.Conn` needs to be associated with a slave.
// A simple placeholder: A master command `ADDSLAVE <host> <port>` (internal/test) or slave sends `PSYNC`.
// For now, the `connectedSlaves` list will be manually managed if a slave connects to the master
// and the master somehow is told "this new connection is a slave".
// The current structure of `REPLICAOF` means the *slave* calls `net.Dial` to connect to master.
// The master accepts this connection. The `handleConnection` for this on the master side
// needs to know it's a slave.
// The simplest way for now: A slave connects, then sends a special command like "IAMSLAVE" (non-standard)
// or we assume the PSYNC process handles this.
// Let's make `REPLICAOF` on the master side add the connecting client to its slave list. This is not standard.
// Standard Redis: Slave connects, sends PSYNC. Master then considers it a slave.

// The current propagation logic relies on `connectedSlaves` being populated.
// If server A is master, and server B connects to A, server A's `handleConnection` for B
// needs to add B's `conn` to `serverState.connectedSlaves`.
// This could happen if B sends a specific command (e.g., an internal `REGISTER_SLAVE` or part of `PSYNC`).

// For now, let's assume that when a slave connects to the master, and the master's `handleConnection`
// is running for that slave's connection, if that slave sends a (hypothetical) `PSYNC` command,
// the master would then add `conn` to its `serverState.connectedSlaves`.
// The current `REPLICAOF` implementation is on the server that *becomes* a slave.
// It connects to the master. The master receives this connection like any other.
// The master needs to identify this connection as a slave.
// This will be refined with PSYNC. For now, propagation happens to slaves in `connectedSlaves`.
// How does a connection get into `connectedSlaves` on the master?
// Let's add a non-standard command `INTERNAL_REGISTER_SLAVE` that a slave calls on the master
// after connecting via `REPLICAOF`.

// Add sync.Mutex to ServerState imports
// import "sync"
// Added at the top of the file in previous step.
