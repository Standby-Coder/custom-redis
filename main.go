package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"strconv"

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
	Name string
	Args []string
	Raw  []byte
}

// ClientSpecificState holds all state for a single client connection
type ClientSpecificState struct {
	inTransaction     bool
	queuedCommands    []QueuedCommand
	transactionFailed bool
	watchedKeys       map[string]struct{}
	isDirty           bool
}

// ServerState holds the global state of the server
type ServerState struct {
	role                  ServerRole
	masterHost            string
	masterPort            string
	connectedSlaves       []net.Conn
	masterConnection      net.Conn
	mu                    sync.Mutex
	watchedKeySubscribers map[string]map[net.Conn]struct{}
	clientStates          map[net.Conn]*ClientSpecificState
}

var serverState = ServerState{
	role:                  RoleMaster,
	connectedSlaves:       make([]net.Conn, 0),
	watchedKeySubscribers: make(map[string]map[net.Conn]struct{}),
	clientStates:          make(map[net.Conn]*ClientSpecificState),
}

var (
	rdbFilename string
)

func unwatchKeys(clientState *ClientSpecificState, conn net.Conn) {
	serverState.mu.Lock()
	defer serverState.mu.Unlock()
	if len(clientState.watchedKeys) > 0 {
		for key := range clientState.watchedKeys {
			if subscribers, ok := serverState.watchedKeySubscribers[key]; ok {
				delete(subscribers, conn)
				if len(subscribers) == 0 {
					delete(serverState.watchedKeySubscribers, key)
				}
			}
		}
		clientState.watchedKeys = make(map[string]struct{})
	}
	clientState.isDirty = false
}

func notifyWatchers(key string) {
	serverState.mu.Lock()
	defer serverState.mu.Unlock()
	if subscribers, ok := serverState.watchedKeySubscribers[key]; ok {
		for connSub := range subscribers {
			if clientData, okClient := serverState.clientStates[connSub]; okClient {
				clientData.isDirty = true
			} else {
				delete(subscribers, connSub)
				if len(subscribers) == 0 {
					delete(serverState.watchedKeySubscribers, key)
				}
			}
		}
	}
}

func parseFields(args []string) map[string]string {
    fields := make(map[string]string)
    if len(args)%2 != 0 { return fields }
    for i := 0; i < len(args); i += 2 {
        fields[args[i]] = args[i+1]
    }
    return fields
}

func main() {
	port := flag.String("port", defaultPort, "Port to listen on")
	flag.StringVar(&rdbFilename, "rdbfilename", "dump.rdb", "RDB filename")
	flag.Parse()

	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil { log.Fatalf("Failed to bind to port %s: %v", *port, err) }
	defer listener.Close()
	log.Printf("GoRedis server listening on port %s\n", *port)
	log.Printf("RDB filename: %s", rdbFilename)

	db, errDbLoad := database.Load(rdbFilename)
	if errDbLoad != nil {
		log.Printf("Failed to load data from %s: %v. Starting with an empty database.", rdbFilename, errDbLoad)
		db = database.NewDataStore()
	} else {
		fmt.Printf("Successfully loaded data from %s\n", rdbFilename)
	}

	for {
		conn, err := listener.Accept()
		if err != nil { log.Printf("Failed to accept connection: %v", err); continue }
		go handleConnection(conn, db)
	}
}

func handleConnection(conn net.Conn, db *database.DataStore) {
	clientState := ClientSpecificState{
		inTransaction:     false,
		queuedCommands:    make([]QueuedCommand, 0),
		transactionFailed: false,
		watchedKeys:       make(map[string]struct{}),
		isDirty:           false,
	}
	serverState.mu.Lock()
	serverState.clientStates[conn] = &clientState
	serverState.mu.Unlock()

	defer func() {
		unwatchKeys(&clientState, conn)
		serverState.mu.Lock()
		delete(serverState.clientStates, conn)
		for i, slaveConn := range serverState.connectedSlaves {
			if slaveConn == conn {
				serverState.connectedSlaves = append(serverState.connectedSlaves[:i], serverState.connectedSlaves[i+1:]...)
				log.Printf("Slave connection closed and removed: %s", conn.RemoteAddr().String())
				break
			}
		}
		if serverState.role == RoleSlave && serverState.masterConnection == conn {
			serverState.masterConnection = nil
			log.Printf("Connection to master lost: %s", conn.RemoteAddr().String())
		}
		serverState.mu.Unlock()
		conn.Close()
	}()

	reader := bufio.NewReader(conn)
	isMasterConnection := false
	serverState.mu.Lock()
	if serverState.role == RoleSlave && serverState.masterConnection == conn {
		isMasterConnection = true
	}
	serverState.mu.Unlock()

	for {
		buffer := make([]byte, 1024)
		n, err := reader.Read(buffer)
		if err != nil {
			if clientState.inTransaction {
				clientState.inTransaction = false; clientState.queuedCommands = make([]QueuedCommand, 0)
				clientState.transactionFailed = false; unwatchKeys(&clientState, conn)
			}
			if err != io.EOF && !strings.Contains(err.Error(), "use of closed network connection") {
				// log.Printf("Read error: %v", err)
			}
			return
		}

		request := buffer[:n]
		parsedCommandName, parsedArgs, parseErr := redis.ParseCommand(request)

		if clientState.inTransaction {
			cmdUpper := ""; if parseErr == nil { cmdUpper = strings.ToUpper(parsedCommandName) }
			if cmdUpper == "MULTI" { conn.Write(redis.SerializeResponse(fmt.Errorf("ERR MULTI calls can not be nested"))); continue }
			if cmdUpper == "EXEC" || cmdUpper == "DISCARD" { /* Fall through */ } else
			if cmdUpper == "WATCH" { conn.Write(redis.SerializeResponse(fmt.Errorf("ERR WATCH inside MULTI is not allowed"))); continue } else {
				if parseErr == nil {
					validSyntax := true
					// Basic syntax checks for queuing based on common argument counts
					switch cmdUpper {
					case "SET": if len(parsedArgs) < 2 { validSyntax = false } // Allows options
					case "GET", "DECR", "INCR", "TTL", "PTTL", "TYPE", "SAVE", "UNWATCH": if len(parsedArgs) != 1 && cmdUpper != "SAVE" && cmdUpper != "UNWATCH" { validSyntax = false } else if (cmdUpper == "SAVE" || cmdUpper == "UNWATCH") && len(parsedArgs) !=0 {validSyntax = false}
					case "XADD": if len(parsedArgs) < 3 || (len(parsedArgs)-2)%2 != 0 { validSyntax = false }
					case "HSET": if len(parsedArgs) != 3 {validSyntax = false}
					case "HGET": if len(parsedArgs) != 2 {validSyntax = false}
					case "HDEL": if len(parsedArgs) < 2 {validSyntax = false}
					case "DEL": if len(parsedArgs) == 0 {validSyntax = false}
					case "PING": if len(parsedArgs) > 1 {validSyntax = false}
					case "ECHO", "EXPIRE", "PEXPIRE", "INCRBY", "DECRBY", "SQUERY": if len(parsedArgs) != 2 && cmdUpper != "SQUERY" { validSyntax = false } else if cmdUpper == "SQUERY" && len(parsedArgs) !=2 {validSyntax = false} // SQUERY needs 2, others here mostly 2
					case "REPLICAOF": if len(parsedArgs) != 2 {validSyntax=false}
					// WATCH (already handled), MULTI, EXEC, DISCARD are not queued this way.
					}
					if !validSyntax { clientState.transactionFailed = true } // Mark TX as failed for EXECABORT
					clientState.queuedCommands = append(clientState.queuedCommands, QueuedCommand{ Name: cmdUpper, Args: parsedArgs, Raw: request })
					conn.Write(redis.SerializeResponse("QUEUED"))
				} else {
					clientState.transactionFailed = true
					conn.Write(redis.SerializeResponse(parseErr))
				}
				continue
			}
		}

		commandName := ""; var args []string
		if parseErr != nil { conn.Write(redis.SerializeResponse(parseErr)); continue } else {
			commandName = strings.ToUpper(parsedCommandName); args = parsedArgs
		}

		var response interface{}; isWriteCommand := false
		var commandsToPropagate [][]byte; var modifiedKey string

		if isMasterConnection && commandName != "REPLICAOF" && commandName != "INTERNAL_REGISTER_SLAVE" {
			switch commandName {
			case "SET": if len(args) >= 2 { db.Set(args[0], args[1], database.SetOptions{}) } // Basic set for replication
			case "XADD": if len(args) >= 3 { _, _ = database.XAdd(db, args[0], args[1], parseFields(args[2:])) }
			case "HSET": if len(args) == 3 { _, _ = db.HSet(args[0], args[1], args[2]) }
			case "HDEL": if len(args) >=2 { _, _ = db.HDel(args[0], args[1:]...) }
			case "DEL": if len(args) >=1 { db.Del(args...) }
			case "INCR": if len(args) == 1 { _, _ = db.Incr(args[0], 1) }
			case "DECR": if len(args) == 1 { _, _ = db.Incr(args[0], -1) }
			case "INCRBY": if len(args) == 2 { incr, _ := strconv.ParseInt(args[1], 10, 64); _, _ = db.Incr(args[0], incr) }
			case "DECRBY": if len(args) == 2 { decr, _ := strconv.ParseInt(args[1], 10, 64); _, _ = db.Incr(args[0], -decr) }
			case "EXPIRE": if len(args) == 2 { secs, _ := strconv.ParseInt(args[1], 10, 64); _, _ = db.Expire(args[0], secs) }
			case "PEXPIRE": if len(args) == 2 { msecs, _ := strconv.ParseInt(args[1], 10, 64); _, _ = db.Pexpire(args[0], msecs) }
			// Read-only commands or commands not modifying keys directly (like PING, ECHO, MULTI, EXEC) are handled by slave as normal client.
			}
			// If it was a write command that was processed, skip sending response from slave to master
			if commandName == "SET" || commandName == "XADD" || commandName == "HSET" || commandName == "HDEL" || commandName == "DEL" ||
			   commandName == "INCR" || commandName == "DECR" || commandName == "INCRBY" || commandName == "DECRBY" ||
			   commandName == "EXPIRE" || commandName == "PEXPIRE" {
                 continue
            }
		}

		switch commandName {
		case "PING":
			if len(args) == 0 { response = "PONG" } else
			if len(args) == 1 { response = []byte(args[0]) } else
			{ response = fmt.Errorf("ERR wrong number of arguments for 'ping' command") }
		case "ECHO":
			if len(args) == 1 { response = []byte(args[0]) } else
			{ response = fmt.Errorf("ERR wrong number of arguments for 'echo' command") }
		case "EXPIRE":
			if len(args) != 2 { response = fmt.Errorf("ERR wrong number of arguments for 'expire' command") } else {
				key := args[0]; secondsStr := args[1]; seconds, err := strconv.ParseInt(secondsStr, 10, 64)
				if err != nil { response = fmt.Errorf("ERR value is not an integer or out of range") } else {
					ok, _ := db.Expire(key, seconds)
					if ok { response = int64(1); isWriteCommand = true; modifiedKey = key } else { response = int64(0) }
				}
			}
		case "TTL":
			if len(args) != 1 { response = fmt.Errorf("ERR wrong number of arguments for 'ttl' command") } else {
				key := args[0]; ttl, _ := db.TTL(key); response = ttl
			}
		case "PEXPIRE":
			if len(args) != 2 { response = fmt.Errorf("ERR wrong number of arguments for 'pexpire' command") } else {
				key := args[0]; millisecondsStr := args[1]; milliseconds, err := strconv.ParseInt(millisecondsStr, 10, 64)
				if err != nil { response = fmt.Errorf("ERR value is not an integer or out of range") } else {
					ok, _ := db.Pexpire(key, milliseconds)
					if ok { response = int64(1); isWriteCommand = true; modifiedKey = key } else { response = int64(0) }
				}
			}
		case "PTTL":
			if len(args) != 1 { response = fmt.Errorf("ERR wrong number of arguments for 'pttl' command") } else {
				key := args[0]; pttl, _ := db.PTTL(key); response = pttl
			}
		case "INCR":
			if len(args) != 1 { response = fmt.Errorf("ERR wrong number of arguments for 'incr' command") } else {
				key := args[0]; newValue, errIncr := db.Incr(key, 1)
				if errIncr != nil { response = errIncr } else { response = newValue; isWriteCommand = true; modifiedKey = key }
			}
		case "DECR":
			if len(args) != 1 { response = fmt.Errorf("ERR wrong number of arguments for 'decr' command") } else {
				key := args[0]; newValue, errDecr := db.Incr(key, -1)
				if errDecr != nil { response = errDecr } else { response = newValue; isWriteCommand = true; modifiedKey = key }
			}
		case "INCRBY":
			if len(args) != 2 { response = fmt.Errorf("ERR wrong number of arguments for 'incrby' command") } else {
				key := args[0]; incrementStr := args[1]; increment, err := strconv.ParseInt(incrementStr, 10, 64)
				if err != nil { response = fmt.Errorf("ERR value is not an integer or out of range") } else {
					newValue, errIncrBy := db.Incr(key, increment)
					if errIncrBy != nil { response = errIncrBy } else { response = newValue; isWriteCommand = true; modifiedKey = key }
				}
			}
		case "DECRBY":
			if len(args) != 2 { response = fmt.Errorf("ERR wrong number of arguments for 'decrby' command") } else {
				key := args[0]; decrementStr := args[1]; decrement, err := strconv.ParseInt(decrementStr, 10, 64)
				if err != nil { response = fmt.Errorf("ERR value is not an integer or out of range") } else {
					newValue, errDecrBy := db.Incr(key, -decrement)
					if errDecrBy != nil { response = errDecrBy } else { response = newValue; isWriteCommand = true; modifiedKey = key }
				}
			}
		case "TYPE":
			if len(args) != 1 { response = fmt.Errorf("ERR wrong number of arguments for 'type' command") } else {
				key := args[0]; typeName, _ := db.Type(key); response = typeName
			}
		case "WATCH":
			if clientState.inTransaction { response = fmt.Errorf("ERR WATCH inside MULTI is not allowed") } else
			if len(args) == 0 { response = fmt.Errorf("ERR wrong number of arguments for 'watch' command") } else {
				serverState.mu.Lock()
				for _, key := range args {
					clientState.watchedKeys[key] = struct{}{}
					if _, okSub := serverState.watchedKeySubscribers[key]; !okSub {
						serverState.watchedKeySubscribers[key] = make(map[net.Conn]struct{})
					}
					serverState.watchedKeySubscribers[key][conn] = struct{}{}
				}
				serverState.mu.Unlock(); response = "OK"
			}
		case "UNWATCH":
			unwatchKeys(&clientState, conn); response = "OK"
		case "MULTI":
			if clientState.inTransaction { response = fmt.Errorf("ERR MULTI calls can not be nested") } else {
				clientState.inTransaction = true; clientState.queuedCommands = make([]QueuedCommand, 0)
				clientState.transactionFailed = false; response = "OK"
			}
		case "DISCARD":
			if !clientState.inTransaction { response = fmt.Errorf("ERR DISCARD without MULTI") } else {
				clientState.inTransaction = false; clientState.queuedCommands = make([]QueuedCommand, 0)
				clientState.transactionFailed = false; unwatchKeys(&clientState, conn); response = "OK"
			}
		case "EXEC":
			if !clientState.inTransaction { response = fmt.Errorf("ERR EXEC without MULTI") } else {
				clientState.inTransaction = false
				if clientState.isDirty { response = nil } else
				if clientState.transactionFailed { response = fmt.Errorf("EXECABORT Transaction discarded because of previous errors") } else {
					execResponses := make([]interface{}, len(clientState.queuedCommands))
					keysModifiedInTx := make(map[string]struct{})
					originalCmdsToPropagate := make([][]byte, 0, len(clientState.queuedCommands)+2)

					if serverState.role == RoleMaster && len(clientState.queuedCommands) > 0 {
						// For propagation, assume MULTI was the first raw command if we had stored it for the transaction.
						// For now, reconstruct MULTI.
						multiRaw, _ := redis.ParseCommand([]byte("*1\r\n$5\r\nMULTI\r\n")) // This is not right for propagation
                                                                                                // We need the *original* raw MULTI.
                                                                                                // Let's just send the queued commands' raw bytes for now.
                        // commandsToPropagate = append(commandsToPropagate, clientState.MULTI_RAW_BYTES_IF_STORED)
					}

					for i, qc := range clientState.queuedCommands {
						var cmdResp interface{}; cmdWriteInExec := false; currentCmdModKeyInExec := ""
						// Re-process commands for EXEC
						switch qc.Name {
						case "SET":
							if len(qc.Args) < 2 { cmdResp = fmt.Errorf("ERR wrong number of arguments for 'set' command in EXEC") } else {
								keyInExec, valueInExec := qc.Args[0], qc.Args[1]; setOpts := database.SetOptions{}; getOptInExec := false
								// Simplified: no parsing of NX/XX/GET options for commands within EXEC for now.
								// A full impl would parse qc.Args[2:]
								setResult := db.Set(keyInExec, valueInExec, setOpts)
								if !setResult.DidSet { cmdResp = nil /* Or specific error if NX/XX were hypothetically parsed and failed */ } else {
									cmdWriteInExec = true; currentCmdModKeyInExec = keyInExec
									if getOptInExec { if setResult.Existed && setResult.OldValue != nil { /* ... serialize old value ...*/ } else { cmdResp = nil }
									} else { cmdResp = "OK" }
								}
							}
						case "GET":
							if len(qc.Args) != 1 { cmdResp = fmt.Errorf("ERR wrong number of arguments for 'get' command")} else {
                                val, okGet := db.Get(qc.Args[0]); if !okGet { cmdResp = nil } else {
                                    if strVal, okStr := val.(string); okStr { cmdResp = []byte(strVal) } else
									if byteVal, okByte := val.([]byte); okByte { cmdResp = byteVal } else
									{ cmdResp = fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value") } } }
						case "XADD":
							if len(qc.Args) < 3 || (len(qc.Args)-2)%2 != 0 { cmdResp = fmt.Errorf("ERR wrong number of arguments for 'xadd' command") } else {
								fields := parseFields(qc.Args[2:]); newID, errXadd := database.XAdd(db, qc.Args[0], qc.Args[1], fields)
								if errXadd != nil {cmdResp = errXadd} else { cmdResp = []byte(newID.String()); cmdWriteInExec = true; currentCmdModKeyInExec = qc.Args[0] } }
						case "HSET":
							if len(qc.Args) != 3 { cmdResp = fmt.Errorf("ERR wrong number of arguments for 'hset' command")} else {
								count, errHset := db.HSet(qc.Args[0], qc.Args[1], qc.Args[2]); if errHset != nil {cmdResp = errHset} else {cmdResp = int64(count)}; cmdWriteInExec = true; currentCmdModKeyInExec = qc.Args[0]}
						case "HDEL":
							if len(qc.Args) < 2 {cmdResp = fmt.Errorf("ERR wrong number of arguments for 'hdel' command")} else {
								delCount, _ := db.HDel(qc.Args[0], qc.Args[1:]...); cmdResp = int64(delCount); if delCount > 0 {cmdWriteInExec = true; currentCmdModKeyInExec = qc.Args[0]}}
						case "DEL":
							if len(qc.Args) == 0 {cmdResp = fmt.Errorf("ERR wrong number of arguments for 'del' command")} else {
								delCount := db.Del(qc.Args...); cmdResp = int64(delCount); if delCount > 0 {cmdWriteInExec = true; for _, k := range qc.Args {keysModifiedInTx[k] = struct{}{}} }}
						case "PING":
							if len(qc.Args) == 0 {cmdResp = "PONG"} else if len(qc.Args) == 1 { cmdResp = []byte(qc.Args[0])} else {cmdResp = fmt.Errorf("ERR wrong number of arguments for 'ping' command")}
						case "INCR":
							if len(qc.Args) != 1 {cmdResp = fmt.Errorf("ERR wrong number of arguments for 'incr' command")} else {
							val, err := db.Incr(qc.Args[0], 1); if err != nil {cmdResp = err} else {cmdResp = val}; cmdWriteInExec = true; currentCmdModKeyInExec = qc.Args[0]}
						case "DECR":
							if len(qc.Args) != 1 {cmdResp = fmt.Errorf("ERR wrong number of arguments for 'decr' command")} else {
							val, err := db.Incr(qc.Args[0], -1); if err != nil {cmdResp = err} else {cmdResp = val}; cmdWriteInExec = true; currentCmdModKeyInExec = qc.Args[0]}
						case "INCRBY":
							if len(qc.Args) != 2 {cmdResp = fmt.Errorf("ERR wrong number of arguments for 'incrby' command")} else {
							incrVal, errPI := strconv.ParseInt(qc.Args[1], 10, 64); if errPI != nil {cmdResp = fmt.Errorf("ERR value is not an integer or out of range")} else {
							val, errDb := db.Incr(qc.Args[0], incrVal); if errDb != nil {cmdResp = errDb} else {cmdResp = val}}; cmdWriteInExec = true; currentCmdModKeyInExec = qc.Args[0]}
						case "DECRBY":
							if len(qc.Args) != 2 {cmdResp = fmt.Errorf("ERR wrong number of arguments for 'decrby' command")} else {
							decrVal, errPI := strconv.ParseInt(qc.Args[1], 10, 64); if errPI != nil {cmdResp = fmt.Errorf("ERR value is not an integer or out of range")} else {
							val, errDb := db.Incr(qc.Args[0], -decrVal); if errDb != nil {cmdResp = errDb} else {cmdResp = val}}; cmdWriteInExec = true; currentCmdModKeyInExec = qc.Args[0]}
						default: cmdResp = fmt.Errorf("ERR unknown command '%s' in EXEC", qc.Name)
						}
						execResponses[i] = cmdResp
						if cmdWriteInExec { if _, isErr := cmdResp.(error); !isErr { /* successfulWriteCmdsInExec = append(successfulWriteCmdsInExec, qc) // Not strictly needed if propagating all original */ } }
						if currentCmdModKeyInExec != "" { keysModifiedInTx[currentCmdModKeyInExec] = struct{}{} }
					}
					response = execResponses
					if serverState.role == RoleMaster && len(clientState.queuedCommands) > 0 {
						commandsToPropagate = append(commandsToPropagate, []byte("*1\r\n$5\r\nMULTI\r\n")) // TODO: Store original MULTI raw bytes
						for _, qCmd := range clientState.queuedCommands { commandsToPropagate = append(commandsToPropagate, qCmd.Raw) }
						commandsToPropagate = append(commandsToPropagate, []byte("*1\r\n$4\r\nEXEC\r\n")) // TODO: Store original EXEC raw bytes
					}
					for k := range keysModifiedInTx { notifyWatchers(k) }
				}
				clientState.queuedCommands = make([]QueuedCommand, 0); unwatchKeys(&clientState, conn)
			}
		case "REPLICAOF":
            if clientState.inTransaction { response = fmt.Errorf("ERR REPLICAOF inside MULTI is not allowed"); break }
            if len(args) == 2 {
				masterHost := args[0]; masterPort := args[1]
				if strings.ToLower(masterHost) == "no" && strings.ToLower(masterPort) == "one" {
					serverState.mu.Lock(); serverState.role = RoleMaster
					if serverState.masterConnection != nil { serverState.masterConnection.Close(); serverState.masterConnection = nil }
					serverState.masterHost = ""; serverState.masterPort = ""
					serverState.mu.Unlock(); response = "OK"; log.Printf("Server changed role to MASTER")
				} else {
					serverState.mu.Lock(); serverState.role = RoleSlave; serverState.masterHost = masterHost; serverState.masterPort = masterPort
					if serverState.masterConnection != nil { serverState.masterConnection.Close() }
					serverState.mu.Unlock()
					masterAddr := fmt.Sprintf("%s:%s", masterHost, masterPort)
					masterConn, errDial := net.Dial("tcp", masterAddr)
					if errDial != nil { log.Printf("ERR failed to connect to master %s:%s - %v", masterHost, masterPort, errDial); response = "OK"
					} else {
						log.Printf("Successfully connected to master at %s", masterAddr)
						serverState.mu.Lock(); serverState.masterConnection = masterConn; serverState.mu.Unlock()
						registerCmd := []byte("*1\r\n$22\r\nINTERNAL_REGISTER_SLAVE\r\n")
						if _, errWrite := masterConn.Write(registerCmd); errWrite != nil { log.Printf("Failed to send INTERNAL_REGISTER_SLAVE to master: %v", errWrite) } else { log.Printf("Sent INTERNAL_REGISTER_SLAVE to master at %s", masterAddr) }
						go handleConnection(masterConn, db)
						response = "OK"
					}
				}
			} else { response = fmt.Errorf("ERR wrong number of arguments for 'replicaof' command") }
		case "INTERNAL_REGISTER_SLAVE":
			if serverState.role == RoleMaster {
				serverState.mu.Lock(); isAlreadyRegistered := false
				for _, slaveConn := range serverState.connectedSlaves { if slaveConn == conn { isAlreadyRegistered = true; break } }
				if !isAlreadyRegistered { serverState.connectedSlaves = append(serverState.connectedSlaves, conn); log.Printf("Registered new slave: %s. Total slaves: %d", conn.RemoteAddr().String(), len(serverState.connectedSlaves)) }
				serverState.mu.Unlock(); continue
			} else { response = fmt.Errorf("ERR INTERNAL_REGISTER_SLAVE received by non-master server") }
		case "SAVE":
            if clientState.inTransaction { clientState.transactionFailed = true; clientState.queuedCommands = append(clientState.queuedCommands, QueuedCommand{Name:"SAVE", Args:args, Raw:request }); conn.Write(redis.SerializeResponse("QUEUED")); continue; }
			if serverState.role == RoleSlave { response = fmt.Errorf("ERR SAVE command not allowed for slave") } else if len(args) != 0 { response = fmt.Errorf("ERR wrong number of arguments for 'save' command")
			} else {
				if errSave := database.Save(db, rdbFilename); errSave != nil {
					response = fmt.Errorf("ERR failed to save database: %v", errSave)
				} else { response = "OK" } }
		case "SET":
			if len(args) < 2 { response = fmt.Errorf("ERR wrong number of arguments for 'set' command"); break }
			key, value := args[0], args[1]; optArgs := args[2:]; setOpts := database.SetOptions{}; getOpt := false
			for i := 0; i < len(optArgs); i++ {
				argUpper := strings.ToUpper(optArgs[i])
				switch argUpper {
				case "NX": if setOpts.XX { response = fmt.Errorf("ERR SET NX and XX options are mutually exclusive"); goto endSetProcessing } setOpts.NX = true
				case "XX": if setOpts.NX { response = fmt.Errorf("ERR SET NX and XX options are mutually exclusive"); goto endSetProcessing } setOpts.XX = true
				case "GET": getOpt = true
				default: response = fmt.Errorf("ERR syntax error in SET options"); goto endSetProcessing
				}
			}
			endSetProcessing:
			if errResp, isErr := response.(error); isErr && errResp != nil { /* Error already set */ } else {
				setResult := db.Set(key, value, setOpts)
				if !setResult.DidSet { response = nil } else {
					isWriteCommand = true; modifiedKey = key
					if getOpt { if setResult.Existed && setResult.OldValue != nil {
							if strVal, ok := setResult.OldValue.(string); ok { response = []byte(strVal) } else
							if byteVal, ok := setResult.OldValue.([]byte); ok { response = byteVal } else
							{ response = fmt.Errorf("WRONGTYPE value for key '%s' is not a string or bytes", key) }
						} else { response = nil }
					} else { response = "OK" } } }
		case "GET":
			if len(args) != 1 { response = fmt.Errorf("ERR wrong number of arguments for 'get' command") } else {
				val, found := db.Get(args[0]); if !found { response = nil } else {
					if strVal, ok := val.(string); ok { response = []byte(strVal) } else
					if byteVal, ok := val.([]byte); ok { response = byteVal } else
					{ response = fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value") } } }
		case "XADD":
			if len(args) < 3 { response = fmt.Errorf("ERR wrong number of arguments for 'xadd' command"); break }
			streamKey := args[0]; idStr := args[1]
			if (len(args)-2)%2 != 0 { response = fmt.Errorf("ERR wrong number of arguments for 'xadd' command: fields and values must be in pairs"); break }
			fields := parseFields(args[2:])
			if serverState.role == RoleSlave && !isMasterConnection { response = fmt.Errorf("READONLY You can't write against a read only replica.") } else {
				newID, errXadd := database.XAdd(db, streamKey, idStr, fields)
				if errXadd != nil { response = errXadd } else { response = []byte(newID.String()); isWriteCommand = true; modifiedKey = streamKey } }
		case "HSET":
			if len(args) != 3 { response = fmt.Errorf("ERR wrong number of arguments for 'hset' command"); break }
			key, field, value := args[0], args[1], args[2]
			count, errHset := db.HSet(key, field, value); if errHset != nil { response = fmt.Errorf("ERR %v", errHset)
			} else { response = int64(count); isWriteCommand = true; modifiedKey = key }
		case "HGET":
			if len(args) != 2 { response = fmt.Errorf("ERR wrong number of arguments for 'hget' command"); break }
			key, field := args[0], args[1]; value, found := db.HGet(key, field)
			if !found { response = nil } else { response = []byte(value) }
		case "HDEL":
            if len(args) < 2 { response = fmt.Errorf("ERR wrong number of arguments for 'hdel' command"); break }
            key := args[0]; fieldsToDel := args[1:]
            count, errHdel := db.HDel(key, fieldsToDel...); if errHdel != nil { response = fmt.Errorf("ERR %v", errHdel)
            } else { response = int64(count); if count > 0 { isWriteCommand = true; modifiedKey = key } }
		case "DEL":
            if len(args) == 0 { response = fmt.Errorf("ERR wrong number of arguments for 'del' command"); break }
            deletedCount := db.Del(args...); response = int64(deletedCount)
            if deletedCount > 0 { isWriteCommand = true; for _, k := range args { notifyWatchers(k) }; modifiedKey = "" }
		case "SQUERY":
			if len(args) != 2 { response = fmt.Errorf("ERR wrong number of arguments for 'squery' command"); break }
			indexName, valueToMatch := args[0], args[1]
			if db.IndexMgr == nil { response = fmt.Errorf("ERR Index Manager not initialized"); break }
			index, found := db.IndexMgr.GetIndex(indexName); if !found { response = fmt.Errorf("ERR index '%s' not found", indexName)
			} else {
				matchingKeysMap := index.Get(valueToMatch)
				if matchingKeysMap == nil || len(matchingKeysMap) == 0 { response = []interface{}{}
				} else {
					matchingKeysList := make([]interface{}, 0, len(matchingKeysMap))
					for k := range matchingKeysMap { matchingKeysList = append(matchingKeysList, []byte(k)) }
					response = matchingKeysList } }
		default: response = fmt.Errorf("ERR unknown command '%s'", commandName)
		}

		if !clientState.inTransaction || commandName == "EXEC" || commandName == "DISCARD" || commandName == "MULTI" || commandName == "WATCH" || commandName == "UNWATCH" {
			if !isMasterConnection { conn.Write(redis.SerializeResponse(response)) }
		}

        if modifiedKey != "" { notifyWatchers(modifiedKey) }

		if serverState.role == RoleMaster {
			if len(commandsToPropagate) > 0 {
				serverState.mu.Lock(); activeSlaves := make([]net.Conn, 0, len(serverState.connectedSlaves))
				for _, slaveConn := range serverState.connectedSlaves {
					propagatedOK := true
					for _, cmdBytes := range commandsToPropagate {
						if _, err := slaveConn.Write(cmdBytes); err != nil { propagatedOK = false; break }
					}
					if propagatedOK { activeSlaves = append(activeSlaves, slaveConn) } }
				serverState.connectedSlaves = activeSlaves; serverState.mu.Unlock()
			} else if isWriteCommand {
				serverState.mu.Lock(); activeSlaves := make([]net.Conn, 0, len(serverState.connectedSlaves))
				for _, slaveConn := range serverState.connectedSlaves {
					if _, err := slaveConn.Write(request); err != nil { } else { activeSlaves = append(activeSlaves, slaveConn) } }
				serverState.connectedSlaves = activeSlaves; serverState.mu.Unlock()
			}
		}
	}
}

[end of main.go]
