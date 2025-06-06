# PostgreSQL MuaDB Integration

This PostgreSQL fork includes integration with MuaDB storage. Every row insertion and deletion in PostgreSQL triggers corresponding operations in MuaDB.

## Integration Points

- **Row Inserts**: `heap_insert()` calls `muadb_store_tuple()`
- **Row Deletes**: `heap_delete()` calls `muadb_remove_tuple()`

## Files Modified/Added

- `src/backend/access/heap/muadb_storage.h` - MuaDB storage function declarations
- `src/backend/access/heap/muadb_storage.c` - MuaDB storage implementation
- `src/backend/access/heap/heapam.c` - Modified to call MuaDB functions
- `src/backend/access/heap/Makefile` - Added muadb_storage.o to build

## Quick Start

Use the provided test script to compile and test the integration:

```bash
# Run complete workflow (compile, initialize, start server, run tests)
./test_muadb_integration.sh full

# Or run individual steps:
./test_muadb_integration.sh compile    # Compile PostgreSQL
./test_muadb_integration.sh init       # Initialize database
./test_muadb_integration.sh start      # Start server
./test_muadb_integration.sh test       # Run integration tests
./test_muadb_integration.sh logs       # Show MuaDB logs
./test_muadb_integration.sh stop       # Stop server
```

## Test Script Features

- **Isolated Testing**: Uses port 5433 and separate directories to avoid conflicts
- **Complete Workflow**: Handles compilation, installation, database initialization, and testing
- **Colored Output**: Easy-to-read status messages
- **MuaDB Log Filtering**: Shows only relevant integration logs
- **Multiple Operations**: Tests INSERT, UPDATE, DELETE operations

## Expected Output

When running tests, you should see log entries like:

```
LOG: MuaDB: Storing tuple for relation muadb_test (OID: 16385)
LOG: MuaDB: Removing tuple for relation muadb_test (OID: 16385)
```

## Current Implementation

The current implementation logs MuaDB operations to PostgreSQL's log file. To integrate with actual MuaDB:

1. Replace the logging functions in `muadb_storage.c` with actual MuaDB client calls
2. Add MuaDB connection configuration
3. Implement error handling for MuaDB operations

## Configuration

The test script uses these default settings:

- **Port**: 5433 (to avoid conflicts with existing PostgreSQL)
- **Data Directory**: `~/pgdata-muadb`
- **Log File**: `~/postgres-muadb.log`
- **Install Directory**: `/tmp/pg-muadb-install`

## Testing Different Operations

The script automatically tests:

1. **Table Creation** - Shows system catalog operations
2. **INSERT** - Multiple row insertions
3. **UPDATE** - Row modifications
4. **DELETE** - Row deletions
5. **SELECT** - Verifies data consistency

## Troubleshooting

- If compilation fails, ensure you have PostgreSQL build dependencies installed
- If server fails to start, check the log file: `~/postgres-muadb.log`
- If tests fail, use `./test_muadb_integration.sh status` to check server status
- Use `./test_muadb_integration.sh cleanup` to start fresh

## Development Workflow

1. Make changes to MuaDB integration code
2. Run `./test_muadb_integration.sh compile` to rebuild
3. Run `./test_muadb_integration.sh restart` to restart with new code
4. Run `./test_muadb_integration.sh test` to verify changes
5. Check `./test_muadb_integration.sh logs` for MuaDB operations 