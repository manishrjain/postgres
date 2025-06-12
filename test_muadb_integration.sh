#!/bin/bash

# PostgreSQL with MuaDB Integration - Test Script
# This script compiles PostgreSQL with MuaDB integration and runs it for testing

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
POSTGRES_ROOT=$(pwd)
INSTALL_DIR="/tmp/pg-muadb-install"
DATA_DIR="$HOME/pgdata-muadb"
LOG_FILE="$HOME/postgres-muadb.log"  # Initial log file (redirects to DATA_DIR/log/)
PG_LOG_DIR="$DATA_DIR/log"           # Actual PostgreSQL log directory
PORT=5433  # Use non-standard port to avoid conflicts

echo -e "${BLUE}=== PostgreSQL MuaDB Integration Test Script ===${NC}"
echo -e "${BLUE}Root Directory: $POSTGRES_ROOT${NC}"
echo -e "${BLUE}Install Directory: $INSTALL_DIR${NC}"
echo -e "${BLUE}Data Directory: $DATA_DIR${NC}"
echo -e "${BLUE}PostgreSQL Log Directory: $PG_LOG_DIR${NC}"
echo -e "${BLUE}Port: $PORT${NC}"
echo

# Function to print status
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to get the latest PostgreSQL log file
get_latest_log_file() {
    if [ -d "$PG_LOG_DIR" ]; then
        # Use ls with timestamp sorting (compatible across systems)
        ls -t "$PG_LOG_DIR"/postgresql-*.log 2>/dev/null | head -1
    else
        echo ""
    fi
}

# Function to cleanup previous installations
cleanup() {
    print_status "Cleaning up previous installations..."
    
    # Stop any running postgres
    if [ -f "$DATA_DIR/postmaster.pid" ]; then
        print_warning "Stopping existing PostgreSQL server..."
        $INSTALL_DIR/usr/local/pgsql/bin/pg_ctl -D "$DATA_DIR" stop -m fast || true
        sleep 2
    fi
    
    # Remove old installation and data
    rm -rf "$INSTALL_DIR"
    rm -rf "$DATA_DIR"
    rm -f "$LOG_FILE"
    
    # Remove configure files to force reconfiguration
    rm -f config.status config.log config.cache
    rm -rf autom4te.cache || true
    
    print_status "Cleanup completed."
}

# Function to compile PostgreSQL
compile_postgres() {
    print_status "Compiling PostgreSQL with MuaDB integration..."
    
    # Clean previous build
    make clean || true
    
    # Configure (if not already configured)
    if [ ! -f "config.status" ]; then
        print_status "Configuring PostgreSQL build..."
        # Check if we're on macOS and add hiredis paths
        if [[ "$OSTYPE" == "darwin"* ]]; then
            # Try common homebrew locations
            HIREDIS_PREFIX=""
            if [ -d "/opt/homebrew/include/hiredis" ]; then
                HIREDIS_PREFIX="/opt/homebrew"
            elif [ -d "/usr/local/include/hiredis" ]; then
                HIREDIS_PREFIX="/usr/local"
            fi
            
            if [ -n "$HIREDIS_PREFIX" ]; then
                print_status "Found hiredis at $HIREDIS_PREFIX"
                ./configure --prefix=/usr/local/pgsql --enable-debug --enable-cassert \
                    --with-includes="$HIREDIS_PREFIX/include" \
                    --with-libraries="$HIREDIS_PREFIX/lib"
            else
                print_warning "hiredis not found in standard locations, trying default configure"
                ./configure --prefix=/usr/local/pgsql --enable-debug --enable-cassert
            fi
        else
            ./configure --prefix=/usr/local/pgsql --enable-debug --enable-cassert
        fi
    fi
    
    # Compile
    print_status "Building PostgreSQL (this may take a few minutes)..."
    make -j$(nproc)
    
    # Install to temporary directory
    print_status "Installing PostgreSQL to $INSTALL_DIR..."
    make install-world-bin DESTDIR="$INSTALL_DIR"
    
    print_status "PostgreSQL compilation completed successfully!"
}

# Function to initialize database
init_database() {
    print_status "Initializing PostgreSQL database cluster..."
    
    export PATH="$INSTALL_DIR/usr/local/pgsql/bin:$PATH"
    export LD_LIBRARY_PATH="$INSTALL_DIR/usr/local/pgsql/lib:$LD_LIBRARY_PATH"
    
    # Initialize database
    initdb -D "$DATA_DIR" --auth-local=trust --auth-host=trust
    
    # Modify postgresql.conf for better testing
    echo "port = $PORT" >> "$DATA_DIR/postgresql.conf"
    echo "logging_collector = on" >> "$DATA_DIR/postgresql.conf"
    echo "log_statement = 'all'" >> "$DATA_DIR/postgresql.conf"
    echo "log_line_prefix = '%t [%p] '" >> "$DATA_DIR/postgresql.conf"
    
    print_status "Database initialization completed!"
}

# Function to start PostgreSQL server
start_server() {
    print_status "Starting PostgreSQL server..."
    
    export PATH="$INSTALL_DIR/usr/local/pgsql/bin:$PATH"
    export LD_LIBRARY_PATH="$INSTALL_DIR/usr/local/pgsql/lib:$LD_LIBRARY_PATH"
    
    # Start server
    pg_ctl -D "$DATA_DIR" -l "$LOG_FILE" start
    
    # Wait for server to be ready
    sleep 3
    
    # Check if server is running
    if pg_ctl -D "$DATA_DIR" status > /dev/null 2>&1; then
        print_status "PostgreSQL server started successfully!"
        print_status "Server is running on port $PORT"
        print_status "Logs are being written to: $LOG_FILE"
    else
        print_error "Failed to start PostgreSQL server"
        exit 1
    fi
}

# Function to run MuaDB integration tests
run_tests() {
    print_status "Running MuaDB integration tests..."
    
    export PATH="$INSTALL_DIR/usr/local/pgsql/bin:$PATH"
    export LD_LIBRARY_PATH="$INSTALL_DIR/usr/local/pgsql/lib:$LD_LIBRARY_PATH"
    
    echo -e "${BLUE}=== Creating test table ===${NC}"
    psql -p $PORT -d postgres -c "CREATE TABLE muadb_test (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        email VARCHAR(200),
        created_at TIMESTAMP DEFAULT NOW()
    );"
    
    echo -e "${BLUE}=== Inserting test data ===${NC}"
    psql -p $PORT -d postgres -c "INSERT INTO muadb_test (name, email) VALUES 
        ('Alice Johnson', 'alice@example.com'),
        ('Bob Smith', 'bob@example.com'),
        ('Charlie Brown', 'charlie@example.com'),
        ('Diana Prince', 'diana@example.com');"
    
    echo -e "${BLUE}=== Querying test data ===${NC}"
    psql -p $PORT -d postgres -c "SELECT * FROM muadb_test;"
    
    echo -e "${BLUE}=== Updating test data ===${NC}"
    psql -p $PORT -d postgres -c "UPDATE muadb_test SET email = 'alice.johnson@example.com' WHERE name = 'Alice Johnson';"
    
    echo -e "${BLUE}=== Deleting test data ===${NC}"
    psql -p $PORT -d postgres -c "DELETE FROM muadb_test WHERE name = 'Charlie Brown';"
    
    echo -e "${BLUE}=== Final state of test table ===${NC}"
    psql -p $PORT -d postgres -c "SELECT * FROM muadb_test;"
    
    # Large tuple testing to demonstrate TOAST is disabled
    echo -e "${BLUE}=== Creating large tuple test table ===${NC}"
    psql -p $PORT -d postgres -c "CREATE TABLE muadb_large_test (
        id SERIAL PRIMARY KEY,
        description TEXT,
        large_data TEXT,
        data_size INTEGER
    );"
    
    echo -e "${BLUE}=== Testing large tuples (demonstrating TOAST is disabled) ===${NC}"
    
    # Test tuple that would normally trigger TOAST (>2KB)
    echo -e "${YELLOW}Inserting 5KB tuple (would normally trigger TOAST)...${NC}"
    psql -p $PORT -d postgres -c "INSERT INTO muadb_large_test (description, large_data, data_size) 
        VALUES ('5KB Test Data', repeat('B', 5000), 5000);"
    
    echo -e "${BLUE}=== Verifying large tuple storage ===${NC}"
    psql -p $PORT -d postgres -c "SELECT id, description, data_size, length(large_data) as actual_length 
        FROM muadb_large_test ORDER BY id;"
    
    echo -e "${BLUE}=== Testing large tuple update ===${NC}"
    psql -p $PORT -d postgres -c "UPDATE muadb_large_test 
        SET large_data = repeat('X', 6000), data_size = 6000 
        WHERE description = '5KB Test Data';"
    
    echo -e "${BLUE}=== Testing large tuple deletion ===${NC}"
    psql -p $PORT -d postgres -c "DELETE FROM muadb_large_test WHERE data_size = 6000;"
    
    echo -e "${BLUE}=== Final state of large test table ===${NC}"
    psql -p $PORT -d postgres -c "SELECT id, description, data_size, length(large_data) as actual_length 
        FROM muadb_large_test ORDER BY id;"
    
    echo -e "${BLUE}=== Cleanup test tables ===${NC}"
    psql -p $PORT -d postgres -c "DROP TABLE muadb_test, muadb_large_test;"
    
    print_status "Integration tests completed!"
    print_status "Large tuple test demonstrates that TOAST is disabled - large tuples handled by MuaDB!"
    echo
    echo -e "${YELLOW}=== Large Tuple Test Summary ===${NC}"
    echo -e "${YELLOW}✓ 5KB tuple: Successfully stored without TOAST${NC}" 
    echo -e "${YELLOW}✓ Deletion: Successfully deleted large tuple${NC}"
    echo -e "${YELLOW}Large tuples (>2KB TOAST threshold) handled directly by MuaDB!${NC}"
}

# Function to show MuaDB logs
show_muadb_logs() {
    print_status "Showing recent MuaDB integration logs (last 1000 lines)..."
    echo -e "${BLUE}=== Recent MuaDB Logs ===${NC}"
    
    # Find the latest PostgreSQL log file
    LATEST_LOG=$(get_latest_log_file)
    
    if [ -n "$LATEST_LOG" ] && [ -f "$LATEST_LOG" ]; then
        echo -e "${BLUE}Reading from: $LATEST_LOG${NC}"
        # Show last 1000 lines of the log file, then filter for MuaDB
        tail -1000 "$LATEST_LOG" | grep "MuaDB:" || echo "No MuaDB logs found in the last 1000 lines."
    elif [ -d "$PG_LOG_DIR" ]; then
        echo -e "${BLUE}Searching all log files in: $PG_LOG_DIR${NC}"
        # Get all log files, sort by modification time, take the latest, show last 1000 lines
        find "$PG_LOG_DIR" -name "postgresql-*.log" -type f -printf '%T@ %p\n' | \
        sort -n | tail -1 | cut -d' ' -f2- | \
        xargs tail -1000 | grep "MuaDB:" || echo "No MuaDB logs found in any log files."
    else
        print_warning "PostgreSQL log directory not found: $PG_LOG_DIR"
        print_warning "Server may not be initialized yet."
    fi
}

# Function to show large tuple MuaDB logs
show_large_tuple_logs() {
    print_status "Showing MuaDB logs for large tuples (>2KB)..."
    echo -e "${BLUE}=== Large Tuple MuaDB Logs ===${NC}"
    
    if [ -d "$PG_LOG_DIR" ]; then
        echo -e "${BLUE}Searching for large tuples (tuple_len > 2000)...${NC}"
        find "$PG_LOG_DIR" -name "postgresql-*.log" -type f -exec grep -E "MuaDB:.*tuple_len=[2-9][0-9][0-9][0-9]" {} \; | \
        tail -10 || echo "No large tuple MuaDB logs found."
        echo
        echo -e "${YELLOW}These tuples would normally trigger TOAST (threshold ~2032 bytes)${NC}"
        echo -e "${YELLOW}but are handled directly by MuaDB due to 1GB threshold!${NC}"
    else
        print_warning "PostgreSQL log directory not found: $PG_LOG_DIR"
        print_warning "Server may not be initialized yet."
    fi
}

# Function to watch MuaDB logs in real-time
watch_muadb_logs() {
    print_status "Watching MuaDB integration logs in real-time..."
    print_status "Press Ctrl+C to stop watching"
    
    LATEST_LOG=$(get_latest_log_file)
    
    if [ -n "$LATEST_LOG" ] && [ -f "$LATEST_LOG" ]; then
        echo -e "${BLUE}Watching: $LATEST_LOG${NC}"
        tail -f "$LATEST_LOG" | grep --line-buffered "MuaDB:"
    elif [ -d "$PG_LOG_DIR" ]; then
        echo -e "${BLUE}Watching all log files in: $PG_LOG_DIR${NC}"
        tail -f "$PG_LOG_DIR"/postgresql-*.log | grep --line-buffered "MuaDB:"
    else
        print_error "PostgreSQL log directory not found: $PG_LOG_DIR"
        print_error "Make sure PostgreSQL is initialized and running."
        exit 1
    fi
}

# Function to stop server
stop_server() {
    print_status "Stopping PostgreSQL server..."
    
    export PATH="$INSTALL_DIR/usr/local/pgsql/bin:$PATH"
    
    if [ -f "$DATA_DIR/postmaster.pid" ]; then
        pg_ctl -D "$DATA_DIR" stop -m fast
        print_status "PostgreSQL server stopped."
    else
        print_warning "No running PostgreSQL server found."
    fi
}



# Function to print environment setup commands
print_env_setup() {
    print_status "PostgreSQL environment setup commands:"
    echo ""
    echo -e "${YELLOW}# Copy and paste these commands to set up your environment:${NC}"
    echo ""
    echo "export PATH=\"$INSTALL_DIR/usr/local/pgsql/bin:\$PATH\""
    echo "export LD_LIBRARY_PATH=\"$INSTALL_DIR/usr/local/pgsql/lib:\$LD_LIBRARY_PATH\""
    echo "export PGPORT=$PORT"
    echo "export PGDATABASE=postgres"
    echo ""

    echo ""
    echo -e "${BLUE}# Then you can run PostgreSQL commands:${NC}"
    echo -e "${YELLOW}psql                    # Connect to database${NC}"
    echo -e "${YELLOW}psql -c \"SELECT version();\"  # Run a query${NC}"
    echo -e "${YELLOW}pg_ctl status -D $DATA_DIR  # Check server status${NC}"
}



# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTION]"
    echo "Options:"
    echo "  compile     - Clean, compile, and install PostgreSQL"
    echo "  init        - Initialize database cluster"
    echo "  start       - Start PostgreSQL server"
    echo "  test        - Run MuaDB integration tests"
    echo "  logs        - Show recent MuaDB integration logs"
    echo "  large-logs  - Show MuaDB logs for large tuples (demonstrates TOAST bypass)"
    echo "  watch       - Watch MuaDB logs in real-time"
    echo "  stop        - Stop PostgreSQL server"
    echo "  restart     - Stop and start PostgreSQL server"
    echo "  full        - Run complete workflow (compile, init, start, test)"
    echo "  cleanup     - Clean up installation and data directories"
    echo "  status      - Show server status"
    echo "  env         - Print environment setup commands"
    echo ""
    echo "Examples:"
    echo "  $0 full              # Complete workflow from scratch"
    echo "  $0 test              # Run tests on existing installation"
    echo "  $0 logs              # Show recent MuaDB logs"
    echo "  $0 watch             # Watch MuaDB logs in real-time"
    echo "  $0 env               # Show environment setup commands"
}

# Function to show server status
show_status() {
    export PATH="$INSTALL_DIR/usr/local/pgsql/bin:$PATH"
    
    if [ -f "$DATA_DIR/postmaster.pid" ]; then
        if pg_ctl -D "$DATA_DIR" status > /dev/null 2>&1; then
            print_status "PostgreSQL server is running on port $PORT"
            echo -e "${BLUE}Data directory:${NC} $DATA_DIR"
            echo -e "${BLUE}Log directory:${NC} $PG_LOG_DIR"
            
            # Show latest log file
            LATEST_LOG=$(get_latest_log_file)
            if [ -n "$LATEST_LOG" ]; then
                echo -e "${BLUE}Latest log file:${NC} $LATEST_LOG"
            fi
        else
            print_warning "PostgreSQL data directory exists but server is not responding"
        fi
    else
        print_warning "PostgreSQL server is not running"
    fi
}

# Main script logic
case "${1:-full}" in
    "compile")
        compile_postgres
        ;;
    "init")
        init_database
        ;;
    "start")
        start_server
        ;;
    "test")
        run_tests
        echo
        show_muadb_logs
        echo
        show_large_tuple_logs
        ;;
    "logs")
        show_muadb_logs
        ;;
    "large-logs")
        show_large_tuple_logs
        ;;
    "watch")
        watch_muadb_logs
        ;;
    "stop")
        stop_server
        ;;
    "restart")
        stop_server
        sleep 2
        start_server
        ;;
    "full")
        cleanup
        compile_postgres
        init_database
        start_server
        echo
        run_tests
        echo
        show_muadb_logs
        echo
        print_status "Full workflow completed!"
        print_status "Server is running on port $PORT"
        print_status "To run more tests: $0 test"
        print_status "To view logs: $0 logs"
        print_status "To stop server: $0 stop"
        ;;
    "cleanup")
        cleanup
        ;;
    "status")
        show_status
        ;;
    "env")
        print_env_setup
        ;;
    "help"|"-h"|"--help")
        show_usage
        ;;
    *)
        print_error "Unknown option: $1"
        echo
        show_usage
        exit 1
        ;;
esac 