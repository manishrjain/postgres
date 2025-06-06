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
        ./configure --prefix=/usr/local/pgsql --enable-debug --enable-cassert
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
    
    print_status "Integration tests completed!"
}

# Function to show MuaDB logs
show_muadb_logs() {
    print_status "Showing recent MuaDB integration logs..."
    echo -e "${BLUE}=== Recent MuaDB Logs ===${NC}"
    
    # Find the latest PostgreSQL log file
    LATEST_LOG=$(get_latest_log_file)
    
    if [ -n "$LATEST_LOG" ] && [ -f "$LATEST_LOG" ]; then
        echo -e "${BLUE}Reading from: $LATEST_LOG${NC}"
        grep "MuaDB:" "$LATEST_LOG" | tail -20 || echo "No MuaDB logs found in the current log file."
    elif [ -d "$PG_LOG_DIR" ]; then
        echo -e "${BLUE}Searching all log files in: $PG_LOG_DIR${NC}"
        find "$PG_LOG_DIR" -name "postgresql-*.log" -type f -exec grep -l "MuaDB:" {} \; | \
        xargs grep "MuaDB:" | tail -20 || echo "No MuaDB logs found in any log files."
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

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTION]"
    echo "Options:"
    echo "  compile     - Clean, compile, and install PostgreSQL"
    echo "  init        - Initialize database cluster"
    echo "  start       - Start PostgreSQL server"
    echo "  test        - Run MuaDB integration tests"
    echo "  logs        - Show recent MuaDB integration logs"
    echo "  watch       - Watch MuaDB logs in real-time"
    echo "  stop        - Stop PostgreSQL server"
    echo "  restart     - Stop and start PostgreSQL server"
    echo "  full        - Run complete workflow (compile, init, start, test)"
    echo "  cleanup     - Clean up installation and data directories"
    echo "  status      - Show server status"
    echo ""
    echo "Examples:"
    echo "  $0 full       # Complete workflow from scratch"
    echo "  $0 test       # Run tests on existing installation"
    echo "  $0 logs       # Show recent MuaDB logs"
    echo "  $0 watch      # Watch MuaDB logs in real-time"
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
        ;;
    "logs")
        show_muadb_logs
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