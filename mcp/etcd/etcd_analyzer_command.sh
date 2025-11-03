#!/bin/bash
#
# OpenShift etcd Analyzer MCP Server Launch Script
# Usage: ./etcd_analyzer_command.sh [start|stop|restart|status|logs|client|help]
#

set -e

# Configuration
MCP_SERVER_SCRIPT="etcd_analyzer_mcp_server.py"
CLIENT_SCRIPT="etcd_analyzer_client_chat.py"
MCP_PID_FILE="/tmp/ocp_etcd_analyzer_mcp.pid"
CLIENT_PID_FILE="/tmp/ocp_etcd_analyzer_client.pid"
MCP_LOG_FILE="/tmp/ocp_etcd_analyzer_mcp.log"
CLIENT_LOG_FILE="/tmp/ocp_etcd_analyzer_client.log"
VENV_DIR="venv"

# Server ports
MCP_SERVER_PORT=8000
CLIENT_SERVER_PORT=8080

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Helper functions
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

warn() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

success() {
    echo -e "${CYAN}[SUCCESS]${NC} $1"
}

# Check if script exists
check_script() {
    local script=$1
    if [[ ! -f "$script" ]]; then
        error "Script not found: $script"
        error "Please ensure you're running this from the correct directory"
        exit 1
    fi
}

# Check Python environment
check_python_env() {
    # Check Python version
    if ! command -v python3 &> /dev/null; then
        error "Python 3 is not installed"
        exit 1
    fi
    
    local python_version=$(python3 --version 2>&1 | awk '{print $2}')
    info "Python version: $python_version"
    
    # Verify Python version meets requirements (>=3.8)
    local major=$(echo "$python_version" | cut -d. -f1)
    local minor=$(echo "$python_version" | cut -d. -f2)
    if [[ $major -lt 3 ]] || [[ $major -eq 3 && $minor -lt 8 ]]; then
        error "Python 3.8 or higher is required (found: $python_version)"
        exit 1
    fi
    
    # Check if virtual environment exists
    if [[ ! -d "$VENV_DIR" ]]; then
        warn "Virtual environment not found. Creating one..."
        python3 -m venv "$VENV_DIR"
        log "Virtual environment created at $VENV_DIR"
    fi
    
    # Activate virtual environment
    source "$VENV_DIR/bin/activate"
    
    # Upgrade pip first
    info "Upgrading pip..."
    pip install --upgrade pip setuptools wheel > /dev/null 2>&1
    
    # Check if required packages are installed
    if ! python -c "import fastmcp" 2>/dev/null; then
        warn "Required packages not found. Installing dependencies..."
        
        # Try to install from pyproject.toml first (preferred method)
        if [[ -f "pyproject.toml" ]]; then
            info "Installing from pyproject.toml..."
            pip install -e . || {
                warn "Failed to install from pyproject.toml, trying alternative methods..."
                # Fallback to requirements.txt if available
                if [[ -f "requirements.txt" ]]; then
                    info "Installing from requirements.txt..."
                    pip install -r requirements.txt
                else
                    error "Neither pyproject.toml nor requirements.txt installation succeeded"
                    error "Please check your project configuration"
                    exit 1
                fi
            }
        elif [[ -f "requirements.txt" ]]; then
            info "Installing from requirements.txt..."
            pip install -r requirements.txt
        else
            error "No pyproject.toml or requirements.txt found"
            error "Cannot install dependencies"
            exit 1
        fi
        
        log "Required packages installed successfully"
    else
        info "Dependencies already installed"
    fi
    
    # Verify critical imports
    info "Verifying critical dependencies..."
    local missing_deps=()
    
    if ! python -c "import fastmcp" 2>/dev/null; then
        missing_deps+=("fastmcp")
    fi
    if ! python -c "import fastapi" 2>/dev/null; then
        missing_deps+=("fastapi")
    fi
    if ! python -c "import langgraph" 2>/dev/null; then
        missing_deps+=("langgraph")
    fi
    if ! python -c "import langchain" 2>/dev/null; then
        missing_deps+=("langchain")
    fi
    if ! python -c "import kubernetes" 2>/dev/null; then
        missing_deps+=("kubernetes")
    fi
    
    if [[ ${#missing_deps[@]} -gt 0 ]]; then
        error "Missing critical dependencies: ${missing_deps[*]}"
        error "Please run: pip install -e . or pip install -r requirements.txt"
        exit 1
    fi
    
    success "All critical dependencies verified"
}

# Check KUBECONFIG
check_kubeconfig() {
    if [[ -z "$KUBECONFIG" ]]; then
        error "KUBECONFIG environment variable is not set"
        error "Please set KUBECONFIG to point to your OpenShift cluster configuration"
        error "Example: export KUBECONFIG=/path/to/your/kubeconfig"
        exit 1
    fi
    
    if [[ ! -f "$KUBECONFIG" ]]; then
        error "KUBECONFIG file not found: $KUBECONFIG"
        exit 1
    fi
    
    info "Using KUBECONFIG: $KUBECONFIG"
}

# Check OpenAI API configuration
check_openai_config() {
    if [[ -z "$OPENAI_API_KEY" ]] && [[ ! -f ".env" ]]; then
        warn "OPENAI_API_KEY not set and .env file not found"
        warn "Client chat functionality may not work properly"
        warn "Set OPENAI_API_KEY or create .env file with OPENAI_API_KEY and BASE_URL"
        return 1
    fi
    
    if [[ -f ".env" ]]; then
        info "Found .env file for OpenAI configuration"
    else
        info "Using OPENAI_API_KEY from environment"
    fi
    return 0
}

# Set timezone to UTC
set_timezone() {
    export TZ=UTC
    info "Timezone set to UTC"
}

# Start the MCP server
start_mcp_server() {
    log "Starting OpenShift etcd Analyzer MCP Server..."
    
    # Check prerequisites
    check_script "$MCP_SERVER_SCRIPT"
    check_python_env
    check_kubeconfig
    set_timezone
    
    # Check if server is already running
    if [[ -f "$MCP_PID_FILE" ]] && kill -0 "$(cat "$MCP_PID_FILE")" 2>/dev/null; then
        warn "MCP Server is already running (PID: $(cat "$MCP_PID_FILE"))"
        return 0
    fi
    
    # Start server in background
    source "$VENV_DIR/bin/activate"
    nohup python "$MCP_SERVER_SCRIPT" > "$MCP_LOG_FILE" 2>&1 &
    SERVER_PID=$!
    
    # Save PID
    echo $SERVER_PID > "$MCP_PID_FILE"
    
    # Wait and check if server started successfully
    sleep 3
    if kill -0 $SERVER_PID 2>/dev/null; then
        success "MCP Server started successfully (PID: $SERVER_PID)"
        log "Server logs: $MCP_LOG_FILE"
        log "Server listening on: http://localhost:$MCP_SERVER_PORT"
        
        # Test MCP endpoint
        info "Testing MCP endpoint..."
        sleep 2
        if curl -s "http://localhost:$MCP_SERVER_PORT/mcp" > /dev/null 2>&1; then
            success "✅ MCP endpoint is accessible"
        else
            warn "⚠️  MCP endpoint not responding yet - server may still be initializing"
        fi
    else
        error "Failed to start MCP server"
        rm -f "$MCP_PID_FILE"
        exit 1
    fi
}

# Start the client chat server
start_client_server() {
    log "Starting etcd Analyzer Client Chat Server..."
    
    # Check prerequisites
    check_script "$CLIENT_SCRIPT"
    check_python_env
    
    # Check OpenAI configuration
    if ! check_openai_config; then
        warn "Proceeding without OpenAI configuration - limited functionality"
    fi
    
    # Check if MCP server is running
    if ! curl -s "http://localhost:$MCP_SERVER_PORT/mcp" > /dev/null 2>&1; then
        error "MCP Server is not accessible at http://localhost:$MCP_SERVER_PORT"
        error "Please start the MCP server first: $0 start"
        exit 1
    fi
    
    # Check if client is already running
    if [[ -f "$CLIENT_PID_FILE" ]] && kill -0 "$(cat "$CLIENT_PID_FILE")" 2>/dev/null; then
        warn "Client Chat Server is already running (PID: $(cat "$CLIENT_PID_FILE"))"
        return 0
    fi
    
    # Start client in background
    source "$VENV_DIR/bin/activate"
    nohup python "$CLIENT_SCRIPT" > "$CLIENT_LOG_FILE" 2>&1 &
    CLIENT_PID=$!
    
    # Save PID
    echo $CLIENT_PID > "$CLIENT_PID_FILE"
    
    # Wait and check if client started successfully
    sleep 3
    if kill -0 $CLIENT_PID 2>/dev/null; then
        success "Client Chat Server started successfully (PID: $CLIENT_PID)"
        log "Client logs: $CLIENT_LOG_FILE"
        log "Web UI available at: http://localhost:$CLIENT_SERVER_PORT"
        log "API endpoints: http://localhost:$CLIENT_SERVER_PORT/docs"
        
        # Test client health
        info "Testing client health..."
        sleep 2
        if curl -s "http://localhost:$CLIENT_SERVER_PORT/api/mcp/health" > /dev/null; then
            success "✅ Client health check passed"
            echo ""
            success "🚀 Access the Web UI at: http://localhost:$CLIENT_SERVER_PORT"
        else
            warn "⚠️  Client not responding yet - may still be initializing"
        fi
    else
        error "Failed to start client server"
        rm -f "$CLIENT_PID_FILE"
        exit 1
    fi
}

# Stop the MCP server
stop_mcp_server() {
    log "Stopping OpenShift etcd Analyzer MCP Server..."
    
    if [[ -f "$MCP_PID_FILE" ]]; then
        local pid=$(cat "$MCP_PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid"
            sleep 2
            
            # Force kill if still running
            if kill -0 "$pid" 2>/dev/null; then
                warn "Force killing MCP server process..."
                kill -9 "$pid" 2>/dev/null || true
            fi
            
            rm -f "$MCP_PID_FILE"
            log "MCP Server stopped"
        else
            warn "MCP Server process not found (PID: $pid)"
            rm -f "$MCP_PID_FILE"
        fi
    else
        warn "MCP Server PID file not found. Server may not be running."
    fi
}

# Stop the client server
stop_client_server() {
    log "Stopping etcd Analyzer Client Chat Server..."
    
    if [[ -f "$CLIENT_PID_FILE" ]]; then
        local pid=$(cat "$CLIENT_PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid"
            sleep 2
            
            # Force kill if still running
            if kill -0 "$pid" 2>/dev/null; then
                warn "Force killing client server process..."
                kill -9 "$pid" 2>/dev/null || true
            fi
            
            rm -f "$CLIENT_PID_FILE"
            log "Client Chat Server stopped"
        else
            warn "Client server process not found (PID: $pid)"
            rm -f "$CLIENT_PID_FILE"
        fi
    else
        warn "Client Server PID file not found. Server may not be running."
    fi
}

# Start both servers
start_all() {
    start_mcp_server
    echo ""
    start_client_server
}

# Stop both servers
stop_all() {
    stop_client_server
    echo ""
    stop_mcp_server
}

# Restart both servers
restart_all() {
    log "Restarting OpenShift etcd Analyzer..."
    stop_all
    sleep 2
    start_all
}

# Check status of both servers
check_status() {
    echo ""
    info "=== MCP Server Status ==="
    if [[ -f "$MCP_PID_FILE" ]]; then
        local pid=$(cat "$MCP_PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            success "✅ MCP Server is running (PID: $pid)"
            
            # Check MCP endpoint
            if curl -s "http://localhost:$MCP_SERVER_PORT/mcp" > /dev/null 2>&1; then
                success "✅ MCP endpoint is accessible"
            else
                warn "⚠️  MCP process running but endpoint not responding"
            fi
        else
            warn "❌ PID file exists but process not running"
            rm -f "$MCP_PID_FILE"
        fi
    else
        info "❌ MCP Server is not running"
    fi
    
    echo ""
    info "=== Client Chat Server Status ==="
    if [[ -f "$CLIENT_PID_FILE" ]]; then
        local pid=$(cat "$CLIENT_PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            success "✅ Client Chat Server is running (PID: $pid)"
            
            # Check client health
            if curl -s "http://localhost:$CLIENT_SERVER_PORT/api/mcp/health" > /dev/null; then
                success "✅ Client health check passed"
                success "🌐 Web UI: http://localhost:$CLIENT_SERVER_PORT"
            else
                warn "⚠️  Client process running but health check failed"
            fi
        else
            warn "❌ PID file exists but process not running"
            rm -f "$CLIENT_PID_FILE"
        fi
    else
        info "❌ Client Chat Server is not running"
    fi
    echo ""
}

# Show server logs
show_logs() {
    local log_type="${1:-both}"
    
    case "$log_type" in
        mcp|server)
            if [[ -f "$MCP_LOG_FILE" ]]; then
                log "MCP Server logs (last 50 lines):"
                echo "========================================"
                tail -n 50 "$MCP_LOG_FILE"
                echo "========================================"
                info "Full logs: $MCP_LOG_FILE"
            else
                warn "MCP log file not found: $MCP_LOG_FILE"
            fi
            ;;
        client|chat)
            if [[ -f "$CLIENT_LOG_FILE" ]]; then
                log "Client Chat Server logs (last 50 lines):"
                echo "========================================"
                tail -n 50 "$CLIENT_LOG_FILE"
                echo "========================================"
                info "Full logs: $CLIENT_LOG_FILE"
            else
                warn "Client log file not found: $CLIENT_LOG_FILE"
            fi
            ;;
        both|*)
            show_logs mcp
            echo ""
            show_logs client
            ;;
    esac
}

# Show usage information
show_usage() {
    cat << EOF
${GREEN}OpenShift etcd Analyzer MCP Server Management${NC}
Usage: $0 [COMMAND] [OPTIONS]

${CYAN}Commands:${NC}
    start           Start both MCP server and Client Chat server
    stop            Stop both servers
    restart         Restart both servers
    status          Check status of both servers
    logs [TYPE]     Show server logs
                    TYPE: mcp|client|both (default: both)
    
    start-mcp       Start only MCP server
    stop-mcp        Stop only MCP server
    start-client    Start only Client Chat server
    stop-client     Stop only Client Chat server
    
    install         Install/update dependencies from pyproject.toml
    verify          Verify all dependencies are installed correctly
    clean           Clean virtual environment and reinstall
    
    help            Show this help message

${CYAN}Environment Variables:${NC}
    KUBECONFIG          Path to OpenShift/Kubernetes config file (required)
    OPENAI_API_KEY      OpenAI API key for chat functionality (optional)
    BASE_URL            Custom OpenAI API base URL (optional)
    TZ                  Timezone (automatically set to UTC)

${CYAN}Examples:${NC}
    # Install dependencies
    $0 install
    
    # Start both servers
    export KUBECONFIG=/path/to/kubeconfig
    export OPENAI_API_KEY=your-api-key
    $0 start
    
    # Check status
    $0 status
    
    # View logs
    $0 logs
    $0 logs mcp
    $0 logs client
    
    # Restart everything
    $0 restart
    
    # Verify installation
    $0 verify

${CYAN}Ports:${NC}
    MCP Server:      http://localhost:$MCP_SERVER_PORT
    Client Web UI:   http://localhost:$CLIENT_SERVER_PORT

${CYAN}Prerequisites:${NC}
    - Python 3.8+
    - Access to OpenShift cluster with KUBECONFIG
    - etcd monitoring enabled in OpenShift
    - OpenAI API key for chat functionality (optional)
    
${CYAN}Project Structure:${NC}
    analysis/       - Performance analysis modules
    config/         - Configuration files
    docs/           - Documentation
    elt/            - Extract-Load-Transform utilities
    ocauth/         - OpenShift authentication
    storage/        - Data storage modules
    tools/          - Metric collection tools
    webroot/        - Web UI files

For more information, see README.md
EOF
}

# Main script logic
case "${1:-help}" in
    start)
        start_all
        ;;
    stop)
        stop_all
        ;;
    restart)
        restart_all
        ;;
    status)
        check_status
        ;;
    logs)
        show_logs "${2:-both}"
        ;;
    start-mcp|start-server)
        start_mcp_server
        ;;
    stop-mcp|stop-server)
        stop_mcp_server
        ;;
    start-client|start-chat)
        start_client_server
        ;;
    stop-client|stop-chat)
        stop_client_server
        ;;
    install)
        install_dependencies
        ;;
    verify)
        verify_dependencies
        ;;
    clean)
        clean_install
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        error "Unknown command: $1"
        echo ""
        show_usage
        exit 1
        ;;
esac