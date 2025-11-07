#!/bin/bash
# OpenShift OVN-Kubernetes Benchmark MCP Server Startup Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${SCRIPT_DIR}/logs"
STORAGE_DIR="${SCRIPT_DIR}/storage"
EXPORTS_DIR="${SCRIPT_DIR}/exports"
CONFIG_DIR="${SCRIPT_DIR}/config"

# Default values
MCP_SERVER_URL="http://localhost:8000"
COLLECTION_DURATION="5m"
REPORT_PERIOD_DAYS="7"
MODE=""
OPENAI_API_KEY=""
KUBECONFIG=""

# Functions
print_banner() {
    echo -e "${BLUE}"
    echo "=========================================="
    echo "OpenShift OVN-Kubernetes Benchmark Tool"
    echo "=========================================="
    echo -e "${NC}"
}

print_usage() {
    echo "Usage: $0 [OPTIONS] <MODE>"
    echo ""
    echo "MODES:"
    echo "  server           - Start the MCP server"
    echo "  collect          - Run performance data collection"
    echo "  report           - Generate performance report"
    echo "  full             - Run collection + report"
    echo "  setup            - Setup environment and dependencies"
    echo "  test             - Test connectivity and configuration"
    echo ""
    echo "OPTIONS:"
    echo "  -k, --kubeconfig PATH    Path to kubeconfig file"
    echo "  -d, --duration DURATION  Collection duration (default: 5m)"
    echo "  -p, --period DAYS        Report period in days (default: 7)"
    echo "  -u, --url URL            MCP server URL (default: http://localhost:8000)"
    echo "  -o, --openai-key KEY     OpenAI API key for AI insights"
    echo "  -h, --help               Show this help message"
    echo ""
    echo "EXAMPLES:"
    echo "  $0 setup"
    echo "  $0 server"
    echo "  $0 -k ~/.kube/config -d 10m collect"
    echo "  $0 -o sk-... -p 14 report"
    echo "  $0 -k ~/.kube/config -o sk-... full"
}

setup_environment() {
    echo -e "${YELLOW}Setting up environment...${NC}"
    
    # Create directories
    mkdir -p "$LOG_DIR" "$STORAGE_DIR" "$EXPORTS_DIR"
    
    # Check Python version
    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}❌ Python 3 is required but not installed${NC}"
        exit 1
    fi
    
    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1-2)
    echo -e "${GREEN}✅ Python version: $PYTHON_VERSION${NC}"
    
    # Install dependencies
    echo -e "${YELLOW}Installing Python dependencies...${NC}"
    pip3 install -r requirements.txt || {
        echo -e "${RED}❌ Failed to install dependencies${NC}"
        exit 1
    }
    
    # Check for required tools
    echo -e "${YELLOW}Checking for required tools...${NC}"
    
    if command -v kubectl &> /dev/null; then
        echo -e "${GREEN}✅ kubectl found${NC}"
    else
        echo -e "${YELLOW}⚠️  kubectl not found - required for cluster access${NC}"
    fi
    
    if command -v oc &> /dev/null; then
        echo -e "${GREEN}✅ oc (OpenShift CLI) found${NC}"
    else
        echo -e "${YELLOW}⚠️  oc not found - recommended for OpenShift clusters${NC}"
    fi
    
    echo -e "${GREEN}✅ Environment setup completed${NC}"
}

test_configuration() {
    echo -e "${YELLOW}Testing configuration...${NC}"
    
    # Test kubeconfig
    if [[ -n "$KUBECONFIG" ]]; then
        export KUBECONFIG="$KUBECONFIG"
        echo -e "${BLUE}Using kubeconfig: $KUBECONFIG${NC}"
    fi
    
    if kubectl cluster-info &> /dev/null; then
        echo -e "${GREEN}✅ Kubernetes cluster connectivity OK${NC}"
        kubectl get nodes --no-headers | wc -l | xargs echo "   Nodes:"
    else
        echo -e "${RED}❌ Cannot connect to Kubernetes cluster${NC}"
        echo -e "${YELLOW}Please check your kubeconfig and cluster connectivity${NC}"
        exit 1
    fi
    
    # Test OpenShift (if available)
    if oc status &> /dev/null; then
        echo -e "${GREEN}✅ OpenShift cluster detected${NC}"
        oc version --short 2>/dev/null | head -1 | xargs echo "   Version:"
    fi
    
    # Test Prometheus access (try to start server briefly to test)
    echo -e "${YELLOW}Testing Prometheus connectivity...${NC}"
    python3 -c "
import sys
sys.path.append('$SCRIPT_DIR')
from ocauth.openshift_auth import OpenShiftAuth
import asyncio

async def test_prometheus():
    try:
        auth = OpenShiftAuth('$KUBECONFIG')
        await auth.initialize()
        if auth.prometheus_url:
            print('✅ Prometheus discovered at:', auth.prometheus_url)
            return True
        else:
            print('❌ Prometheus not found')
            return False
    except Exception as e:
        print('❌ Prometheus test failed:', str(e))
        return False

result = asyncio.run(test_prometheus())
sys.exit(0 if result else 1)
    " && echo -e "${GREEN}✅ Prometheus connectivity OK${NC}" || echo -e "${YELLOW}⚠️  Prometheus connectivity issues${NC}"
    
    echo -e "${GREEN}✅ Configuration test completed${NC}"
}

start_server() {
    echo -e "${YELLOW}Starting MCP server...${NC}"
    
    # Set environment variables
    export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
    export TZ="UTC"
    
    # Create log file
    LOG_FILE="$LOG_DIR/mcp_server_$(date +%Y%m%d_%H%M%S).log"
    
    echo -e "${BLUE}Server starting on port 8000...${NC}"
    echo -e "${BLUE}Logs: $LOG_FILE${NC}"
    echo -e "${BLUE}Press Ctrl+C to stop${NC}"
    
    cd "$SCRIPT_DIR"
    python3 ovnk_benchmark_mcp_server.py 2>&1 | tee "$LOG_FILE"
}

run_collection() {
    echo -e "${YELLOW}Running performance data collection...${NC}"
    
    # Set environment variables
    export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
    export MCP_SERVER_URL="$MCP_SERVER_URL"
    export COLLECTION_DURATION="$COLLECTION_DURATION"
    export OPENAI_API_KEY="$OPENAI_API_KEY"
    export TZ="UTC"
    
    # Create log file
    LOG_FILE="$LOG_DIR/collection_$(date +%Y%m%d_%H%M%S).log"
    
    echo -e "${BLUE}Collection duration: $COLLECTION_DURATION${NC}"
    echo -e "${BLUE}MCP server: $MCP_SERVER_URL${NC}"
    echo -e "${BLUE}Logs: $LOG_FILE${NC}"
    
    cd "$SCRIPT_DIR"
    python3 ovnk_benchmark_mcp_agent_perfdata.py 2>&1 | tee "$LOG_FILE"
}

generate_report() {
    echo -e "${YELLOW}Generating performance report...${NC}"
    
    # Check for OpenAI API key
    if [[ -z "$OPENAI_API_KEY" ]]; then
        echo -e "${RED}❌ OpenAI API key is required for report generation${NC}"
        echo -e "${YELLOW}Please provide it with -o flag or set OPENAI_API_KEY environment variable${NC}"
        exit 1
    fi
    
    # Set environment variables
    export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
    export MCP_SERVER_URL="$MCP_SERVER_URL"
    export REPORT_PERIOD_DAYS="$REPORT_PERIOD_DAYS"
    export OPENAI_API_KEY="$OPENAI_API_KEY"
    export REPORT_OUTPUT_DIR="$EXPORTS_DIR"
    export TZ="UTC"
    
    # Create log file
    LOG_FILE="$LOG_DIR/report_$(date +%Y%m%d_%H%M%S).log"
    
    echo -e "${BLUE}Report period: $REPORT_PERIOD_DAYS days${NC}"
    echo -e "${BLUE}MCP server: $MCP_SERVER_URL${NC}"
    echo -e "${BLUE}Output directory: $EXPORTS_DIR${NC}"
    echo -e "${BLUE}Logs: $LOG_FILE${NC}"
    
    cd "$SCRIPT_DIR"
    python3 ovnk_benchmark_mcp_agent_report.py 2>&1 | tee "$LOG_FILE"
}

run_full_workflow() {
    echo -e "${YELLOW}Running full workflow: collection + report...${NC}"
    
    echo -e "${BLUE}Step 1/2: Performance Data Collection${NC}"
    run_collection
    
    if [[ $? -eq 0 ]]; then
        echo -e "${GREEN}✅ Collection completed successfully${NC}"
        echo -e "${BLUE}Step 2/2: Report Generation${NC}"
        sleep 2
        generate_report
        
        if [[ $? -eq 0 ]]; then
            echo -e "${GREEN}✅ Full workflow completed successfully${NC}"
            echo -e "${BLUE}Check the exports directory for generated reports${NC}"
        else
            echo -e "${RED}❌ Report generation failed${NC}"
            exit 1
        fi
    else
        echo -e "${RED}❌ Collection failed, skipping report generation${NC}"
        exit 1
    fi
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -k|--kubeconfig)
            KUBECONFIG="$2"
            shift 2
            ;;
        -d|--duration)
            COLLECTION_DURATION="$2"
            shift 2
            ;;
        -p|--period)
            REPORT_PERIOD_DAYS="$2"
            shift 2
            ;;
        -u|--url)
            MCP_SERVER_URL="$2"
            shift 2
            ;;
        -o|--openai-key)
            OPENAI_API_KEY="$2"
            shift 2
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        server|collect|report|full|setup|test)
            MODE="$1"
            shift
            ;;
        *)
            echo -e "${RED}❌ Unknown option: $1${NC}"
            print_usage
            exit 1
            ;;
    esac
done

# Main execution
print_banner

# Check if mode is provided
if [[ -z "$MODE" ]]; then
    echo -e "${RED}❌ No mode specified${NC}"
    print_usage
    exit 1
fi

# Execute based on mode
case $MODE in
    setup)
        setup_environment
        ;;
    test)
        test_configuration
        ;;
    server)
        start_server
        ;;
    collect)
        run_collection
        ;;
    report)
        generate_report
        ;;
    full)
        run_full_workflow
        ;;
    *)
        echo -e "${RED}❌ Invalid mode: $MODE${NC}"
        print_usage
        exit 1
        ;;
esac