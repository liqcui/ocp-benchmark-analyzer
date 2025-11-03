# ETCD Analyzer - AI-Powered Performance Analysis Platform

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green)](LICENSE)
[![MCP Protocol](https://img.shields.io/badge/MCP-Protocol-orange)](https://modelcontextprotocol.io)

## Overview

The **ETCD Analyzer** is a comprehensive AI-powered performance analysis and monitoring platform for OpenShift/Kubernetes etcd clusters. It combines the Model Context Protocol (MCP) server architecture with LangGraph-based AI agents to provide deep performance insights, automated root cause analysis, and actionable recommendations.

## Table of Contents

- [Architecture](#architecture)
- [Directory Structure](#directory-structure)
- [Key Components](#key-components)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage Examples](#usage-examples)
- [Configuration](#configuration)
- [Performance Metrics](#performance-metrics)
- [API Reference](#api-reference)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## Architecture

### Interactive Visual Topology

> 🎨 **[Click here to view the interactive, colorful architecture topology](./docs/architecture-topology.html)**

The ETCD Analyzer features a beautiful, interactive architecture visualization with:
- Color-coded layers with gradients
- Animated components and transitions
- Hover effects and interactions
- Responsive design for all devices
- Clear data flow visualization

To view the interactive diagram:
1. Save the topology HTML file from `docs/architecture-topology.html`
2. Open it in any modern web browser
3. Explore the interactive architecture

### High-Level System Design (Text Version)

```
                    ┌─────────────────────────┐
                    │  Clients (Web/CLI/API)  │
                    └───────────┬─────────────┘
                                │ HTTP/SSE
                                ▼
                ┌───────────────────────────────────┐
                │   AI Agent Layer (Port 8080)      │
                │  ┌─────────────────────────────┐  │
                │  │ ChatBot (LangGraph Agent)   │  │
                │  │ • Streaming Chat Interface  │  │
                │  │ • Tool Execution & Memory   │  │
                │  └─────────────────────────────┘  │
                │  ┌─────────────────────────────┐  │
                │  │ Specialized Agents          │  │
                │  │ • Performance Report Agent  │  │
                │  │ • Storage Agent (DuckDB)    │  │
                │  │ • Bottleneck Analysis Agent │  │
                │  └─────────────────────────────┘  │
                └───────────────┬───────────────────┘
                                │ MCP Protocol
                                ▼
            ┌───────────────────────────────────────────┐
            │   MCP Server Layer (Port 8000)            │
            │  ┌─────────────────────────────────────┐  │
            │  │ 15+ Performance Analysis Tools:     │  │
            │  │ • Cluster Status & Health           │  │
            │  │ • WAL Fsync & Backend Commit        │  │
            │  │ • Disk I/O & Network Performance    │  │
            │  │ • Node Usage & Resource Metrics     │  │
            │  │ • Deep Drive & Bottleneck Analysis  │  │
            │  │ • Performance Report Generation     │  │
            │  └─────────────────────────────────────┘  │
            └───────────────┬───────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Collectors   │  │   Analysis   │  │   Storage    │
│ • General    │  │ • Deep Drive │  │ • DuckDB     │
│ • WAL Fsync  │  │ • Reports    │  │ • Time Series│
│ • Disk I/O   │  │ • Bottleneck │  │ • ELT        │
│ • Network    │  │ • Utilities  │  │ • Queries    │
│ • Node Usage │  └──────────────┘  └──────────────┘
└──────┬───────┘
       │
       ▼
┌─────────────────────────────────────────────────┐
│  OpenShift/Kubernetes Cluster Infrastructure   │
│  ┌───────────────────────────────────────────┐ │
│  │ ETCD Cluster: etcd-0, etcd-1, etcd-2      │ │
│  │ Master Nodes: CPU, Memory, Network, Disk  │ │
│  │ Prometheus: Metrics Collection & Storage  │ │
│  └───────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘
```

### Architecture Layers Explained

#### 1. **Client Layer** (Purple Gradient 💻)
- **Web UI**: Interactive browser-based interface with real-time chat
- **CLI Client**: Terminal interface for command-line operations
- **REST API**: HTTP/HTTPS endpoints for programmatic access
- **Communication**: HTTP/SSE for streaming responses

#### 2. **AI Agent Layer** (Pink Gradient 🤖) - Port 8080
- **ChatBot Agent**: LangGraph React agent with conversation memory
- **Performance Report Agent**: Deep drive analysis with AI-powered insights
- **Storage Agent**: DuckDB ELT pipeline for historical data
- **Features**: Streaming, tool execution, custom prompts

#### 3. **MCP Server Layer** (Cyan Gradient 🔧) - Port 8000
- **15+ MCP Tools**: Health checks, metrics collection, analysis
- **Categories**: Status, general metrics, disk/network performance, advanced analysis
- **Protocol**: Model Context Protocol over HTTP/SSE
- **Transport**: FastMCP with streamable HTTP

#### 4. **Data Collection Layer** (Green Gradient 📦)
- **Collectors**: Specialized metric collectors for each subsystem
- **Analysis**: Deep drive, bottleneck detection, performance reports
- **ELT Pipeline**: Data transformation and visualization
- **Storage**: DuckDB time-series database

#### 5. **Infrastructure Layer** (Yellow-Pink Gradient ☁️)
- **ETCD Cluster**: 3-node cluster with Raft consensus
- **Master Nodes**: Control plane resources and management
- **Prometheus**: Metrics collection and PromQL engine

## Directory Structure

## Directory Structure

The ETCD Analyzer follows a modular architecture with components organized under the `etcd` namespace for multi-cluster support and extensibility.

```
etcd-analyzer/
│
├── analysis/etcd/                     # AI-powered analysis modules
│   └── etcd/                          # ETCD-specific analysis modules
│       ├── etcd_analyzer_performance_deepdrive.py
│       │   └── Comprehensive multi-subsystem performance analysis
│       ├── performance_analysis_report.py
│       │   └── Report generation with insights and recommendations
│       └── analysis_utility.py
│           └── Shared utilities for analysis (latency, summaries)
│
├── config/                            # Configuration management
│   ├── metrics_config_reader.py
│   │   └── Unified metrics configuration loader and validator
│   ├── metrics-etcd.yml
│   │   └── ETCD-specific metrics: WAL fsync, backend commit, proposals
│   ├── metrics-disk.yml
│   │   └── Disk I/O metrics: throughput, IOPS, device statistics
│   ├── metrics-net.yml
│   │   └── Network metrics: bandwidth, latency, packet statistics
│   ├── metrics-node.yml
│   │   └── Node resource metrics: CPU, memory, cgroup usage
│   ├── test_metrics_loading.py
│   │   └── Unit tests for metrics configuration loading
│   └── README.md
│       └── Configuration documentation and usage guide
│
├── elt/etcd/                          # Extract-Load-Transform pipeline
│   ├── etcd_analyzer_elt_backend_commit.py
│   ├── etcd_analyzer_elt_bottleneck.py
│   ├── etcd_analyzer_elt_cluster_info.py
│   ├── etcd_analyzer_elt_cluster_status.py
│   ├── etcd_analyzer_elt_compact_defrag.py
│   ├── etcd_analyzer_elt_deep_drive.py
│   ├── etcd_analyzer_elt_disk_io.py
│   ├── etcd_analyzer_elt_general_info.py
│   ├── etcd_analyzer_elt_network_io.py
│   ├── etcd_analyzer_elt_node_usage.py
│   ├── etcd_analyzer_elt_wal_fsync.py
│   │   └── Metric-specific ELT transformations
│   ├── etcd_analyzer_elt_json2table.py
│   │   └── JSON to HTML table converter for visualization
│   ├── etcd_analyzer_performance_elt_report.py
│   │   └── ELT-based performance reporting
│   └── etcd_analyzer_elt_utility.py
│       └── Shared ELT utilities
│
├── mcp/etcd/                          # MCP server and agents
│   ├── etcd_analyzer_mcp_server.py
│   │   └── FastMCP server with 15+ performance tools
│   ├── etcd_analyzer_client_chat.py
│   │   └── FastAPI app with LangGraph agent and streaming
│   ├── etcd_analyzer_mcp_agent_report.py
│   │   └── LangGraph StateGraph for comprehensive reporting
│   ├── etcd_analyzer_mcp_agent_stor2db.py
│   │   └── LangGraph StateGraph for DuckDB ELT
│   ├── etcd_analyzer_command.sh
│   │   └── Management script (start/stop/status/logs/client)
│   ├── etcd_analyzer_cluster.duckdb
│   │   └── DuckDB database file (created at runtime)
│   ├── exports/                       # Report exports directory
│   ├── logs/                          # Application logs
│   └── storage/etcd/                  # DuckDB storage modules
│       ├── etcd_analyzer_stor_backend_commit.py
│       ├── etcd_analyzer_stor_cluster_info.py
│       ├── etcd_analyzer_stor_compact_defrag.py
│       ├── etcd_analyzer_stor_disk_io.py
│       ├── etcd_analyzer_stor_disk_wal_fsync.py
│       ├── etcd_analyzer_stor_general_info.py
│       ├── etcd_analyzer_stor_network_io.py
│       │   └── Per-metric storage modules with schema management
│       └── etcd_analyzer_stor_utility.py
│           └── Storage utilities, time range handling
│
├── ocauth/                            # OpenShift authentication
│   └── openshift_auth.py
│       └── K8s/OCP authentication, token management, API clients
│
├── tools/                             # Metric collection tools (category-based)
│   ├── disk/                          # Disk I/O collectors
│   │   └── disk_io.py
│   │       └── Disk throughput, IOPS, device statistics
│   │
│   ├── etcd/                          # ETCD-specific collectors
│   │   ├── etcd_cluster_status.py
│   │   │   └── Cluster health, member list, leadership info
│   │   ├── etcd_general_info.py
│   │   │   └── CPU, memory, proposals, leadership metrics
│   │   ├── etcd_disk_wal_fsync.py
│   │   │   └── WAL fsync latency and performance
│   │   ├── etcd_disk_backend_commit.py
│   │   │   └── Backend commit operation performance
│   │   ├── etcd_disk_compact_defrag.py
│   │   │   └── Compaction and defragmentation metrics
│   │   └── etcd_network_io.py
│   │       └── Network bandwidth, latency, packet statistics
│   │
│   ├── net/                           # Network collectors
│   │   ├── network_io.py
│   │   │   └── Network I/O bandwidth and utilization
│   │   ├── network_l1.py
│   │   │   └── Layer 1 network statistics
│   │   ├── network_netstat4tcp.py
│   │   │   └── TCP netstat metrics
│   │   ├── network_netstat4udp.py
│   │   │   └── UDP netstat metrics
│   │   ├── network_socket4ip.py
│   │   │   └── IP socket statistics
│   │   ├── network_socket4tcp.py
│   │   │   └── TCP socket statistics
│   │   ├── network_socket4udp.py
│   │   │   └── UDP socket statistics
│   │   ├── network_socket4mem.py
│   │   │   └── Network memory statistics
│   │   └── network_socket4softnet.py
│   │       └── Softnet statistics
│   │
│   ├── node/                          # Node resource collectors
│   │   └── node_usage.py
│   │       └── Master node CPU, memory, cgroup metrics
│   │
│   ├── ocp/                           # OpenShift/Kubernetes collectors
│   │   └── cluster_info.py
│   │       └── Cluster information, node inventory
│   │
│   ├── ovnk/                          # OVN-Kubernetes collectors
│   │   └── (Future: OVN-Kubernetes networking metrics)
│   │
│   └── utils/                         # Shared utilities
│       ├── promql_basequery.py
│       │   └── Base Prometheus query utilities
│       └── promql_utility.py
│           └── PromQL helper functions and formatters
│
├── webroot/etcd/                      # Web interface
│   └── etcd_analyzer_mcp_llm.html
│       └── Interactive web UI for chat interface
│
├── pyproject.toml                     # Python project configuration
└── README.md                          # This file
```

### Directory Organization Principles

The new structure follows these principles:

1. **Namespace Isolation**: All ETCD-specific components are under `etcd/` subdirectories
2. **Modular Design**: Easy to add support for other distributed systems (e.g., `ceph/`, `cassandra/`)
3. **Category-Based Tools**: Collectors organized by metric category (disk, net, node, etcd, ocp, ovnk)
4. **Clear Separation**: MCP layer, tools, analysis, and storage are cleanly separated
5. **Centralized Configuration**: Unified metrics configuration system in `config/` with category-based YAML files
6. **Runtime Artifacts**: Logs, exports, and databases in dedicated directories

### Tools Architecture

The `tools/` directory follows a category-based organization matching the metrics configuration structure:

- **disk/**: Disk I/O performance collectors
- **etcd/**: ETCD-specific metrics (WAL, backend, proposals, leadership)
- **net/**: Network performance collectors (L1, TCP/UDP, sockets, netstat)
- **node/**: Node resource utilization collectors
- **ocp/**: OpenShift/Kubernetes cluster collectors
- **ovnk/**: OVN-Kubernetes networking collectors (extensible)
- **utils/**: Shared utilities (PromQL queries, formatters)

This structure enables:
- ✅ Easy discovery of collectors by category
- ✅ Parallel development of different metric categories
- ✅ Clear separation of concerns
- ✅ Extensibility for new platforms (OVN-K, storage, etc.)

## Features

### 🎨 Interactive Architecture Visualization
- **Beautiful, Colorful Topology**: Gradient color-coded layers
- **Animated Components**: Smooth transitions and hover effects
- **Responsive Design**: Works on desktop, tablet, and mobile
- **Clear Data Flow**: Visual arrows showing request/response flow
- **Technology Tags**: Shows frameworks and tools used in each layer
- **Statistics Dashboard**: Key metrics at a glance

### 🤖 AI-Powered Analysis
- **Conversational Interface**: Natural language queries
- **Streaming Responses**: Real-time result streaming
- **Tool Orchestration**: Automatic tool selection and execution
- **Context-Aware**: Maintains conversation history
- **Custom Prompts**: Configurable system prompts per conversation

### 📊 Comprehensive Metrics Collection
- **15+ MCP Tools**: Complete performance analysis toolkit
- **Real-time Monitoring**: Live cluster status and metrics
- **Historical Analysis**: DuckDB-based time-series storage
- **Multi-Dimensional**: CPU, memory, disk, network, consensus metrics

### 🔍 Advanced Performance Analysis
- **Deep Drive Analysis**: Multi-subsystem comprehensive review
- **Bottleneck Detection**: Automated performance issue identification
- **Root Cause Analysis**: Script-based + AI-powered RCA
- **Performance Reports**: Executive-ready documentation
- **Baseline Comparison**: Current vs. target performance

### 💾 Persistent Storage & ELT
- **DuckDB Backend**: High-performance analytical database
- **Time-Series Data**: Efficient temporal data storage
- **Query Interface**: SQL-based data access
- **Data Transformation**: JSON to HTML table conversion
- **Historical Trends**: Long-term performance tracking

## Key Components

### 1. MCP Server (`etcd_analyzer_mcp_server.py`)

**Core server implementing Model Context Protocol:**

- **Protocol**: FastMCP with streamable HTTP/SSE transport
- **Default Port**: 8000
- **Transport**: HTTP with Server-Sent Events (SSE)
- **Tools Exposed**: 15+ performance analysis tools

**Tool Categories:**

**Health & Status:**
- `get_server_health` - Server and collector initialization status
- `get_etcd_cluster_status` - Real-time cluster health via etcdctl
- `get_ocp_cluster_info` - Cluster details, node inventory, operators

**General Metrics:**
- `get_etcd_general_info` - CPU, memory, proposals, leadership, DB size
- `get_etcd_node_usage` - Master node CPU/memory/cgroup metrics

**Disk Performance:**
- `get_etcd_disk_wal_fsync` - WAL fsync P99 latency (critical metric)
- `get_etcd_disk_backend_commit` - Backend commit P99 latency
- `get_etcd_disk_io` - Disk throughput, IOPS, device statistics
- `get_etcd_disk_compact_defrag` - Compaction and defrag performance

**Network Performance:**
- `get_etcd_network_io` - Container network, peer latency, node utilization

**Advanced Analysis:**
- `get_etcd_performance_deep_drive` - Multi-subsystem comprehensive analysis
- `get_etcd_bottleneck_analysis` - Automated bottleneck detection
- `generate_etcd_performance_report` - Executive performance report

### 2. AI Agent Layer

#### ChatBot Client (`etcd_analyzer_client_chat.py`)

**FastAPI-based conversational AI interface:**

**Technology Stack:**
- **Framework**: LangChain + LangGraph
- **Agent**: React Agent with tool execution
- **LLM**: OpenAI-compatible API (Gemini, GPT-4, Claude, etc.)
- **Transport**: Server-Sent Events (SSE) for streaming

**Key Features:**
- Real-time streaming responses
- Multi-conversation management with memory
- Custom system prompt support per conversation
- Automatic tool result formatting (JSON → HTML tables)
- MCP tool integration via wrapper

**API Endpoints:**
- `GET /` or `GET /ui` - Web interface
- `POST /chat/stream` - Streaming chat (SSE)
- `POST /chat` - Non-streaming chat
- `GET /api/mcp/health` - MCP server connectivity
- `GET /api/etcd/health` - ETCD analyzer health
- `GET /api/tools` - List available MCP tools
- `POST /api/system-prompt` - Set conversation system prompt
- `GET /api/system-prompt/{id}` - Get current system prompt
- `DELETE /api/system-prompt/{id}` - Reset to default prompt

#### Performance Report Agent (`etcd_analyzer_mcp_agent_report.py`)

**LangGraph StateGraph agent for comprehensive reporting:**

**Workflow Nodes:**
1. **Initialize** - Setup test ID, duration, time range
2. **Collect Metrics** - Call MCP deep drive tool
3. **Analyze Performance** - Threshold-based analysis
4. **Script Analysis** - Rule-based root cause analysis
5. **AI Analysis** - LLM-powered deep analysis
6. **Generate Report** - Executive summary and recommendations

**Features:**
- Full streaming output for real-time progress
- Script-based + AI-based dual analysis
- Threshold violation detection
- Performance grading (excellent/good/fair/poor)
- Prioritized recommendations

#### Storage Agent (`etcd_analyzer_mcp_agent_stor2db.py`)

**LangGraph StateGraph agent for DuckDB persistence:**

**Workflow Nodes:**
1. **Initialize** - Setup testing ID and parameters
2. **Collect Cluster Info** - OCP cluster metadata
3. **Collect General Info** - General etcd metrics
4. **Collect WAL Fsync** - WAL performance data
5. **Collect Disk I/O** - Disk performance data
6. **Collect Network I/O** - Network performance data
7. **Collect Backend Commit** - Commit performance data
8. **Collect Compact Defrag** - Maintenance metrics
9. **Store Data** - Persist to DuckDB with summaries
10. **Finalize** - Generate final results

**Features:**
- Automatic schema management
- Time-series data storage
- Query by duration or time range
- Terminal summary tables
- Query stored data without re-collection

### 3. Data Collection Layer

#### Metric Collectors (`tools/`)

**Specialized collectors for each metric category:**

**General Info Collector:**
- CPU and memory usage per pod
- Proposal rates (commit, apply, failures)
- Leadership changes and stability
- Slow applies and read indexes
- Database size and key counts

**WAL Fsync Collector:**
- P99 latency per pod (critical SLA: <10ms)
- Operation rates and duration sums
- Cluster-wide performance analysis
- Health scoring

**Backend Commit Collector:**
- P99 latency per pod (target: <25ms)
- Commit rates and efficiency
- Performance recommendations

**Disk I/O Collector:**
- Container disk write rates
- Node disk read/write throughput
- IOPS by device
- Device inventory and mapping

**Network I/O Collector:**
- Container network RX/TX
- Peer-to-peer latency P99
- Client gRPC bandwidth
- Node network utilization
- Packet drops and errors
- Active watch/lease streams

**Compact/Defrag Collector:**
- Compaction duration and rates
- Defragmentation performance
- Page fault statistics (vmstat)

**Node Usage Collector:**
- CPU usage by mode (user, system, idle, iowait)
- Memory used, cache, buffers
- Cgroup CPU and RSS usage
- Resource capacity tracking

### 4. Analysis Layer (`analysis/`)

#### Deep Drive Analyzer (`etcd_analyzer_performance_deepdrive.py`)

**Comprehensive multi-subsystem analysis:**

**Subsystems Analyzed:**
- General cluster metrics
- WAL fsync performance
- Disk I/O performance
- Network I/O performance
- Backend commit performance
- Compact/defrag operations
- Node resource utilization

**Analysis Features:**
- Latency pattern analysis
- Cross-subsystem correlation
- Performance summary generation
- Health scoring
- Bottleneck detection with severity

**Bottleneck Categories:**
- Disk I/O bottlenecks (WAL fsync >100ms, commit >50ms)
- Network bottlenecks (peer latency >100ms, utilization >80%)
- Memory bottlenecks (usage >80%)
- Consensus bottlenecks (proposal failures, leader changes)
- Node resource bottlenecks (CPU >85%, memory >85%)

#### Performance Report Analyzer (`performance_analysis_report.py`)

**Enterprise-grade performance analysis and reporting:**

**Analysis Components:**
1. **Critical Metrics Analysis**
   - WAL fsync latency vs. 10ms threshold
   - Backend commit latency vs. 25ms threshold
   - Health status determination

2. **Supporting Metrics Analysis**
   - CPU usage per pod
   - Memory usage per pod
   - Network performance
   - Disk I/O throughput

3. **Node Usage Analysis**
   - Node CPU utilization
   - Node memory utilization
   - Cgroup resource consumption
   - Top resource consumers

4. **Baseline Comparison**
   - Current vs. target performance
   - Performance grade calculation
   - Pass/fail status per metric

5. **Recommendations**
   - Prioritized by severity (high/medium/low)
   - Category-based (disk, network, node, consensus)
   - Actionable with rationale

6. **Alerts**
   - Critical performance issues
   - Impact assessment
   - Required actions

**Report Features:**
- Executive summary
- Critical alerts section
- Detailed metric tables
- Baseline comparison
- Prioritized recommendations
- Analysis methodology documentation

### 5. Storage Layer (`storage/`)

**DuckDB-based persistent storage:**

**Features:**
- Schema auto-creation and migration
- Time-series data optimized tables
- Query by duration or time range
- Summary report generation
- Testing ID-based data isolation

**Storage Modules:**
- Cluster info storage
- General info metrics storage
- WAL fsync metrics storage
- Disk I/O metrics storage
- Network I/O metrics storage
- Backend commit metrics storage
- Compact/defrag metrics storage

**Query Capabilities:**
- Duration-based queries (e.g., "1h", "24h")
- Time range queries (UTC timestamps)
- Testing ID filtering
- Aggregated summaries
- Cross-metric joins

### 6. ELT Pipeline (`elt/`)

**Extract-Load-Transform for data processing:**

**Features:**
- JSON to HTML table conversion
- Data validation and cleaning
- Metric-specific transformations
- Performance-optimized processing
- Visualization-ready output

**ELT Modules:**
- Per-metric ELT processors
- JSON to HTML converter
- Performance report ELT
- Bottleneck analysis ELT
- Deep drive analysis ELT

## Installation

### Prerequisites

- Python 3.8 or higher
- Access to OpenShift/Kubernetes cluster
- KUBECONFIG configured
- Prometheus/Thanos accessible
- Node.js (optional, for MCP Inspector)

### Step 1: Clone Repository

```bash
git clone https://github.com/liqcui/etcd-performance-analyzer.git
cd etcd-performance-analyzer
```

### Step 2: Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate  # Windows
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt 
```
or 
```bash
pip3 install -e . 
```

**Required packages:**
```
fastmcp>=1.12.4
fastapi>=0.115.7
pydantic>=2.0.0
kubernetes>=29.0.0
prometheus-api-client>=0.5.3
requests>=2.31.0
pyyaml>=6.0.1
aiohttp>=3.8.0
uvicorn>=0.20.0
python-dateutil>=2.8.0
pytz>=2023.3
langchain>=0.1.0
langchain-openai>=0.0.5
langgraph>=0.0.20
duckdb>=0.9.0
```

### Step 4: Configure Environment

Create `.env` file:

```bash
# OpenAI-compatible API configuration
OPENAI_API_KEY=your-api-key-here
BASE_URL=https://your-llm-api-endpoint

# OpenShift configuration
KUBECONFIG=/path/to/your/kubeconfig

# Optional: MCP Inspector
ENABLE_MCP_INSPECTOR=0
MCP_INSPECTOR_URL=http://127.0.0.1:8000/sse
```

### Step 5: Verify KUBECONFIG

```bash
export KUBECONFIG=/path/to/kubeconfig
kubectl get nodes
oc get clusterversion  # For OpenShift
```

### Step 6: Setup Interactive Documentation (Optional)

```bash
# Create docs directory
mkdir -p docs

# The interactive architecture topology HTML is available in webroot/
cp webroot/architecture-topology.html docs/

# Open in browser to view
open docs/architecture-topology.html  # macOS
xdg-open docs/architecture-topology.html  # Linux
start docs/architecture-topology.html  # Windows
```

## Quick Start

### Prerequisites Check

Before starting, ensure you have:
- ✅ Python 3.8+ installed
- ✅ KUBECONFIG environment variable set
- ✅ Access to OpenShift/Kubernetes cluster
- ✅ Prometheus/Thanos accessible
- ✅ Virtual environment activated

### Setup Interactive Architecture Visualization

The project includes a beautiful, interactive architecture topology. To set it up:

```bash
# The HTML topology file should be in webroot/
# If missing, create it from the artifact provided in this README

# Create the file
cat > webroot/architecture-topology.html << 'EOF'
[Paste the complete HTML content from the topology artifact]
EOF

# View in browser
open webroot/architecture-topology.html
```

**Alternative**: The interactive topology HTML is also available as a GitHub artifact or can be generated from the documentation.

### Option 1: Management Script (Recommended)

```bash
# Make script executable
chmod +x etcd_analyzer_command.sh

# Start MCP server
./etcd_analyzer_command.sh start

# Check server status
./etcd_analyzer_command.sh status

# View logs
./etcd_analyzer_command.sh logs

# Start interactive client
./etcd_analyzer_command.sh client

# Stop server
./etcd_analyzer_command.sh stop
```

### Option 2: Manual Start

**Terminal 1 - Start MCP Server:**
```bash
source venv/bin/activate
export KUBECONFIG=/path/to/kubeconfig
python etcd_analyzer_mcp_server.py
```

**Terminal 2 - Start Chat Client:**
```bash
source venv/bin/activate
export KUBECONFIG=/path/to/kubeconfig
python etcd_analyzer_client_chat.py
```

**Access Web UI:**
```
http://localhost:8080/ui
```

### Option 3: Performance Report Agent

```bash
source venv/bin/activate
export KUBECONFIG=/path/to/kubeconfig
python etcd_analyzer_mcp_agent_report.py
```

### Option 4: Storage Agent

```bash
source venv/bin/activate
export KUBECONFIG=/path/to/kubeconfig
python etcd_analyzer_mcp_agent_stor2db.py
```

## Usage Examples

### Example 1: Chat Interface Analysis

**Web UI (Recommended):**
1. Open `http://localhost:8080/ui`
2. Type your question:
   ```
   Analyze etcd performance for the last hour
   ```
3. View streaming results with formatted tables

**CLI Chat:**
```bash
./etcd_analyzer_command.sh client
```

**Example Questions:**
- "Show me etcd cluster status"
- "Analyze WAL fsync performance for 2 hours"
- "What are the current performance bottlenecks?"
- "Generate a performance report for the last 24 hours"
- "Show master node resource utilization"
- "Compare current performance against baseline"

### Example 2: Direct API Calls

```bash
# Get server health
curl http://localhost:8080/api/mcp/health

# Get ETCD analyzer health
curl http://localhost:8080/api/etcd/health

# List available tools
curl http://localhost:8080/api/tools

# Non-streaming chat
curl -X POST http://localhost:8080/chat \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Analyze etcd performance",
    "conversation_id": "test-001"
  }'

# Call MCP tool directly
curl -X POST http://localhost:8080/api/tools/get_etcd_disk_wal_fsync \
  -H "Content-Type: application/json" \
  -d '{"duration": "1h"}'
```

### Example 3: Performance Report Agent

```bash
# Interactive mode
python etcd_analyzer_mcp_agent_report.py

# Select duration mode (option 1)
# Enter duration: 1h

# Or select time range mode (option 2)
# Enter start time: 2025-01-15 10:00:00
# Enter end time: 2025-01-15 11:00:00
```

**Output includes:**
- Real-time streaming progress
- Metrics collection status
- Script-based analysis
- AI-powered root cause analysis
- Comprehensive performance report
- Executive summary
- Prioritized recommendations

### Example 4: Storage Agent with DuckDB

```bash
# Run storage agent
python etcd_analyzer_mcp_agent_stor2db.py
```

**Features:**
- Collects all metrics
- Stores in DuckDB (`etcd_analyzer_cluster.duckdb`)
- Prints summary tables in terminal
- Enables historical analysis

**Query stored data:**
```python
import duckdb

# Connect to database
conn = duckdb.connect('etcd_analyzer_cluster.duckdb')

# Query WAL fsync data
result = conn.execute("""
    SELECT pod_name, avg_latency_ms, max_latency_ms
    FROM wal_fsync_p99_latency
    WHERE testing_id = 'your-test-id'
    ORDER BY avg_latency_ms DESC
""").fetchall()

print(result)
```

### Example 5: Python API Usage

```python
import asyncio
from etcd_analyzer_client_chat import MCPClient

async def analyze_performance():
    # Initialize client
    client = MCPClient(mcp_server_url="http://localhost:8000")
    
    # Connect to MCP server
    await client.connect()
    
    # Call performance analysis tool
    result = await client.call_tool(
        "get_etcd_performance_deep_drive",
        {"duration": "2h"}
    )
    
    print(result)

# Run analysis
asyncio.run(analyze_performance())
```

## Configuration

### Environment Variables

```bash
# Required
export KUBECONFIG=/path/to/kubeconfig

# Optional - automatically set to UTC
export TZ=UTC

# LLM Configuration
export OPENAI_API_KEY=your-api-key
export BASE_URL=https://api.openai.com/v1

# MCP Inspector (optional)
export ENABLE_MCP_INSPECTOR=1
export MCP_INSPECTOR_URL=http://127.0.0.1:8000/sse
```

### Metrics Configuration (`config/metrics-etcd.yml`)

**Structure:**
```yaml
metrics:
  - name: disk_wal_fsync_seconds_duration_p99
    query: |
      histogram_quantile(0.99, 
        rate(etcd_disk_wal_fsync_duration_seconds_bucket[5m])
      )
    description: "WAL fsync P99 latency"
    unit: seconds
    threshold: 0.01  # 10ms
    severity: critical
    
  - name: etcd_pods_cpu_usage
    query: |
      sum(rate(container_cpu_usage_seconds_total{
        namespace="openshift-etcd",
        container="etcd"
      }[5m])) by (pod)
    description: "etcd pod CPU usage"
    unit: percent
    threshold: 70
    severity: warning
```

**Customization:**
- Add custom metrics
- Modify PromQL queries
- Adjust thresholds
- Define severity levels
- Set units and descriptions

### Performance Thresholds

**Default thresholds (in `performance_analysis_report.py`):**

```python
thresholds = {
    'wal_fsync_p99_ms': 10.0,              # Critical for write performance
    'backend_commit_p99_ms': 25.0,         # Critical for persistence
    'cpu_usage_warning': 70.0,             # Pod CPU warning
    'cpu_usage_critical': 85.0,            # Pod CPU critical
    'memory_usage_warning': 70.0,          # Pod memory warning
    'memory_usage_critical': 85.0,         # Pod memory critical
    'peer_latency_warning_ms': 50.0,       # Network warning
    'peer_latency_critical_ms': 100.0,     # Network critical
    'network_utilization_warning': 70.0,   # Network utilization warning
    'network_utilization_critical': 85.0,  # Network utilization critical
}
```

**Customize thresholds:**
```python
# In your code
from analysis.etcd.performance_analysis_report import etcdReportAnalyzer

analyzer = etcdReportAnalyzer()
analyzer.thresholds['wal_fsync_p99_ms'] = 15.0  # More relaxed
```

## Performance Metrics

### Critical Metrics (SLA-defining)

#### 1. WAL Fsync P99 Latency
- **Metric**: `disk_wal_fsync_seconds_duration_p99`
- **Target**: < 10ms
- **Unit**: milliseconds
- **Impact**: Directly affects write performance
- **Critical Threshold**: > 100ms
- **Analysis**: Per-pod and cluster-wide

#### 2. Backend Commit P99 Latency
- **Metric**: `disk_backend_commit_duration_seconds_p99`
- **Target**: < 25ms
- **Unit**: milliseconds
- **Impact**: Database persistence performance
- **Critical Threshold**: > 50ms
- **Analysis**: Per-pod efficiency

### Supporting Metrics

#### 3. CPU Usage
- **Metrics**: `etcd_pods_cpu_usage`, `node_cpu_usage`
- **Target**: < 70%
- **Unit**: percent
- **Impact**: Resource contention, throttling
- **Collection**: Pod-level and node-level

#### 4. Memory Usage
- **Metrics**: `etcd_pods_memory_usage`, `node_memory_used`
- **Target**: < 70%
- **Unit**: percent or GB
- **Impact**: OOM risk, performance degradation
- **Collection**: Pod-level and node-level

#### 5. Network Performance
- **Metrics**: Peer latency, container network, node utilization
- **Target**: Peer latency < 50ms, utilization < 70%
- **Unit**: milliseconds, bytes/sec, percent
- **Impact**: Consensus, data replication

#### 6. Disk I/O
- **Metrics**: Throughput, IOPS, device statistics
- **Target**: > 100 MB/s write throughput
- **Unit**: bytes/sec, operations/sec
- **Impact**: Overall cluster performance

#### 7. Proposals
- **Metrics**: Commit rate, apply rate, failures, pending
- **Target**: 0 failures, < 10 pending
- **Unit**: operations/sec, count
- **Impact**: Consensus health

#### 8. Leadership
- **Metrics**: Leader changes, has_leader
- **Target**: < 1 change/hour
- **Unit**: changes/sec, boolean
- **Impact**: Cluster stability

#### 9. Database Size
- **Metrics**: DB size, compacted keys
- **Target**: Monitor growth, regular compaction
- **Unit**: bytes, count
- **Impact**: Performance, storage

#### 10. Node Resources
- **Metrics**: CPU by mode, memory, cgroup usage
- **Target**: < 70% utilization
- **Unit**: percent, GB
- **Impact**: Control plane stability

## API Reference

### MCP Server Tools

#### Health & Status Tools

**`get_server_health()`**
```python
Returns:
  status: str              # "healthy" or "unhealthy"
  timestamp: str           # ISO timestamp
  collectors_initialized: bool
  details: Dict[str, bool] # Per-collector status
```

**`get_etcd_cluster_status()`**
```python
Returns:
  status: str         # "success" or "error"
  data: Dict containing:
    - health: cluster health status
    - members: list of etcd members
    - endpoints: endpoint status
    - leadership: leader information
  timestamp: str
```

**`get_ocp_cluster_info()`**
```python
Returns:
  status: str         # "success" or "error"
  data: Dict containing:
    - cluster_name: str
    - cluster_version: str
    - platform: str (AWS/Azure/GCP/etc.)
    - nodes: Dict[str, Any]  # Node inventory
    - resources: Dict[str, int]  # Resource counts
  timestamp: str
```

#### General Metrics Tools

**`get_etcd_general_info(duration: str = "1h")`**
```python
Parameters:
  duration: str  # "15m", "30m", "1h", "2h", "6h", "12h", "24h", "1d"

Returns:
  status: str
  data: Dict containing:
    - cpu_usage: per-pod metrics
    - memory_usage: per-pod metrics
    - proposals: commit/apply/failure rates
    - leadership: changes and stability
    - database_size: size metrics
  timestamp: str
  duration: str
```

**`get_etcd_node_usage(duration: str = "1h")`**
```python
Parameters:
  duration: str

Returns:
  status: str
  data: Dict containing:
    - cpu_usage: per-node CPU by mode
    - memory_used: per-node memory consumption
    - memory_cache: cache and buffer stats
    - cgroup_cpu_usage: per-cgroup CPU
    - cgroup_rss_usage: per-cgroup memory
  timestamp: str
  duration: str
```

#### Disk Performance Tools

**`get_etcd_disk_wal_fsync(duration: str = "1h")`**
```python
Parameters:
  duration: str

Returns:
  status: str
  data: Dict containing:
    - p99_latency: per-pod P99 latency (ms)
    - operation_rates: ops/sec per pod
    - duration_stats: cumulative stats
    - cluster_analysis: cluster-wide health
  timestamp: str
  duration: str
```

**`get_etcd_disk_backend_commit(duration: str = "1h")`**
```python
Parameters:
  duration: str

Returns:
  status: str
  data: Dict containing:
    - p99_latency: per-pod commit latency (ms)
    - operation_rates: commits/sec
    - efficiency_analysis: performance assessment
  timestamp: str
  duration: str
```

**`get_etcd_disk_io(duration: str = "1h")`**
```python
Parameters:
  duration: str

Returns:
  status: str
  data: Dict containing:
    - container_write_rate: per-pod disk writes
    - node_throughput: read/write bytes/sec
    - node_iops: read/write operations/sec
    - device_inventory: storage device list
  timestamp: str
  duration: str
```

**`get_etcd_disk_compact_defrag(duration: str = "1h")`**
```python
Parameters:
  duration: str

Returns:
  status: str
  data: Dict containing:
    - compaction_duration: per-pod duration
    - defragmentation_duration: defrag stats
    - page_faults: vmstat pgmajfault
  timestamp: str
  duration: str
```

#### Network Performance Tools

**`get_etcd_network_io(duration: str = "1h")`**
```python
Parameters:
  duration: str

Returns:
  status: str
  data: Dict containing:
    - container_network: RX/TX per pod
    - peer_latency: peer-to-peer P99 latency
    - client_grpc: client bandwidth
    - node_utilization: network utilization %
    - packet_drops: drop rates
    - active_streams: watch/lease streams
  timestamp: str
  duration: str
```

#### Advanced Analysis Tools

**`get_etcd_performance_deep_drive(duration: str = "1h")`**
```python
Parameters:
  duration: str

Returns:
  status: str
  test_id: str  # Unique test identifier
  data: Dict containing all subsystem metrics
  analysis: Dict with:
    - latency_patterns: cross-subsystem analysis
    - correlations: performance correlations
  summary: Dict with:
    - key_findings: list of findings
    - health_scores: per-subsystem scores
  timestamp: str
  duration: str
```

**`get_etcd_bottleneck_analysis(duration: str = "1h")`**
```python
Parameters:
  duration: str

Returns:
  status: str
  test_id: str
  bottleneck_analysis: Dict with:
    - disk_io_bottlenecks: list of disk issues
    - network_bottlenecks: list of network issues
    - memory_bottlenecks: list of memory issues
    - consensus_bottlenecks: list of consensus issues
  root_cause_analysis: List[Dict] with:
    - category: str
    - root_cause: str
    - evidence: str
    - likelihood: str
  performance_recommendations: List[Dict] with:
    - priority: str (high/medium/low)
    - category: str
    - recommendation: str
    - rationale: str
  timestamp: str
  duration: str
```

**`generate_etcd_performance_report(duration: str = "1h", test_id: Optional[str] = None)`**
```python
Parameters:
  duration: str
  test_id: Optional[str]  # Custom test ID (auto-generated if not provided)

Returns:
  status: str
  test_id: str
  analysis_results: Dict with:
    - critical_metrics_analysis: WAL/backend analysis
    - performance_summary: supporting metrics
    - node_usage_analysis: node resource analysis
    - baseline_comparison: current vs. target
    - recommendations: prioritized list
    - alerts: critical issues
  performance_report: str  # Formatted text report
  timestamp: str
  duration: str
```

### Chat Client API

#### Chat Endpoints

**`POST /chat/stream`**
```json
Request:
{
  "message": "Analyze etcd performance",
  "conversation_id": "optional-id",
  "system_prompt": "optional custom prompt"
}

Response: Server-Sent Events (SSE)
data: {"type": "message", "content": "...", "streaming": true}
data: {"type": "message", "content": "<table>...</table>", "tool_result": true}
data: {"type": "message_complete", "timestamp": "..."}
```

**`POST /chat`**
```json
Request:
{
  "message": "Show cluster status",
  "conversation_id": "test-001"
}

Response:
{
  "responses": [
    {"type": "message", "content": "..."},
    {"type": "message_complete"}
  ]
}
```

#### System Prompt Management

**`POST /api/system-prompt`**
```json
Request:
{
  "system_prompt": "You are an etcd expert...",
  "conversation_id": "test-001"
}

Response:
{
  "status": "success",
  "message": "System prompt updated",
  "conversation_id": "test-001"
}
```

**`GET /api/system-prompt/{conversation_id}`**
```json
Response:
{
  "conversation_id": "test-001",
  "system_prompt": "You are an etcd expert...",
  "is_default": false
}
```

**`DELETE /api/system-prompt/{conversation_id}`**
```json
Response:
{
  "status": "success",
  "message": "System prompt reset to default",
  "conversation_id": "test-001"
}
```

## Troubleshooting

### Common Issues

#### 1. MCP Server Won't Start

**Symptoms:**
```
Failed to start server
```

**Solutions:**
```bash
# Check KUBECONFIG
echo $KUBECONFIG
kubectl get nodes

# Check if port 8000 is already in use
lsof -i :8000
netstat -tuln | grep 8000

# Check logs
./etcd_analyzer_command.sh logs

# Check virtual environment
source venv/bin/activate
pip list | grep fastmcp
```

#### 2. Authentication Failures

**Symptoms:**
```
Failed to initialize OpenShift authentication
```

**Solutions:**
```bash
# Verify KUBECONFIG
export KUBECONFIG=/path/to/kubeconfig
kubectl auth can-i get pods -n openshift-etcd

# Check Prometheus access
kubectl get route -n openshift-monitoring

# Verify token
oc whoami -t

# Check permissions
oc get clusterrolebinding | grep cluster-monitoring
```

#### 3. Chat Client Connection Issues

**Symptoms:**
```
MCP server not accessible
```

**Solutions:**
```bash
# Check MCP server status
curl http://localhost:8000/health

# Restart MCP server
./etcd_analyzer_command.sh restart

# Check network connectivity
ping localhost
telnet localhost 8000

# Verify firewall rules
sudo iptables -L | grep 8000
```

#### 4. Missing Metrics

**Symptoms:**
```
No data returned for metrics
```

**Solutions:**
```bash
# Verify Prometheus is accessible
oc get pods -n openshift-monitoring | grep prometheus

# Check metric availability
oc exec -n openshift-monitoring prometheus-k8s-0 -- \
  promtool query instant http://localhost:9090 \
  'etcd_disk_wal_fsync_duration_seconds_bucket'

# Verify etcd pods are running
oc get pods -n openshift-etcd

# Check time range
# Ensure duration is within data retention period
```

#### 5. LLM API Errors

**Symptoms:**
```
Error in chat stream
```

**Solutions:**
```bash
# Check .env file
cat .env | grep OPENAI_API_KEY
cat .env | grep BASE_URL

# Test API connection
curl -H "Authorization: Bearer $OPENAI_API_KEY" \
  $BASE_URL/models

# Verify API key permissions
# Check rate limits
# Try different model
```

#### 6. DuckDB Storage Issues

**Symptoms:**
```
Failed to store data in DuckDB
```

**Solutions:**
```bash
# Check file permissions
ls -la etcd_analyzer_cluster.duckdb

# Check disk space
df -h

# Remove corrupted database
rm etcd_analyzer_cluster.duckdb

# Run agent again
python etcd_analyzer_mcp_agent_stor2db.py
```

#### 7. Memory/Performance Issues

**Symptoms:**
```
Slow performance or OOM errors
```

**Solutions:**
```bash
# Reduce collection duration
# Instead of "24h", use "1h" or "2h"

# Limit concurrent operations
# Reduce number of pods/nodes analyzed

# Increase system resources
# Add more RAM
# Use faster storage (SSD)

# Optimize queries
# Use specific time ranges instead of duration
```

### Debug Mode

Enable verbose logging:

```python
# In any Python file
import logging
logging.basicConfig(level=logging.DEBUG)
```

Or set environment variable:
```bash
export LOG_LEVEL=DEBUG
python etcd_analyzer_mcp_server.py
```

### Health Check Commands

```bash
# Check all components
./etcd_analyzer_command.sh status

# MCP server health
curl http://localhost:8000/health

# Chat client health
curl http://localhost:8080/api/mcp/health
curl http://localhost:8080/api/etcd/health

# ETCD cluster health
oc get pods -n openshift-etcd
oc rsh -n openshift-etcd etcd-master-0 \
  etcdctl endpoint health --cluster

# Prometheus health
oc get pods -n openshift-monitoring | grep prometheus
```

## Best Practices

### 1. Performance Analysis

**Recommended workflow:**
1. Start with cluster status check
2. Run general info collection (1h duration)
3. Analyze critical metrics (WAL fsync, backend commit)
4. Check node resource utilization
5. Run deep drive analysis for comprehensive view
6. Generate performance report for stakeholders

**Example:**
```bash
# Interactive chat
./etcd_analyzer_command.sh client

# Commands:
> show cluster status
> analyze general metrics for 1 hour
> check WAL fsync performance for 2 hours
> show node resource usage
> run deep drive analysis
> generate performance report
```

### 2. Regular Monitoring

**Schedule regular reports:**
```bash
# Daily performance report
0 2 * * * /path/to/etcd-analyzer/daily_report.sh

# daily_report.sh
#!/bin/bash
cd /path/to/etcd-analyzer
source venv/bin/activate
export KUBECONFIG=/path/to/kubeconfig
python etcd_analyzer_mcp_agent_report.py --duration 24h \
  > /var/log/etcd-reports/$(date +%Y%m%d).log
```

### 3. Baseline Establishment

**Create performance baselines:**
```python
# Collect baseline during normal operations
python etcd_analyzer_mcp_agent_stor2db.py

# Store multiple samples
# Run during different time periods
# Compare against baseline in future analyses
```

### 4. Alert Thresholds

**Customize for your environment:**
```python
# Adjust in performance_analysis_report.py
thresholds = {
    'wal_fsync_p99_ms': 15.0,  # Relaxed for slower storage
    'cpu_usage_warning': 60.0,  # More aggressive warning
}
```

### 5. Data Retention

**Manage DuckDB size:**
```bash
# Periodic cleanup
duckdb etcd_analyzer_cluster.duckdb

# Delete old testing IDs
DELETE FROM wal_fsync_p99_latency 
WHERE testing_id IN (
  SELECT DISTINCT testing_id 
  FROM cluster_info 
  WHERE timestamp < NOW() - INTERVAL '30 days'
);

# Vacuum database
VACUUM;
```

## Contributing

### Development Setup

```bash
# Clone repository
git clone https://github.com/your-org/etcd-analyzer.git
cd etcd-analyzer

# Create development environment
python3 -m venv venv
source venv/bin/activate

# Install in development mode
pip install -e .

# Install development dependencies
pip install pytest pytest-asyncio black flake8 mypy
```

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_collectors.py

# Run with coverage
pytest --cov=. --cov-report=html
```

### Code Style

```bash
# Format code
black .

# Lint code
flake8 .

# Type checking
mypy .
```

### Adding New Metrics

1. **Define metric in `config/metrics-etcd.yml`:**
```yaml
- name: your_new_metric
  query: |
    your_promql_query_here
  description: "Metric description"
  unit: your_unit
  threshold: your_threshold
  severity: warning|critical
```

2. **Add collector method in `tools/etcd_your_category.py`:**
```python
async def collect_your_metric(self, duration: str):
    # Implementation
    pass
```

3. **Add MCP tool in `etcd_analyzer_mcp_server.py`:**
```python
@mcp.tool()
async def get_etcd_your_metric(duration: str = "1h"):
    """Tool description"""
    # Implementation
    pass
```

4. **Add storage in `storage/etcd_analyzer_stor_your_metric.py`:**
```python
class YourMetricStorELT:
    # Implementation
    pass
```

5. **Update documentation**

### Pull Request Process

1. Fork the repository
2. Create feature branch (`git checkout -b feature/your-feature`)
3. Commit changes (`git commit -am 'Add new feature'`)
4. Push to branch (`git push origin feature/your-feature`)
5. Create Pull Request

**PR Guidelines:**
- Clear description of changes
- Tests for new features
- Documentation updates
- Follow code style guidelines
- Reference related issues

## License

Apache License 2.0 - see LICENSE file for details

## Support

- **Issues**: https://github.com/your-org/etcd-analyzer/issues
- **Discussions**: https://github.com/your-org/etcd-analyzer/discussions
- **Documentation**: https://docs.your-org.com/etcd-analyzer
- **Email**: support@your-org.com

## Acknowledgments

- **MCP Protocol**: [Model Context Protocol](https://modelcontextprotocol.io)
- **LangChain**: [LangChain Framework](https://langchain.com)
- **LangGraph**: [LangGraph](https://langchain-ai.github.io/langgraph/)
- **FastMCP**: [FastMCP Library](https://github.com/jlowin/fastmcp)
- **ETCD**: [ETCD Project](https://etcd.io)
- **OpenShift**: [Red Hat OpenShift](https://www.redhat.com/en/technologies/cloud-computing/openshift)

## Roadmap

### Planned Features

- [ ] Multi-cluster support
- [ ] Historical trend analysis
- [ ] Anomaly detection with ML
- [ ] Custom alert rules
- [ ] Grafana integration
- [ ] Slack/Teams notifications
- [ ] Performance prediction
- [ ] Automated remediation suggestions
- [ ] Multi-language support
- [ ] Web-based configuration UI

### Version History

- **v1.0.0** - Initial release with MCP server and AI agents
- **v1.1.0** - Added node usage metrics and enhanced analysis
- **v1.2.0** - DuckDB storage and ELT pipeline
- **v2.0.0** - (Planned) Multi-cluster and ML features

---

**Built with ❤️ for the ETCD and OpenShift community**