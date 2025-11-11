#!/usr/bin/env python3
"""
OpenShift OVN-K Benchmark MCP Client with FastAPI and LangGraph Integration
Provides AI-powered chat interface for interacting with MCP tools
"""

import asyncio
import json
import logging
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, AsyncGenerator
import aiohttp
import traceback
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
from pydantic import BaseModel, Field

# MCP and LangGraph imports
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

# LangChain and LangGraph imports
from langchain.tools import BaseTool
from langchain.schema import BaseMessage
from langchain_openai import ChatOpenAI
from langchain.memory import ConversationBufferMemory
from langgraph.prebuilt import create_react_agent
from langgraph.graph.state import CompiledStateGraph
from langgraph.checkpoint.memory import MemorySaver
from dotenv import load_dotenv

# Ensure project root is on sys.path so package imports like `elt.*` work when run directly
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Fallback-safe import for HTML conversion utility
try:
    from elt.utils.analyzer_elt_json2table import convert_json_to_html_table
except Exception:
    def convert_json_to_html_table(json_data):
        # Minimal fallback: pretty JSON string when converter module not available
        try:
            return json.dumps(json_data, indent=2) if not isinstance(json_data, str) else json_data
        except Exception:
            return str(json_data)

import warnings
# Suppress urllib3 deprecation warning triggered by kubernetes client using HTTPResponse.getheaders()
warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    message=r"HTTPResponse\.getheaders\(\) is deprecated"
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add filter to suppress invalid HTTP request logs
class _SuppressInvalidHttp(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return "Invalid HTTP request received" not in msg

logging.getLogger("uvicorn").addFilter(_SuppressInvalidHttp())
logging.getLogger("uvicorn.error").addFilter(_SuppressInvalidHttp())
logging.getLogger("uvicorn.access").addFilter(_SuppressInvalidHttp())
logging.getLogger("uvicorn.protocols.http.h11_impl").addFilter(_SuppressInvalidHttp())
logging.getLogger("uvicorn.protocols.http.httptools_impl").addFilter(_SuppressInvalidHttp())

class ChatRequest(BaseModel):
    """Chat request model"""
    message: str = Field(..., description="User message to process")
    conversation_id: str = Field(default="default", description="Conversation identifier")
    system_prompt: Optional[str] = Field(default=None, description="Optional system prompt override")

class SystemPromptRequest(BaseModel):
    """System prompt configuration request"""
    system_prompt: str = Field(..., description="System prompt to set")
    conversation_id: str = Field(default="default", description="Conversation identifier")

class HealthResponse(BaseModel):
    """Health check response model"""
    status: str = Field(..., description="Health status")
    timestamp: str = Field(..., description="Check timestamp")
    details: Dict[str, Any] = Field(default_factory=dict, description="Additional details")

class MCPTool(BaseTool):
    """LangChain tool wrapper for MCP tools"""
    
    name: str
    description: str
    mcp_client: 'MCPClient'
    
    def __init__(self, name: str, description: str, mcp_client: 'MCPClient'):
        # Initialize BaseTool (pydantic model) with fields
        super().__init__(name=name, description=description, mcp_client=mcp_client)
    
    def _run(self, **kwargs) -> str:
        """Synchronous run - not used"""
        raise NotImplementedError("Use async version")
    
    async def _arun(self, **kwargs) -> str:
        """Asynchronous tool execution"""
        try:
            # Handle both direct kwargs and nested kwargs
            params = kwargs.get('kwargs', kwargs) or {}
            if isinstance(params, dict):
                # Remove None values to use tool defaults
                params = {k: v for k, v in params.items() if v is not None}
            
            # Wrap parameters in 'request' key as expected by MCP server
            wrapped_params = {"request": params} if params else {}
            
            result = await self.mcp_client.call_tool(self.name, wrapped_params)
            return json.dumps(result, indent=2)
        except Exception as e:
            logger.error(f"Error calling MCP tool {self.name}: {e}")
            return f"Error: {str(e)}"

class MCPClient:
    """MCP Client for interacting with the benchmark server"""
    
    def __init__(self, mcp_server_url: str = "http://localhost:8002"):
        self.mcp_server_url = mcp_server_url
        self.session = None
        self.available_tools: List[Dict[str, Any]] = []
        self.langchain_tools: List[MCPTool] = []
        
    async def connect(self):
        """Connect to MCP server and initialize tools"""
        try:
            url = f"{self.mcp_server_url}/mcp"

            # Connect to the server using Streamable HTTP
            async with streamablehttp_client(url) as (read_stream, write_stream, get_session_id):
                async with ClientSession(read_stream, write_stream) as session:
                    # Initialize the connection
                    await session.initialize()
 
                    # Get session id once connection established
                    session_id = get_session_id()
                    logger.info(f"Session ID: {session_id}")
                    tools_result = await session.list_tools()
                    logger.info(f"Discovered {len(tools_result.tools)} MCP tools")
                   
                    for tool in tools_result.tools:
                        logger.info(f"Tool discovered: {tool.name}")
                        # Handle schema that may be a dict or a Pydantic model
                        input_schema = {}
                        try:
                            if tool.inputSchema:
                                if isinstance(tool.inputSchema, dict):
                                    input_schema = tool.inputSchema
                                elif hasattr(tool.inputSchema, "model_dump"):
                                    input_schema = tool.inputSchema.model_dump()
                                elif hasattr(tool.inputSchema, "dict"):
                                    input_schema = tool.inputSchema.dict()
                        except Exception:
                            input_schema = {}
                        self.available_tools.append({
                            "name": tool.name,
                            "description": tool.description,
                            "input_schema": input_schema
                        })
                    self._create_langchain_tools()
                    logger.info(f"Loaded {len(self.available_tools)} MCP tools")
                    return True
                    
        except Exception as e:
            traceback.print_exc()
            logger.error(f"Failed to connect to MCP server: {e}")
            return False

    def _create_langchain_tools(self):
        """Create LangChain tool wrappers for MCP tools"""
        self.langchain_tools = []
        for tool_info in self.available_tools:
            langchain_tool = MCPTool(
                name=tool_info["name"],
                description=tool_info["description"],
                mcp_client=self
            )
            self.langchain_tools.append(langchain_tool)
    
    async def call_tool(self, tool_name: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Call an MCP tool with parameters"""
        try:
            url = f"{self.mcp_server_url}/mcp"

            async with streamablehttp_client(url) as (read_stream, write_stream, get_session_id):
                try:
                    async with ClientSession(read_stream, write_stream) as session:
                        await session.initialize()
                        if get_session_id:
                            logger.info(f"Session ID in call_tool: {get_session_id()}")
                        logger.info(f"Calling tool {tool_name} with params {params}")

                        # Parameters should already be wrapped in 'request' key by MCPTool._arun
                        # If not wrapped, wrap them here
                        if params is None:
                            request_params = {"request": {}}
                        elif "request" not in params:
                            request_params = {"request": params}
                        else:
                            request_params = params

                        result = await session.call_tool(tool_name, request_params)

                        # Enhanced error handling for JSON parsing
                        if not result.content:
                            logger.warning(f"Tool {tool_name} returned empty content")
                            return {"error": "Empty response from tool", "tool": tool_name}
                        
                        if len(result.content) == 0:
                            logger.warning(f"Tool {tool_name} returned no content items")
                            return {"error": "No content items in response", "tool": tool_name}
                        
                        content_text = result.content[0].text
                        if not content_text or content_text.strip() == "":
                            logger.warning(f"Tool {tool_name} returned empty text content")
                            return {"error": "Empty text content from tool", "tool": tool_name}
                        
                        # Handle different response formats
                        content_text = content_text.strip()
                        
                        # Try to parse as JSON first
                        try:
                            # Check if content looks like JSON
                            if content_text[:1] in ['{', '[']:
                                json_data = json.loads(content_text)
                                logger.info(f"Parsed JSON from {tool_name}")
                                
                                # Return health check data directly
                                if tool_name in ["get_mcp_health_status"]:
                                    return json_data
                                else:
                                    # Format other data as HTML table
                                    print("call_tool("+tool_name+"):",json_data)
                                    print("convert_json_to_html_table(json_data):",convert_json_to_html_table(json_data))
                                    return convert_json_to_html_table(json_data)
                                    # return json_data
                            else:
                                # Return plain text
                                return content_text
                                
                        except json.JSONDecodeError as json_err:
                            logger.error(f"Failed to parse JSON from tool {tool_name}. Content: '{content_text[:200]}...'")
                            logger.error(f"JSON decode error: {json_err}")
                            
                            # Check if it's a simple string response that should be JSON
                            if content_text.startswith('{') or content_text.startswith('['):
                                # Looks like malformed JSON
                                return {
                                    "error": f"Malformed JSON response: {str(json_err)}",
                                    "tool": tool_name,
                                    "raw_content": content_text[:500],
                                    "content_type": "malformed_json"
                                }
                            else:
                                # Return as plain text response
                                return {
                                    "result": content_text,
                                    "tool": tool_name,
                                    "content_type": "text",
                                    "message": "Tool returned plain text instead of JSON"
                                }
                except Exception as e:
                    logger.error(f"MCP ClientSession error for tool {tool_name}: {e}")
                    return {
                        "error": str(e),
                        "tool": tool_name,
                        "error_type": type(e).__name__
                    }
                    
        except Exception as e:
            logger.error(f"Error calling tool {tool_name}: {e}")
            # Return a structured error response instead of raising
            return {
                "error": str(e),
                "tool": tool_name,
                "error_type": type(e).__name__
            }

    async def check_mcp_connectivity_health(self):
        """Connect to MCP server and initialize tools"""
        try:
            url = f"{self.mcp_server_url}/mcp"

            # Connect to the server using Streamable HTTP
            async with streamablehttp_client(url) as (read_stream, write_stream, get_session_id):
                async with ClientSession(read_stream, write_stream) as session:
                    # Initialize the connection
                    await session.initialize()
 
                    # Get session id once connection established
                    session_id = get_session_id()
                    logger.info(f"Health check session ID: {session_id}")
                    if session_id:
                       return {
                        "status": "healthy",
                        "mcp_connection": "ok",
                        "last_check": datetime.now(timezone.utc).isoformat()
                      }
                    else:
                       return {
                        "status": "unhealthy",
                        "mcp_connection": "disconnected",
                        "last_check": datetime.now(timezone.utc).isoformat()
                      }
                    
        except Exception as e:
            traceback.print_exc()
            logger.error(f"Failed to connect to MCP server: {e}")
            return {
                "status": "unhealthy",
                "mcp_connection": "disconnected", 
                "error": str(e),
                "last_check": datetime.now(timezone.utc).isoformat()
            }

    async def check_cluster_connectivity_health(self) -> Dict[str, Any]:
        """Check cluster health via MCP tools"""
        try:
            # Test basic connectivity first
            await self.connect()
            
            # Try different health check tools in order of preference
            health_tools = ["get_mcp_health_status"]
            
            health_result = None
            tool_used = None
            
            for tool_name in health_tools:
                # Check if tool exists in available tools
                if any(tool["name"] == tool_name for tool in self.available_tools):
                    try:
                        logger.info(f"Trying health check with tool: {tool_name}")
                        health_result = await self.call_tool(tool_name, {})
                        
                        # Check if we got a valid result
                        if (isinstance(health_result, dict) and 
                            "error" not in health_result and 
                            health_result):
                            tool_used = tool_name
                            break
                        elif isinstance(health_result, dict) and "error" in health_result:
                            logger.warning(f"Tool {tool_name} returned error: {health_result.get('error')}")
                            continue
                            
                    except Exception as tool_error:
                        logger.warning(f"Tool {tool_name} failed: {str(tool_error)}")
                        continue
            
            # Analyze the health result
            if health_result and tool_used:
                # Extract health information based on tool type
                overall_health = "unknown"
                if tool_used == "get_mcp_health_status":
                    overall_health = health_result.get("status", "unknown")
                
                return {
                    "status": "healthy",
                    "prometheus_connection": "ok",
                    "tools_available": len(self.available_tools),
                    "overall_cluster_health": overall_health,
                    "tool_used": tool_used,
                    "last_check": datetime.now(timezone.utc).isoformat(),
                    "health_details": health_result
                }
            else:
                # No tools worked, but MCP connection is established
                return {
                    "status": "partial",
                    "prometheus_connection": "error",
                    "tools_available": len(self.available_tools),
                    "tool_error": "All health check tools failed or returned empty responses",
                    "overall_cluster_health": "unknown",
                    "last_check": datetime.now(timezone.utc).isoformat()
                }
                
        except Exception as e:
            logger.error(f"Cluster connectivity check failed: {e}")
            return {
                "status": "unhealthy",
                "prometheus_connection": "unknown", 
                "error": str(e),
                "overall_cluster_health": "unknown",
                "last_check": datetime.now(timezone.utc).isoformat()
            }

class ChatBot:
    """LangGraph-powered chatbot for MCP interaction"""
    
    def __init__(self, mcp_client: MCPClient):
        self.mcp_client = mcp_client
        self.memory = MemorySaver()
        self.conversations: Dict[str, CompiledStateGraph] = {}
        self.conversation_system_prompts: Dict[str, str] = {}
        
        # Default system prompt based on OVN-K benchmark capabilities
        self.default_system_prompt = """You are an expert OpenShift and OVN-Kubernetes network analysis assistant with deep knowledge of:

CLUSTER INFORMATION:
- OpenShift cluster infrastructure (AWS/Azure/GCP/bare-metal platforms)
- Node topology and roles (controlplane/master, worker, infra, workload nodes)
- Resource inventory (namespaces, pods, services, secrets, configmaps)
- Network policy resources (NetworkPolicies, AdminNetworkPolicies, BaselineAdminNetworkPolicies)
- Network resources (EgressFirewalls, EgressIPs, UserDefinedNetworks)
- Cluster operator status and Machine Config Pool (MCP) status

NETWORK LAYER 1 (PHYSICAL):
- Network interface operational status and link detection
- Traffic carrier status and physical connectivity
- Interface speed configuration and capacity (bits per second)
- MTU (Maximum Transmission Unit) settings
- ARP table entries and address resolution

NETWORK I/O PERFORMANCE:
- RX/TX bandwidth utilization and throughput
- Packet rates and processing statistics
- Packet drops, errors, and retransmissions
- Network saturation and congestion metrics
- Interface status and carrier detection
- gRPC active streams for API server load
- Connection tracking (conntrack entries and limits)
- FIFO queue depths and buffer management

SOCKET STATISTICS:
TCP Sockets:
- Socket allocations and in-use connections
- Orphan connections (detached from processes)
- TIME_WAIT accumulation and port exhaustion
- Total socket usage across protocols
- Fragment buffers and raw sockets

UDP Sockets:
- UDP and UDP lite socket consumption
- Datagram socket resource usage

Socket Memory:
- Fragment reassembly buffer memory
- TCP/UDP kernel buffer memory (pages and bytes)
- Socket memory allocation patterns
- Kernel buffer tuning requirements

Softnet Statistics:
- Packet processing performance at kernel level
- Softnet layer drops and backlog exhaustion
- CPU RPS (Receive Packet Steering) effectiveness
- Flow limit events and congestion indicators

IP/ICMP Metrics:
- IP-level incoming/outgoing traffic (octets)
- ICMP message rates and connectivity monitoring
- ICMP error rates and network issues

NETSTAT TCP METRICS:
- TCP segment traffic (in/out)
- Connection states (established, time-wait)
- Error conditions (listen overflow, drops, retransmissions)
- SYN cookie statistics for flood protection
- TCP timeout events
- Receive queue drops and out-of-order packets
- Connection capacity and limit management

NETSTAT UDP METRICS:
- UDP packet traffic (in/out)
- Receive errors and packet corruption
- No-listen errors (port not bound)
- UDP lite errors
- Buffer exhaustion (receive/transmit)

AVAILABLE MCP TOOLS:
1. get_mcp_health_status() - Check server and connectivity status
2. get_ocp_cluster_info() - Comprehensive cluster infrastructure details

Time-Range Capable Tools (support duration OR start_time/end_time):
3. get_network_l1_metrics(duration=None, start_time=None, end_time=None) - Layer 1 physical network metrics
4. get_cluster_network_io(duration=None, start_time=None, end_time=None) - Network I/O performance and throughput
5. get_network_socket_tcp_stats(duration=None, start_time=None, end_time=None) - TCP socket resource consumption
6. get_network_socket_udp_stats(duration=None, start_time=None, end_time=None) - UDP socket statistics
7. get_network_socket_memory(duration=None, start_time=None, end_time=None) - Socket memory and kernel buffers
8. get_network_socket_softnet_stats(duration=None, start_time=None, end_time=None, step="15s") - Softnet packet processing
9. get_network_socket_ip_metrics(duration=None, start_time=None, end_time=None, step="15s") - IP/ICMP layer statistics
10. get_network_netstat_tcp_metrics(duration=None, start_time=None, end_time=None, step="15s") - TCP protocol metrics
11. get_network_netstat_udp_metrics(duration=None, start_time=None, end_time=None) - UDP protocol metrics

TIME RANGE SPECIFICATION:
You can specify time ranges in THREE ways:

1. DURATION (relative to current time):
   - Format: '5m', '15m', '30m', '1h', '2h', '6h', '12h', '24h', '1d', '7d'
   - Example: duration="1h" means "last 1 hour from now"
   - Use for recent analysis and real-time monitoring

2. ABSOLUTE TIME RANGE (specific time window):
   - Format: ISO 8601 - "YYYY-MM-DDTHH:MM:SSZ" (UTC timezone)
   - Example: start_time="2025-01-15T10:00:00Z", end_time="2025-01-15T11:00:00Z"
   - Use for historical analysis and incident investigation
   - Maximum range: depends on Prometheus retention (typically 15 days)

3. MIXED (start_time + duration):
   - Specify start_time and duration
   - Example: start_time="2025-01-15T10:00:00Z", duration="2h"
   - End time calculated as start_time + duration

IMPORTANT TIME RANGE RULES:
- If ONLY duration provided: end_time=NOW, start_time=NOW-duration
- If start_time AND end_time provided: use exact range (duration ignored)
- If start_time AND duration provided: end_time=start_time+duration
- All times are in UTC timezone
- Default durations: '5m' for L1, '1h' for others if not specified

METRICS ORGANIZATION:
- Metrics grouped by node role: controlplane, worker (top 3), infra, workload
- Statistics include avg and max values over time period
- Step interval: '15s' for time-series queries (customizable for softnet/IP/TCP stats)

ANALYSIS APPROACH:
1. Start with cluster context (get_ocp_cluster_info)
2. Check physical layer connectivity (L1 metrics)
3. Analyze network I/O performance and saturation
4. Examine socket resource consumption (TCP/UDP/memory)
5. Review protocol-level statistics (netstat TCP/UDP)
6. Investigate packet processing (softnet statistics)
7. Correlate metrics across layers for root cause analysis

TIME-BASED ANALYSIS PATTERNS:

For Real-Time Monitoring:
- Use duration: "5m", "15m", "1h"
- Example: "Show me current network I/O for the last 15 minutes"
- Tool call: get_cluster_network_io(duration="15m")

For Incident Investigation:
- Use start_time and end_time for specific incident window
- Example: "Analyze network during incident from 10:00 to 11:00 on Jan 15"
- Tool call: get_cluster_network_io(start_time="2025-01-15T10:00:00Z", end_time="2025-01-15T11:00:00Z")

For Trend Analysis:
- Use longer durations: "6h", "12h", "24h"
- Example: "Show TCP socket trends over the last 24 hours"
- Tool call: get_network_socket_tcp_stats(duration="24h")

For Comparing Time Periods:
- Make multiple calls with different time ranges
- Example: "Compare network I/O between 09:00-10:00 and 14:00-15:00"
- Call 1: get_cluster_network_io(start_time="2025-01-15T09:00:00Z", end_time="2025-01-15T10:00:00Z")
- Call 2: get_cluster_network_io(start_time="2025-01-15T14:00:00Z", end_time="2025-01-15T15:00:00Z")

USE CASES:
- Network bottleneck identification and capacity planning
- Packet drop and error troubleshooting
- Socket exhaustion and resource leak detection
- Connection tracking table monitoring
- Buffer tuning and kernel parameter optimization
- Physical layer connectivity validation
- Protocol-level performance analysis
- API server load monitoring via gRPC streams
- Historical incident analysis and root cause investigation
- Performance trend analysis over time
- Before/after comparison for change validation
- Peak usage pattern identification

When analyzing network issues:
- Always start with health check and cluster info for context
- Choose appropriate time range based on analysis needs
- Look for patterns across node roles and time periods
- Correlate physical layer issues with higher-layer symptoms
- Check for resource exhaustion (sockets, memory, conntrack)
- Identify saturation points and bottlenecks
- Compare worker nodes to identify outliers
- Consider impact of network policies and OVN-Kubernetes configuration
- Use historical data to establish baselines and detect anomalies
- Correlate time ranges with known incidents or changes

TIME RANGE EXAMPLES IN USER QUERIES:
- "last 5 minutes" → duration="5m"
- "past hour" → duration="1h"
- "last 24 hours" → duration="24h"
- "from 10am to 11am today" → calculate UTC times for start_time/end_time
- "during the incident at 2pm yesterday" → calculate UTC time range
- "between Jan 15 10:00 and Jan 15 12:00" → start_time="2025-01-15T10:00:00Z", end_time="2025-01-15T12:00:00Z"

Always structure your responses with:
- Explain metrics and typical scenario that used for metrics
- Executive summary of findings
- Detailed technical analysis
- Specific recommendations with priority levels
- Next steps for investigation or remediation, always provides suggested tuning method, highlight with bold and green words
- Highlight critial issue or higher than threshold via red charactor/words, Using bold green or bold orange, or bold purple and other colour to distiguish different info/warning/status.
- If no specify the value of threshold, based on the industry standard.
- Make the analysis result readable and clear
- Don't create table.

Provide actionable insights with specific metric values, trends over time, and recommendations based on temporal patterns. explain the business impact of technical issues."""

        load_dotenv()
        api_key = os.getenv("OPENAI_API_KEY")
        base_url = os.getenv("BASE_URL")    

        self.llm = ChatOpenAI(
            model="gemini-2.5-pro",
            base_url=base_url,
            api_key=api_key,
            temperature=0.1,
            streaming=True         
        )

    def set_system_prompt(self, conversation_id: str, system_prompt: str):
        """Set custom system prompt for a conversation"""
        self.conversation_system_prompts[conversation_id] = system_prompt
        # Clear existing agent to force recreation with new prompt
        if conversation_id in self.conversations:
            del self.conversations[conversation_id]

    def get_system_prompt(self, conversation_id: str) -> str:
        """Get system prompt for a conversation"""
        return self.conversation_system_prompts.get(conversation_id, self.default_system_prompt)
    
    def get_or_create_agent(self, conversation_id: str) -> CompiledStateGraph:
        """Get or create a conversation agent with memory"""
        if conversation_id not in self.conversations:
            # Create agent with memory
            agent = create_react_agent(
                self.llm,
                self.mcp_client.langchain_tools,
                checkpointer=self.memory
            )
            self.conversations[conversation_id] = agent
            
        return self.conversations[conversation_id]
    
    async def chat_stream(self, message: str, conversation_id: str, system_prompt: Optional[str] = None) -> AsyncGenerator[Dict[str, Any], None]:
        """Stream chat response with proper message streaming"""
        try:
            # Set custom system prompt if provided
            if system_prompt:
                self.set_system_prompt(conversation_id, system_prompt)
            
            agent = self.get_or_create_agent(conversation_id)
            
            # Create thread config for this conversation
            config = {"configurable": {"thread_id": conversation_id}}
            
            # Track what we've already sent to avoid duplicates
            tool_results_sent = set()
            last_ai_content = ""
            
            # Get effective system prompt
            system_prompt_effective = self.get_system_prompt(conversation_id)
            
            # Construct input with system prompt
            input_messages = [("system", system_prompt_effective), ("user", message)]
            
            # Stream the response
            async for chunk in agent.astream({"messages": input_messages}, config=config):
                # Process different types of chunks
                if "agent" in chunk:
                    agent_data = chunk["agent"]
                    if "messages" in agent_data:
                        for msg in agent_data["messages"]:
                            if hasattr(msg, 'content') and msg.content:
                                content = str(msg.content)
                                
                                # Only send new content (avoid duplicates)
                                if content != last_ai_content:
                                    last_ai_content = content
                                    yield {
                                        "type": "message",
                                        "content": content,
                                        "timestamp": datetime.now(timezone.utc).isoformat(),
                                        "streaming": True,
                                        "tool_result": False
                                    }
                
                elif "tools" in chunk:
                    tool_data = chunk["tools"]
                    if "messages" in tool_data:
                        for msg in tool_data["messages"]:
                            if hasattr(msg, 'content') and msg.content:
                                try:
                                    content = msg.content
                                    
                                    # Handle different content types
                                    if isinstance(content, dict):
                                        # Check for HTML table or formatted content
                                        if any(key in content for key in ['html', 'table', 'content']):
                                            formatted_content = content.get('html') or content.get('table') or content.get('content') or json.dumps(content, indent=2)
                                        else:
                                            formatted_content = json.dumps(content, indent=2)
                                    elif isinstance(content, str):
                                        # Check if it's HTML or JSON
                                        if content.strip().startswith('<table') or content.strip().startswith('<!DOCTYPE') or '<html>' in content:
                                            formatted_content = content
                                        elif content[:1] in ['{', '[']:
                                            try:
                                                parsed = json.loads(content)
                                                formatted_content = json.dumps(parsed, indent=2)
                                            except:
                                                formatted_content = content
                                        else:
                                            formatted_content = content
                                    else:
                                        formatted_content = str(content)
                                    
                                    # Use hash to track duplicates
                                    tool_result_id = hash(str(content))
                                    if tool_result_id not in tool_results_sent:
                                        tool_results_sent.add(tool_result_id)
                                        
                                        yield {
                                            "type": "message",
                                            "content": formatted_content,
                                            "timestamp": datetime.now(timezone.utc).isoformat(),
                                            "tool_result": True,
                                            "streaming": False
                                        }
                                except Exception as e:
                                    logger.error(f"Error formatting tool result: {e}")
                                    yield {
                                        "type": "message",
                                        "content": str(msg.content),
                                        "timestamp": datetime.now(timezone.utc).isoformat(),
                                        "tool_result": True,
                                        "streaming": False
                                    }
            
            # Signal completion
            yield {
                "type": "message_complete",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
                        
        except Exception as e:
            logger.error(f"Error in chat stream: {e}")
            yield {
                "type": "error",
                "content": f"An error occurred: {str(e)}",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

# Global instances
mcp_client = MCPClient()
chatbot = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan manager"""
    global chatbot
    
    logger.info("Starting MCP Client...")
    
    # Initialize MCP client
    connected = await mcp_client.connect()
    if connected:
        logger.info("MCP Client connected successfully")
        # Initialize chatbot
        chatbot = ChatBot(mcp_client)
    else:
        logger.warning("Failed to connect to MCP server, continuing with limited functionality")
        chatbot = ChatBot(mcp_client)  # Create anyway for graceful degradation
    
    yield
    
    logger.info("Shutting down MCP Client...")

# Create FastAPI app
app = FastAPI(
    title="OVN-K Benchmark MCP Client",
    description="AI-powered chat interface for OpenShift OVN-K performance analysis",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve the web UI HTML (moved to project-level webroot)
HTML_FILE_PATH = os.path.join(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")),
    "webroot",
    "net",
    "network_analyzer_mcp_llm.html",
)

@app.get("/", include_in_schema=False)
async def serve_root_html():
    if os.path.exists(HTML_FILE_PATH):
        return FileResponse(HTML_FILE_PATH, media_type="text/html")
    raise HTTPException(status_code=404, detail="network_analyzer_mcp_llm.html not found")

@app.get("/ui", include_in_schema=False)
async def serve_ui_html():
    if os.path.exists(HTML_FILE_PATH):
        return FileResponse(HTML_FILE_PATH, media_type="text/html")
    raise HTTPException(status_code=404, detail="network_analyzer_mcp_llm.html not found")

@app.get("/api/mcp/health", response_model=HealthResponse)
async def mcp_health_check():
    """MCP connectivity health check endpoint"""
    health_data = await mcp_client.check_mcp_connectivity_health()
    return HealthResponse(
        status=health_data["status"],
        timestamp=health_data["last_check"],
        details=health_data
    )

@app.get("/api/cluster/health", response_model=HealthResponse)
async def cluster_health_check():
    """Cluster health check endpoint"""
    health_data = await mcp_client.check_cluster_connectivity_health()
    return HealthResponse(
        status=health_data["status"],
        timestamp=health_data["last_check"],
        details=health_data
    )

@app.get("/api/tools")
async def list_tools():
    """List available MCP tools"""
    return {
        "tools": mcp_client.available_tools,
        "count": len(mcp_client.available_tools)
    }

@app.post("/api/tools/{tool_name}")
async def call_tool_direct(tool_name: str, params: Dict[str, Any] = None):
    """Direct tool call endpoint"""
    try:
        # Wrap parameters in 'request' key as expected by MCP server
        wrapped_params = {"request": params} if params else {"request": {}}
        
        result = await mcp_client.call_tool(tool_name, wrapped_params)
        logger.info(f"Direct tool call result type: {type(result)}")
        
        # If the tool already returned a formatted HTML string, return as is
        if isinstance(result, str):
            return result
        # If the result is a dict with an error from the tool call, pass through
        if isinstance(result, dict) and result.get("error") and result.get("tool"):
            return result
        # Otherwise, return the result
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/system-prompt")
async def set_system_prompt(request: SystemPromptRequest):
    """Set custom system prompt for a conversation"""
    if not chatbot:
        raise HTTPException(status_code=503, detail="Chatbot not initialized")
    
    try:
        chatbot.set_system_prompt(request.conversation_id, request.system_prompt)
        return {
            "status": "success",
            "message": f"System prompt updated for conversation {request.conversation_id}",
            "conversation_id": request.conversation_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to set system prompt: {str(e)}")

@app.get("/api/system-prompt/{conversation_id}")
async def get_system_prompt(conversation_id: str):
    """Get current system prompt for a conversation"""
    if not chatbot:
        raise HTTPException(status_code=503, detail="Chatbot not initialized")
    
    try:
        system_prompt = chatbot.get_system_prompt(conversation_id)
        return {
            "conversation_id": conversation_id,
            "system_prompt": system_prompt,
            "is_default": system_prompt == chatbot.default_system_prompt
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get system prompt: {str(e)}")

@app.delete("/api/system-prompt/{conversation_id}")
async def reset_system_prompt(conversation_id: str):
    """Reset system prompt to default for a conversation"""
    if not chatbot:
        raise HTTPException(status_code=503, detail="Chatbot not initialized")
    
    try:
        if conversation_id in chatbot.conversation_system_prompts:
            del chatbot.conversation_system_prompts[conversation_id]
        if conversation_id in chatbot.conversations:
            del chatbot.conversations[conversation_id]
        
        return {
            "status": "success",
            "message": f"System prompt reset to default for conversation {conversation_id}",
            "conversation_id": conversation_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to reset system prompt: {str(e)}")

@app.post("/chat/stream")
async def chat_stream(request: ChatRequest):
    """Streaming chat endpoint"""
    if not chatbot:
        raise HTTPException(status_code=503, detail="Chatbot not initialized")
    
    async def generate():
        try:
            async for chunk in chatbot.chat_stream(request.message, request.conversation_id, request.system_prompt):
                # Format as Server-Sent Events
                yield f"data: {json.dumps(chunk)}\n\n"
        except Exception as e:
            error_chunk = {
                "type": "error",
                "content": f"Stream error: {str(e)}",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            yield f"data: {json.dumps(error_chunk)}\n\n"
        
        # Signal stream end
        completion_chunk = {
            "type": "stream_end",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        yield f"data: {json.dumps(completion_chunk)}\n\n"
    
    return StreamingResponse(
        generate(),
        media_type="text/plain",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "text/event-stream",
            "X-Accel-Buffering": "no"
        }
    )

@app.post("/chat")
async def chat_simple(request: ChatRequest):
    """Simple chat endpoint (non-streaming)"""
    if not chatbot:
        raise HTTPException(status_code=503, detail="Chatbot not initialized")
    
    responses = []
    async for chunk in chatbot.chat_stream(request.message, request.conversation_id, request.system_prompt):
        responses.append(chunk)
    
    return {"responses": responses}

if __name__ == "__main__":
    import uvicorn
    
    # Run the server
    uvicorn.run(
        "network_analyzer_client_chat:app",
        host="0.0.0.0",
        port=8082,
        ws="wsproto",
        reload=True,
        log_level="info"
    )