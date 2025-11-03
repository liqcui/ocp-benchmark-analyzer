#!/usr/bin/env python3
"""
OVN-Kubernetes Benchmark MCP Agent - Performance Data Collection with Storage
AI agent using LangGraph to collect performance data via MCP server and store in DuckDB
Updated to match ETCD analyzer pattern with comprehensive storage integration
"""

import asyncio
import json
import uuid
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, TypedDict
import traceback
import os
from dotenv import load_dotenv

# MCP Streamable HTTP client imports
try:
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client
    MCP_CLIENT_AVAILABLE = True
except ImportError:
    MCP_CLIENT_AVAILABLE = False
    print("Warning: MCP client not available. Install with: pip install mcp")

# LangGraph imports
try:
    from langgraph.graph import StateGraph, END, START
    from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, BaseMessage
    from langchain_openai import ChatOpenAI
    AI_AVAILABLE = True
except ImportError:
    AI_AVAILABLE = False
    print("LangGraph/LangChain not available. Running in non-AI mode.")

# Storage imports
try:
    from storage.ovnk_benchmark_storage_ovnk import ovnkMetricStor
    STORAGE_AVAILABLE = True
except ImportError:
    STORAGE_AVAILABLE = False
    print("Warning: Storage module not available.")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AgentState(TypedDict):
    """State for the performance data collection agent"""
    messages: List[Any]
    current_step: str
    cluster_info: Optional[Dict[str, Any]]
    node_usage_data: Optional[Dict[str, Any]]
    basic_info_data: Optional[Dict[str, Any]]
    api_metrics_data: Optional[Dict[str, Any]]
    multus_metrics_data: Optional[Dict[str, Any]]
    ovnk_pods_data: Optional[Dict[str, Any]]
    ovnk_containers_data: Optional[Dict[str, Any]]
    ovs_metrics_data: Optional[Dict[str, Any]]
    latency_metrics_data: Optional[Dict[str, Any]]
    duration: str
    start_time: Optional[str]
    end_time: Optional[str]
    testing_id: str
    query_params: Dict[str, Any]
    results: Dict[str, Any]
    error: Optional[str]


class PerformanceDataAgent:
    """AI agent for collecting OVN-K performance data via MCP server with DuckDB storage"""
    
    def __init__(self, 
                mcp_server_url: str = "http://localhost:8000",
                openai_api_key: Optional[str] = None,
                duration: Optional[str] = "5m",
                start_time: Optional[str] = None,
                end_time: Optional[str] = None,
                use_ai: bool = True,
                db_path: str = "storage/ovnk_benchmark.db"):
        self.mcp_server_url = mcp_server_url.rstrip('/')
        self.duration = duration
        self.start_time = start_time
        self.end_time = end_time
        self.run_uuid = str(uuid.uuid4())
        self.use_ai = use_ai and AI_AVAILABLE
        self.db_path = db_path
        self.storage: Optional[ovnkMetricStor] = None
        self._storage_initialized = False
        
        if self.use_ai:
            load_dotenv()
            api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
            base_url = os.getenv("BASE_URL")
            
            if not api_key:
                logger.warning("OPENAI_API_KEY not provided. Running in non-AI mode.")
                self.use_ai = False
            else:
                self.llm = ChatOpenAI(
                    model="gemini-1.5-flash",
                    base_url=base_url,
                    api_key=api_key,
                    temperature=0.1,
                    streaming=True         
                )
        
        if self.use_ai:
            self.graph = self._create_graph()
        else:
            logger.info("Running in non-AI mode - direct collection without LangGraph")

    def _create_graph(self) -> StateGraph:
        """Create the LangGraph StateGraph workflow"""
        workflow = StateGraph(AgentState)
        
        # Add nodes
        workflow.add_node("initialize", self._initialize_node)
        workflow.add_node("collect_cluster_info", self._collect_cluster_info_node)
        workflow.add_node("collect_node_usage", self._collect_node_usage_node)
        workflow.add_node("collect_basic_info", self._collect_basic_info_node)
        workflow.add_node("collect_api_metrics", self._collect_api_metrics_node)
        workflow.add_node("collect_multus_metrics", self._collect_multus_metrics_node)
        workflow.add_node("collect_ovnk_pods", self._collect_ovnk_pods_node)
        workflow.add_node("collect_ovnk_containers", self._collect_ovnk_containers_node)
        workflow.add_node("collect_ovs_metrics", self._collect_ovs_metrics_node)
        workflow.add_node("collect_latency_metrics", self._collect_latency_metrics_node)
        workflow.add_node("store_data", self._store_data_node)
        workflow.add_node("finalize", self._finalize_node)
        workflow.add_node("handle_error", self._handle_error_node)
        
        # Set entry point
        workflow.add_edge(START, "initialize")
        
        # Add conditional edges with error handling
        workflow.add_conditional_edges(
            "initialize",
            self._should_handle_error,
            {
                "error": "handle_error",
                "continue": "collect_cluster_info"
            }
        )
        
        workflow.add_conditional_edges(
            "collect_cluster_info",
            self._should_handle_error,
            {
                "error": "handle_error",
                "continue": "collect_node_usage"
            }
        )
        
        workflow.add_conditional_edges(
            "collect_node_usage",
            self._should_handle_error,
            {
                "error": "handle_error",
                "continue": "collect_basic_info"
            }
        )
        
        workflow.add_conditional_edges(
            "collect_basic_info",
            self._should_handle_error,
            {
                "error": "handle_error",
                "continue": "collect_api_metrics"
            }
        )
        
        workflow.add_conditional_edges(
            "collect_api_metrics",
            self._should_handle_error,
            {
                "error": "handle_error",
                "continue": "collect_multus_metrics"
            }
        )
        
        workflow.add_conditional_edges(
            "collect_multus_metrics",
            self._should_handle_error,
            {
                "error": "handle_error",
                "continue": "collect_ovnk_pods"
            }
        )
        
        workflow.add_conditional_edges(
            "collect_ovnk_pods",
            self._should_handle_error,
            {
                "error": "handle_error",
                "continue": "collect_ovnk_containers"
            }
        )
        
        workflow.add_conditional_edges(
            "collect_ovnk_containers",
            self._should_handle_error,
            {
                "error": "handle_error",
                "continue": "collect_ovs_metrics"
            }
        )
        
        workflow.add_conditional_edges(
            "collect_ovs_metrics",
            self._should_handle_error,
            {
                "error": "handle_error",
                "continue": "collect_latency_metrics"
            }
        )
        
        workflow.add_conditional_edges(
            "collect_latency_metrics",
            self._should_handle_error,
            {
                "error": "handle_error",
                "continue": "store_data"
            }
        )
        
        # Set workflow path
        workflow.add_edge("store_data", "finalize")
        workflow.add_edge("handle_error", END)
        workflow.add_edge("finalize", END)
        
        return workflow.compile()
    
    def _should_handle_error(self, state: AgentState) -> str:
        """Check if there's a critical error to handle"""
        return "error" if state.get("error") else "continue"
    
    def _get_time_params(self) -> Dict[str, Any]:
        """Get time parameters for MCP tool calls"""
        params = {}
        if self.duration:
            params["duration"] = self.duration
        if self.start_time:
            params["start_time"] = self.start_time
        if self.end_time:
            params["end_time"] = self.end_time
        return params
    
    async def _initialize_node(self, state: AgentState) -> AgentState:
        """Initialize the data collection process"""
        query_params = state.get("query_params", {})
        testing_id = query_params.get("testing_id") or self.run_uuid
        
        duration = query_params.get("duration", self.duration)
        start_time = query_params.get("start_time", self.start_time)
        end_time = query_params.get("end_time", self.end_time)
        
        time_range = f"duration: {duration}" if duration else f"from {start_time} to {end_time}"
        logger.info(f"Starting performance data collection - Testing ID: {testing_id} ({time_range})")
        
        if self.use_ai:
            initialization_message = SystemMessage(content=f"""You are a performance data collection agent for OpenShift OVN-Kubernetes clusters.
Your task is to collect comprehensive performance metrics from various components.

Collection steps:
1. Collect OpenShift cluster information
2. Collect cluster node usage metrics
3. Collect Prometheus basic info
4. Collect API server metrics  
5. Collect Multus CNI metrics
6. Collect OVN-K pods metrics
7. Collect OVN-K containers metrics
8. Collect OVS metrics
9. Collect OVN-K latency metrics

Time range: {time_range}

Be thorough and handle errors gracefully.""")
            
            start_message = HumanMessage(content=f"Start performance data collection for {time_range}")
            messages = [initialization_message, start_message]
        else:
            messages = []
        
        return {
            **state,
            "testing_id": testing_id,
            "duration": duration,
            "start_time": start_time,
            "end_time": end_time,
            "results": {},
            "messages": messages,
            "current_step": "initialize"
        }

    async def _initialize_storage(self) -> bool:
        """Initialize storage with retry logic for database locks"""
        if self._storage_initialized or not STORAGE_AVAILABLE:
            return self._storage_initialized
        
        try:
            self.storage = ovnkMetricStor(self.db_path)
            await self.storage.initialize()
            self._storage_initialized = True
            logger.info(f"✅ Storage initialized: {self.db_path}")
            return True
        except Exception as e:
            err_msg = str(e)
            # Detect lock conflict and attempt seamless fallback without noisy warning
            is_lock_conflict = ("Could not set lock on file" in err_msg) or ("Conflicting lock" in err_msg)
            if is_lock_conflict:
                logger.info("Database is locked by another process; switching to a per-run database file.")
            else:
                logger.warning(f"⚠️  Warning: Failed to initialize storage: {err_msg}")
            
            # Check if it's a lock error and retry with per-run database
            if is_lock_conflict:
                try:
                    base_no_ext, _ = os.path.splitext(os.path.abspath(self.db_path))
                    alt_db_path = f"{base_no_ext}_{self.run_uuid}.db"
                    logger.info(f"➡️  Retrying storage with per-run DB: {alt_db_path}")
                    self.storage = ovnkMetricStor(alt_db_path)
                    await self.storage.initialize()
                    self.db_path = alt_db_path
                    self._storage_initialized = True
                    logger.info(f"✅ Storage initialized with per-run DB: {self.db_path}")
                    return True
                except Exception as e2:
                    logger.warning(f"⚠️  Warning: Fallback storage init failed: {e2}")
                    self.storage = None
                    return False
            else:
                self.storage = None
                return False

    async def _collect_cluster_info_node(self, state: AgentState) -> AgentState:
        """Collect OpenShift cluster information"""
        logger.info("Collecting OpenShift cluster information...")
        state["current_step"] = "collect_cluster_info"
        
        try:
            data = await self._call_mcp_tool("get_openshift_cluster_info", {
                "include_node_details": True,
                "include_resource_counts": True,
                "include_network_policies": True,
                "include_operator_status": True,
                "include_mcp_status": True
            })
            
            total_nodes = data.get("total_nodes", 0)
            cluster_version = data.get("cluster_version", "unknown")
            
            msg = f"Successfully collected cluster information. Version: {cluster_version}, Nodes: {total_nodes}"
            logger.info(f"  → {msg}")
            
            return {
                **state,
                "cluster_info": data,
                "messages": state["messages"] + [AIMessage(content=msg)] if self.use_ai else []
            }
            
        except Exception as e:
            error_msg = f"Failed to collect cluster info: {str(e)}"
            logger.warning(f"  ✗ {error_msg}")
            
            return {
                **state,
                "cluster_info": None,
                "messages": state["messages"] + [AIMessage(content=f"Error: {error_msg}")] if self.use_ai else []
            }
    
    async def _collect_node_usage_node(self, state: AgentState) -> AgentState:
        """Collect cluster node usage metrics"""
        logger.info("Collecting cluster node usage metrics...")
        state["current_step"] = "collect_node_usage"
        
        try:
            params = self._get_time_params()
            data = await self._call_mcp_tool("query_cluster_node_usage", params)
            
            groups = data.get("groups", {})
            node_count = sum(len(g.get("nodes", [])) for g in groups.values())
            
            msg = f"Node usage metrics collected. Total nodes analyzed: {node_count}"
            logger.info(f"  → {msg}")
            
            return {
                **state,
                "node_usage_data": data,
                "messages": state["messages"] + [AIMessage(content=msg)] if self.use_ai else []
            }
            
        except Exception as e:
            error_msg = f"Failed to collect node usage: {str(e)}"
            logger.warning(f"  ✗ {error_msg}")
            
            return {
                **state,
                "node_usage_data": None,
                "messages": state["messages"] + [AIMessage(content=f"Error: {error_msg}")] if self.use_ai else []
            }
    
    async def _collect_basic_info_node(self, state: AgentState) -> AgentState:
        """Collect Prometheus basic OVN info"""
        logger.info("Collecting Prometheus basic OVN information...")
        state["current_step"] = "collect_basic_info"
        
        try:
            data = await self._call_mcp_tool("query_prometheus_basic_info", {
                "include_pod_status": True,
                "include_db_metrics": True
            })
            
            msg = "Prometheus basic OVN info collected successfully"
            logger.info(f"  → {msg}")
            
            return {
                **state,
                "basic_info_data": data,
                "messages": state["messages"] + [AIMessage(content=msg)] if self.use_ai else []
            }
            
        except Exception as e:
            error_msg = f"Failed to collect basic info: {str(e)}"
            logger.warning(f"  ✗ {error_msg}")
            
            return {
                **state,
                "basic_info_data": None,
                "messages": state["messages"] + [AIMessage(content=f"Error: {error_msg}")] if self.use_ai else []
            }
    
    async def _collect_api_metrics_node(self, state: AgentState) -> AgentState:
        """Collect API server metrics"""
        logger.info("Collecting Kubernetes API server metrics...")
        state["current_step"] = "collect_api_metrics"
        
        try:
            params = self._get_time_params()
            data = await self._call_mcp_tool("query_kube_api_metrics", params)
            
            summary = data.get("summary", {})
            health_score = summary.get("health_score", 0)
            status = summary.get("overall_status", "unknown")
            
            msg = f"API server metrics collected. Health score: {health_score}, Status: {status}"
            logger.info(f"  → {msg}")
            
            return {
                **state,
                "api_metrics_data": data,
                "messages": state["messages"] + [AIMessage(content=msg)] if self.use_ai else []
            }
            
        except Exception as e:
            error_msg = f"Failed to collect API server metrics: {str(e)}"
            logger.warning(f"  ✗ {error_msg}")
            
            return {
                **state,
                "api_metrics_data": None,
                "messages": state["messages"] + [AIMessage(content=f"Error: {error_msg}")] if self.use_ai else []
            }
    
    async def _collect_multus_metrics_node(self, state: AgentState) -> AgentState:
        """Collect Multus CNI metrics"""
        logger.info("Collecting Multus CNI metrics...")
        state["current_step"] = "collect_multus_metrics"
        
        try:
            params = {
                "pod_pattern": "multus.*|network-metrics.*",
                "namespace_pattern": "openshift-multus",
                **self._get_time_params()
            }
            data = await self._call_mcp_tool("query_multus_metrics", params)
            
            collection_timestamp = data.get("collection_timestamp", "unknown")
            
            msg = f"Multus metrics collected. Timestamp: {collection_timestamp}"
            logger.info(f"  → {msg}")
            
            return {
                **state,
                "multus_metrics_data": data,
                "messages": state["messages"] + [AIMessage(content=msg)] if self.use_ai else []
            }
            
        except Exception as e:
            error_msg = f"Failed to collect Multus metrics: {str(e)}"
            logger.warning(f"  ✗ {error_msg}")
            
            return {
                **state,
                "multus_metrics_data": None,
                "messages": state["messages"] + [AIMessage(content=f"Error: {error_msg}")] if self.use_ai else []
            }
    
    async def _collect_ovnk_pods_node(self, state: AgentState) -> AgentState:
        """Collect OVN-K pods metrics"""
        logger.info("Collecting OVN-K pods metrics...")
        state["current_step"] = "collect_ovnk_pods"
        
        try:
            params = {
                "pod_pattern": "ovnkube.*",
                "namespace_pattern": "openshift-ovn-kubernetes",
                **self._get_time_params()
            }
            data = await self._call_mcp_tool("query_ovnk_pods_metrics", params)
            
            collection_timestamp = data.get("collection_timestamp", "unknown")
            
            msg = f"OVN-K pods metrics collected. Timestamp: {collection_timestamp}"
            logger.info(f"  → {msg}")
            
            return {
                **state,
                "ovnk_pods_data": data,
                "messages": state["messages"] + [AIMessage(content=msg)] if self.use_ai else []
            }
            
        except Exception as e:
            error_msg = f"Failed to collect OVN-K pods metrics: {str(e)}"
            logger.warning(f"  ✗ {error_msg}")
            
            return {
                **state,
                "ovnk_pods_data": None,
                "messages": state["messages"] + [AIMessage(content=f"Error: {error_msg}")] if self.use_ai else []
            }
    
    async def _collect_ovnk_containers_node(self, state: AgentState) -> AgentState:
        """Collect OVN-K containers metrics"""
        logger.info("Collecting OVN-K containers metrics...")
        state["current_step"] = "collect_ovnk_containers"
        
        try:
            params = {
                "pod_pattern": "ovnkube.*",
                "container_pattern": "ovnkube-controller",
                "namespace_pattern": "openshift-ovn-kubernetes",
                **self._get_time_params()
            }
            data = await self._call_mcp_tool("query_ovnk_containers_metrics", params)
            
            collection_timestamp = data.get("collection_timestamp", "unknown")
            
            msg = f"OVN-K containers metrics collected. Timestamp: {collection_timestamp}"
            logger.info(f"  → {msg}")
            
            return {
                **state,
                "ovnk_containers_data": data,
                "messages": state["messages"] + [AIMessage(content=msg)] if self.use_ai else []
            }
            
        except Exception as e:
            error_msg = f"Failed to collect OVN-K containers metrics: {str(e)}"
            logger.warning(f"  ✗ {error_msg}")
            
            return {
                **state,
                "ovnk_containers_data": None,
                "messages": state["messages"] + [AIMessage(content=f"Error: {error_msg}")] if self.use_ai else []
            }
    
    async def _collect_ovs_metrics_node(self, state: AgentState) -> AgentState:
        """Collect OVS metrics"""
        logger.info("Collecting OVS metrics...")
        state["current_step"] = "collect_ovs_metrics"
        
        try:
            params = {
                "pod_pattern": "ovnkube.*",
                "namespace_pattern": "openshift-ovn-kubernetes",
                **self._get_time_params()
            }
            data = await self._call_mcp_tool("query_ovnk_ovs_metrics", params)
            
            msg = "OVS metrics collected successfully"
            logger.info(f"  → {msg}")
            
            return {
                **state,
                "ovs_metrics_data": data,
                "messages": state["messages"] + [AIMessage(content=msg)] if self.use_ai else []
            }
            
        except Exception as e:
            error_msg = f"Failed to collect OVS metrics: {str(e)}"
            logger.warning(f"  ✗ {error_msg}")
            
            return {
                **state,
                "ovs_metrics_data": None,
                "messages": state["messages"] + [AIMessage(content=f"Error: {error_msg}")] if self.use_ai else []
            }
    
    async def _collect_latency_metrics_node(self, state: AgentState) -> AgentState:
        """Collect OVN-K latency metrics"""
        logger.info("Collecting OVN-K latency metrics...")
        state["current_step"] = "collect_latency_metrics"
        
        try:
            params = {
                "include_controller_metrics": True,
                "include_node_metrics": True,
                "include_extended_metrics": True,
                **self._get_time_params()
            }
            data = await self._call_mcp_tool("get_ovnk_latency_metrics", params)
            
            summary = data.get("summary", {})
            total_metrics = summary.get("total_metrics", 0)
            
            msg = f"OVN-K latency metrics collected. Total metrics: {total_metrics}"
            logger.info(f"  → {msg}")
            
            return {
                **state,
                "latency_metrics_data": data,
                "messages": state["messages"] + [AIMessage(content=msg)] if self.use_ai else []
            }
            
        except Exception as e:
            error_msg = f"Failed to collect latency metrics: {str(e)}"
            logger.warning(f"  ✗ {error_msg}")
            
            return {
                **state,
                "latency_metrics_data": None,
                "messages": state["messages"] + [AIMessage(content=f"Error: {error_msg}")] if self.use_ai else []
            }

    async def collect_performance_data(self) -> Dict[str, Any]:
        """Main method to collect performance data"""
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"Starting {'AI-driven' if self.use_ai else 'direct'} performance data collection...")
            logger.info(f"{'='*60}\n")
            
            # Initialize storage before starting collection
            await self._initialize_storage()
            
            if self.use_ai:
                # Use LangGraph workflow
                query_params = {
                    "duration": self.duration,
                    "start_time": self.start_time,
                    "end_time": self.end_time,
                    "testing_id": self.run_uuid
                }
                result = await self.analyze(query_params)
            else:
                # Direct collection without AI
                result = await self._direct_collection()
            
            # Close storage connection
            if self.storage:
                await self.storage.close()
            
            return result
            
        except Exception as e:
            logger.error(f"Critical error in performance data collection: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            if self.storage:
                try:
                    await self.storage.close()
                except:
                    pass
            
            return {
                "run_id": self.run_uuid,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error": str(e),
                "success": False,
                "success_rate": 0,
                "mode": "ai" if self.use_ai else "direct",
                "saved_to_db": False
            }

    async def _store_data_node(self, state: AgentState) -> AgentState:
        """Store collected data in DuckDB"""
        try:
            logger.info("Storing data in DuckDB...")
            storage_results = {}
            
            if not self.storage:
                logger.warning("Storage not initialized, skipping data storage")
                return {
                    **state,
                    "results": {
                        **state.get("results", {}),
                        "storage_results": {"error": "Storage not initialized"}
                    }
                }
            
            # Prepare collection result for storage
            collection_result = {
                "run_id": state["testing_id"],
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "duration": state.get("duration"),
                "start_time": state.get("start_time"),
                "end_time": state.get("end_time"),
                "collected_data": {},
                "errors": [],
                "success_count": 0,
                "total_steps": 9,
                "mode": "ai" if self.use_ai else "direct"
            }
            
            # Add collected data
            data_mapping = {
                "cluster_info": state.get("cluster_info"),
                "node_usage": state.get("node_usage_data"),
                "basic_info": state.get("basic_info_data"),
                "api_server": state.get("api_metrics_data"),
                "multus": state.get("multus_metrics_data"),
                "ovnk_pods": state.get("ovnk_pods_data"),
                "ovnk_containers": state.get("ovnk_containers_data"),
                "ovs_metrics": state.get("ovs_metrics_data"),
                "latency_metrics": state.get("latency_metrics_data")
            }
            
            for key, value in data_mapping.items():
                if value is not None:
                    collection_result["collected_data"][key] = value
                    collection_result["success_count"] += 1
                    storage_results[key] = "success"
                else:
                    storage_results[key] = "skipped"
            
            collection_result["success_rate"] = (collection_result["success_count"] / collection_result["total_steps"]) * 100
            collection_result["success"] = collection_result["success_count"] > 0
            
            # Save to storage
            try:
                run_id = await self.storage.save_collection_result(collection_result)
                storage_results["saved_to_db"] = True
                storage_results["db_run_id"] = run_id
                logger.info(f"✅ Results saved to database - Run ID: {run_id}")
            except Exception as e:
                logger.error(f"Failed to save to database: {e}")
                storage_results["saved_to_db"] = False
                storage_results["db_error"] = str(e)
            
            return {
                **state,
                "results": {
                    **state.get("results", {}),
                    "storage_results": storage_results
                },
                "messages": state["messages"] + [AIMessage(content="Successfully stored data in DuckDB")] if self.use_ai else []
            }
            
        except Exception as e:
            error_msg = f"Error storing data: {str(e)}"
            logger.error(error_msg)
            return {
                **state,
                "error": error_msg,
                "messages": state["messages"] + [AIMessage(content=error_msg)] if self.use_ai else []
            }

    async def _finalize_node(self, state: AgentState) -> AgentState:
        """Finalize results with comprehensive summary"""
        try:
            logger.info("Finalizing performance data collection...")
            
            # Count successful collections
            successful_collections = sum([
                1 if state.get("cluster_info") else 0,
                1 if state.get("node_usage_data") else 0,
                1 if state.get("basic_info_data") else 0,
                1 if state.get("api_metrics_data") else 0,
                1 if state.get("multus_metrics_data") else 0,
                1 if state.get("ovnk_pods_data") else 0,
                1 if state.get("ovnk_containers_data") else 0,
                1 if state.get("ovs_metrics_data") else 0,
                1 if state.get("latency_metrics_data") else 0
            ])
            
            total_steps = 9
            success_rate = (successful_collections / total_steps) * 100
            
            results = {
                "testing_id": state["testing_id"],
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "duration": state.get("duration"),
                "start_time": state.get("start_time"),
                "end_time": state.get("end_time"),
                "cluster_info": state.get("cluster_info"),
                "node_usage": state.get("node_usage_data"),
                "basic_info": state.get("basic_info_data"),
                "api_metrics": state.get("api_metrics_data"),
                "multus_metrics": state.get("multus_metrics_data"),
                "ovnk_pods": state.get("ovnk_pods_data"),
                "ovnk_containers": state.get("ovnk_containers_data"),
                "ovs_metrics": state.get("ovs_metrics_data"),
                "latency_metrics": state.get("latency_metrics_data"),
                "storage_results": state.get("results", {}).get("storage_results", {}),
                "success_count": successful_collections,
                "total_steps": total_steps,
                "success_rate": success_rate,
                "status": "success" if successful_collections > 0 else "failed"
            }
            
            logger.info(f"Analysis completed - Success rate: {success_rate:.1f}% ({successful_collections}/{total_steps})")
            
            return {
                **state,
                "results": results,
                "messages": state["messages"] + [AIMessage(content="Analysis completed successfully")] if self.use_ai else []
            }
            
        except Exception as e:
            error_msg = f"Error finalizing results: {str(e)}"
            logger.error(error_msg)
            return {
                **state,
                "error": error_msg,
                "results": {"status": "error", "error": error_msg}
            }
    
    async def _handle_error_node(self, state: AgentState) -> AgentState:
        """Handle critical errors during collection"""
        error_msg = state.get("error", "Unknown error occurred")
        logger.error(f"Handling critical error: {error_msg}")
        
        return {
            **state,
            "results": {
                "testing_id": state.get("testing_id"),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": "error",
                "error": error_msg
            },
            "messages": state["messages"] + [AIMessage(content=f"Analysis failed: {error_msg}")] if self.use_ai else []
        }
    
    async def _call_mcp_tool(self, tool_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Call MCP server tool via Streamable HTTP"""
        if not MCP_CLIENT_AVAILABLE:
            raise Exception("MCP client not available. Install with: pip install mcp")
        
        try:
            url = f"{self.mcp_server_url}/mcp"
            async with streamablehttp_client(url) as (
                read_stream,
                write_stream,
                get_session_id,
            ):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    _ = get_session_id()
                    request_data = {"request": params or {}}
                    result = await session.call_tool(tool_name, request_data)
                    if result.content and len(result.content) > 0:
                        text_content = result.content[0].text
                        return json.loads(text_content)
                    raise Exception(f"Empty response from tool {tool_name}")
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse JSON response from {tool_name}: {str(e)}")
        except Exception as e:
            raise Exception(f"Error calling MCP tool {tool_name}: {str(e)}")
    
    async def analyze(self, query_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Run the complete analysis workflow"""
        initial_state = AgentState(
            messages=[HumanMessage(content="Start OVN-K performance data collection")],
            current_step="",
            cluster_info=None,
            node_usage_data=None,
            basic_info_data=None,
            api_metrics_data=None,
            multus_metrics_data=None,
            ovnk_pods_data=None,
            ovnk_containers_data=None,
            ovs_metrics_data=None,
            latency_metrics_data=None,
            duration=self.duration or "5m",
            start_time=self.start_time,
            end_time=self.end_time,
            testing_id="",
            query_params=query_params or {},
            results={},
            error=None
        )
        
        try:
            final_state = await self.graph.ainvoke(initial_state)
            return final_state["results"]
            
        except Exception as e:
            logger.error(f"Analysis workflow failed: {str(e)}")
            return {
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    
    async def collect_performance_data(self) -> Dict[str, Any]:
        """Main method to collect performance data"""
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"Starting {'AI-driven' if self.use_ai else 'direct'} performance data collection...")
            logger.info(f"{'='*60}\n")
            
            # Initialize storage if not already initialized
            if STORAGE_AVAILABLE and not self.storage:
                try:
                    self.storage = ovnkMetricStor(self.db_path)
                    await self.storage.initialize()
                    logger.info(f"✅ Storage initialized: {self.db_path}")
                except Exception as e:
                    err_msg = str(e)
                    logger.warning(f"⚠️  Warning: Failed to initialize storage: {err_msg}")
                    if "Could not set lock on file" in err_msg or "Conflicting lock" in err_msg:
                        try:
                            alt_db_path = f"{os.path.splitext(self.db_path)[0]}_{self.run_uuid}.db"
                            logger.info(f"➡️  Retrying storage with per-run DB: {alt_db_path}")
                            self.storage = ovnkMetricStor(alt_db_path)
                            await self.storage.initialize()
                            self.db_path = alt_db_path
                            logger.info(f"✅ Storage initialized with per-run DB: {self.db_path}")
                        except Exception as e2:
                            logger.warning(f"⚠️  Warning: Fallback storage init failed: {e2}")
                            self.storage = None
                    else:
                        self.storage = None
            
            if self.use_ai:
                # Use LangGraph workflow
                query_params = {
                    "duration": self.duration,
                    "start_time": self.start_time,
                    "end_time": self.end_time,
                    "testing_id": self.run_uuid
                }
                result = await self.analyze(query_params)
            else:
                # Direct collection without AI
                result = await self._direct_collection()
            
            # Close storage connection
            if self.storage:
                await self.storage.close()
            
            return result
            
        except Exception as e:
            logger.error(f"Critical error in performance data collection: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            if self.storage:
                try:
                    await self.storage.close()
                except:
                    pass
            
            return {
                "run_id": self.run_uuid,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error": str(e),
                "success": False,
                "success_rate": 0,
                "mode": "ai" if self.use_ai else "direct",
                "saved_to_db": False
            }
    
    async def _direct_collection(self) -> Dict[str, Any]:
        """Direct collection without AI workflow"""
        state = {
            "testing_id": self.run_uuid,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duration": self.duration,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "cluster_info": None,
            "node_usage_data": None,
            "basic_info_data": None,
            "api_metrics_data": None,
            "multus_metrics_data": None,
            "ovnk_pods_data": None,
            "ovnk_containers_data": None,
            "ovs_metrics_data": None,
            "latency_metrics_data": None,
            "results": {},
            "messages": [],
            "query_params": {},
            "current_step": "",
            "error": None
        }
        
        # Initialize and collect
        state = await self._initialize_node(state)
        state = await self._collect_cluster_info_node(state)
        state = await self._collect_node_usage_node(state)
        state = await self._collect_basic_info_node(state)
        state = await self._collect_api_metrics_node(state)
        state = await self._collect_multus_metrics_node(state)
        state = await self._collect_ovnk_pods_node(state)
        state = await self._collect_ovnk_containers_node(state)
        state = await self._collect_ovs_metrics_node(state)
        state = await self._collect_latency_metrics_node(state)
        
        # Store and finalize
        state = await self._store_data_node(state)
        state = await self._finalize_node(state)
        
        return state.get("results", {})
    
    async def query_by_duration(self, duration: str = "1h", query_stored_only: bool = False) -> Dict[str, Any]:
        """Query metrics data by duration.
        
        If query_stored_only is True, returns results from DuckDB without collecting new data.
        Otherwise, collects new data and stores it.
        """
        if query_stored_only:
            if not self._storage_initialized:
                storage_ready = await self._initialize_storage()
                if not storage_ready:
                    return {"status": "error", "error": "Failed to initialize storage"}
            
            try:
                # Query stored data
                end_time = datetime.now(timezone.utc)
                
                # Parse duration to get start time
                duration_seconds = self._parse_duration_to_seconds(duration)
                start_time = end_time - timedelta(seconds=duration_seconds)
                
                snapshots = await self.storage.get_performance_snapshots(
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    limit=10
                )
                
                return {
                    "status": "success",
                    "query_mode": "stored_only",
                    "duration": duration,
                    "snapshots": snapshots,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            except Exception as e:
                return {"status": "error", "error": str(e)}
        
        # Collect new data
        self.duration = duration
        self.start_time = None
        self.end_time = None
        return await self.collect_performance_data()

    async def query_by_time_range(self, start_time: str, end_time: str, query_stored_only: bool = False) -> Dict[str, Any]:
        """Query metrics data by time range (UTC timezone).
        
        If query_stored_only is True, returns results from DuckDB without collecting new data.
        Otherwise, collects new data and stores it.
        """
        if query_stored_only:
            if not self._storage_initialized:
                storage_ready = await self._initialize_storage()
                if not storage_ready:
                    return {"status": "error", "error": "Failed to initialize storage"}
            
            try:
                # Query stored data
                snapshots = await self.storage.get_performance_snapshots(
                    start_time=start_time,
                    end_time=end_time,
                    limit=10
                )
                
                return {
                    "status": "success",
                    "query_mode": "stored_only",
                    "start_time": start_time,
                    "end_time": end_time,
                    "snapshots": snapshots,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            except Exception as e:
                return {"status": "error", "error": str(e)}
        
        # Collect new data
        self.duration = None
        self.start_time = start_time
        self.end_time = end_time
        return await self.collect_performance_data()

    def _parse_duration_to_seconds(self, duration: str) -> int:
        """Parse Prometheus duration string to seconds"""
        import re
        
        match = re.match(r'^(\d+)([smhd])$', duration.lower())
        if not match:
            return 3600  # Default 1 hour
        
        value = int(match.group(1))
        unit = match.group(2)
        
        multipliers = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
        return value * multipliers.get(unit, 60)


async def main():
    """Main function for standalone execution"""
    import argparse
    from datetime import timedelta
    
    parser = argparse.ArgumentParser(description="OVN-K Performance Data Collection Agent")
    parser.add_argument("--mcp-url", default="http://localhost:8000", 
                       help="MCP server URL (default: http://localhost:8000)")
    parser.add_argument("--duration", default="5m",
                       help="Collection duration (e.g., 5m, 1h, 1d) - default: 5m")
    parser.add_argument("--start-time", default=None,
                       help="Start time in ISO format (YYYY-MM-DDTHH:MM:SSZ)")
    parser.add_argument("--end-time", default=None,
                       help="End time in ISO format (YYYY-MM-DDTHH:MM:SSZ)")
    parser.add_argument("--no-ai", action="store_true",
                       help="Disable AI mode and use direct collection")
    parser.add_argument("--db-path", default="storage/ovnk_benchmark.db",
                       help="Database path (default: storage/ovnk_benchmark.db)")
    parser.add_argument("--output", default=None,
                       help="Output file for collected data (JSON)")
    parser.add_argument("--query-stored", action="store_true",
                       help="Query stored data only without collecting new data")
    
    args = parser.parse_args()
    
    # Get configuration from environment or command line
    mcp_server_url = os.getenv("MCP_SERVER_URL", args.mcp_url)
    openai_api_key = os.getenv("OPENAI_API_KEY")
    
    # Determine time parameters
    if args.start_time and args.end_time:
        duration = None
        start_time = args.start_time
        end_time = args.end_time
        logger.info(f"Using time range: {start_time} to {end_time}")
    else:
        duration = args.duration
        start_time = None
        end_time = None
        logger.info(f"Using duration: {duration}")
    
    # Create agent
    agent = PerformanceDataAgent(
        mcp_server_url=mcp_server_url,
        openai_api_key=openai_api_key,
        duration=duration,
        start_time=start_time,
        end_time=end_time,
        use_ai=not args.no_ai,
        db_path=args.db_path
    )
    
    # Run data collection or query
    if args.query_stored:
        logger.info("Querying stored data only...")
        if duration:
            result = await agent.query_by_duration(duration, query_stored_only=True)
        else:
            result = await agent.query_by_time_range(start_time, end_time, query_stored_only=True)
    else:
        result = await agent.collect_performance_data()
    
    # Print results
    logger.info("\n" + "="*60)
    logger.info("PERFORMANCE DATA COLLECTION RESULTS")
    logger.info("="*60)
    
    if args.query_stored:
        logger.info(f"Query Mode: STORED DATA ONLY")
        logger.info(f"Snapshots Found: {len(result.get('snapshots', []))}")
        logger.info(f"Duration: {result.get('duration', 'N/A')}")
        logger.info(f"Time Range: {result.get('start_time', 'N/A')} to {result.get('end_time', 'N/A')}")
    else:
        testing_id = result.get('testing_id') or result.get('run_id')
        logger.info(f"Testing ID: {testing_id}")
        logger.info(f"Mode: {result.get('mode', 'unknown').upper()}")
        logger.info(f"Success Rate: {result.get('success_rate', 0):.1f}%")
        logger.info(f"Successful: {result.get('success_count', 0)}/{result.get('total_steps', 9)}")
        
        if result.get('duration'):
            logger.info(f"Duration: {result['duration']}")
        else:
            logger.info(f"Time Range: {result.get('start_time')} to {result.get('end_time')}")
        
        storage_results = result.get('storage_results', {})
        if storage_results.get('saved_to_db'):
            logger.info(f"✅ Saved to database: {args.db_path}")
            logger.info(f"   DB Run ID: {storage_results.get('db_run_id')}")
        else:
            logger.info(f"⚠️  Not saved to database: {storage_results.get('db_error', 'Unknown error')}")
        
        logger.info(f"\nCollected Categories:")
        categories = ['cluster_info', 'node_usage', 'basic_info', 'api_metrics', 
                     'multus_metrics', 'ovnk_pods', 'ovnk_containers', 'ovs_metrics', 
                     'latency_metrics']
        for category in categories:
            if result.get(category):
                logger.info(f"  ✓ {category}")
            else:
                logger.info(f"  ✗ {category}")
    
    # Save to file if requested
    if args.output:
        output_file = args.output
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        logger.info(f"\n✓ Results saved to: {output_file}")
    
    # Return exit code based on success
    if args.query_stored:
        return 0 if result.get('status') == 'success' else 1
    else:
        return 0 if result.get('success_rate', 0) > 50 else 1


if __name__ == "__main__":
    asyncio.run(main())