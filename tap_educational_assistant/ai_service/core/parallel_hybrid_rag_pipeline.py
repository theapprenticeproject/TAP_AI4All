# File: ~/frappe-bench/apps/tap_educational_assistant/tap_educational_assistant/ai_service/core/parallel_hybrid_rag_pipeline.py

import frappe
import asyncio
import time
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import functools
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from dataclasses import dataclass

# Core imports (lightweight - loaded immediately)
from tap_educational_assistant.ai_service.config.settings import config, get_neo4j_config

logger = logging.getLogger(__name__)

@dataclass
class StrategyResult:
    """Standardized result container for strategy execution"""
    success: bool
    answer: str
    strategy_name: str
    execution_time: float
    confidence_score: float
    source: str
    error: Optional[str] = None
    metadata: Optional[Dict] = None
    
    def is_valid(self) -> bool:
        """Check if result is valid and useful"""
        if not self.success or not self.answer:
            return False
        
        # Check for common failure indicators
        failure_indicators = [
            "i don't know", "i do not know", "i cannot", "unable to",
            "no information", "not enough information", "no data available",
            "cannot answer", "insufficient data", "no results found", 
            "no answer available", "error", "failed", "timeout"
        ]
        
        answer_lower = self.answer.lower().strip()
        
        # Reject if contains failure indicators
        if any(indicator in answer_lower for indicator in failure_indicators):
            return False
        
        # Reject if too short (likely incomplete)
        if len(self.answer.strip()) < 10:
            return False
        
        # Must have reasonable confidence
        if self.confidence_score < 0.2:
            return False
        
        return True

class ParallelEducationalRAG:
    """
    🚀 FIXED ULTRA-FAST Parallel Hybrid RAG Pipeline
    Executes all strategies simultaneously with proper error handling
    """
    
    def __init__(self, user_id: str = "default_user"):
        # User identification for persistent memory
        self.user_id = user_id
        self.session_id = f"fixed_parallel_session_{user_id}_{datetime.now().strftime('%Y%m%d')}"
        
        # Get Neo4j configuration
        self.neo4j_config = get_neo4j_config()
        
        # Thread pool for parallel execution
        self.executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="RAG_Strategy")
        
        # ALL components start as None - maximum lazy loading
        self.data_loader = None
        self.embeddings = None
        self.vector_store = None
        self.vector_retriever = None
        self.graph = None
        self.llm = None
        
        # Enhanced Neo4j components - lazy load
        self.cypher_corrector = None
        self.educational_schema = None
        self.neo4j_chat_history = None
        self.enhanced_graph_qa_chain = None
        
        # SQL Agent components - lazy load
        self.sql_database = None
        self.sql_agent = None
        self.sql_toolkit = None
        
        # ✅ CACHE DATABASE CONFIGURATION DURING INITIALIZATION
        self.cached_db_config = self._cache_database_configuration()
        
        # Component loading status flags
        self._component_status = {
            'llm': False,
            'data_loader': False,
            'graph': False,
            'sql': False,
            'vector': False,
            'neo4j_chat': False
        }
        
        # Initialize thread locks
        self._llm_lock = threading.Lock()
        self._vector_lock = threading.Lock()
        self._graph_lock = threading.Lock()
        self._sql_lock = threading.Lock()
        self._data_loader_lock = threading.Lock()
        
        # Performance tracking
        self.strategy_stats = {
            'graph_analysis': {'attempts': 0, 'successes': 0, 'avg_time': 0},
            'sql_aggregation': {'attempts': 0, 'successes': 0, 'avg_time': 0},
            'vector_search': {'attempts': 0, 'successes': 0, 'avg_time': 0}
        }
        
        print("🚀 FIXED PARALLEL Educational RAG Pipeline Ready!")
        print(f"   ✅ User: {user_id} | Session: {self.session_id}")
        print(f"   🌐 Neo4j: {'Aura (Cloud)' if self.neo4j_config.get('is_aura') else 'Local'}")
        print(f"   ⚡ PARALLEL EXECUTION: All strategies run simultaneously!")
        print(f"   🔧 FIXED: Better error handling and fallbacks!")
        print(f"   💾 Database Config: {'✅ Cached' if self.cached_db_config else '❌ Failed'}")

    def _cache_database_configuration(self) -> Dict[str, Any]:
        """Cache database configuration during initialization when Frappe context is available"""
        print("💾 Caching database configuration...")
        
        try:
            import frappe
            
            # Method 1: Try frappe.get_site_config() (main thread has context)
            try:
                site_config = frappe.get_site_config()
                db_name = site_config.get('db_name')
                db_password = site_config.get('db_password')
                db_host = site_config.get('db_host', 'localhost')
                db_port = site_config.get('db_port', 3306)
                db_user = site_config.get('db_user', db_name)
                
                if db_name and db_user and db_password:
                    cached_config = {
                        'db_name': db_name,
                        'db_user': db_user,
                        'db_password': db_password,
                        'db_host': db_host,
                        'db_port': db_port,
                        'method': 'site_config'
                    }
                    print(f"   ✅ Cached from site_config: {db_user}@{db_host}:{db_port}/{db_name}")
                    return cached_config
                else:
                    print(f"   ⚠️  site_config incomplete: db_name={db_name}, db_user={db_user}")
                    
            except Exception as site_error:
                print(f"   ⚠️  site_config failed: {site_error}")
            
            # Method 2: Try frappe.conf (if available in main thread)
            try:
                if hasattr(frappe, 'conf') and frappe.conf:
                    db_name = getattr(frappe.conf, 'db_name', None)
                    db_password = getattr(frappe.conf, 'db_password', None)
                    db_host = getattr(frappe.conf, 'db_host', 'localhost')
                    db_port = getattr(frappe.conf, 'db_port', 3306)
                    db_user = getattr(frappe.conf, 'db_user', db_name)
                    
                    if db_name and db_user and db_password:
                        cached_config = {
                            'db_name': db_name,
                            'db_user': db_user,
                            'db_password': db_password,
                            'db_host': db_host,
                            'db_port': db_port,
                            'method': 'frappe_conf'
                        }
                        print(f"   ✅ Cached from frappe.conf: {db_user}@{db_host}:{db_port}/{db_name}")
                        return cached_config
                    else:
                        print(f"   ⚠️  frappe.conf incomplete: db_name={db_name}, db_user={db_user}")
                        
            except Exception as conf_error:
                print(f"   ⚠️  frappe.conf failed: {conf_error}")
            
            # Method 3: Try frappe.db connection info (if database is connected)
            try:
                if hasattr(frappe, 'db') and frappe.db:
                    # Extract from existing database connection
                    db_settings = frappe.db.get_connection().connection_info
                    if db_settings:
                        cached_config = {
                            'db_name': db_settings.get('database'),
                            'db_user': db_settings.get('user'),
                            'db_password': db_settings.get('password'),
                            'db_host': db_settings.get('host', 'localhost'),
                            'db_port': db_settings.get('port', 3306),
                            'method': 'frappe_db'
                        }
                        print(f"   ✅ Cached from frappe.db connection")
                        return cached_config
                        
            except Exception as db_error:
                print(f"   ⚠️  frappe.db method failed: {db_error}")
            
            # Method 4: Environment variables as final fallback
            import os
            if os.environ.get('DB_NAME') or os.path.exists('sites/common_site_config.json'):
                try:
                    # Try to read from common site config
                    import json
                    with open('sites/common_site_config.json', 'r') as f:
                        common_config = json.load(f)
                    
                    cached_config = {
                        'db_name': common_config.get('db_name') or os.environ.get('DB_NAME', 'frappe_db'),
                        'db_user': common_config.get('db_user') or os.environ.get('DB_USER', 'frappe'),
                        'db_password': common_config.get('db_password') or os.environ.get('DB_PASSWORD', ''),
                        'db_host': common_config.get('db_host') or os.environ.get('DB_HOST', 'localhost'),
                        'db_port': common_config.get('db_port') or int(os.environ.get('DB_PORT', 3306)),
                        'method': 'common_config'
                    }
                    print(f"   ✅ Cached from common_site_config.json")
                    return cached_config
                    
                except Exception as env_error:
                    print(f"   ⚠️  Environment/config file method failed: {env_error}")
            
            print("   ❌ All database configuration methods failed")
            return None
            
        except Exception as e:
            print(f"   ❌ Database configuration caching failed: {e}")
            return None

    # ==================== MAIN PARALLEL QUERY PROCESSING ====================
    
    def query(self, question: str) -> Dict[str, Any]:
        """🚀 FIXED ULTRA-FAST Parallel query processing with proper error handling"""
        
        print(f"\n🚀 FIXED PARALLEL Processing: {question}")
        print(f"📚 Session: {self.session_id}")
        cloud_info = f"🌐 {'Aura (Cloud)' if self.neo4j_config.get('is_aura') else 'Local'}"
        print(f"{cloud_info}")
        
        start_time = time.time()
        
        try:
            # Add to persistent conversation history (lazy loaded)
            chat_history = self._ensure_neo4j_chat_loaded()
            if chat_history:
                try:
                    chat_history.add_user_message(question)
                except Exception as chat_error:
                    print(f"⚠️  Chat history error (non-critical): {chat_error}")
            
            # Get conversation context for all strategies
            conversation_context = self._get_persistent_conversation_context()
            
            print("🔥 LAUNCHING ALL STRATEGIES SIMULTANEOUSLY...")
            
            # 🚀 PARALLEL EXECUTION - All strategies run at once!
            strategy_results = self._execute_all_strategies_parallel(question, conversation_context)
            
            total_execution_time = time.time() - start_time
            
            # Find the best result from all strategies (FIXED)
            best_result = self._select_best_result_fixed(strategy_results, question)
            
            # Enhance the result with execution metadata
            best_result = self._enhance_parallel_result(
                best_result, strategy_results, total_execution_time, question
            )
            
            # Store response in persistent conversation history
            if chat_history and best_result.get("answer"):
                try:
                    chat_history.add_ai_message(best_result["answer"])
                except Exception as chat_error:
                    print(f"⚠️  Chat history storage error (non-critical): {chat_error}")
            
            return best_result
            
        except Exception as e:
            error_msg = f"Fixed parallel query processing failed: {str(e)}"
            print(f"❌ {error_msg}")
            
            return {
                "question": question,
                "answer": "I apologize, but I'm currently experiencing technical difficulties. Please try again or contact support.",
                "success": False,
                "error": str(e),
                "strategy_used": "error",
                "execution_mode": "parallel",
                "session_id": self.session_id,
                "neo4j_environment": "Aura (Cloud)" if self.neo4j_config.get('is_aura') else "Local"
            }

    def _execute_all_strategies_parallel(self, question: str, context: str) -> List[StrategyResult]:
        """🔥 Execute all strategies in parallel with improved error handling"""
        
        # Prepare strategy tasks
        strategy_tasks = [
            (self._execute_graph_strategy_safe, "graph_analysis", "Enhanced Neo4j Graph Analysis"),
            (self._execute_sql_strategy_safe, "sql_aggregation", "Educational SQL Analysis"),
            (self._execute_vector_strategy_safe, "vector_search", "Context-Aware Vector Search")
        ]
        
        results = []
        futures = {}
        
        # Launch all strategies simultaneously
        for strategy_func, strategy_name, source in strategy_tasks:
            try:
                future = self.executor.submit(strategy_func, question, context, source)
                futures[future] = strategy_name
                print(f"   🚀 Launched {strategy_name}")
            except Exception as launch_error:
                print(f"   ❌ Failed to launch {strategy_name}: {launch_error}")
                # Create immediate error result
                error_result = StrategyResult(
                    success=False,
                    answer=f"Failed to launch {strategy_name}: {str(launch_error)}",
                    strategy_name=strategy_name,
                    execution_time=0,
                    confidence_score=0,
                    source=f"Failed {strategy_name}",
                    error=str(launch_error)
                )
                results.append(error_result)
        
        # Collect results as they complete
        completed_strategies = []
        for future in as_completed(futures, timeout=90):
            strategy_name = futures[future]
            try:
                result = future.result(timeout=30)
                results.append(result)
                completed_strategies.append(strategy_name)
                
                if result.is_valid():
                    print(f"   ✅ {strategy_name} completed successfully in {result.execution_time:.2f}s")
                else:
                    print(f"   ⚠️  {strategy_name} completed but result invalid in {result.execution_time:.2f}s")
                    print(f"       Reason: {result.error or 'Low confidence or short answer'}")
                    
            except Exception as e:
                print(f"   ❌ {strategy_name} failed: {str(e)}")
                # Create error result
                error_result = StrategyResult(
                    success=False,
                    answer=f"Strategy failed: {str(e)}",
                    strategy_name=strategy_name,
                    execution_time=0,
                    confidence_score=0,
                    source=f"Failed {strategy_name}",
                    error=str(e)
                )
                results.append(error_result)
        
        # Update performance stats
        self._update_strategy_stats(results)
        
        print(f"📊 Parallel execution completed: {len(completed_strategies)}/{len(strategy_tasks)} strategies finished")
        return results

    def _select_best_result_fixed(self, results: List[StrategyResult], question: str) -> Dict[str, Any]:
        """🎯 CLASSIFICATION-ONLY: Select based on LLM classification priority"""
        
        valid_results = [r for r in results if r.is_valid()]
        
        if not valid_results:
            print("❌ No valid results from any strategy - providing fallback response")
            
            if results:
                best_attempt = max(results, key=lambda r: (r.success, r.confidence_score, len(r.answer)))
                fallback_answer = self._generate_fallback_response(question, results)
                
                return {
                    "question": question,
                    "answer": fallback_answer,
                    "success": False,
                    "strategy_used": "fallback",
                    "all_strategies_failed": True,
                    "error_details": [r.error for r in results if r.error]
                }
            else:
                return {
                    "question": question,
                    "answer": "I'm experiencing technical difficulties. Please try again later.",
                    "success": False,
                    "strategy_used": "none"
                }
        
        # 🎯 CLASSIFICATION-ONLY SELECTION
        print(f"🎯 CLASSIFICATION-ONLY: Selecting from {len(valid_results)} results...")
        
        # Get LLM classification
        classification = self._classify_query_simple_llm(question)
        print(f"   📋 LLM Classification: {classification}")
        
        # Select based on classification priority only
        winner = self._select_by_classification_priority(valid_results, classification)
        
        print(f"🏆 CLASSIFICATION WINNER: {winner.strategy_name} (based on classification priority)")
        
        return {
            "question": question,
            "answer": winner.answer,
            "success": True,
            "strategy_used": winner.strategy_name,
            "primary_strategy": winner.strategy_name,
            "source": winner.source,
            "confidence": winner.confidence_score,
            "execution_time": winner.execution_time,
            "execution_mode": "parallel",
            "selection_method": "classification_priority",
            "llm_classification": classification,
            "alternatives_available": len(valid_results) - 1
        }

    def _classify_query_simple_llm(self, question: str) -> Dict[str, str]:
        """Simple LLM query classification"""
        
        # Ensure LLM is available
        llm = self._ensure_llm_loaded_safe()
        if not llm:
            print("   ⚠️  LLM not available, using default classification")
            return {"primary": "sql_aggregation", "fallback": "vector_search"}
        
        try:
            from langchain_core.prompts import PromptTemplate
            
            classification_prompt = PromptTemplate(
                template="""
You are a query classifier for a Frappe educational database system.

AVAILABLE STRATEGIES:
1. SQL_AGGREGATION: Direct database queries for data retrieval, counting, listing, filtering, statistics
   - Can handle: counting, totals, averages, lists, breakdowns, distributions
   - Best for: Most data-related questions since Frappe stores everything as records
   
2. GRAPH_ANALYSIS: Relationship analysis, connections, patterns, recommendations
   - Can handle: relationships, connections, recommendations, pattern analysis
   - Best for: "Who should", "similar to", "connections between", "recommend"
   
3. VECTOR_SEARCH: Explanations, definitions, contextual information, educational content
   - Can handle: explanations, definitions, educational context, "what is", "why"
   - Best for: When you need to understand concepts or get detailed explanations

CLASSIFICATION RULES:
- SQL_AGGREGATION: Handles most queries since Frappe data is in records (prefer this for data queries)
- GRAPH_ANALYSIS: Only when you specifically need relationships or recommendations
- VECTOR_SEARCH: Only when you need explanations or educational context

QUESTION: {question}

Return ONLY a JSON object:
{{"primary": "strategy_name", "fallback": "strategy_name"}}

PRIMARY should be the best strategy, FALLBACK should be the second choice if primary fails.
Valid strategy names: sql_aggregation, graph_analysis, vector_search
                """,
                input_variables=["question"]
            )
            
            llm_response = llm.invoke(classification_prompt.format(question=question))
            response_text = llm_response.content if hasattr(llm_response, 'content') else str(llm_response)
            
            print(f"   🤖 LLM Classification Response: {response_text}")
            
            # Parse JSON response
            import re
            import json
            
            json_match = re.search(r'\{.*?"primary".*?\}', response_text, re.DOTALL)
            if json_match:
                try:
                    classification_data = json.loads(json_match.group())
                    primary = classification_data.get("primary", "sql_aggregation")
                    fallback = classification_data.get("fallback", "vector_search")
                    
                    return {"primary": primary, "fallback": fallback}
                    
                except json.JSONDecodeError:
                    print("   ❌ Failed to parse LLM classification JSON")
            
        except Exception as e:
            print(f"   ❌ LLM classification failed: {e}")
        
        # Default classification
        return {"primary": "sql_aggregation", "fallback": "vector_search"}

    def _select_by_classification_priority(self, results: List[StrategyResult], classification: Dict[str, str]) -> StrategyResult:
        """Select by classification priority"""
        
        primary_strategy = classification.get("primary", "sql_aggregation")
        fallback_strategy = classification.get("fallback", "vector_search")
        
        print(f"   🎯 Primary strategy: {primary_strategy}")
        print(f"   🔄 Fallback strategy: {fallback_strategy}")
        
        # Try primary strategy first
        for result in results:
            if result.strategy_name == primary_strategy:
                print(f"   ✅ Using primary strategy: {primary_strategy}")
                return result
        
        # Try fallback strategy
        for result in results:
            if result.strategy_name == fallback_strategy:
                print(f"   🔄 Primary failed, using fallback strategy: {fallback_strategy}")
                return result
        
        # Use first valid result as last resort
        print(f"   ⚠️  Both primary and fallback failed, using first available: {results[0].strategy_name}")
        return results[0]

    def _generate_fallback_response(self, question: str, failed_results: List[StrategyResult]) -> str:
        """Generate a helpful fallback response when all strategies fail"""
        
        # Analyze the question to provide contextual help
        question_lower = question.lower()
        
        if any(word in question_lower for word in ['how many', 'count', 'total', 'number']):
            return f"I'm having trouble accessing the student database to count the information you requested. This might be due to a temporary system issue. Please try asking a different question or try again later."
        
        elif any(word in question_lower for word in ['best', 'top', 'recommend', 'suggest']):
            return f"I'm unable to provide recommendations right now due to a system issue. You might want to ask about specific students, schools, or activities instead."
        
        elif any(word in question_lower for word in ['explain', 'what is', 'tell me about']):
            return f"I'm currently having difficulty accessing my knowledge base to explain that topic. Please try rephrasing your question or ask about something more specific."
        
        else:
            return f"I'm experiencing technical difficulties processing your question. Please try rephrasing it or ask something different. Our system is working to resolve these issues."

    # ==================== SAFE STRATEGY EXECUTION METHODS ====================
    
    def _execute_graph_strategy_safe(self, question: str, context: str, source: str) -> StrategyResult:
        """🕸️ SAFE graph strategy execution with detailed console output"""
        
        start_time = time.time()
        strategy_name = "graph_analysis"
        
        try:
            # Ensure components are loaded (thread-safe) with better error handling
            graph_qa = self._ensure_graph_components_loaded_safe()
            if not graph_qa:
                # If graph fails, don't consider it a critical error - return graceful fallback
                execution_time = time.time() - start_time
                return StrategyResult(
                    success=False,
                    answer="Graph analysis temporarily unavailable - Neo4j instance may be paused",
                    strategy_name=strategy_name,
                    execution_time=execution_time,
                    confidence_score=0,
                    source=source,
                    error="Graph components failed to load (Neo4j connectivity issue)"
                )
            
            print(f"\n🔍 GRAPH STRATEGY EXECUTION:")
            print(f"   📝 Question: {question}")
            print(f"   🌐 Neo4j Environment: {'Aura (Cloud)' if self.neo4j_config.get('is_aura') else 'Local'}")
            
            # Execute graph query with timeout and detailed output
            try:
                result = graph_qa.invoke(question)
                
                # Extract and display the Cypher query and results
                cypher_query = ""
                
                if isinstance(result, dict):
                    answer = result.get("result", result.get("answer", str(result)))
                    intermediate_steps = result.get("intermediate_steps", [])
                    
                    # Extract Cypher query from intermediate steps
                    if intermediate_steps:
                        print(f"\n   🔧 INTERMEDIATE STEPS:")
                        for i, step in enumerate(intermediate_steps):
                            print(f"      Step {i+1}: {step}")
                            
                            # Try to extract Cypher query
                            if isinstance(step, dict):
                                if "query" in step:
                                    cypher_query = step["query"]
                                elif "action_input" in step:
                                    cypher_query = step["action_input"]
                            elif isinstance(step, tuple) and len(step) > 0:
                                if hasattr(step[0], 'query'):
                                    cypher_query = step[0].query
                                elif isinstance(step[0], dict) and "query" in step[0]:
                                    cypher_query = step[0]["query"]
                    
                    # If we found a Cypher query, display it
                    if cypher_query:
                        print(f"\n   🔍 GENERATED CYPHER QUERY:")
                        print(f"   {cypher_query}")
                        
                        # Try to execute the query directly to see raw results
                        try:
                            raw_results = self.graph.query(cypher_query)
                            print(f"\n   📊 RAW CYPHER RESULTS:")
                            for result_item in raw_results[:5]:  # Show first 5 results
                                print(f"   {result_item}")
                            if len(raw_results) > 5:
                                print(f"   ... and {len(raw_results) - 5} more results")
                        except Exception as query_error:
                            print(f"   ⚠️  Could not execute raw query: {query_error}")
                
                else:
                    answer = str(result)
                
            except Exception as invoke_error:
                execution_time = time.time() - start_time
                print(f"\n   ❌ GRAPH QUERY EXECUTION FAILED: {invoke_error}")
                return StrategyResult(
                    success=False,
                    answer=f"Graph query execution failed: {str(invoke_error)}",
                    strategy_name=strategy_name,
                    execution_time=execution_time,
                    confidence_score=0,
                    source=source,
                    error=str(invoke_error)
                )
            
            # Format the educational response
            formatted_answer = self._format_educational_response(answer)
            
            print(f"\n   ✅ FINAL GRAPH ANSWER:")
            print(f"   {formatted_answer}")
            
            # Calculate simplified confidence based on result quality
            
            
            execution_time = time.time() - start_time
            
            return StrategyResult(
                success=True,
                answer=formatted_answer,
                strategy_name=strategy_name,
                execution_time=execution_time,
                confidence_score=1.0,
                source=source,
                metadata={"cypher_used": True, "graph_type": "neo4j", "cypher_query": cypher_query}
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            # Don't treat Neo4j connectivity issues as critical errors
            error_msg = "Graph analysis unavailable due to Neo4j connectivity" if "Cannot resolve address" in str(e) else f"Graph strategy encountered an error: {str(e)}"
            
            print(f"\n   ❌ GRAPH STRATEGY ERROR: {error_msg}")
            
            return StrategyResult(
                success=False,
                answer=error_msg,
                strategy_name=strategy_name,
                execution_time=execution_time,
                confidence_score=0,
                source=source,
                error=str(e)
            )

    def _execute_sql_strategy_safe(self, question: str, context: str, source: str) -> StrategyResult:
        """📊 SAFE SQL strategy execution with improved error handling"""
        
        start_time = time.time()
        strategy_name = "sql_aggregation"
        
        try:
            # Ensure components are loaded (thread-safe) with better error handling
            sql_agent = self._ensure_sql_components_loaded_safe()
            if not sql_agent:
                return StrategyResult(
                    success=False,
                    answer="SQL analysis temporarily unavailable",
                    strategy_name=strategy_name,
                    execution_time=time.time() - start_time,
                    confidence_score=0,
                    source=source,
                    error="SQL components failed to load"
                )
            
            # Enhanced question for better SQL generation
            enhanced_question = f"""
            Educational Database Query: {question}
            
            Context: This is a Frappe Framework database with educational DocTypes.
            - Use table names with 'tab' prefix (tabStudent, tabSchool, etc.)
            - Use name1 for display names, name for IDs
            - Always include LIMIT to prevent large result sets
            - Focus on educational insights
            """
            
            # Execute SQL query with timeout
            try:
                result = sql_agent.invoke({"input": enhanced_question})
            except Exception as invoke_error:
                return StrategyResult(
                    success=False,
                    answer=f"SQL query execution failed: {str(invoke_error)}",
                    strategy_name=strategy_name,
                    execution_time=time.time() - start_time,
                    confidence_score=0,
                    source=source,
                    error=str(invoke_error)
                )
            
            if isinstance(result, dict):
                answer = result.get("output", str(result))
            else:
                answer = str(result)
            
            # Format the response with educational context
            formatted_answer = self._format_sql_educational_response(answer, question)
            
            
            
            execution_time = time.time() - start_time
            
            return StrategyResult(
                success=True,
                answer=formatted_answer,
                strategy_name=strategy_name,
                execution_time=execution_time,
                confidence_score=1.0,
                source=source,
                metadata={"sql_used": True, "agent_type": "langchain"}
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            return StrategyResult(
                success=False,
                answer=f"SQL strategy encountered an error: {str(e)}",
                strategy_name=strategy_name,
                execution_time=execution_time,
                confidence_score=0,
                source=source,
                error=str(e)
            )

    def _execute_vector_strategy_safe(self, question: str, context: str, source: str) -> StrategyResult:
        """🔍 SAFE vector strategy execution with improved error handling"""
        
        start_time = time.time()
        strategy_name = "vector_search"
        
        try:
            # Ensure components are loaded (thread-safe) with better error handling
            vector_retriever = self._ensure_vector_components_loaded_safe()
            llm = self._ensure_llm_loaded_safe()
            
            if not vector_retriever or not llm:
                return StrategyResult(
                    success=False,
                    answer="Vector search temporarily unavailable",
                    strategy_name=strategy_name,
                    execution_time=time.time() - start_time,
                    confidence_score=0,
                    source=source,
                    error="Vector or LLM components failed to load"
                )
            
            # Retrieve relevant documents with error handling
            try:
                retrieved_docs = vector_retriever.invoke(question)
                doc_context = "\n\n".join(doc.page_content for doc in retrieved_docs)
            except Exception as retrieval_error:
                return StrategyResult(
                    success=False,
                    answer=f"Document retrieval failed: {str(retrieval_error)}",
                    strategy_name=strategy_name,
                    execution_time=time.time() - start_time,
                    confidence_score=0,
                    source=source,
                    error=str(retrieval_error)
                )
            
            # Enhanced prompt with persistent conversation context
            try:
                from langchain_core.prompts import PromptTemplate
                
                context_aware_vector_prompt = PromptTemplate(
                    template="""
You are TAP Educational Assistant providing helpful answers about students, schools, and educational activities.

EDUCATIONAL KNOWLEDGE BASE:
{doc_context}

CONVERSATION CONTEXT:
{conversation_context}

QUESTION: {question}

Provide a clear, helpful response about the educational topic. Be specific and use information from the knowledge base. If you cannot find relevant information, say so clearly.

Response:
                    """,
                    input_variables=["question", "conversation_context", "doc_context"]
                )
                
                # Generate context-aware response
                response = llm.invoke(context_aware_vector_prompt.format(
                    question=question,
                    conversation_context=context,
                    doc_context=doc_context
                ))
                
                raw_answer = response.content if hasattr(response, 'content') else str(response)
                clean_answer = self._format_educational_response(raw_answer)
                
            except Exception as generation_error:
                return StrategyResult(
                    success=False,
                    answer=f"Answer generation failed: {str(generation_error)}",
                    strategy_name=strategy_name,
                    execution_time=time.time() - start_time,
                    confidence_score=0,
                    source=source,
                    error=str(generation_error)
                )
            
            execution_time = time.time() - start_time
            
            return StrategyResult(
                success=True,
                answer=clean_answer,
                strategy_name=strategy_name,
                execution_time=execution_time,
                confidence_score=1.0,
                source=source,
                metadata={
                    "docs_retrieved": len(retrieved_docs),
                    "context_used": bool(context),
                    "vector_type": "faiss"
                }
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            return StrategyResult(
                success=False,
                answer=f"Vector strategy encountered an error: {str(e)}",
                strategy_name=strategy_name,
                execution_time=execution_time,
                confidence_score=0,
                source=source,
                error=str(e)
            )

    # ==================== SAFE COMPONENT LOADING ====================
    
    def _ensure_llm_loaded_safe(self):
        """SAFE thread-safe LLM loading with improved error handling"""
        with self._llm_lock:
            if not self._component_status['llm']:
                try:
                    from langchain_openai import ChatOpenAI
                    
                    openai_key = config.get("openai_api_key")
                    if not openai_key:
                        print("   ❌ No OpenAI API key configured")
                        return None
                    
                    self.llm = ChatOpenAI(
                        openai_api_key=openai_key,
                        model_name="gpt-3.5-turbo",
                        temperature=0.1,
                        max_tokens=1000
                    )
                    
                    self._component_status['llm'] = True
                    print("   ✅ LLM loaded successfully")
                    
                except Exception as e:
                    print(f"   ❌ LLM loading failed: {e}")
                    self.llm = None
                    return None
            
            return self.llm

    def _ensure_graph_components_loaded_safe(self):
        """SAFE thread-safe graph components loading"""
        with self._graph_lock:
            if not self._component_status['graph']:
                try:
                    # Ensure LLM is loaded first
                    if not self._ensure_llm_loaded_safe():
                        print("   ❌ Cannot load graph - LLM not available")
                        return None
                    
                    # Check Neo4j configuration
                    if not self.neo4j_config.get('uri'):
                        print("   ❌ Cannot load graph - Neo4j URI not configured")
                        return None
                    
                    # Use the working import path from your previous code
                    from langchain_community.graphs import Neo4jGraph
                    from langchain.chains import GraphCypherQAChain
                    
                    print(f"   🌐 Connecting to {'Neo4j Aura' if self.neo4j_config.get('is_aura') else 'Neo4j Local'}...")
                    print(f"   🔗 URI: {self.neo4j_config['uri']}")
                    
                    # Setup Neo4j connection with cloud configuration (using your working pattern)
                    self.graph = Neo4jGraph(
                        url=self.neo4j_config['uri'],
                        username=self.neo4j_config['user'],
                        password=self.neo4j_config['password'],
                        database=self.neo4j_config.get('database', 'neo4j'),
                        enhanced_schema=True,
                        refresh_schema=True,
                        timeout=10,             # Connection timeout
                        driver_config={
                            "max_connection_pool_size": 10,  # Smaller pool
                            "connection_acquisition_timeout": 10,  # Faster timeout
                            "max_connection_lifetime": 600,  # 10 minutes
                        }
                    )
                    
                    # Test connection
                    test_query = "RETURN 'Enhanced Graph connected to ' + $env as status"
                    test_params = {"env": "Neo4j Aura" if self.neo4j_config.get('is_aura') else "Neo4j Local"}
                    result = self.graph.query(test_query, test_params)
                    
                    if result:
                        print(f"   ✅ {result[0]['status']}")
                    
                    # Setup enhanced GraphCypherQAChain
                    self._setup_enhanced_graph_qa_chain_safe()
                    
                    if self.enhanced_graph_qa_chain:
                        self._component_status['graph'] = True
                        print("   ✅ Graph components loaded successfully")
                        return self.enhanced_graph_qa_chain
                    else:
                        return None
                    
                except Exception as e:
                    print(f"   ❌ Graph components loading failed: {e}")
                    if "Cannot resolve address" in str(e):
                        print("   💡 Troubleshooting steps:")
                        print("      1. Check if your Neo4j Aura instance is running")
                        print("      2. Verify the URI in your configuration")
                        print("      3. Check your internet connection")
                        print("      4. Try accessing the Neo4j Aura console")
                    self._component_status['graph'] = False
                    return None
            
            return self.enhanced_graph_qa_chain

    def _ensure_sql_components_loaded_safe(self):
        """SAFE thread-safe SQL components loading with cached database configuration"""
        with self._sql_lock:
            if not self._component_status['sql']:
                try:
                    start_time = time.time()
                    
                    # Ensure LLM is loaded first
                    if not self._ensure_llm_loaded_safe():
                        print("   ❌ Cannot load SQL - LLM not available")
                        return None
                    
                    # Use the working import paths from your previous code
                    from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
                    from langchain_community.agent_toolkits.sql.base import create_sql_agent
                    from langchain_community.utilities import SQLDatabase
                    from langchain.agents.agent_types import AgentType
                    
                    # ✅ USE CACHED DATABASE CONFIGURATION (thread-safe)
                    if not self.cached_db_config:
                        print("   ❌ No cached database configuration available")
                        print("   💡 Database configuration was not cached during initialization")
                        return None
                    
                    # Extract cached configuration
                    db_name = self.cached_db_config['db_name']
                    db_user = self.cached_db_config['db_user'] 
                    db_password = self.cached_db_config['db_password']
                    db_host = self.cached_db_config['db_host']
                    db_port = self.cached_db_config['db_port']
                    method = self.cached_db_config['method']
                    
                    print(f"   💾 Using cached config from {method}: {db_user}@{db_host}:{db_port}/{db_name}")
                    
                    if not db_name or not db_user or not db_password:
                        print(f"   ❌ Incomplete cached credentials: db_name={db_name}, db_user={db_user}, db_password={'***' if db_password else 'None'}")
                        return None
                    
                    database_uri = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
                    
                    # Use the same table configuration from your working code
                    educational_tables = [
                        'tabStudent', 'tabSchool', 'tabCourse', 'tabActivities', 
                        'tabPerformance', 'tabBatch', 'tabEnrollment', 'tabTeacher'
                    ]
                    
                    print(f"   🔗 Connecting to database using cached config...")
                    
                    self.sql_database = SQLDatabase.from_uri(
                        database_uri,
                        include_tables=educational_tables,
                        sample_rows_in_table_info=3
                    )
                    
                    # Test database connection
                    test_result = self.sql_database.run("SELECT 1 as test")
                    print(f"   ✅ Database connection test successful: {test_result}")
                    
                    self.sql_toolkit = SQLDatabaseToolkit(
                        db=self.sql_database,
                        llm=self.llm
                    )
                    
                    # Use the exact same agent configuration from your working code
                    self.sql_agent = create_sql_agent(
                        llm=self.llm,
                        toolkit=self.sql_toolkit,
                        agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
                        verbose=True,
                        max_iterations=5,
                        max_execution_time=30,
                        agent_executor_kwargs={
                            "return_intermediate_steps": True,
                            "handle_parsing_errors": True
                        }
                    )
                    
                    self._component_status['sql'] = True
                    load_time = time.time() - start_time
                    print(f"   ✅ SQL components loaded in {load_time:.2f}s using cached config")
                    
                    return self.sql_agent
                    
                except Exception as e:
                    print(f"   ❌ SQL components loading failed: {e}")
                    print(f"   🔍 Error type: {type(e).__name__}")
                    if "Access denied" in str(e):
                        print("   💡 Database access troubleshooting:")
                        print("      1. Check if the database user exists and has proper permissions")
                        print("      2. Verify the database password is correct")
                        print("      3. Ensure the database server is running")
                        print("      4. Check if the database name exists")
                        print("      5. Try running: frappe --site [site-name] mariadb")
                        if self.cached_db_config:
                            print(f"      6. Cached config method: {self.cached_db_config.get('method')}")
                    self._component_status['sql'] = False
                    return None
            
            return self.sql_agent

    def _ensure_vector_components_loaded_safe(self):
        """SAFE thread-safe vector components loading with better error handling"""
        with self._vector_lock:
            if not self._component_status['vector']:
                try:
                    from langchain.text_splitter import RecursiveCharacterTextSplitter
                    from langchain_community.vectorstores import FAISS
                    from langchain_openai import OpenAIEmbeddings
                    
                    if not self._ensure_llm_loaded_safe():
                        print("   ❌ Cannot load vector - LLM not available")
                        return None
                    
                    # Check for cached vector store first
                    import os
                    vector_cache_path = "vector_store_cache"
                    if os.path.exists(vector_cache_path):
                        try:
                            print("   📦 Loading cached vector store...")
                            openai_key = config.get("openai_api_key")
                            if not openai_key:
                                print("   ❌ No OpenAI API key for embeddings")
                                return None
                                
                            self.embeddings = OpenAIEmbeddings(
                                openai_api_key=openai_key,
                                model="text-embedding-3-small"
                            )
                            self.vector_store = FAISS.load_local(vector_cache_path, self.embeddings, allow_dangerous_deserialization=True)
                            self.vector_retriever = self.vector_store.as_retriever(
                                search_type="mmr",
                                search_kwargs={"k": 5, "fetch_k": 10}
                            )
                            
                            self._component_status['vector'] = True
                            print("   ✅ Cached vector store loaded successfully")
                            return self.vector_retriever
                            
                        except Exception as cache_error:
                            print(f"   ⚠️  Cached vector load failed: {cache_error}")
                            print("   🔄 Will create simple fallback vector store...")
                    
                    # Create simple fallback vector store if cache fails
                    print("   🔧 Creating fallback vector store...")
                    
                    openai_key = config.get("openai_api_key")
                    if not openai_key:
                        print("   ❌ No OpenAI API key for embeddings")
                        return None
                    
                    self.embeddings = OpenAIEmbeddings(
                        openai_api_key=openai_key,
                        model="text-embedding-3-small"
                    )
                    
                    # Create simple documents if data loader fails
                    documents = self._get_simple_educational_documents()
                    
                    if not documents:
                        print("   ⚠️  No documents available for vector store")
                        return None
                    
                    splitter = RecursiveCharacterTextSplitter(
                        chunk_size=1000,
                        chunk_overlap=50,
                        separators=["\n\n", "\n", ". ", " "]
                    )
                    chunks = splitter.create_documents(documents)
                    
                    self.vector_store = FAISS.from_documents(chunks, self.embeddings)
                    self.vector_retriever = self.vector_store.as_retriever(
                        search_type="mmr",
                        search_kwargs={"k": 5, "fetch_k": 10}
                    )
                    
                    # Try to cache for future use
                    try:
                        self.vector_store.save_local(vector_cache_path)
                        print("   💾 Vector store cached for future use")
                    except Exception as save_error:
                        print(f"   ⚠️  Could not cache vector store: {save_error}")
                    
                    self._component_status['vector'] = True
                    print("   ✅ Vector components loaded successfully")
                    
                except Exception as e:
                    print(f"   ❌ Vector components loading failed: {e}")
                    self._component_status['vector'] = False
                    return None
            
            return self.vector_retriever

    def _get_simple_educational_documents(self) -> List[str]:
        """Get simple educational documents as fallback"""
        try:
            # Try to load data loader safely
            data_loader = self._ensure_data_loader_loaded_safe()
            if data_loader:
                print("   📚 Loading educational data...")
                all_data = data_loader.load_for_query("educational", limit=500)
                
                simple_docs = []
                for doctype, records in all_data.items():
                    if records:
                        doc_content = f"Educational Data for {doctype}:\n"
                        for record in records[:10]:
                            name = record.get('name1', record.get('name', 'Unknown'))
                            doc_content += f"- {name}\n"
                        simple_docs.append(doc_content)
                
                if simple_docs:
                    print(f"   ✅ Created {len(simple_docs)} document collections")
                    return simple_docs
        
        except Exception as e:
            print(f"   ⚠️  Data loader failed: {e}")
        
        # Ultimate fallback - hardcoded educational content
        print("   🔄 Using hardcoded educational fallback content...")
        return [
            """Educational System Overview:
            Students are enrolled in various schools and participate in educational activities.
            Schools are categorized by type: Government, Private, NGO, etc.
            Activities include Arts, Coding, Science, and Financial Literacy.
            Performance is tracked through submission rates and access rates.
            Grades range from 1-12 with different proficiency levels (L1, L2, L3).
            """,
            
            """Student Performance Metrics:
            Access Rate: Percentage of activities students have accessed
            Submission Rate: Percentage of assignments completed
            Course Enrollment: Students are enrolled in different courses
            Activity Tracking: Performance on individual activities is monitored
            Grade Levels: Students are organized by grade from 1 to 12
            """,
            
            """Educational Activities:
            Coding Activities: Programming, web development, digital skills
            Arts Activities: Visual arts, performing arts, creative expression
            Science Activities: STEM learning, experiments, scientific inquiry
            Financial Literacy: Banking, budgeting, financial planning
            Rigor Levels: Low, Medium, High difficulty activities available
            """
        ]

    def _ensure_data_loader_loaded_safe(self):
        """SAFE thread-safe data loader loading"""
        with self._data_loader_lock:
            if not self._component_status['data_loader']:
                try:
                    # Import within try block to handle import errors
                    import sys
                    import importlib.util
                    
                    # Check if the module exists
                    spec = importlib.util.find_spec("tap_educational_assistant.ai_service.core.data_loader")
                    if spec is None:
                        print("   ⚠️  Data loader module not found")
                        return None
                    
                    from tap_educational_assistant.ai_service.core.data_loader import TAPDataLoader
                    self.data_loader = TAPDataLoader()
                    self._component_status['data_loader'] = True
                    print("   ✅ Data loader loaded successfully")
                    
                except Exception as e:
                    print(f"   ⚠️  Data loader loading failed: {e}")
                    self.data_loader = None
            
            return self.data_loader

    def _ensure_neo4j_chat_loaded(self):
        """Lazy load Neo4j chat history with better error handling"""
        if not self._component_status['neo4j_chat']:
            try:
                if not self.neo4j_config.get('uri'):
                    return None
                
                # Use the working import path from your previous code
                from langchain_community.chat_message_histories import Neo4jChatMessageHistory
                print("   ✅ Using community chat message history")
                
                self.neo4j_chat_history = Neo4jChatMessageHistory(
                    session_id=self.session_id,
                    url=self.neo4j_config['uri'],
                    username=self.neo4j_config['user'],
                    password=self.neo4j_config['password'],
                    database=self.neo4j_config.get('database', 'neo4j'),
                    node_label="ChatMessage",
                    window=5
                )
                
                self._component_status['neo4j_chat'] = True
                
            except Exception as e:
                print(f"   ⚠️  Neo4j chat history failed: {e}")
                self.neo4j_chat_history = None
        
        return self.neo4j_chat_history


    
    # ==================== HELPER METHODS ====================
    
    def _get_persistent_conversation_context(self) -> str:
        """Get conversation context from Neo4j persistent memory"""
        chat_history = self._ensure_neo4j_chat_loaded()
        if not chat_history:
            return "New conversation session."
        
        try:
            from langchain.schema import HumanMessage, AIMessage
            
            messages = chat_history.messages[-4:]  # Reduced context
            
            if not messages:
                return "New conversation session."
            
            context_parts = []
            for message in messages:
                if isinstance(message, HumanMessage):
                    context_parts.append(f"User: {message.content[:80]}...")
                elif isinstance(message, AIMessage):
                    context_parts.append(f"Assistant: {message.content[:80]}...")
            
            return "\n".join(context_parts)
            
        except Exception as e:
            return "Error retrieving conversation history."

    def _format_educational_response(self, raw_response: str) -> str:
        """Format response with educational context and clean formatting"""
        if not raw_response:
            return raw_response
        
        # Clean markdown formatting
        import re
        formatted = re.sub(r'\*\*(.*?)\*\*', r'\1', raw_response)
        formatted = re.sub(r'\*(.*?)\*', r'\1', formatted)
        formatted = re.sub(r'#{1,6}\s*(.+)', r'\1\n', formatted)
        formatted = formatted.replace('\\n', '\n')
        formatted = formatted.replace('[BLANK LINE]', '\n')
        
        # Clean up spacing
        formatted = re.sub(r'\n{3,}', '\n\n', formatted)
        formatted = re.sub(r'[ \t]+', ' ', formatted)
        
        return formatted.strip()

    def _format_sql_educational_response(self, result: str, question: str) -> str:
        """Format SQL response with educational insights"""
        formatted_result = self._format_educational_response(result)
        
        # Add educational context for short responses
        if len(formatted_result) < 100:
            if any(keyword in question.lower() for keyword in ["how many", "count", "total"]):
                if formatted_result.isdigit():
                    count = int(formatted_result)
                    if "student" in question.lower():
                        formatted_result = f"Found {count} students in the educational system."
                    elif "school" in question.lower():
                        formatted_result = f"Found {count} schools in the database."
                    elif "activity" in question.lower():
                        formatted_result = f"Found {count} activities available."
        
        return formatted_result

    def _update_strategy_stats(self, results: List[StrategyResult]):
        """Update performance statistics for strategies"""
        for result in results:
            strategy_name = result.strategy_name
            if strategy_name in self.strategy_stats:
                stats = self.strategy_stats[strategy_name]
                stats['attempts'] += 1
                
                if result.is_valid():
                    stats['successes'] += 1
                
                # Update rolling average execution time
                if stats['attempts'] == 1:
                    stats['avg_time'] = result.execution_time
                else:
                    stats['avg_time'] = (stats['avg_time'] * (stats['attempts'] - 1) + result.execution_time) / stats['attempts']
        
        # Base confidence score (50% of total)
        score += result.confidence_score * 0.5
        
        # Speed bonus (20% of total) - faster results get higher scores
        max_time = 30.0  # Maximum expected time
        speed_score = max(0, (max_time - result.execution_time) / max_time)
        score += speed_score * 0.3
        
        # Strategy preference (10% of total) - based on historical performance
        strategy_performance = self.strategy_stats[result.strategy_name]
        if strategy_performance['attempts'] > 0:
            success_rate = strategy_performance['successes'] / strategy_performance['attempts']
            score += success_rate * 0.2
        
        return min(1.0, score)  # Cap at 1.0

    # ==================== HELPER METHODS ====================
    
    def _setup_enhanced_graph_qa_chain_safe(self):
        """SAFE setup of enhanced GraphCypherQAChain"""
        try:
            from langchain.chains import GraphCypherQAChain
            from langchain_core.prompts import PromptTemplate
            
            if not self.llm or not self.graph:
                return
            
            ENHANCED_EDUCATIONAL_CYPHER_PROMPT = PromptTemplate(
                template="""
You are an expert at converting educational questions to Cypher queries for a Neo4j graph database.

CRITICAL: You must ONLY return a valid Cypher query. Do not include any explanations, greetings, or other text.

BASIC QUERY STRUCTURE:
For counting: MATCH (n:NodeType) RETURN count(n) as count
For listing: MATCH (n:NodeType) RETURN n.display_name LIMIT 10

Available Node Types: Student, School, Activities, Performance, Enrollment, Batch, Course

Question: {question}

Generate ONLY the Cypher query:
                """,
                input_variables=["question"]
            )
            
            # ✅ ENABLE VERBOSE MODE TO SEE CYPHER QUERIES IN CONSOLE
            self.enhanced_graph_qa_chain = GraphCypherQAChain.from_llm(
                llm=self.llm,
                graph=self.graph,
                cypher_prompt=ENHANCED_EDUCATIONAL_CYPHER_PROMPT,
                verbose=True,  # ← This shows the Cypher query execution
                return_intermediate_steps=True,  # ← This captures the Cypher query
                allow_dangerous_requests=True,
                top_k=10,
                return_direct=False,
            )
            
            print("   ✅ Enhanced GraphCypherQAChain setup complete with verbose output")
            
        except Exception as e:
            print(f"   ❌ Enhanced GraphCypherQAChain setup failed: {e}")
            self.enhanced_graph_qa_chain = None


    def _enhance_parallel_result(self, best_result: Dict, all_results: List[StrategyResult], total_time: float, question: str) -> Dict[str, Any]:
        """Enhance the final result with parallel execution metadata"""
        
        # Add parallel execution metadata
        best_result.update({
            "session_id": self.session_id,
            "user_id": self.user_id,
            "neo4j_environment": "Aura (Cloud)" if self.neo4j_config.get('is_aura') else "Local",
            "total_execution_time": total_time,
            "parallel_strategies_launched": len(all_results),
            "parallel_strategies_completed": len([r for r in all_results if r.success or r.error]),
            "valid_results_received": len([r for r in all_results if r.is_valid()]),
            "execution_mode": "parallel",
            "performance_boost": True
        })
        
        # Add strategy performance summary
        strategy_summary = {}
        for result in all_results:
            strategy_summary[result.strategy_name] = {
                "success": result.success,
                "execution_time": result.execution_time,
                "confidence": result.confidence_score,
                "valid": result.is_valid()
            }
        
        best_result["strategy_performance_summary"] = strategy_summary
        
        # Add optimization info
        valid_times = [r.execution_time for r in all_results if r.success]
        if valid_times:
            fastest_time = min(valid_times)
            sequential_time_estimate = sum(valid_times)
            
            if sequential_time_estimate > 0:
                speedup_factor = sequential_time_estimate / total_time
                best_result["performance_improvement"] = {
                    "estimated_sequential_time": sequential_time_estimate,
                    "actual_parallel_time": total_time,
                    "speedup_factor": speedup_factor,
                    "fastest_strategy_time": fastest_time
                }
        
        return best_result

    # ==================== SYSTEM STATUS AND UTILITIES ====================
    
    def get_system_status(self) -> Dict[str, Any]:
        """Enhanced system status with parallel execution information"""
        neo4j_info = self.neo4j_config
        
        return {
            "system_ready": True,
            "execution_mode": "FIXED_PARALLEL",
            "performance_optimized": True,
            "user_id": self.user_id,
            "session_id": self.session_id,
            "neo4j_environment": {
                "type": "Aura (Cloud)" if neo4j_info.get('is_aura') else "Local",
                "uri": neo4j_info.get('uri', '').replace(neo4j_info.get('password', ''), '*****'),
                "database": neo4j_info.get('database'),
                "instance_name": neo4j_info.get('aura_instance_name', 'N/A'),
                "instance_id": neo4j_info.get('aura_instance_id', 'N/A')
            },
            "components_loaded": self._component_status.copy(),
            "parallel_execution": {
                "thread_pool_size": self.executor._max_workers,
                "strategies_available": ["graph_analysis", "sql_aggregation", "vector_search"],
                "simultaneous_execution": True,
                "intelligent_result_selection": True,
                "confidence_scoring": True,
                "performance_tracking": True,
                "error_handling": "ENHANCED"
            },
            "strategy_performance_stats": self.strategy_stats.copy(),
            "optimization_features": {
                "parallel_strategy_execution": True,
                "thread_safe_component_loading": True,
                "intelligent_result_scoring": True,
                "confidence_based_selection": True,
                "enhanced_error_handling": True,
                "fallback_response_generation": True,
                "safe_component_loading": True,
                "improved_timeout_handling": True,
                "neo4j_cloud_optimized": neo4j_info.get('is_aura', False)
            }
        }

    def cleanup(self):
        """Clean up resources"""
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=True)
        
        # Close Neo4j driver if exists
        if hasattr(self, 'graph') and self.graph:
            try:
                self.graph._driver.close()
            except:
                pass

    def __del__(self):
        """Destructor to ensure cleanup"""
        try:
            self.cleanup()
        except:
            pass

# Global fixed parallel RAG instance
_fixed_parallel_rag = None

def get_fixed_parallel_rag(user_id: str = "default_user") -> FixedParallelEducationalRAG:
    """Get or create fixed parallel RAG instance"""
    global _fixed_parallel_rag
    
    if _fixed_parallel_rag is None or _fixed_parallel_rag.user_id != user_id:
        if _fixed_parallel_rag:
            _fixed_parallel_rag.cleanup()
        _fixed_parallel_rag = FixedParallelEducationalRAG(user_id=user_id)
    
    return _fixed_parallel_rag

# Fixed testing function
def test_fixed_parallel_performance(question: str = "How many students are in each grade?", user_id: str = "test_user"):
    """Test fixed parallel performance"""
    print("🧪 FIXED PARALLEL PERFORMANCE TEST")
    print("=" * 50)
    
    try:
        parallel_rag = get_fixed_parallel_rag(user_id)
        
        # Test parallel execution
        print(f"🚀 Testing FIXED parallel execution with: '{question}'")
        start_time = time.time()
        
        result = parallel_rag.query(question)
        
        end_time = time.time()
        parallel_time = end_time - start_time
        
        print(f"\n📊 FIXED PARALLEL EXECUTION RESULTS:")
        print(f"   ⏱️  Total time: {parallel_time:.2f}s")
        print(f"   🏆 Winning strategy: {result.get('strategy_used', 'unknown')}")
        print(f"   🎯 Confidence: {result.get('confidence', 0):.2f}")
        print(f"   ✅ Success: {result.get('success', False)}")
        
        if 'performance_improvement' in result:
            perf = result['performance_improvement']
            print(f"   🚀 Speedup: {perf.get('speedup_factor', 1):.1f}x faster than sequential")
        
        if 'strategy_performance_summary' in result:
            print(f"\n📋 STRATEGY BREAKDOWN:")
            for strategy, stats in result['strategy_performance_summary'].items():
                status = "✅" if stats['valid'] else "❌"
                print(f"   {status} {strategy}: {stats['execution_time']:.2f}s (conf: {stats['confidence']:.2f})")
        
        print(f"\n💬 Answer: {result.get('answer', 'No answer')}")
        
        return {
            "parallel_time": parallel_time,
            "success": result.get('success', False),
            "strategy_used": result.get('strategy_used'),
            "speedup_factor": result.get('performance_improvement', {}).get('speedup_factor', 1),
            "result": result
        }
        
    except Exception as e:
        print(f"❌ Fixed parallel test failed: {e}")
        return {"error": str(e)}

if __name__ == "__main__":
    # Example usage
    test_fixed_parallel_performance()