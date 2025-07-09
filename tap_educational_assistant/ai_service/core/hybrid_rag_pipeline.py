# File: ~/frappe-bench/apps/tap_educational_assistant/tap_educational_assistant/ai_service/core/hybrid_rag_pipeline.py

import frappe
from typing import Dict, List, Any, Optional
import logging
import re
import json
import time
from datetime import datetime
import functools

# Core imports (lightweight - loaded immediately)
from tap_educational_assistant.ai_service.config.settings import config, get_neo4j_config

logger = logging.getLogger(__name__)

class IntelligentHybridEducationalRAG:
    """
    Enhanced Hybrid RAG Pipeline with Neo4j Cloud Support
    """
    
    def __init__(self, user_id: str = "default_user"):
        # User identification for persistent memory
        self.user_id = user_id
        self.session_id = f"edu_session_{user_id}_{datetime.now().strftime('%Y%m%d')}"
        
        # Get Neo4j configuration
        self.neo4j_config = get_neo4j_config()
        
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
        
        # Component loading status flags
        self._component_status = {
            'llm': False,
            'data_loader': False,
            'graph': False,
            'sql': False,
            'vector': False,
            'neo4j_chat': False
        }
        
        # Keep lightweight query patterns
        self.query_patterns = {
            "aggregation": [
                r"how many\s+.*\s+(in|by|per|across)",
                r"count\s+.*\s+(students|teachers|activities|schools)",
                r"total\s+(students|teachers|activities|courses)",
                r"average\s+.*\s+(score|rate|performance)",
                r"percentage\s+of",
                r"distribution\s+(of|by|across)",
                r"show\s+.*\s+(statistics|stats|numbers)",
                r"breakdown\s+(of|by)",
                r"sum\s+of",
                r"calculate\s+.*\s+(total|average|mean)"
            ],
            "relationships": [
                r"recommend\s+.*\s+(for|to)",
                r"similar\s+(students|activities|courses)",
                r"learning\s+path",
                r"who\s+(should|can|might)",
                r"students\s+like",
                r"connections\s+between",
                r"influence\s+of",
                r"relationship\s+between",
                r"compare\s+.*\s+(performance|progress)",
                r"peer\s+(analysis|comparison)"
            ],
            "explanatory": [
                r"what\s+is\s+.*\s+(about|like)",
                r"explain\s+.*",
                r"describe\s+.*",
                r"tell\s+me\s+about",
                r"details\s+(of|about)",
                r"information\s+(on|about)",
                r"help\s+.*\s+(understand|with)",
                r"why\s+.*",
                r"how\s+does\s+.*\s+work"
            ]
        }
        
        print("🚀 Enhanced Educational RAG Pipeline Ready!")
        print(f"   ✅ User: {user_id} | Session: {self.session_id}")
        print(f"   🌐 Neo4j: {'Aura (Cloud)' if self.neo4j_config.get('is_aura') else 'Local'}")
        print(f"   ⚡ Proper fallback flow: Graph+Vector→SQL→Pure Vector")
        print(f"   🎯 Initialization: Instant!")

    # ==================== LAZY LOADING METHODS ====================
    
    @functools.lru_cache(maxsize=1)
    def _ensure_llm_loaded(self):
        """Lazy load LLM with caching"""
        if not self._component_status['llm']:
            print("🔧 Loading LLM on demand...")
            start_time = time.time()
            
            try:
                # Lazy import
                from langchain_openai import ChatOpenAI
                
                openai_key = config.get("openai_api_key")
                if not openai_key:
                    print("⚠️  No OpenAI API key - LLM features limited")
                    return None
                
                self.llm = ChatOpenAI(
                    openai_api_key=openai_key,
                    model="gpt-4o-mini",
                    temperature=0.1,
                    max_tokens=2000
                )
                
                self._component_status['llm'] = True
                load_time = time.time() - start_time
                print(f"   ✅ LLM loaded in {load_time:.2f}s")
                
            except Exception as e:
                print(f"❌ LLM loading failed: {e}")
                self.llm = None
        
        return self.llm
    
    @functools.lru_cache(maxsize=1)
    def _ensure_data_loader_loaded(self):
        """Lazy load data loader with caching"""
        if not self._component_status['data_loader']:
            print("🔧 Loading Data Loader on demand...")
            start_time = time.time()
            
            try:
                from tap_educational_assistant.ai_service.core.data_loader import TAPDataLoader
                self.data_loader = TAPDataLoader()
                self._component_status['data_loader'] = True
                
                load_time = time.time() - start_time
                print(f"   ✅ Data Loader loaded in {load_time:.2f}s")
                
            except Exception as e:
                print(f"❌ Data Loader loading failed: {e}")
                self.data_loader = None
        
        return self.data_loader
    
    def _ensure_graph_components_loaded(self):
        """Lazy load graph components with Neo4j Cloud support"""
        if not self._component_status['graph']:
            print("🔧 Loading Graph Analysis components for Neo4j Cloud...")
            start_time = time.time()
            
            try:
                # Lazy imports
                from langchain_community.graphs import Neo4jGraph
                from langchain.chains import GraphCypherQAChain
                from langchain_core.prompts import PromptTemplate
                
                # Ensure LLM is loaded first
                if not self._ensure_llm_loaded():
                    print("❌ Cannot load graph - LLM not available")
                    return None
                
                # Check Neo4j configuration
                if not self.neo4j_config.get('uri'):
                    print("❌ Cannot load graph - Neo4j URI not configured")
                    return None
                
                print(f"   🌐 Connecting to {'Neo4j Aura' if self.neo4j_config.get('is_aura') else 'Neo4j Local'}...")
                
                # Setup Neo4j connection with cloud configuration
                self.graph = Neo4jGraph(
                    url=self.neo4j_config['uri'],
                    username=self.neo4j_config['user'],
                    password=self.neo4j_config['password'],
                    database=self.neo4j_config.get('database', 'neo4j'),
                    enhanced_schema=True,
                    refresh_schema=True,
                    timeout=10,             # ADDED: Connection timeout
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
                
                # Setup enhanced graph QA chain
                self._setup_enhanced_graph_qa_chain()
                
                self._component_status['graph'] = True
                load_time = time.time() - start_time
                print(f"   ✅ Graph components loaded in {load_time:.2f}s")
                
                return self.enhanced_graph_qa_chain
                
            except Exception as e:
                print(f"❌ Graph components loading failed: {e}")
                self._component_status['graph'] = False
                return None
        
        return self.enhanced_graph_qa_chain
    
    def _ensure_neo4j_chat_loaded(self):
        """Lazy load Neo4j chat history with Cloud support"""
        if not self._component_status['neo4j_chat']:
            print("🔧 Loading Neo4j Chat History for Cloud...")
            
            try:
                # Check if Neo4j is available
                if not self.neo4j_config.get('uri'):
                    print("⚠️  Neo4j not configured - chat history disabled")
                    return None
                
                # Lazy import
                from langchain_community.chat_message_histories import Neo4jChatMessageHistory
                
                self.neo4j_chat_history = Neo4jChatMessageHistory(
                    session_id=self.session_id,
                    url=self.neo4j_config['uri'],
                    username=self.neo4j_config['user'],
                    password=self.neo4j_config['password'],
                    database=self.neo4j_config.get('database', 'neo4j'),
                    node_label="ChatMessage",
                    window=10
                )
                
                self._component_status['neo4j_chat'] = True
                cloud_type = "Aura (Cloud)" if self.neo4j_config.get('is_aura') else "Local"
                print(f"   ✅ Neo4j Chat History loaded for {cloud_type} - Session: {self.session_id}")
                
            except Exception as e:
                print(f"❌ Neo4j Chat History loading failed: {e}")
                self.neo4j_chat_history = None
        
        return self.neo4j_chat_history
            
    
    def _ensure_sql_components_loaded(self):
        """Lazy load SQL components with caching"""
        if not self._component_status['sql']:
            print("🔧 Loading SQL Analysis components on demand...")
            start_time = time.time()
            
            try:
                # Lazy imports
                from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
                from langchain_community.agent_toolkits.sql.base import create_sql_agent
                from langchain_community.utilities import SQLDatabase
                from langchain.agents.agent_types import AgentType
                
                # Ensure LLM is loaded first
                if not self._ensure_llm_loaded():
                    print("❌ Cannot load SQL - LLM not available")
                    return None
                
                # Setup database connection
                site_config = frappe.get_site_config()
                db_name = site_config.get('db_name')
                db_password = site_config.get('db_password')
                db_host = site_config.get('db_host', 'localhost')
                db_port = site_config.get('db_port', 3306)
                db_user = site_config.get('db_user', db_name)
                
                database_uri = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
                
                educational_tables = [
                    'tabStudent', 'tabSchool', 'tabCourse', 'tabActivities', 
                    'tabPerformance', 'tabBatch', 'tabEnrollment', 'tabTeacher'
                ]
                
                self.sql_database = SQLDatabase.from_uri(
                    database_uri,
                    include_tables=educational_tables,
                    sample_rows_in_table_info=3
                )
                
                self.sql_toolkit = SQLDatabaseToolkit(
                    db=self.sql_database,
                    llm=self.llm
                )
                
                self.sql_agent = create_sql_agent(
                    llm=self.llm,
                    toolkit=self.sql_toolkit,
                    agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
                    verbose=True,
                    max_iterations=5,
                    max_execution_time=30,
                    #early_stopping_method="generate",
                    agent_executor_kwargs={
                        "return_intermediate_steps": True,
                        "handle_parsing_errors": True
                    }
                )
                
                self._component_status['sql'] = True
                load_time = time.time() - start_time
                print(f"   ✅ SQL components loaded in {load_time:.2f}s")
                
                return self.sql_agent
                
            except Exception as e:
                print(f"❌ SQL components loading failed: {e}")
                self._component_status['sql'] = False
                return None
        
        return self.sql_agent
    
    def _ensure_vector_components_loaded(self):
        """Lazy load vector components with caching"""
        if not self._component_status['vector']:
            print("🔧 Loading Vector Search components on demand...")
            start_time = time.time()
            
            try:
                # Lazy imports
                from langchain.text_splitter import RecursiveCharacterTextSplitter
                from langchain_community.vectorstores import FAISS
                from langchain_openai import OpenAIEmbeddings
                
                # Ensure LLM and data loader are available
                if not self._ensure_llm_loaded():
                    print("❌ Cannot load vector - LLM not available")
                    return None
                
                if not self._ensure_data_loader_loaded():
                    print("❌ Cannot load vector - Data Loader not available")
                    return None
                
                # Setup embeddings
               # OPTIMIZED: Check for cached vector store first
                import os
                vector_cache_path = "vector_store_cache"
                if os.path.exists(vector_cache_path):
                    try:
                        print("   📦 Loading cached vector store...")
                        openai_key = config.get("openai_api_key")
                        self.embeddings = OpenAIEmbeddings(
                            openai_api_key=openai_key,
                            model="text-embedding-3-small"
                        )
                        self.vector_store = FAISS.load_local(vector_cache_path, self.embeddings)
                        self.vector_retriever = self.vector_store.as_retriever(
                            search_type="mmr",
                            search_kwargs={"k": 8, "fetch_k": 16}  # REDUCED: From k=15, fetch_k=30
                        )
                        
                        self._component_status['vector'] = True
                        load_time = time.time() - start_time
                        print(f"   ✅ CACHED Vector store loaded in {load_time:.2f}s")
                        
                        return self.vector_retriever
                        
                    except Exception as cache_error:
                        print(f"   ⚠️  Cached vector load failed: {cache_error}")
                        print("   🔄 Building new vector store...")
                
                # OPTIMIZED: Build minimal vector store if cache fails
                if not self._ensure_data_loader_loaded():
                    print("❌ Cannot load vector - Data Loader not available")
                    return None
                
                # Setup embeddings
                openai_key = config.get("openai_api_key")
                self.embeddings = OpenAIEmbeddings(
                    openai_api_key=openai_key,
                    model="text-embedding-3-small"
                )                        
    
                
                # Build vector store with cached documents
                aggregated_documents = self._get_cached_aggregated_documents()
                
                # Create chunks and build FAISS index
                splitter = RecursiveCharacterTextSplitter(
                    chunk_size=2000,
                    chunk_overlap=100,
                    separators=["\n\n=== END RECORD ===\n\n", "\n\n", "\n", ". ", " "]
                )
                chunks = splitter.create_documents(aggregated_documents)
                
                self.vector_store = FAISS.from_documents(chunks, self.embeddings)
                self.vector_retriever = self.vector_store.as_retriever(
                    search_type="mmr",
                    search_kwargs={"k": 8, "fetch_k": 16}
                )
                
                try:
                    self.vector_store.save_local(vector_cache_path)
                    print("   💾 Vector store cached for future use")
                except Exception as save_error:
                    print(f"   ⚠️  Could not cache vector store: {save_error}")
                
                self._component_status['vector'] = True
                load_time = time.time() - start_time
                print(f"   ✅ OPTIMIZED Vector components built in {load_time:.2f}s")
                
                return self.vector_retriever
                
            except Exception as e:
                print(f"❌ Vector components loading failed: {e}")
                self._component_status['vector'] = False
                return None
        
        return self.vector_retriever
        
    

    # ==================== CACHED HELPER METHODS ====================
    
    @functools.lru_cache(maxsize=1)
    def _get_cached_aggregated_documents(self) -> List[str]:
        """Cache aggregated documents creation (expensive operation)"""
        print("🏗️  Creating aggregated documents (cached)...")
        
        # Ensure data loader is available
        data_loader = self._ensure_data_loader_loaded()
        if not data_loader:
            return []
        
        all_data = data_loader.load_for_query("general", limit=1000)
        aggregated_docs = []
        
        for doctype, records in all_data.items():
            if not records:
                continue
                
            doc_parts = []
            
            # Document header
            doc_parts.append(f"""EDUCATIONAL DATA - {doctype.upper()} COMPLETE COLLECTION
Total Records: {len(records)}
Document Type: Complete aggregated collection for comprehensive analysis
Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

=== COMPLETE RECORD COLLECTION ===
""")
            
            # Add each record
            for i, record in enumerate(records, 1):
                record_content = self._create_detailed_record(doctype, record, i, len(records))
                doc_parts.append(record_content)
            
            aggregated_docs.append("\n".join(doc_parts))
        
        print(f"✅ Created and cached {len(aggregated_docs)} document collections")
        return aggregated_docs
    
    def _create_detailed_record(self, doctype: str, record: Dict, record_num: int, total_records: int) -> str:
        """Create detailed record representation"""
        record_parts = [f"""
--- RECORD {record_num} of {total_records} ---
{doctype} ID: {record.get('name', f'Record_{record_num}')}
{doctype} Name: {record.get('name1', record.get('display_name', 'Unknown'))}"""]
        
        # Add all available fields
        field_parts = []
        for key, value in record.items():
            if key not in ['name', 'owner', 'creation', 'modified', 'modified_by'] and value is not None:
                field_label = key.replace('_', ' ').title()
                field_parts.append(f"{field_label}: {value}")
        
        if field_parts:
            record_parts.append("Details:")
            record_parts.extend([f"  {field}" for field in field_parts])
        
        record_parts.append(f"=== END RECORD {record_num} ===\n")
        
        return "\n".join(record_parts)

    # ==================== ENHANCED SETUP METHODS ====================
    
    def _setup_enhanced_graph_qa_chain(self):
        """Setup enhanced GraphCypherQAChain with educational intelligence"""
        try:
            # Lazy import
            from langchain.chains import GraphCypherQAChain
            from langchain_core.prompts import PromptTemplate
            
            if not self.llm or not self.graph:
                print("⚠️  Cannot setup enhanced Graph QA chain - missing LLM or Graph")
                return
            
            print("🔧 Setting up Enhanced GraphCypherQAChain for Cloud...")
            
            # Enhanced Cypher generation prompt for educational domain
            ENHANCED_EDUCATIONAL_CYPHER_PROMPT = PromptTemplate(
                template="""
You are an expert at converting educational questions to Cypher queries for a Neo4j graph database.

CRITICAL: You must ONLY return a valid Cypher query. Do not include any explanations, greetings, or other text.

ACTUAL DATA STRUCTURE:
Nodes: Student, School, Activities, Performance, Enrollment, Batch, Course

VERIFIED RELATIONSHIPS:
- (Student)-[:STUDIES_AT]->(School)
- (Performance)-[:TRACKS_STUDENT]->(Student)  
- (Performance)-[:MEASURES_ACTIVITY]->(Activities)

VERIFIED PROPERTY NAMES:
- Student: display_name, grade, school_id, access_rate, submission_rate
- Activities: display_name, content_skill, rigor
- Performance: student, activity, sent_, accessed_, submitted_

NOTE: This query will run on Neo4j Cloud (Aura) - ensure compatibility.

Schema: {schema}
Question: {question}

Generate ONLY the Cypher query:
                """,
                input_variables=["schema", "question"]
            )
            
            # Create enhanced GraphCypherQAChain
            self.enhanced_graph_qa_chain = GraphCypherQAChain.from_llm(
                llm=self.llm,
                graph=self.graph,
                cypher_prompt=ENHANCED_EDUCATIONAL_CYPHER_PROMPT,
                verbose=True,
                return_intermediate_steps=True,
                allow_dangerous_requests=True,
                


                top_k=15,
                return_direct=True,
            )
            
            print("✅ Enhanced GraphCypherQAChain setup complete for Cloud")
            
        except Exception as e:
            print(f"❌ Enhanced GraphCypherQAChain setup failed: {e}")
            self.enhanced_graph_qa_chain = None


    # ==================== MAIN QUERY PROCESSING ====================
    
    def query(self, question: str) -> Dict[str, Any]:
        """Enhanced query processing with proper fallback flow"""
        
        print(f"\n🤖 Processing: {question}")
        print(f"📚 Session: {self.session_id}")
        cloud_info = f"🌐 {'Aura (Cloud)' if self.neo4j_config.get('is_aura') else 'Local'}"
        print(f"{cloud_info}")
        
        try:
            # Add to persistent conversation history (lazy loaded)
            chat_history = self._ensure_neo4j_chat_loaded()
            if chat_history:
                chat_history.add_user_message(question)
            
            # Get conversation context
            conversation_context = self._get_persistent_conversation_context()
            
            # Step 1: Classify the query type with conversation context
            query_classification = self._classify_query_intelligently(question, conversation_context)
            
            print(f"🔍 Query Type: {query_classification['query_type']}")
            print(f"🎯 Optimal Strategy: {query_classification['optimal_strategy']}")
            print(f"🧠 Confidence: {query_classification['confidence']}")
            
            # Step 2: Execute with proper fallback flow
            primary_result = self._execute_strategy_with_proper_fallback(
                question, query_classification, conversation_context
            )
            
            # Step 3: Add classification info
            primary_result["query_classification"] = query_classification
            primary_result["optimal_strategy"] = query_classification["optimal_strategy"]
            primary_result["session_id"] = self.session_id
            primary_result["user_id"] = self.user_id
            primary_result["neo4j_environment"] = "Aura (Cloud)" if self.neo4j_config.get('is_aura') else "Local"
            
            # Store response in persistent conversation history
            if chat_history and primary_result.get("answer"):
                chat_history.add_ai_message(primary_result["answer"])
            
            return primary_result
            
        except Exception as e:
            error_msg = f"Enhanced query processing failed: {str(e)}"
            print(f"❌ {error_msg}")
            
            fallback_result = {
                "question": question,
                "answer": error_msg,
                "success": False,
                "error": str(e),
                "optimal_strategy": "error",
                "session_id": self.session_id,
                "neo4j_environment": "Aura (Cloud)" if self.neo4j_config.get('is_aura') else "Local"
            }
            
            chat_history = self._ensure_neo4j_chat_loaded()
            if chat_history:
                chat_history.add_ai_message(f"Error: {error_msg}")
            
            return fallback_result


    # ==================== PROPER FALLBACK FLOW IMPLEMENTATION ====================
    
    def _execute_strategy_with_proper_fallback(self, question: str, classification: Dict, context: str) -> Dict[str, Any]:
        """Execute strategy with proper fallback flow"""
        
        optimal_strategy = classification["optimal_strategy"]
        
        if optimal_strategy == "graph_analysis":
            return self._execute_graph_fallback_chain(question, context)
        elif optimal_strategy == "sql_aggregation":
            return self._execute_sql_fallback_chain(question, context)
        elif optimal_strategy == "vector_search":
            return self._execute_pure_vector_search(question, context)
        else:
            return {
                "success": False,
                "answer": f"Unknown strategy: {optimal_strategy}",
                "error": "Invalid strategy"
            }
    
    def _execute_graph_fallback_chain(self, question: str, context: str) -> Dict[str, Any]:
        """🔄 Graph + Vector → SQL → Pure Vector Chain"""
        
        print("🔄 **Starting Graph Fallback Chain**")
        print("**1. Graph + Vector Enhancement (First Choice)**")
        
        # Step 1: Try Graph Analysis with Vector Enhancement
        try:
            graph_qa = self._ensure_graph_components_loaded()
            if not graph_qa:
                print("❌ Graph components failed to load → Skip to SQL")
                return self._fallback_to_sql_in_graph_chain(question, context, "graph_load_failed")
            
            graph_result = self._execute_graph_query(question)
            
            if self._is_graph_success(graph_result):
                print("✅ Graph succeeds → Extract entities → Enhance with Vector context")
                
                # Extract entities from graph result
                entities = self._extract_educational_entities(graph_result.get("answer", ""))
                
                if entities and any(entities.values()):
                    # Enhance with vector context
                    enhanced_result = self._enhance_with_vector_context(question, graph_result, entities)
                    enhanced_result["fallback_chain"] = ["graph_success", "vector_enhanced"]
                    enhanced_result["strategy_used"] = "graph_analysis"
                    enhanced_result["enhancement_applied"] = True
                    return enhanced_result
                else:
                    # Graph succeeded but no entities to enhance
                    graph_result["fallback_chain"] = ["graph_success", "no_enhancement"]
                    graph_result["strategy_used"] = "graph_analysis"
                    graph_result["enhancement_applied"] = False
                    return graph_result
            
            else:
                print("❌ Graph fails → Try SQL")
                return self._fallback_to_sql_in_graph_chain(question, context, "graph_failed")
                
        except Exception as e:
            print(f"❌ Graph chain error: {e} → Try SQL")
            return self._fallback_to_sql_in_graph_chain(question, context, "graph_error")
    
    def _fallback_to_sql_in_graph_chain(self, question: str, context: str, reason: str) -> Dict[str, Any]:
        """**2. SQL Fallback in Graph Chain**"""
        
        print("**2. SQL Fallback (Second Choice)**")
        
        try:
            sql_agent = self._ensure_sql_components_loaded()
            if not sql_agent:
                print("❌ SQL components failed to load → Skip to Pure Vector")
                return self._fallback_to_pure_vector_in_graph_chain(question, context, f"{reason}_sql_load_failed")
            
            sql_result = self._execute_sql_query(question)
            
            if self._is_sql_success(sql_result):
                print("✅ SQL succeeds in fallback")
                sql_result["fallback_chain"] = [reason, "sql_fallback_success"]
                sql_result["strategy_used"] = "sql_aggregation"
                sql_result["primary_strategy_failed"] = "graph_analysis"
                return sql_result
            else:
                print("❌ SQL also fails → Pure Vector")
                return self._fallback_to_pure_vector_in_graph_chain(question, context, f"{reason}_sql_failed")
                
        except Exception as e:
            print(f"❌ SQL fallback error: {e} → Pure Vector")
            return self._fallback_to_pure_vector_in_graph_chain(question, context, f"{reason}_sql_error")
    
    def _fallback_to_pure_vector_in_graph_chain(self, question: str, context: str, reason: str) -> Dict[str, Any]:
        """**3. Pure Vector Fallback in Graph Chain**"""
        
        print("**3. Pure Vector Fallback (Final Choice)**")
        
        try:
            vector_result = self._execute_pure_vector_search(question, context)
            vector_result["fallback_chain"] = [reason, "pure_vector_final"]
            vector_result["strategy_used"] = "vector_search"
            vector_result["primary_strategy_failed"] = "graph_analysis"
            vector_result["source"] = "Pure Vector Search (Final Fallback from Graph)"
            return vector_result
            
        except Exception as e:
            print(f"❌ All strategies failed: {e}")
            return {
                "success": False,
                "answer": f"All analysis strategies failed. Please try rephrasing your question.",
                "error": "Complete fallback chain failure",
                "strategy_used": "none",
                "fallback_chain": [reason, "complete_failure"]
            }
    
    def _execute_sql_fallback_chain(self, question: str, context: str) -> Dict[str, Any]:
        """🔄 SQL → Graph + Vector → Pure Vector Chain"""
        
        print("🔄 **Starting SQL Fallback Chain**")
        print("**1. SQL Analysis (First Choice)**")
        
        # Step 1: Try SQL Analysis
        try:
            sql_agent = self._ensure_sql_components_loaded()
            if not sql_agent:
                print("❌ SQL components failed to load → Skip to Graph")
                return self._fallback_to_graph_in_sql_chain(question, context, "sql_load_failed")
            
            sql_result = self._execute_sql_query(question)
            
            if self._is_sql_success(sql_result):
                print("✅ SQL succeeds")
                sql_result["fallback_chain"] = ["sql_success"]
                sql_result["strategy_used"] = "sql_aggregation"
                sql_result["enhancement_applied"] = False
                return sql_result
            
            else:
                print("❌ SQL fails → Try Graph + Vector")
                return self._fallback_to_graph_in_sql_chain(question, context, "sql_failed")
                
        except Exception as e:
            print(f"❌ SQL chain error: {e} → Try Graph")
            return self._fallback_to_graph_in_sql_chain(question, context, "sql_error")
    
    def _fallback_to_graph_in_sql_chain(self, question: str, context: str, reason: str) -> Dict[str, Any]:
        """**2. Graph + Vector Fallback in SQL Chain**"""
        
        print("**2. Graph + Vector Fallback (Second Choice)**")
        
        try:
            graph_qa = self._ensure_graph_components_loaded()
            if not graph_qa:
                print("❌ Graph components failed to load → Skip to Pure Vector")
                return self._fallback_to_pure_vector_in_sql_chain(question, context, f"{reason}_graph_load_failed")
            
            graph_result = self._execute_graph_query(question)
            
            if self._is_graph_success(graph_result):
                print("✅ Graph succeeds in fallback → Extract entities → Enhance with Vector")
                
                # Extract entities and enhance with vector context
                entities = self._extract_educational_entities(graph_result.get("answer", ""))
                
                if entities and any(entities.values()):
                    enhanced_result = self._enhance_with_vector_context(question, graph_result, entities)
                    enhanced_result["fallback_chain"] = [reason, "graph_vector_fallback_success"]
                    enhanced_result["strategy_used"] = "graph_analysis"
                    enhanced_result["primary_strategy_failed"] = "sql_aggregation"
                    enhanced_result["enhancement_applied"] = True
                    return enhanced_result
                else:
                    graph_result["fallback_chain"] = [reason, "graph_fallback_success"]
                    graph_result["strategy_used"] = "graph_analysis"
                    graph_result["primary_strategy_failed"] = "sql_aggregation"
                    graph_result["enhancement_applied"] = False
                    return graph_result
            
            else:
                print("❌ Graph also fails → Pure Vector")
                return self._fallback_to_pure_vector_in_sql_chain(question, context, f"{reason}_graph_failed")
                
        except Exception as e:
            print(f"❌ Graph fallback error: {e} → Pure Vector")
            return self._fallback_to_pure_vector_in_sql_chain(question, context, f"{reason}_graph_error")
    
    def _fallback_to_pure_vector_in_sql_chain(self, question: str, context: str, reason: str) -> Dict[str, Any]:
        """**3. Pure Vector Fallback in SQL Chain**"""
        
        print("**3. Pure Vector Fallback (Final Choice)**")
        
        try:
            vector_result = self._execute_pure_vector_search(question, context)
            vector_result["fallback_chain"] = [reason, "pure_vector_final"]
            vector_result["strategy_used"] = "vector_search"
            vector_result["primary_strategy_failed"] = "sql_aggregation"
            vector_result["source"] = "Pure Vector Search (Final Fallback from SQL)"
            return vector_result
            
        except Exception as e:
            print(f"❌ All strategies failed: {e}")
            return {
                "success": False,
                "answer": f"All analysis strategies failed. Please try rephrasing your question.",
                "error": "Complete fallback chain failure",
                "strategy_used": "none",
                "fallback_chain": [reason, "complete_failure"]
            }

    # ==================== INDIVIDUAL STRATEGY EXECUTION ====================
    
    def _execute_graph_query(self, question: str) -> Dict[str, Any]:
        """Execute graph query with lazy loading"""
        
        try:
            graph_qa = self._ensure_graph_components_loaded()
            if not graph_qa:
                return {"success": False, "error": "Graph analysis not available"}
            
            result = graph_qa.invoke(question)
            
            if isinstance(result, dict):
                answer = result.get("result", result.get("answer", str(result)))
                intermediate_steps = result.get("intermediate_steps", [])
                
                # Extract Cypher query if available
                cypher_query = ""
                if intermediate_steps:
                    for step in intermediate_steps:
                        if isinstance(step, dict) and "query" in step:
                            cypher_query = step["query"]
                            break
                        elif isinstance(step, tuple) and len(step) > 0:
                            if hasattr(step[0], 'query'):
                                cypher_query = step[0].query
                            elif isinstance(step[0], dict) and "query" in step[0]:
                                cypher_query = step[0]["query"]
            else:
                answer = str(result)
                cypher_query = ""
            
            # Format the educational response
            formatted_answer = self._format_educational_response(answer)
            
            return {
                "success": True,
                "answer": formatted_answer,
                "source": "Enhanced Neo4j Graph Analysis",
                "cypher_query": cypher_query,
                "query_type": "enhanced_graph_analysis"
            }
            
        except Exception as e:
            print(f"❌ Graph query failed: {e}")
            return {
                "success": False,
                "answer": f"Graph query failed: {str(e)}",
                "error": str(e),
                "query_type": "enhanced_graph_analysis"
            }
    
    def _execute_sql_query(self, question: str) -> Dict[str, Any]:
        """Execute SQL query with lazy loading"""
        
        try:
            sql_agent = self._ensure_sql_components_loaded()
            if not sql_agent:
                return {"success": False, "error": "SQL analysis not available"}
            
            enhanced_question = f"""
            Educational Database Query: {question}
            
            Context: This is a Frappe Framework database with educational DocTypes.
            - Use table names with 'tab' prefix (tabStudent, tabSchool, etc.)
            - Use name1 for display names, name for IDs
            - Always include LIMIT to prevent large result sets
            - Focus on educational insights
            """
            
            result = sql_agent.invoke({"input": enhanced_question})
            
            if isinstance(result, dict):
                answer = result.get("output", str(result))
                intermediate_steps = result.get("intermediate_steps", [])
            else:
                answer = str(result)
                intermediate_steps = []
            
            # Format the response with educational context
            formatted_answer = self._format_sql_educational_response(answer, question)
            
            return {
                "success": True,
                "answer": formatted_answer,
                "source": "Educational SQL Analysis via LangChain Agent",
                "query_type": "sql_aggregation",
                "agent_used": True,
                "intermediate_steps": len(intermediate_steps)
            }
            
        except Exception as e:
            print(f"❌ SQL query failed: {e}")
            return {
                "success": False,
                "answer": f"SQL analysis failed: {str(e)}",
                "error": str(e),
                "query_type": "sql_aggregation"
            }
    
    def _execute_pure_vector_search(self, question: str, context: str) -> Dict[str, Any]:
        """🔄 Pure Vector Search (when vector is the optimal strategy or final fallback)"""
        
        print("🔄 **Pure Vector Search**")
        
        try:
            # Load vector components on demand
            vector_retriever = self._ensure_vector_components_loaded()
            if not vector_retriever:
                return {
                    "success": False,
                    "answer": "Vector search components failed to load. Please try a different type of question.",
                    "error": "Vector components unavailable",
                    "strategy_used": "vector_search",
                    "fallback_chain": ["vector_load_failed"]
                }
            
            # Load LLM on demand
            llm = self._ensure_llm_loaded()
            if not llm:
                return {
                    "success": False,
                    "answer": "LLM not available for vector search.",
                    "error": "LLM unavailable",
                    "strategy_used": "vector_search"
                }
            
            # Retrieve relevant documents
            retrieved_docs = vector_retriever.invoke(question)
            doc_context = "\n\n".join(doc.page_content for doc in retrieved_docs)
            
            # Enhanced prompt with persistent conversation context
            from langchain_core.prompts import PromptTemplate
            
            context_aware_vector_prompt = PromptTemplate(
                template="""
You are TAP Educational Assistant with access to persistent conversation memory.

PERSISTENT CONVERSATION HISTORY:
{conversation_context}

EDUCATIONAL KNOWLEDGE BASE:
{doc_context}

CONTEXT-AWARE RESPONSE INSTRUCTIONS:
- If the question refers to "the students", "those activities", "mentioned before", look for context in conversation history
- Maintain continuity with previous educational discussions
- Provide specific educational insights based on both current query and conversation history
- Use educational terminology and provide actionable recommendations

EDUCATIONAL FORMATTING:
- Use clear paragraphs with proper line breaks
- Start each new entity (student, activity, etc.) on a new line
- NO markdown formatting (*, #, **, [])
- Provide reasoning and educational context

CURRENT QUESTION: {question}

Generate a comprehensive, context-aware educational response:
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
            
            return {
                "success": True,
                "answer": clean_answer,
                "source": "Context-Aware Vector Search",
                "retrieved_docs": len(retrieved_docs),
                "query_type": "context_aware_vector_search",
                "context_used": bool(context)
            }
            
        except Exception as e:
            print(f"❌ Pure vector search error: {e}")
            return {
                "success": False,
                "answer": f"Vector search failed: {str(e)}",
                "error": str(e),
                "strategy_used": "vector_search"
            }

    # ==================== ENTITY EXTRACTION AND VECTOR ENHANCEMENT ====================
    
    def _extract_educational_entities(self, text: str) -> Dict[str, List[str]]:
        """Extract educational entities for vector enhancement"""
        
        if not text or len(text.strip()) < 10:
            return {}
        
        entities = {
            "students": [],
            "schools": [],
            "activities": [],
            "subjects": [],
            "performance_indicators": []
        }
        
        import re
        
        # Extract student names (capitalized names)
        student_patterns = [
            r'\b[A-Z][a-z]+\s+[A-Z][a-z]+\b',  # First Last
            r'student[s]?\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)',  # "student John" 
        ]
        
        for pattern in student_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                if isinstance(match, tuple):
                    match = match[0]
                if len(match.split()) <= 3 and len(match) > 3:  # Reasonable name length
                    entities["students"].append(match.strip())
        
        # Extract school names
        school_patterns = [
            r'([A-Z][a-zA-Z\s]+(?:School|Academy|Institute|International|High|Elementary))',
            r'at\s+([A-Z][a-zA-Z\s]+)',  # "at Brightlands International"
        ]
        
        for pattern in school_patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                if len(match) > 5 and len(match) < 50:  # Reasonable school name length
                    entities["schools"].append(match.strip())
        
        # Extract activity names and subjects
        activity_keywords = [
            'Programming', 'Photography', 'Banking', 'Environmental', 'Physics', 
            'Mathematics', 'Arts', 'Digital', 'Financial', 'Budgeting'
        ]
        
        for keyword in activity_keywords:
            if keyword.lower() in text.lower():
                # Try to extract full activity name around the keyword
                activity_pattern = rf'([A-Z][a-zA-Z\s]*{keyword}[a-zA-Z\s]*)'
                matches = re.findall(activity_pattern, text, re.IGNORECASE)
                for match in matches:
                    if len(match) > 5 and len(match) < 50:
                        entities["activities"].append(match.strip())
        
        # Extract performance indicators
        performance_patterns = [
            r'(high\s+performance)', r'(excellent)', r'(struggling)', 
            r'(top\s+performing)', r'(needs\s+attention)', r'(\d+%\s+completion)'
        ]
        
        for pattern in performance_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            entities["performance_indicators"].extend(matches)
        
        # Extract subject areas (content_skill values)
        subjects = [
            'Digital Arts', 'Programming', 'Web Development', 'Environmental Science',
            'Financial Literacy', 'Mathematics', 'Photography', 'Physics'
        ]
        
        for subject in subjects:
            if subject.lower() in text.lower():
                entities["subjects"].append(subject)
        
        # Clean up duplicates and empty entries
        for key in entities:
            entities[key] = list(set([item for item in entities[key] if item and len(item.strip()) > 2]))
        
        return entities
    
    def _enhance_with_vector_context(self, question: str, primary_result: Dict, entities: Dict[str, List[str]]) -> Dict[str, Any]:
        """Enhance primary result with targeted vector context based on extracted entities"""
        
        vector_retriever = self._ensure_vector_components_loaded()
        llm = self._ensure_llm_loaded()
        
        if not vector_retriever or not llm:
            print("⚠️  Vector enhancement not available")
            primary_result["enhancement_applied"] = False
            return primary_result
        
        try:
            print("🔄 Enhancing with targeted vector context...")
            
            # Create targeted queries based on extracted entities
            enhancement_queries = []
            
            if entities.get("students"):
                student_names = ", ".join(entities["students"][:3])
                enhancement_queries.append(f"Educational background and learning characteristics of students: {student_names}")
            
            if entities.get("activities"):
                activity_names = ", ".join(entities["activities"][:3]) 
                enhancement_queries.append(f"Learning objectives, benefits, and educational context of activities: {activity_names}")
            
            if entities.get("subjects"):
                subject_names = ", ".join(entities["subjects"][:3])
                enhancement_queries.append(f"Educational importance and skill development in: {subject_names}")
            
            if entities.get("schools"):
                school_names = ", ".join(entities["schools"][:2])
                enhancement_queries.append(f"Educational context and characteristics of: {school_names}")
            
            if not enhancement_queries:
                print("⚠️  No entities found for enhancement")
                primary_result["enhancement_applied"] = False
                return primary_result
            
            # Retrieve vector context for each enhancement query
            vector_contexts = []
            for query in enhancement_queries[:2]:  # Limit to 2 queries to avoid overwhelming
                try:
                    docs = vector_retriever.invoke(query)
                    context = "\n".join([doc.page_content for doc in docs[:3]])  # Top 3 docs
                    if context.strip():
                        vector_contexts.append(context)
                except Exception as e:
                    print(f"⚠️  Vector query failed: {e}")
                    continue
            
            if not vector_contexts:
                print("⚠️  No vector context retrieved")
                primary_result["enhancement_applied"] = False
                return primary_result
            
            # Synthesize primary result with vector context using LLM
            from langchain_core.prompts import PromptTemplate
            
            synthesis_prompt = PromptTemplate(
                template="""
You are enhancing an educational analysis result with additional context.

ORIGINAL QUESTION: {question}

PRIMARY ANALYSIS RESULT: {primary_result}

ADDITIONAL EDUCATIONAL CONTEXT: {vector_context}

EXTRACTED ENTITIES:
- Students: {students}
- Activities: {activities}
- Subjects: {subjects}
- Schools: {schools}

ENHANCEMENT INSTRUCTIONS:
1. Start with the primary analysis result (keep it as the foundation)
2. Add relevant educational insights from the additional context
3. Provide actionable recommendations based on the combined information
4. Maintain educational terminology and focus
5. Use clear paragraphs with proper line breaks
6. NO markdown formatting (*, #, **, [])

Create an enhanced educational response that combines both sources naturally:
                """,
                input_variables=["question", "primary_result", "vector_context", "students", "activities", "subjects", "schools"]
            )
            
            enhanced_response = llm.invoke(synthesis_prompt.format(
                question=question,
                primary_result=primary_result.get("answer", ""),
                vector_context="\n\n".join(vector_contexts),
                students=", ".join(entities.get("students", [])[:3]) or "None identified",
                activities=", ".join(entities.get("activities", [])[:5]) or "None identified", 
                subjects=", ".join(entities.get("subjects", [])[:3]) or "None identified",
                schools=", ".join(entities.get("schools", [])[:2]) or "None identified"
            ))
            
            enhanced_answer = enhanced_response.content if hasattr(enhanced_response, 'content') else str(enhanced_response)
            clean_enhanced_answer = self._format_educational_response(enhanced_answer)
            
            # Update the result with enhancement
            enhanced_result = primary_result.copy()
            enhanced_result["answer"] = clean_enhanced_answer
            enhanced_result["enhancement_applied"] = True
            enhanced_result["extracted_entities"] = entities
            enhanced_result["vector_contexts_used"] = len(vector_contexts)
            enhanced_result["original_answer"] = primary_result.get("answer", "")
            
            print(f"✅ Enhanced with {len(vector_contexts)} vector contexts")
            return enhanced_result
            
        except Exception as e:
            print(f"❌ Vector enhancement failed: {e}")
            primary_result["enhancement_applied"] = False
            primary_result["enhancement_error"] = str(e)
            return primary_result

    # ==================== SUCCESS VALIDATION METHODS ====================
    
    def _is_graph_success(self, graph_result: Dict) -> bool:
        """Enhanced graph success detection"""
        
        if not graph_result.get("success", True):  # Default to True for compatibility
            return False
        
        answer = graph_result.get("answer", "").lower().strip()
        
        # Check for "I don't know" responses
        failure_indicators = [
            "i don't know", "i do not know", "i cannot", "unable to",
            "no information", "not enough information", "no data available",
            "cannot answer", "insufficient data", "no results found", "no answer available"
        ]
        
        if any(indicator in answer for indicator in failure_indicators):
            print(f"🔍 Graph semantic failure detected: '{answer[:50]}...'")
            return False
        
        # Check for substantial content
        if len(answer.strip()) < 20:
            print("🔍 Graph result too short")
            return False
        
        return True
    
    def _is_sql_success(self, sql_result: Dict) -> bool:
        """Enhanced SQL success detection"""
        
        if not sql_result.get("success", True):  # Default to True for compatibility
            return False
        
        answer = sql_result.get("answer", "")
        
        # Check for substantial content  
        if len(answer.strip()) < 15:
            print("🔍 SQL result too short")
            return False
        
        # Check for error indicators
        error_indicators = ["failed", "error", "could not", "no data", "empty result", "timeout"]
        if any(indicator in answer.lower() for indicator in error_indicators):
            print("🔍 SQL error indicators detected")
            return False
        
        return True

    # ==================== HELPER METHODS ====================
    
    def _get_persistent_conversation_context(self) -> str:
        """Get conversation context from Neo4j persistent memory"""
        
        chat_history = self._ensure_neo4j_chat_loaded()
        if not chat_history:
            return "No persistent conversation history available."
        
        try:
            # Lazy import
            from langchain.schema import HumanMessage, AIMessage
            
            messages = chat_history.messages[-6:]  # Last 3 exchanges
            
            if not messages:
                return "New conversation session."
            
            context_parts = []
            for message in messages:
                if isinstance(message, HumanMessage):
                    context_parts.append(f"User: {message.content[:100]}...")
                elif isinstance(message, AIMessage):
                    context_parts.append(f"Assistant: {message.content[:100]}...")
            
            return "\n".join(context_parts)
            
        except Exception as e:
            print(f"⚠️  Error retrieving conversation context: {e}")
            return "Error retrieving conversation history."
    
    def _classify_query_intelligently(self, question: str, context: str = "") -> Dict[str, Any]:
        """Enhanced query classification with conversation context"""
        
        # Check for follow-up questions using conversation context
        follow_up_indicators = [
            "the student", "those student", "mentioned student", "the activit", 
            "reasoning", "why", "explain", "provide more", "tell me more",
            "the ones", "them", "they", "these"
        ]
        
        if any(indicator in question.lower() for indicator in follow_up_indicators) and context:
            # Try to maintain strategy continuity for follow-ups
            if "graph" in context.lower() or "relationship" in context.lower():
                return {
                    "query_type": "follow_up_relationships",
                    "optimal_strategy": "graph_analysis",
                    "confidence": 0.9,
                    "reasoning": "Follow-up question with relationship context detected",
                    "classification_method": "context_aware_follow_up",
                    "context_influence": True
                }
        
        # Pattern-based classification (enhanced)
        pattern_classification = self._classify_by_patterns(question)
        
        # LLM-based classification for complex cases (with lazy loading)
        if pattern_classification["confidence"] < 0.8:
            llm_classification = self._classify_by_llm_with_context(question, context)
            
            if llm_classification and llm_classification["confidence"] > pattern_classification["confidence"]:
                return llm_classification
        
        return pattern_classification
    
    def _classify_by_patterns(self, question: str) -> Dict[str, Any]:
        """Pattern-based classification (enhanced)"""
        question_lower = question.lower().strip()
        
        # Relationship patterns (highest priority)
        relationship_score = 0
        for pattern in self.query_patterns["relationships"]:
            if re.search(pattern, question_lower):
                relationship_score += 1
        
        # Enhanced relationship detection
        relationship_indicators = [
            "which schools perform best", "students who excel", "top performing",
            "best students", "similar performance", "peer analysis", "correlation"
        ]
        
        for indicator in relationship_indicators:
            if indicator in question_lower:
                relationship_score += 2  # Higher weight
        
        # SQL patterns
        sql_score = 0
        if relationship_score == 0:  # Only if no strong relationship patterns
            for pattern in self.query_patterns["aggregation"]:
                if re.search(pattern, question_lower):
                    sql_score += 1
        
        # Explanatory patterns
        explanatory_score = 0
        for pattern in self.query_patterns["explanatory"]:
            if re.search(pattern, question_lower):
                explanatory_score += 1
        
        # Determine best match
        scores = {
            "relationships": relationship_score,
            "sql_retrieval": sql_score,
            "explanatory": explanatory_score
        }
        
        best_type = max(scores, key=scores.get)
        best_score = scores[best_type]
        total_score = sum(scores.values())
        
        # Strategy mapping
        strategy_mapping = {
            "relationships": "graph_analysis",
            "sql_retrieval": "sql_aggregation",
            "explanatory": "vector_search"
        }
        
        # Calculate confidence
        if total_score == 0:
            confidence = 0.5
            best_type = "explanatory"
        else:
            confidence = min(0.9, 0.5 + (best_score / total_score) * 0.4)
            
            # Boost confidence for relationship detection
            if best_type == "relationships" and relationship_score > 0:
                confidence = min(0.95, confidence + 0.15)
        
        return {
            "query_type": best_type,
            "optimal_strategy": strategy_mapping[best_type],
            "confidence": confidence,
            "reasoning": f"Pattern analysis: {best_type} patterns matched (score: {best_score}/{total_score})",
            "pattern_scores": scores,
            "classification_method": "enhanced_pattern_based"
        }
    
    def _classify_by_llm_with_context(self, question: str, context: str) -> Optional[Dict[str, Any]]:
        """Enhanced LLM classification with conversation context and lazy loading"""
        
        # Lazy load LLM
        llm = self._ensure_llm_loaded()
        if not llm:
            return None
        
        try:
            from langchain_core.prompts import PromptTemplate
            
            classification_prompt = PromptTemplate(
                template="""
You are an expert query classifier for educational data analysis with conversation awareness.

CONVERSATION CONTEXT:
{context}

CLASSIFY THIS QUERY:
"{question}"

QUERY TYPES AND STRATEGIES:
1. RELATIONSHIPS (→ graph_analysis): Performance comparisons, recommendations, connections, patterns
2. SQL_RETRIEVAL (→ sql_aggregation): Counting, statistics, data listing, simple filtering  
3. EXPLANATORY (→ vector_search): Understanding, definitions, detailed explanations

CONTEXT CONSIDERATIONS:
- If context mentions specific students/schools/activities, lean toward RELATIONSHIPS
- If this is a follow-up asking "why" or "explain", use same strategy as previous
- If asking for more details about previous results, maintain strategy continuity

Respond in JSON format:
{{
    "query_type": "relationships|sql_retrieval|explanatory",
    "optimal_strategy": "graph_analysis|sql_aggregation|vector_search", 
    "confidence": 0.1-1.0,
    "reasoning": "explanation including context influence",
    "context_influenced": true/false
}}
                """,
                input_variables=["question", "context"]
            )
            
            response = llm.invoke(classification_prompt.format(
                question=question, 
                context=context
            ))
            
            classification_text = response.content if hasattr(response, 'content') else str(response)
            
            # Extract JSON
            json_match = re.search(r'\{.*\}', classification_text, re.DOTALL)
            if json_match:
                classification = json.loads(json_match.group())
                classification["classification_method"] = "llm_with_context_lazy"
                return classification
            
        except Exception as e:
            print(f"❌ LLM classification failed: {e}")
        
        return None
    
    def _format_educational_response(self, raw_response: str) -> str:
        """Format response with educational context and clean formatting"""
        
        if not raw_response:
            return raw_response
        
        # Clean markdown formatting
        formatted = re.sub(r'\*\*(.*?)\*\*', r'\1', raw_response)  # Remove bold
        formatted = re.sub(r'\*(.*?)\*', r'\1', formatted)        # Remove italic
        formatted = re.sub(r'#{1,6}\s*(.+)', r'\1\n', formatted)  # Remove headers
        formatted = formatted.replace('\\n', '\n')                # Fix line breaks
        formatted = formatted.replace('[BLANK LINE]', '\n')       # Fix blank lines
        
        # Clean up spacing
        formatted = re.sub(r'\n{3,}', '\n\n', formatted)         # Max 2 newlines
        formatted = re.sub(r'[ \t]+', ' ', formatted)            # Clean spaces
        
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
                        formatted_result = f"📊 Found {count} students matching your criteria.\n\nThis represents the current enrollment data in your educational system."
                    elif "school" in question.lower():
                        formatted_result = f"📊 Found {count} schools in the database.\n\nThis includes all registered educational institutions in the system."
                    elif "activity" in question.lower():
                        formatted_result = f"📊 Found {count} activities available.\n\nThese activities span across different subjects and difficulty levels."
        
        return formatted_result

    # ==================== SYSTEM STATUS AND UTILITIES ====================
    
    def get_system_status(self) -> Dict[str, Any]:
        """Enhanced system status with Neo4j Cloud information"""
        
        neo4j_info = self.neo4j_config
        
        return {
            "system_ready": True,
            "proper_fallback_flow_enabled": True,
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
            "fallback_flows": {
                "graph_strategy": "Graph + Vector → SQL → Pure Vector",
                "sql_strategy": "SQL → Graph + Vector → Pure Vector", 
                "vector_strategy": "Pure Vector (direct)",
                "entity_extraction": "✅ Enabled",
                "vector_enhancement": "✅ Enabled",
                "intelligent_synthesis": "✅ Enabled"
            },
            "optimization_features": {
                "complete_lazy_loading": True,
                "aggressive_caching": True,
                "proper_fallback_chains": True,
                "entity_based_enhancement": True,
                "persistent_conversation_memory": self._component_status.get('neo4j_chat', False),
                "context_aware_classification": True,
                "neo4j_cloud_optimized": neo4j_info.get('is_aura', False)
            }
        }