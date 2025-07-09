# File: ~/frappe-bench/apps/tap_educational_assistant/tap_educational_assistant/quick_query.py

import frappe
import re
import time
import hashlib
import json
from datetime import datetime
from typing import Dict, List, Any, Optional

# Use our optimized components
from tap_educational_assistant.ai_service.core.redis_cache_manager import get_cache_manager
from tap_educational_assistant.utils.response_formatter import (
    clean_response_text, 
    extract_answer_from_output,
    format_cache_metrics,
    format_strategy_display,
    format_error_response
)

class OptimizedCachedRAG:
    """
    Optimized cached RAG implementation with simplified dependencies
    """
    
    def __init__(self, user_id: str = "default_user"):
        self.user_id = user_id
        self.session_id = f"session_{user_id}_{datetime.now().strftime('%Y%m%d')}"
        self.cache_manager = get_cache_manager()
        
        # Lazy load the RAG pipeline only when needed
        self.rag_pipeline = None
        self.rag_available = False
        
        print("🚀 Optimized Educational RAG Pipeline Ready!")
        print(f"   ✅ User: {user_id} | Session: {self.session_id}")
        print(f"   ⚡ Components load on-demand for maximum speed")

    def _ensure_rag_loaded(self):
        """Lazy load RAG pipeline only when needed"""
        if self.rag_pipeline is None:
            try:
                print("🔧 Loading RAG pipeline on demand...")
                start_time = time.time()
                
                # Use lazy import to avoid slow startup
                from tap_educational_assistant.ai_service.core.hybrid_rag_pipeline import IntelligentHybridEducationalRAG
                self.rag_pipeline = IntelligentHybridEducationalRAG(user_id=self.user_id)
                self.rag_available = True
                
                load_time = time.time() - start_time
                print(f"   ✅ RAG pipeline loaded in {load_time:.2f}s")
                
            except Exception as e:
                print(f"⚠️  RAG pipeline failed to load: {e}")
                self.rag_pipeline = None
                self.rag_available = False
        
        return self.rag_pipeline
    
    def query(self, question: str) -> Dict[str, Any]:
        """Ultra-fast query with optimized caching"""
        
        try:
            # Step 1: Ultra-fast cache check
            start_cache_time = time.time()
            
            user_context = {
                "user_id": self.user_id,
                "session_id": self.session_id,
                "date": datetime.now().strftime('%Y%m%d')
            }
            
            # Check cache first
            cached_result = self.cache_manager.get_cached_query_result(question, user_context)
            if cached_result:
                cache_time = time.time() - start_cache_time
                print(f"⚡ ULTRA-FAST CACHE HIT: {cache_time:.3f}s retrieval time")
                cached_result["cache_hit"] = True
                cached_result["cache_retrieval_time"] = cache_time
                return cached_result
            
            print(f"🔄 CACHE MISS: Loading pipeline and processing...")
            
            # Step 2: Lazy load and process
            rag_pipeline = self._ensure_rag_loaded()
            if not rag_pipeline:
                return format_error_response(question, "RAG pipeline not available")
            
            try:
                processing_start = time.time()
                result = rag_pipeline.query(question)
                processing_time = time.time() - processing_start
                
                result["cache_hit"] = False
                result["processing_time"] = processing_time
                
                # Step 3: Fast caching
                if result.get("success", False):
                    try:
                        cache_success = self.cache_manager.cache_query_result(question, user_context, result)
                        if cache_success:
                            print(f"✅ Result cached for future queries")
                        else:
                            print(f"⚠️  Cache storage failed")
                    except Exception as cache_error:
                        print(f"⚠️  Cache error: {cache_error}")
                
                return result
                
            except Exception as e:
                print(f"⚠️  RAG pipeline error: {e}")
                return format_error_response(question, str(e))
                
        except Exception as e:
            print(f"❌ Query processing failed: {e}")
            return format_error_response(question, str(e))
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        try:
            return self.cache_manager.get_cache_stats()
        except Exception as e:
            return {"error": f"Failed to get cache stats: {e}"}
    
    def health_check(self) -> Dict[str, bool]:
        """Check system health"""
        try:
            health = self.cache_manager.health_check()
            health["rag_pipeline"] = self.rag_available
            return health
        except Exception as e:
            return {"error": str(e)}

# Global optimized RAG instance
_optimized_rag = None

def q(question: str, user_id: str = "default_user"):
    """Optimized quick query with simplified caching"""
    global _optimized_rag

    try:
        # Initialize optimized RAG pipeline once
        if _optimized_rag is None or _optimized_rag.user_id != user_id:
            print("🚀 Initializing Optimized Educational RAG Pipeline...")
            _optimized_rag = OptimizedCachedRAG(user_id=user_id)
            print("✅ Optimized RAG ready!")

            # Show system status only during initialization
            health = _optimized_rag.health_check()
            cache_stats = _optimized_rag.get_cache_stats()
            
            print(f"📊 System Health:")
            for component, status in health.items():
                if isinstance(status, bool):
                    status_icon = "✅" if status else "❌"
                    print(f"   {status_icon} {component}")
                else:
                    print(f"   ❌ {component}: {status}")
            
            if not isinstance(cache_stats, dict) or "error" not in cache_stats:
                print(f"📊 Cache Performance:")
                print(f"   Hit Ratio: {cache_stats.get('hit_ratio', 0):.2%}")
                print(f"   Total Operations: {cache_stats.get('total_operations', 0):,}")

        # Process query
        result = _optimized_rag.query(question)

        # Extract information for display
        strategy = result.get('primary_strategy', result.get('strategy_used', result.get('optimal_strategy', 'unknown')))
        source = result.get('source', 'Unknown')
        answer = result.get('answer', 'No answer available')
        cache_hit = result.get('cache_hit', False)
        
        # Clean up strategy name and answer
        strategy_display = format_strategy_display(strategy)
        clean_answer = clean_response_text(answer, "educational")

        # Display results
        print(f"\n🤖 Q: {question}")
        print("=" * 80)
        
        # Show cache and performance status
        if cache_hit:
            print(f"⚡ CACHE HIT: Instant response from Redis")
        else:
            print(f"🔄 CACHE MISS: Processed and cached for future queries")
        
        print(f"🎯 Strategy: {strategy_display}")
        print(f"📊 Source: {source}")
        
        # Add classification info if available
        if result.get('query_classification'):
            classification = result['query_classification']
            print(f"🔍 Query Type: {classification.get('query_type', 'unknown').title()}")
            print(f"🧠 Confidence: {classification.get('confidence', 0):.2f}")
        
        # Show enhancement info if available
        if result.get('enhancement_applied'):
            print(f"✨ Enhanced with {result.get('vector_contexts_used', 0)} additional contexts")
        
        # Show performance metrics
        if result.get('processing_time'):
            processing_time = result['processing_time']
            performance_msg = format_cache_metrics(cache_hit, processing_time)
            print(f"⚡ {performance_msg}")
        
        print("-" * 80)
        print(clean_answer)
        print("=" * 80)

        # Return clean result
        return {
            "question": question,
            "answer": clean_answer,
            "success": result.get('success', True),
            "strategy": strategy,
            "source": source,
            "primary_strategy": strategy,
            "cache_hit": cache_hit,
            "enhancement_applied": result.get('enhancement_applied', False),
            "fallback_used": result.get('fallback_used', False),
            "processing_time": result.get('processing_time', 0)
        }

    except Exception as e:
        error_msg = f"Optimized query processing failed: {str(e)}"
        print(f"❌ Error: {error_msg}")
        
        return format_error_response(question, str(e))

def cache_stats():
    """Quick cache status check"""
    global _optimized_rag
    
    try:
        if _optimized_rag is None:
            _optimized_rag = OptimizedCachedRAG()
        
        stats = _optimized_rag.get_cache_stats()
        health = _optimized_rag.health_check()
        
        print("📊 Quick Cache Status")
        print("=" * 30)
        
        if "error" not in stats:
            print(f"Hit Ratio: {stats['hit_ratio']:.2%}")
            print(f"Total Operations: {stats['total_operations']:,}")
            print(f"Cache Hits: {stats['runtime_stats']['hits']:,}")
            print(f"Cache Misses: {stats['runtime_stats']['misses']:,}")
        else:
            print(f"Stats Error: {stats['error']}")
        
        print("\nCache Health:")
        for layer, is_healthy in health.items():
            if isinstance(is_healthy, bool):
                status_icon = "✅" if is_healthy else "❌"
                print(f"  {status_icon} {layer}")
            else:
                print(f"  ❌ {layer}: {is_healthy}")
        
        return {"stats": stats, "health": health}
        
    except Exception as e:
        print(f"❌ Cache status check failed: {e}")
        return {"error": str(e)}

def performance_test():
    """Quick performance test"""
    global _optimized_rag
    
    print("🧪 Performance Test")
    print("=" * 30)

    try:
        if _optimized_rag is None:
            _optimized_rag = OptimizedCachedRAG()

        test_question = "How many students are in each grade?"
        
        # Test 1: First call (cache miss)
        print(f"Test 1: Cache Miss")
        start_time = time.time()
        result1 = _optimized_rag.query(test_question)
        first_time = time.time() - start_time
        
        # Test 2: Second call (cache hit)
        print(f"Test 2: Cache Hit")
        start_time = time.time()
        result2 = _optimized_rag.query(test_question)
        second_time = time.time() - start_time
        
        # Calculate speedup
        speedup = first_time / second_time if second_time > 0 else float('inf')
        
        print(f"\nResults:")
        print(f"  First call: {first_time:.2f}s (Cache: {'HIT' if result1.get('cache_hit') else 'MISS'})")
        print(f"  Second call: {second_time:.2f}s (Cache: {'HIT' if result2.get('cache_hit') else 'MISS'})")
        print(f"  Speedup: {speedup:.1f}x faster")
        
        cache_working = result2.get('cache_hit', False)
        print(f"  Cache Status: {'✅ Working' if cache_working else '❌ Not working'}")
        
        return {
            "first_call_time": first_time,
            "second_call_time": second_time,
            "speedup": speedup,
            "cache_working": cache_working,
            "success": result1.get('success') and result2.get('success')
        }

    except Exception as e:
        print(f"❌ Performance test failed: {e}")
        return {"error": str(e)}

def clear_cache(confirm: bool = False):
    """Clear all caches"""
    if not confirm:
        print("⚠️  Use clear_cache(confirm=True) to actually clear all caches")
        return {"warning": "Confirmation required"}
    
    try:
        cache_manager = get_cache_manager()
        success = cache_manager.clear_cache()
        
        if success:
            print("🗑️  Cache cleared successfully")
            
            # Reset global RAG instance
            global _optimized_rag
            _optimized_rag = None
            
            return {"success": True}
        else:
            return {"success": False, "error": "Cache clear failed"}
        
    except Exception as e:
        print(f"❌ Cache clear failed: {e}")
        return {"error": str(e)}

def quick_test(question: str = "How many students are in each grade?"):
    """Quick test with a single question"""
    try:
        print(f"🧪 Quick test with: '{question}'")
        
        start_time = time.time()
        result = q(question)
        end_time = time.time()
        
        print(f"⏱️  Total time: {end_time - start_time:.2f}s")
        print(f"✅ Success: {result.get('success', False)}")
        print(f"📦 Cache hit: {result.get('cache_hit', False)}")
        
        return {
            "question": question,
            "success": result.get("success", False),
            "cache_hit": result.get("cache_hit", False),
            "response_time": end_time - start_time,
            "answer_length": len(result.get("answer", ""))
        }
        
    except Exception as e:
        print(f"❌ Quick test failed: {e}")
        return {"error": str(e)}

# Convenience aliases
def status():
    """Alias for cache_stats()"""
    return cache_stats()

def test():
    """Alias for performance_test()"""
    return performance_test()

# Export main functions
__all__ = [
    'q', 'cache_stats', 'performance_test', 'clear_cache', 
    'quick_test', 'status', 'test'
]