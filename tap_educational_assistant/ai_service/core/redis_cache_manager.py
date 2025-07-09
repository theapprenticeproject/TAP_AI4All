# File: ~/frappe-bench/apps/tap_educational_assistant/tap_educational_assistant/ai_service/core/redis_cache_manager.py

import frappe
import redis
import json
import hashlib
import time
import logging
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class TAPRedisCacheManager:
    """
    Lightweight Redis cache manager - removed complex features for speed
    """
    
    def __init__(self):
        self.redis_client = None
        self.cache_config = self._load_cache_config()
        self._setup_redis_client()
        
        # Simple statistics
        self.stats = {
            "hits": 0,
            "misses": 0,
            "sets": 0
        }
        
        print("🚀 Lightweight TAP Redis Cache Manager initialized")
    
    def _load_cache_config(self) -> Dict[str, Any]:
        """Load minimal cache configuration"""
        try:
            site_config = frappe.get_site_config()
            redis_config = site_config.get("redis_cache_config", {})
            
            return {
                "query_cache_ttl": redis_config.get("query_cache_ttl", 3600),
                "conversation_cache_ttl": redis_config.get("conversation_cache_ttl", 86400),
                "compression_enabled": redis_config.get("compression_enabled", False),  # Disabled for speed
            }
            
        except Exception as e:
            logger.warning(f"Could not load cache config: {e}")
            return {
                "query_cache_ttl": 3600,
                "conversation_cache_ttl": 86400,
                "compression_enabled": False
            }
    
    def _setup_redis_client(self):
        """Setup single Redis client - simplified"""
        try:
            site_config = frappe.get_site_config()
            redis_url = site_config.get("redis_cache", "redis://127.0.0.1:6379")
            
            self.redis_client = redis.from_url(
                f"{redis_url}/3",  # Use DB 3 for query cache
                decode_responses=False,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
            
            self.redis_client.ping()
            print("   ✅ Redis client connected")
            
        except Exception as e:
            logger.error(f"Redis client setup failed: {e}")
            self.redis_client = None
    
    def _get_cache_key(self, cache_type: str, identifier: str) -> str:
        """Generate simple cache key"""
        return f"tap:{cache_type}:v1:{identifier}"
    
    def _serialize_data(self, data: Any) -> bytes:
        """Simple JSON serialization"""
        try:
            return json.dumps(data, default=str).encode('utf-8')
        except Exception as e:
            logger.error(f"Serialization failed: {e}")
            return b'{"error": "serialization_failed"}'
    
    def _deserialize_data(self, data: bytes) -> Any:
        """Simple JSON deserialization"""
        try:
            return json.loads(data.decode('utf-8'))
        except Exception as e:
            logger.error(f"Deserialization failed: {e}")
            return {"error": "deserialization_failed"}
    
    def cache_query_result(self, question: str, user_context: Dict, result: Dict, ttl: Optional[int] = None) -> bool:
        """Cache query result - simplified"""
        if not self.redis_client:
            return False
        
        try:
            # Simple key generation
            context_hash = hashlib.md5(json.dumps(user_context, sort_keys=True).encode()).hexdigest()[:8]
            question_hash = hashlib.md5(question.encode()).hexdigest()[:8]
            cache_key = self._get_cache_key("query", f"{question_hash}:{context_hash}")
            
            # Simple cache data structure
            cache_data = {
                "result": result,
                "question": question,
                "cached_at": datetime.now().isoformat(),
                "ttl": ttl or self.cache_config["query_cache_ttl"]
            }
            
            serialized_data = self._serialize_data(cache_data)
            
            success = self.redis_client.setex(
                cache_key,
                ttl or self.cache_config["query_cache_ttl"],
                serialized_data
            )
            
            if success:
                self.stats["sets"] += 1
                print(f"📦 Cached query result: {cache_key[:30]}...")
            
            return success
            
        except Exception as e:
            logger.error(f"Query cache set failed: {e}")
            return False
    
    def get_cached_query_result(self, question: str, user_context: Dict) -> Optional[Dict]:
        """Retrieve cached query result - simplified"""
        if not self.redis_client:
            return None
        
        try:
            # Generate same key as cache_query_result
            context_hash = hashlib.md5(json.dumps(user_context, sort_keys=True).encode()).hexdigest()[:8]
            question_hash = hashlib.md5(question.encode()).hexdigest()[:8]
            cache_key = self._get_cache_key("query", f"{question_hash}:{context_hash}")
            
            cached_data = self.redis_client.get(cache_key)
            
            if cached_data:
                try:
                    if isinstance(cached_data, str):
                        cached_data = cached_data.encode()
                    
                    cache_obj = self._deserialize_data(cached_data)
                    self.stats["hits"] += 1
                    
                    print(f"⚡ Cache HIT: {cache_key[:30]}...")
                    return cache_obj["result"]
                    
                except Exception as e:
                    logger.error(f"Cache deserialization failed: {e}")
                    self.redis_client.delete(cache_key)
            
            self.stats["misses"] += 1
            return None
            
        except Exception as e:
            logger.error(f"Query cache get failed: {e}")
            return None
    
    def health_check(self) -> Dict[str, bool]:
        """Simple health check"""
        try:
            if self.redis_client:
                self.redis_client.ping()
                return {"redis_cache": True}
            else:
                return {"redis_cache": False}
        except Exception:
            return {"redis_cache": False}
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get simple cache statistics"""
        total_operations = self.stats["hits"] + self.stats["misses"]
        hit_ratio = self.stats["hits"] / total_operations if total_operations > 0 else 0.0
        
        stats = {
            "runtime_stats": self.stats.copy(),
            "hit_ratio": hit_ratio,
            "total_operations": total_operations,
            "cache_layers": {}
        }
        
        if self.redis_client:
            try:
                info = self.redis_client.info("memory")
                keyspace = self.redis_client.info("keyspace")
                
                stats["cache_layers"]["query_cache"] = {
                    "memory_used": info.get("used_memory_human", "Unknown"),
                    "keys": 0,
                    "connected": True
                }
                
                # Get key count for current DB
                db_info = keyspace.get("db3")  # We use DB 3
                if db_info:
                    stats["cache_layers"]["query_cache"]["keys"] = db_info.get("keys", 0)
                
            except Exception as e:
                stats["cache_layers"]["query_cache"] = {
                    "error": str(e),
                    "connected": False
                }
        
        return stats
    
    def clear_cache(self) -> bool:
        """Clear all cached data"""
        if not self.redis_client:
            return False
        
        try:
            self.redis_client.flushdb()
            return True
        except Exception as e:
            logger.error(f"Cache clear failed: {e}")
            return False

# Global cache manager instance
_cache_manager = None

def get_cache_manager() -> TAPRedisCacheManager:
    """Get cache manager instance"""
    global _cache_manager
    if _cache_manager is None:
        _cache_manager = TAPRedisCacheManager()
    return _cache_manager

def test_cache():
    """Test the cache system"""
    try:
        cache_mgr = get_cache_manager()
        health = cache_mgr.health_check()
        stats = cache_mgr.get_cache_stats()
        
        print("🧪 Cache Test Results:")
        print(f"   Health: {health}")
        print(f"   Hit ratio: {stats['hit_ratio']:.2%}")
        
        return {
            "health": health,
            "stats": stats,
            "working": health.get("redis_cache", False)
        }
        
    except Exception as e:
        print(f"❌ Cache test failed: {e}")
        return {"error": str(e)}

if __name__ == "__main__":
    test_cache()