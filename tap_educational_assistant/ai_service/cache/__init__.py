# File: ~/frappe-bench/apps/tap_educational_assistant/tap_educational_assistant/ai_service/cache/__init__.py

# Cache management scheduled tasks

import frappe
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any
from tap_educational_assistant.ai_service.core.redis_cache_manager import get_simplified_cache_manager

logger = logging.getLogger(__name__)

def initialize_cache_on_boot():
    """Initialize cache manager on Frappe boot"""
    try:
        cache_manager = get_simplified_cache_manager()
        health = cache_manager.health_check()
        
        healthy_caches = sum(1 for is_healthy in health.values() if is_healthy)
        total_caches = len(health)
        
        print(f"🚀 TAP Cache System Boot Check: {healthy_caches}/{total_caches} caches healthy")
        
        if healthy_caches == 0:
            print("⚠️  No cache layers available - system will run without caching")
        elif healthy_caches < total_caches:
            print(f"⚠️  Some cache layers unavailable: {[k for k, v in health.items() if not v]}")
        else:
            print("✅ All cache layers healthy")
            
    except Exception as e:
        logger.error(f"Cache initialization on boot failed: {e}")

def scheduled_cache_warming():
    """Hourly cache warming with common educational queries"""
    try:
        # Only warm cache during active hours (6 AM to 10 PM)
        current_hour = datetime.now().hour
        if current_hour < 6 or current_hour > 22:
            print("⏰ Outside active hours - skipping cache warming")
            return

        cache_manager = get_simplified_cache_manager()

        # Check if cache warming is needed
        stats = cache_manager.get_cache_stats()
        hit_ratio = stats.get("hit_ratio", 0)
        
        # If hit ratio is already high, skip warming
        if hit_ratio > 0.8:
            print(f"📊 Cache hit ratio is good ({hit_ratio:.2%}) - skipping warming")
            return
        
        # Common educational queries for warming
        warming_queries = [
            "How many students are in each grade?",
            "What are the names of activities?",
            "Which students need attention?",
            "Show me top performing students",
            "What is the submission rate by school?",
            "How many schools are in Mumbai?",
            "List all courses available",
            "Tell me about coding activities",
            "What are the different activity rigor levels?",
            "How many students have high submission rates?"
        ]
        
        print(f"🔥 Starting scheduled cache warming with {len(warming_queries)} queries...")
        
        # Import here to avoid circular imports
        from tap_educational_assistant.ai_service.core.hybrid_rag_pipeline import IntelligentHybridEducationalRAG
        
        # Create a system RAG instance for warming
        system_rag = IntelligentHybridEducationalRAG(user_id="system_cache_warming")

        warmed = 0
        for query in warming_queries:
            try:
                result = system_rag.query(query)
                if result.get("success", False):
                    warmed += 1
                    print(f"   ✅ Warmed: {query}")
                else:
                    print(f"   ❌ Failed: {query}")
                    
            except Exception as e:
                logger.warning(f"Cache warming failed for query '{query}': {e}")
                continue
        
        print(f"🔥 Scheduled cache warming complete: {warmed}/{len(warming_queries)} queries cached")
        
        # Log warming statistics
        frappe.db.sql("""
            INSERT INTO `tabError Log` (name, creation, modified, owner, modified_by, 
                                      error, method, traceback, reference_doctype, reference_name)
            VALUES (%(name)s, %(creation)s, %(modified)s, %(owner)s, %(modified_by)s,
                   %(error)s, %(method)s, %(traceback)s, %(reference_doctype)s, %(reference_name)s)
        """, {
            "name": f"cache_warming_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "creation": datetime.now(),
            "modified": datetime.now(),
            "owner": "Administrator",
            "modified_by": "Administrator",
            "error": f"Cache Warming Report: {warmed}/{len(warming_queries)} queries cached. Hit ratio: {hit_ratio:.2%}",
            "method": "scheduled_cache_warming",
            "traceback": "Scheduled cache warming completed successfully",
            "reference_doctype": "Cache Management",
            "reference_name": "Hourly Warming"
        })
        
    except Exception as e:
        logger.error(f"Scheduled cache warming failed: {e}")

def daily_cache_cleanup():
    """Daily cache cleanup and maintenance"""
    try:
        print("🧹 Starting daily cache cleanup...")
        
        cache_manager = get_simplified_cache_manager()
        
        # Get cache statistics before cleanup
        stats_before = cache_manager.get_cache_stats()
        
        # Clean up expired entries (Redis handles this automatically, but we can force it)
        cleanup_results = {}
        
        for cache_name, client in cache_manager.redis_clients.items():
            try:
                # Get memory info before
                info_before = client.info("memory")
                memory_before = info_before.get("used_memory", 0)
                
                # Force memory cleanup (remove expired keys)
                # Redis SCAN for expired keys and cleanup
                cursor = 0
                expired_keys = []
                
                while True:
                    cursor, keys = client.scan(cursor=cursor, match="tap:*", count=1000)
                    
                    for key in keys:
                        ttl = client.ttl(key)
                        if ttl == -2:  # Key doesn't exist or expired
                            expired_keys.append(key)
                    
                    if cursor == 0:
                        break
                
                # Remove expired keys
                if expired_keys:
                    client.delete(*expired_keys[:1000])  # Batch delete, max 1000 at a time
                
                # Get memory info after
                info_after = client.info("memory")
                memory_after = info_after.get("used_memory", 0)
                memory_freed = memory_before - memory_after
                
                cleanup_results[cache_name] = {
                    "expired_keys_removed": len(expired_keys),
                    "memory_freed": memory_freed,
                    "memory_before": memory_before,
                    "memory_after": memory_after
                }
                
                print(f"   🧹 {cache_name}: Removed {len(expired_keys)} expired keys, freed {memory_freed} bytes")
                
            except Exception as e:
                logger.warning(f"Cleanup failed for {cache_name}: {e}")
                cleanup_results[cache_name] = {"error": str(e)}
        
        # Get cache statistics after cleanup
        stats_after = cache_manager.get_cache_stats()
        
        # Log cleanup report
        cleanup_report = {
            "timestamp": datetime.now().isoformat(),
            "stats_before": stats_before,
            "stats_after": stats_after,
            "cleanup_results": cleanup_results,
            "total_memory_freed": sum(
                result.get("memory_freed", 0) 
                for result in cleanup_results.values() 
                if isinstance(result, dict) and "memory_freed" in result
            )
        }
        
        print(f"🧹 Daily cache cleanup complete. Total memory freed: {cleanup_report['total_memory_freed']} bytes")
        
        # Store cleanup report
        frappe.db.sql("""
            INSERT INTO `tabError Log` (name, creation, modified, owner, modified_by, 
                                      error, method, traceback, reference_doctype, reference_name)
            VALUES (%(name)s, %(creation)s, %(modified)s, %(owner)s, %(modified_by)s,
                   %(error)s, %(method)s, %(traceback)s, %(reference_doctype)s, %(reference_name)s)
        """, {
            "name": f"cache_cleanup_{datetime.now().strftime('%Y%m%d')}",
            "creation": datetime.now(),
            "modified": datetime.now(),
            "owner": "Administrator",
            "modified_by": "Administrator",
            "error": f"Daily Cache Cleanup Report: {cleanup_report['total_memory_freed']} bytes freed",
            "method": "daily_cache_cleanup",
            "traceback": str(cleanup_report),
            "reference_doctype": "Cache Management",
            "reference_name": "Daily Cleanup"
        })
        
    except Exception as e:
        logger.error(f"Daily cache cleanup failed: {e}")

def weekly_cache_report():
    """Weekly comprehensive cache performance report"""
    try:
        print("📊 Generating weekly cache performance report...")

        cache_manager = get_simplified_cache_manager()

        # Get comprehensive statistics
        stats = cache_manager.get_cache_stats()
        health = cache_manager.health_check()
        
        # Calculate performance metrics
        hit_ratio = stats.get("hit_ratio", 0)
        total_operations = stats.get("total_operations", 0)
        
        # Performance grade
        if hit_ratio >= 0.8:
            performance_grade = "A (Excellent)"
        elif hit_ratio >= 0.6:
            performance_grade = "B (Good)"
        elif hit_ratio >= 0.4:
            performance_grade = "C (Fair)"
        elif hit_ratio >= 0.2:
            performance_grade = "D (Poor)"
        else:
            performance_grade = "F (Very Poor)"
        
        # Memory usage analysis
        memory_analysis = {}
        total_memory_used = 0
        
        for cache_name, client in cache_manager.redis_clients.items():
            try:
                info = client.info("memory")
                memory_used = info.get("used_memory", 0)
                memory_human = info.get("used_memory_human", "Unknown")
                
                memory_analysis[cache_name] = {
                    "memory_bytes": memory_used,
                    "memory_human": memory_human,
                    "connected": True
                }
                
                total_memory_used += memory_used
                
            except Exception as e:
                memory_analysis[cache_name] = {
                    "error": str(e),
                    "connected": False
                }
        
        # Generate recommendations
        recommendations = []
        
        if hit_ratio < 0.5:
            recommendations.append("Consider increasing cache TTL values for better hit ratios")
        
        if total_operations < 100:
            recommendations.append("Cache usage is low - consider promoting RAG system to users")
        
        if len([h for h in health.values() if not h]) > 0:
            recommendations.append("Some cache layers are unhealthy - check Redis connections")
        
        if total_memory_used > 1024 * 1024 * 1024:  # 1GB
            recommendations.append("High memory usage detected - consider cache cleanup or size limits")
        
        if not recommendations:
            recommendations.append("Cache system is performing well - no immediate actions needed")
        
        # Create comprehensive report
        report = {
            "report_date": datetime.now().isoformat(),
            "reporting_period": "Weekly",
            "performance_summary": {
                "hit_ratio": f"{hit_ratio:.2%}",
                "performance_grade": performance_grade,
                "total_operations": total_operations,
                "cache_hits": stats.get("runtime_stats", {}).get("hits", 0),
                "cache_misses": stats.get("runtime_stats", {}).get("misses", 0),
                "cache_sets": stats.get("runtime_stats", {}).get("sets", 0),
                "invalidations": stats.get("runtime_stats", {}).get("invalidations", 0)
            },
            "cache_layer_health": health,
            "memory_analysis": memory_analysis,
            "total_memory_used_bytes": total_memory_used,
            "total_memory_used_human": f"{total_memory_used / (1024*1024):.1f} MB",
            "recommendations": recommendations,
            "detailed_stats": stats
        }
        
        print(f"📊 Weekly Cache Report Generated:")
        print(f"   📈 Performance Grade: {performance_grade}")
        print(f"   📊 Hit Ratio: {hit_ratio:.2%}")
        print(f"   💾 Total Memory Used: {report['total_memory_used_human']}")
        print(f"   🏥 Healthy Cache Layers: {sum(1 for h in health.values() if h)}/{len(health)}")
        print(f"   💡 Recommendations: {len(recommendations)}")
        
        # Store detailed report
        frappe.db.sql("""
            INSERT INTO `tabError Log` (name, creation, modified, owner, modified_by, 
                                      error, method, traceback, reference_doctype, reference_name)
            VALUES (%(name)s, %(creation)s, %(modified)s, %(owner)s, %(modified_by)s,
                   %(error)s, %(method)s, %(traceback)s, %(reference_doctype)s, %(reference_name)s)
        """, {
            "name": f"cache_report_{datetime.now().strftime('%Y%m%d')}",
            "creation": datetime.now(),
            "modified": datetime.now(),
            "owner": "Administrator",
            "modified_by": "Administrator",
            "error": f"Weekly Cache Report: {performance_grade}, Hit Ratio: {hit_ratio:.2%}, Memory: {report['total_memory_used_human']}",
            "method": "weekly_cache_report",
            "traceback": str(report),
            "reference_doctype": "Cache Management",
            "reference_name": "Weekly Report"
        })
        
        # Send email report to administrators (optional)
        try:
            admin_users = frappe.get_all("User", filters={"role_profile_name": "System Manager"}, fields=["email"])
            
            if admin_users:
                email_content = f"""
                <h2>TAP Educational Assistant - Weekly Cache Performance Report</h2>
                
                <h3>Performance Summary</h3>
                <ul>
                    <li><strong>Performance Grade:</strong> {performance_grade}</li>
                    <li><strong>Hit Ratio:</strong> {hit_ratio:.2%}</li>
                    <li><strong>Total Operations:</strong> {total_operations:,}</li>
                    <li><strong>Memory Usage:</strong> {report['total_memory_used_human']}</li>
                </ul>
                
                <h3>Cache Layer Health</h3>
                <ul>
                    {''.join([f"<li><strong>{layer}:</strong> {'✅ Healthy' if healthy else '❌ Unhealthy'}</li>" for layer, healthy in health.items()])}
                </ul>
                
                <h3>Recommendations</h3>
                <ul>
                    {''.join([f"<li>{rec}</li>" for rec in recommendations])}
                </ul>
                
                <p><em>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em></p>
                """
                
                for user in admin_users[:2]:  # Send to max 2 admins
                    frappe.sendmail(
                        recipients=[user.email],
                        subject="TAP Educational Assistant - Weekly Cache Report",
                        message=email_content
                    )
                
                print(f"   📧 Email report sent to {len(admin_users[:2])} administrators")
                
        except Exception as e:
            logger.warning(f"Failed to send email report: {e}")
        
        return report
        
    except Exception as e:
        logger.error(f"Weekly cache report generation failed: {e}")
        return None

def manual_cache_operations():
    """Manual cache operations for debugging and maintenance"""
    
    def clear_all_caches():
        """Clear all cache layers"""
        try:
            cache_manager = get_simplified_cache_manager()
            results = cache_manager.clear_all_cache(confirm=True)
            print(f"🗑️  Cleared all caches: {results}")
            return results
        except Exception as e:
            logger.error(f"Manual cache clear failed: {e}")
            return {"error": str(e)}
    
    def get_cache_status():
        """Get current cache status"""
        try:
            cache_manager = get_simplified_cache_manager()
            stats = cache_manager.get_cache_stats()
            health = cache_manager.health_check()
            
            status = {
                "timestamp": datetime.now().isoformat(),
                "statistics": stats,
                "health": health,
                "summary": {
                    "hit_ratio": f"{stats.get('hit_ratio', 0):.2%}",
                    "healthy_layers": sum(1 for h in health.values() if h),
                    "total_layers": len(health),
                    "total_operations": stats.get("total_operations", 0)
                }
            }
            
            print(f"📊 Current Cache Status:")
            print(f"   Hit Ratio: {status['summary']['hit_ratio']}")
            print(f"   Healthy Layers: {status['summary']['healthy_layers']}/{status['summary']['total_layers']}")
            print(f"   Total Operations: {status['summary']['total_operations']:,}")
            
            return status
            
        except Exception as e:
            logger.error(f"Get cache status failed: {e}")
            return {"error": str(e)}
    
    def warm_cache_manually(queries: List[str] = None):
        """Manually warm cache with specific queries"""
        try:
            if not queries:
                # Use default educational queries
                queries = [
                    "How many students are in each grade?",
                    "What are the names of activities?",
                    "Which students need attention?",
                    "Show me top performing students"
                ]
            
            from tap_educational_assistant.ai_service.core.hybrid_rag_pipeline import CachedIntelligentHybridEducationalRAG
            
            rag = CachedIntelligentHybridEducationalRAG(user_id="manual_cache_warming")
            warmed = rag.warm_cache(queries)
            
            print(f"🔥 Manual cache warming: {warmed}/{len(queries)} queries cached")
            return {"warmed": warmed, "total": len(queries), "queries": queries}
            
        except Exception as e:
            logger.error(f"Manual cache warming failed: {e}")
            return {"error": str(e)}
    
    return {
        "clear_all_caches": clear_all_caches,
        "get_cache_status": get_cache_status,
        "warm_cache_manually": warm_cache_manually
    }

# Export manual operations for console access
cache_ops = manual_cache_operations()