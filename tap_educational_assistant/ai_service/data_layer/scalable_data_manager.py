# File: ~/frappe-bench/apps/tap_educational_assistant/tap_educational_assistant/ai_service/data_layer/scalable_data_manager.py

import frappe
from typing import Dict, List, Any, Optional, Iterator, Set, Union
import logging
import time
import hashlib
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import threading
from dataclasses import dataclass

# Use only available packages - avoid SQLAlchemy dependencies
try:
    from cachetools import TTLCache, LRUCache
except ImportError:
    # Fallback to simple dict if cachetools not available
    class TTLCache(dict):
        def __init__(self, maxsize, ttl):
            super().__init__()
            self.maxsize = maxsize
            self.ttl = ttl

    class LRUCache(dict):
        def __init__(self, maxsize):
            super().__init__()
            self.maxsize = maxsize

logger = logging.getLogger(__name__)

@dataclass
class QueryMetrics:
    """Track query performance metrics"""
    query_hash: str
    execution_time: float
    record_count: int
    cache_hit: bool
    strategy_used: str
    timestamp: datetime

@dataclass
class DocTypeMetadata:
    """Lightweight DocType metadata"""
    name: str
    record_count: int
    last_updated: datetime
    key_fields: List[str]
    searchable_fields: List[str]
    relationship_fields: Dict[str, str]
    indexes: List[str]
    estimated_size_mb: float

class ScalableEducationalDataManager:
    """
    Enterprise-grade data layer for TAP Educational Assistant
    Uses Frappe's native database connections - no external dependencies
    """
    
    def __init__(self):
        # Multi-tier caching strategy using simple dicts if cachetools unavailable
        self.metadata_cache = TTLCache(maxsize=500, ttl=3600)  # 1 hour for metadata
        self.query_cache = TTLCache(maxsize=10000, ttl=1800)   # 30 min for query results
        self.hot_data_cache = LRUCache(maxsize=50000)          # Frequently accessed data
        
        # Performance tracking
        self.query_metrics = []
        self.performance_stats = {}
        
        # Thread safety
        self._lock = threading.RLock()
        
        # Lazy initialization flags
        self._metadata_loaded = False
        self._indexes_optimized = False
        
        print("🚀 Scalable Educational Data Manager Initialized")
        print("   ⚡ Using Frappe Native Database Connection")
        print("   📊 Multi-tier Caching: Active")
        print("   🔍 Performance Monitoring: Enabled")
        
        # Test database connection
        self._test_database_connection()
    
    def _test_database_connection(self):
        """Test database connection using Frappe's native methods"""
        try:
            result = frappe.db.sql("SELECT 'Database Ready' as status", as_dict=True)
            if result:
                print(f"   ✅ Database Connection: {result[0]['status']}")
        except Exception as e:
            print(f"   ❌ Database Connection Failed: {e}")
            raise
    
    @lru_cache(maxsize=100)
    def get_doctype_metadata(self, doctype_name: str, force_refresh: bool = False) -> Optional[DocTypeMetadata]:
        """Get comprehensive DocType metadata with intelligent caching"""
        
        cache_key = f"metadata_{doctype_name}"
        
        # Check cache first
        if not force_refresh and cache_key in self.metadata_cache:
            return self.metadata_cache[cache_key]
        
        try:
            start_time = time.time()
            
            # Check if DocType exists
            if not frappe.db.exists("DocType", doctype_name):
                return None
            
            # Get field metadata efficiently using Frappe meta
            meta = frappe.get_meta(doctype_name)
            
            # Categorize fields efficiently
            key_fields = ['name']
            searchable_fields = []
            relationship_fields = {}
            
            for field in meta.fields:
                if not field.fieldname:
                    continue
                    
                if field.fieldtype in ['Data', 'Text', 'Small Text', 'Long Text']:
                    searchable_fields.append(field.fieldname)
                    
                elif field.fieldtype == 'Link' and field.options:
                    relationship_fields[field.fieldname] = field.options
                
                # Add commonly used fields to key fields
                if field.fieldname in ['name1', 'title', 'subject', 'status', 'display_name']:
                    key_fields.append(field.fieldname)
            
            # Get record count using Frappe's native method
            try:
                record_count = frappe.db.count(doctype_name)
            except:
                record_count = 0
            
            # Get table size estimate using native SQL
            try:
                size_result = frappe.db.sql("""
                    SELECT 
                        ROUND(((data_length + index_length) / 1024 / 1024), 2) AS size_mb
                    FROM information_schema.tables 
                    WHERE table_schema = DATABASE() 
                    AND table_name = %s
                """, (f"tab{doctype_name}",), as_dict=True)
                
                estimated_size_mb = size_result[0]['size_mb'] if size_result else 0.0
            except:
                estimated_size_mb = 0.0
            
            # Get existing indexes
            try:
                index_result = frappe.db.sql("""
                    SELECT DISTINCT index_name 
                    FROM information_schema.statistics 
                    WHERE table_schema = DATABASE() 
                    AND table_name = %s 
                    AND index_name != 'PRIMARY'
                """, (f"tab{doctype_name}",), as_dict=True)
                
                indexes = [row['index_name'] for row in index_result]
            except:
                indexes = []
            
            # Create metadata object
            metadata = DocTypeMetadata(
                name=doctype_name,
                record_count=record_count,
                last_updated=datetime.now(),
                key_fields=list(set(key_fields)),  # Remove duplicates
                searchable_fields=searchable_fields,
                relationship_fields=relationship_fields,
                indexes=indexes,
                estimated_size_mb=estimated_size_mb
            )
            
            # Cache the metadata
            self.metadata_cache[cache_key] = metadata
            
            load_time = time.time() - start_time
            print(f"   📊 {doctype_name}: {record_count:,} records, {estimated_size_mb:.1f}MB ({load_time:.2f}s)")
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error getting metadata for {doctype_name}: {e}")
            print(f"   ❌ Error analyzing {doctype_name}: {e}")
            return None
    
    def discover_all_educational_doctypes(self) -> Dict[str, DocTypeMetadata]:
        """Efficiently discover all educational DocTypes"""
        
        print("🔍 Discovering Educational DocTypes...")
        start_time = time.time()
        
        try:
            # Strategy 1: Try to find DocTypes by module
            doctypes = []
            
            # Common variations for TAP module
            module_variations = [
                "TAP Educational Assistant",
                "Tap Educational Assistant", 
                "tap_educational_assistant",
                "TAP LMS",
                "Tap Lms"
            ]
            
            for module_name in module_variations:
                try:
                    found_doctypes = frappe.get_all("DocType", 
                        filters={"module": module_name, "issingle": 0},
                        fields=["name"])
                    if found_doctypes:
                        doctypes.extend(found_doctypes)
                        print(f"   ✅ Found {len(found_doctypes)} DocTypes in module: {module_name}")
                        break
                except:
                    continue
            
            # Strategy 2: If no module found, search for common educational DocTypes
            if not doctypes:
                print("   🔍 No module DocTypes found, searching for common educational DocTypes...")
                educational_names = [
                    'Student', 'Teacher', 'School', 'Course', 'Activities', 
                    'Performance', 'Enrollment', 'Batch', 'Assessment',
                    'Subject', 'Grade', 'Class', 'Exam', 'Assignment'
                ]
                
                for name in educational_names:
                    try:
                        if frappe.db.exists("DocType", name):
                            doctypes.append({"name": name})
                            print(f"   ✅ Found educational DocType: {name}")
                    except:
                        continue
            
            # Strategy 3: If still nothing, get all custom DocTypes
            if not doctypes:
                print("   🔍 Searching all custom DocTypes...")
                try:
                    all_custom = frappe.get_all("DocType", 
                        filters={"custom": 1, "issingle": 0},
                        fields=["name"])
                    doctypes.extend(all_custom[:10])  # Limit to first 10 custom DocTypes
                    print(f"   ✅ Found {len(doctypes)} custom DocTypes")
                except:
                    pass
            
            if not doctypes:
                print("   ⚠️  No educational DocTypes found")
                return {}
            
            # Load metadata for discovered DocTypes
            discovered_metadata = {}
            
            for dt in doctypes:
                doctype_name = dt["name"]
                try:
                    metadata = self.get_doctype_metadata(doctype_name)
                    if metadata and metadata.record_count > 0:
                        discovered_metadata[doctype_name] = metadata
                except Exception as e:
                    print(f"   ⚠️  Error loading {doctype_name}: {e}")
                    continue
            
            total_time = time.time() - start_time
            total_records = sum(meta.record_count for meta in discovered_metadata.values())
            
            print(f"✅ Discovery Complete: {len(discovered_metadata)} DocTypes, {total_records:,} total records ({total_time:.2f}s)")
            
            # Update performance stats
            self.performance_stats['last_discovery'] = {
                'doctypes_found': len(discovered_metadata),
                'total_records': total_records,
                'discovery_time': total_time
            }
            
            return discovered_metadata
            
        except Exception as e:
            logger.error(f"DocType discovery failed: {e}")
            print(f"   ❌ Discovery failed: {e}")
            return {}
    
    def get_strategic_data_sample(self, doctype_name: str, query_context: str = "", 
                                sample_size: int = 1000) -> List[Dict]:
        """Get intelligent data sample based on query context and access patterns"""
        
        metadata = self.get_doctype_metadata(doctype_name)
        if not metadata:
            return []
        
        # Create cache key including context
        context_hash = hashlib.md5(query_context.encode()).hexdigest()[:8]
        cache_key = f"sample_{doctype_name}_{sample_size}_{context_hash}"
        
        # Check cache first
        if cache_key in self.query_cache:
            print(f"   📦 Sample cache hit: {doctype_name}")
            return self.query_cache[cache_key]
        
        try:
            start_time = time.time()
            
            # Determine sampling strategy based on record count and context
            if metadata.record_count <= sample_size:
                # Small table - get all records
                sampling_strategy = "complete"
                filters = {}
                limit = None
                order_by = "name"
            elif metadata.record_count < 100000:
                # Medium table - use Frappe's built-in random sampling
                sampling_strategy = "random"
                filters = {}
                limit = sample_size
                order_by = "RAND()"
            else:
                # Large table - strategic sampling
                sampling_strategy = "strategic"
                filters, limit, order_by = self._build_strategic_sample_params(doctype_name, query_context, sample_size)
            
            # Use only key fields to prevent memory issues
            selected_fields = metadata.key_fields[:10] if len(metadata.key_fields) > 10 else metadata.key_fields
            
            # Execute using Frappe's native get_all method
            try:
                if order_by == "RAND()":
                    # For random sampling, use raw SQL since Frappe doesn't support RAND() in order_by
                    field_list = ', '.join(f'`{field}`' for field in selected_fields)
                    sql = f"""
                        SELECT {field_list}
                        FROM `tab{doctype_name}`
                        WHERE name IS NOT NULL
                        ORDER BY RAND()
                        LIMIT {limit}
                    """
                    records = frappe.db.sql(sql, as_dict=True)
                else:
                    # Use Frappe's get_all for other cases
                    records = frappe.get_all(
                        doctype_name,
                        fields=selected_fields,
                        filters=filters,
                        limit=limit,
                        order_by=order_by
                    )
            except Exception as e:
                print(f"   ⚠️  Fallback to simple query for {doctype_name}: {e}")
                # Fallback to simple query
                records = frappe.get_all(
                    doctype_name,
                    fields=['name', 'name1'] if 'name1' in metadata.key_fields else ['name'],
                    limit=min(sample_size, 100)  # Conservative fallback
                )
            
            # Cache the results
            self.query_cache[cache_key] = records
            
            query_time = time.time() - start_time
            print(f"   📊 {doctype_name} sample: {len(records)} records ({sampling_strategy}, {query_time:.2f}s)")
            
            # Track metrics
            self._track_query_metrics(cache_key, query_time, len(records), False, sampling_strategy)
            
            return records
            
        except Exception as e:
            logger.error(f"Error sampling {doctype_name}: {e}")
            print(f"   ❌ Error sampling {doctype_name}: {e}")
            return []
    
    def _build_strategic_sample_params(self, doctype_name: str, query_context: str, sample_size: int):
        """Build intelligent sampling parameters based on context"""
        
        # Analyze query context for clues
        context_lower = query_context.lower()
        
        # Recent data bias for time-sensitive queries
        if any(word in context_lower for word in ['recent', 'latest', 'current', 'new']):
            return {}, sample_size, "creation desc"
        
        # Modified data for performance queries
        elif any(word in context_lower for word in ['performance', 'score', 'grade', 'rating']):
            return {}, sample_size, "modified desc"
        
        # Default: get mix of recent and older records
        else:
            return {}, sample_size, "name"
    
    def stream_doctype_data(self, doctype_name: str, batch_size: int = 10000,
                           filters: Dict = None, fields: List[str] = None) -> Iterator[List[Dict]]:
        """Stream DocType data in batches - memory efficient for large datasets"""
        
        metadata = self.get_doctype_metadata(doctype_name)
        if not metadata:
            return
        
        # Use key fields if no specific fields requested
        if not fields:
            fields = metadata.key_fields[:10]  # Limit to prevent memory issues
        
        print(f"🌊 Streaming {doctype_name} data (batch_size={batch_size:,})...")
        
        # Stream data in batches using Frappe's pagination
        start = 0
        total_processed = 0
        
        while True:
            try:
                # Use Frappe's get_all with pagination
                batch = frappe.get_all(
                    doctype_name,
                    fields=fields,
                    filters=filters or {},
                    limit_start=start,
                    limit_page_length=batch_size,
                    order_by="name"  # Consistent ordering for reliable pagination
                )
                
                if not batch:
                    break
                
                yield batch
                
                total_processed += len(batch)
                start += batch_size
                
                # Stop if we got less than requested (end of data)
                if len(batch) < batch_size:
                    break
                    
            except Exception as e:
                logger.error(f"Error streaming batch at start {start}: {e}")
                break
        
        print(f"   ✅ Streamed {total_processed:,} records from {doctype_name}")
    
    def execute_optimized_query(self, doctype_name: str, query_params: Dict) -> List[Dict]:
        """Execute optimized queries with intelligent caching and performance monitoring"""
        
        # Create cache key from query parameters
        cache_key = f"query_{doctype_name}_{hashlib.md5(json.dumps(query_params, sort_keys=True).encode()).hexdigest()}"
        
        # Check cache first
        if cache_key in self.query_cache:
            print(f"   ⚡ Query cache hit: {doctype_name}")
            return self.query_cache[cache_key]
        
        start_time = time.time()
        
        try:
            # Extract parameters
            fields = query_params.get('fields', ['*'])
            filters = query_params.get('filters', {})
            order_by = query_params.get('order_by', 'name')
            limit = query_params.get('limit', 1000)
            
            # Optimize fields for large tables
            if fields == ['*']:
                metadata = self.get_doctype_metadata(doctype_name)
                if metadata and metadata.record_count > 100000:
                    fields = metadata.key_fields[:8]  # Use only key fields for large tables
            
            # Execute using Frappe's get_all
            records = frappe.get_all(
                doctype_name,
                fields=fields,
                filters=filters,
                order_by=order_by,
                limit=limit
            )
            
            # Cache results if reasonable size
            if len(records) < 10000:  # Don't cache huge result sets
                self.query_cache[cache_key] = records
            
            query_time = time.time() - start_time
            
            # Track performance metrics
            self._track_query_metrics(cache_key, query_time, len(records), False, "optimized_query")
            
            print(f"   📊 Query executed: {len(records)} records in {query_time:.2f}s")
            
            return records
            
        except Exception as e:
            logger.error(f"Optimized query failed for {doctype_name}: {e}")
            print(f"   ❌ Query failed for {doctype_name}: {e}")
            return []
    
    def _track_query_metrics(self, query_hash: str, execution_time: float, 
                           record_count: int, cache_hit: bool, strategy: str):
        """Track query performance metrics for optimization"""
        
        metric = QueryMetrics(
            query_hash=query_hash,
            execution_time=execution_time,
            record_count=record_count,
            cache_hit=cache_hit,
            strategy_used=strategy,
            timestamp=datetime.now()
        )
        
        with self._lock:
            self.query_metrics.append(metric)
            
            # Keep only recent metrics (last 1000 queries)
            if len(self.query_metrics) > 1000:
                self.query_metrics = self.query_metrics[-1000:]
    
    def get_performance_analytics(self) -> Dict[str, Any]:
        """Get comprehensive performance analytics"""
        
        if not self.query_metrics:
            return {"message": "No metrics available"}
        
        # Calculate performance statistics
        total_queries = len(self.query_metrics)
        cache_hits = sum(1 for m in self.query_metrics if m.cache_hit)
        avg_execution_time = sum(m.execution_time for m in self.query_metrics) / total_queries
        
        # Strategy performance
        strategy_stats = {}
        for metric in self.query_metrics:
            strategy = metric.strategy_used
            if strategy not in strategy_stats:
                strategy_stats[strategy] = {"count": 0, "total_time": 0}
            strategy_stats[strategy]["count"] += 1
            strategy_stats[strategy]["total_time"] += metric.execution_time
        
        # Calculate average times per strategy
        for strategy in strategy_stats:
            stats = strategy_stats[strategy]
            stats["avg_time"] = stats["total_time"] / stats["count"]
        
        return {
            "total_queries": total_queries,
            "cache_hit_ratio": cache_hits / total_queries,
            "average_execution_time": avg_execution_time,
            "strategy_performance": strategy_stats,
            "recent_queries": [
                {
                    "execution_time": m.execution_time,
                    "record_count": m.record_count,
                    "cache_hit": m.cache_hit,
                    "strategy": m.strategy_used
                }
                for m in self.query_metrics[-10:]
            ]
        }
    
    def optimize_database_indexes(self, doctypes: List[str] = None):
        """Create optimized indexes for better query performance"""
        
        if self._indexes_optimized:
            print("✅ Indexes already optimized")
            return
            
        print("🔧 Optimizing Database Indexes...")
        
        if not doctypes:
            # Get all educational DocTypes
            metadata_dict = self.discover_all_educational_doctypes()
            doctypes = list(metadata_dict.keys())
        
        for doctype_name in doctypes:
            try:
                self._create_doctype_indexes(doctype_name)
            except Exception as e:
                logger.error(f"Error creating indexes for {doctype_name}: {e}")
        
        self._indexes_optimized = True
        print("✅ Database index optimization complete")
    
    def _create_doctype_indexes(self, doctype_name: str):
        """Create strategic indexes for a DocType"""
        
        metadata = self.get_doctype_metadata(doctype_name)
        if not metadata or metadata.record_count < 1000:
            return  # Skip indexing for small tables
        
        table_name = f"tab{doctype_name}"
        
        # Index relationship fields (most important)
        for field in metadata.relationship_fields.keys():
            index_name = f"idx_{doctype_name.lower()}_{field.lower()}"
            try:
                frappe.db.sql(f"""
                    CREATE INDEX IF NOT EXISTS `{index_name}` 
                    ON `{table_name}` (`{field}`)
                """)
                print(f"   ✅ Created index: {index_name}")
            except Exception as e:
                # Index might already exist or field might not exist
                pass
        
        # Index commonly searched fields for large tables
        if metadata.record_count > 10000:
            for field in metadata.searchable_fields[:2]:  # Only top 2 to avoid too many indexes
                index_name = f"idx_{doctype_name.lower()}_{field.lower()}"
                try:
                    frappe.db.sql(f"""
                        CREATE INDEX IF NOT EXISTS `{index_name}` 
                        ON `{table_name}` (`{field}`)
                    """)
                    print(f"   ✅ Created index: {index_name}")
                except Exception as e:
                    pass
    
    def clear_caches(self):
        """Clear all caches for fresh data"""
        with self._lock:
            self.metadata_cache.clear()
            self.query_cache.clear()
            self.hot_data_cache.clear()
            print("🗑️  All caches cleared")
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        
        # Cache statistics
        cache_stats = {
            "metadata_cache": {
                "size": len(self.metadata_cache),
                "max_size": getattr(self.metadata_cache, 'maxsize', 'unlimited')
            },
            "query_cache": {
                "size": len(self.query_cache),
                "max_size": getattr(self.query_cache, 'maxsize', 'unlimited')
            }
        }
        
        # Database connection test
        db_status = "unknown"
        try:
            frappe.db.sql("SELECT 1")
            db_status = "connected"
        except:
            db_status = "disconnected"
        
        return {
            "database_connection": db_status,
            "cache_statistics": cache_stats,
            "performance_stats": self.performance_stats,
            "indexes_optimized": self._indexes_optimized,
            "metadata_loaded": self._metadata_loaded
        }


# Global instance for reuse
_scalable_data_manager = None

def get_scalable_data_manager() -> ScalableEducationalDataManager:
    """Get singleton instance of scalable data manager"""
    global _scalable_data_manager
    if _scalable_data_manager is None:
        _scalable_data_manager = ScalableEducationalDataManager()
    return _scalable_data_manager

# Convenience functions for easy access
def discover_educational_data():
    """Quick function to discover all educational data"""
    manager = get_scalable_data_manager()
    return manager.discover_all_educational_doctypes()

def get_strategic_sample(doctype: str, context: str = "", size: int = 1000):
    """Quick function to get strategic data sample"""
    manager = get_scalable_data_manager()
    return manager.get_strategic_data_sample(doctype, context, size)

def stream_data(doctype: str, batch_size: int = 10000):
    """Quick function to stream data"""
    manager = get_scalable_data_manager()
    return manager.stream_doctype_data(doctype, batch_size)

def optimize_indexes():
    """Quick function to optimize database indexes"""
    manager = get_scalable_data_manager()
    manager.optimize_database_indexes()

def get_performance_report():
    """Quick function to get performance analytics"""
    manager = get_scalable_data_manager()
    return manager.get_performance_analytics()

# Test function
def test_scalable_data_layer():
    """Test the scalable data layer"""
    print("🧪 Testing Scalable Data Layer...")
    
    try:
        manager = get_scalable_data_manager()
        
        # Test 1: Discovery
        print("\n1. Testing DocType Discovery...")
        discovered = manager.discover_all_educational_doctypes()
        print(f"   Found {len(discovered)} DocTypes")
        
        # Test 2: Sampling
        if discovered:
            first_doctype = list(discovered.keys())[0]
            print(f"\n2. Testing Strategic Sampling ({first_doctype})...")
            sample = manager.get_strategic_data_sample(first_doctype, "performance analysis", 100)
            print(f"   Retrieved {len(sample)} sample records")
            
            # Show sample data structure
            if sample:
                print(f"   Sample record fields: {list(sample[0].keys())}")
        
        # Test 3: Performance
        print("\n3. Performance Analytics...")
        analytics = manager.get_performance_analytics()
        if isinstance(analytics, dict) and 'cache_hit_ratio' in analytics:
            print(f"   Cache hit ratio: {analytics['cache_hit_ratio']:.2%}")
        else:
            print(f"   {analytics}")
        
        # Test 4: System status
        print("\n4. System Status...")
        status = manager.get_system_status()
        print(f"   Database: {status['database_connection']}")
        print(f"   Metadata cache: {status['cache_statistics']['metadata_cache']['size']} items")
        
        # Test 5: Index optimization
        if discovered:
            print("\n5. Testing Index Optimization...")
            manager.optimize_database_indexes(list(discovered.keys())[:2])  # Test with first 2 DocTypes
        
        print("\n✅ Scalable Data Layer Test Complete!")
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_scalable_data_layer()