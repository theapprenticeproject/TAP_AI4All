# File: ~/frappe-bench/apps/tap_educational_assistant/tap_educational_assistant/ai_service/core/data_loader.py

import frappe
from typing import Dict, List, Any, Optional
import logging
import functools
import time

logger = logging.getLogger(__name__)

class TAPDataLoader:
    """Enhanced data loader for TAP Educational DocTypes with simple caching"""
    
    def __init__(self):
        # Use cached schema discovery to avoid repeated DB calls
        self.doctype_schemas = self._get_cached_schemas()
        self.relationship_map = self._build_relationships()
    
    @functools.lru_cache(maxsize=1)
    def _get_cached_schemas(self) -> Dict[str, Any]:
        """Cache schema discovery to run only once per session"""
        print("🔧 Running schema discovery (cached for session)...")
        return self._discover_schemas()
    
    def _discover_schemas(self) -> Dict[str, Any]:
        """Discover all TAP DocType schemas - ENHANCED VERSION OF YOUR EXISTING CODE"""
        schemas = {}
        
        try:
            # Get TAP DocTypes - enhanced to be more flexible
            tap_doctypes = frappe.get_all("DocType", 
                filters={"module": "TAP Educational Assistant"},
                fields=["name"])
            
            # If no TAP module DocTypes found, try broader search
            if not tap_doctypes:
                print("   No TAP module DocTypes found, searching for educational DocTypes...")
                # Look for common educational DocType names
                common_educational = ['Student', 'Teacher', 'School', 'Course', 'Activities', 'Performance', 'Enrollment', 'Batch']
                tap_doctypes = []
                for dt_name in common_educational:
                    if frappe.db.exists("DocType", dt_name):
                        tap_doctypes.append({'name': dt_name})
            
            print(f"   Found {len(tap_doctypes)} DocTypes to analyze")

            for doctype in tap_doctypes:
                dt_name = doctype.name
                
                try:
                    meta = frappe.get_meta(dt_name)

                    # Build field schema - KEEPING YOUR EXISTING LOGIC
                    fields = {}
                    searchable_fields = []
                    link_fields = {}
                    numeric_fields = []
                    select_fields = []
                    check_fields = []
                    table_fields = []
                    richtext_fields = []
                    attachment_fields = []
                    code_fields = []
                    
                    for field in meta.fields:
                        if field.fieldname:
                            fields[field.fieldname] = {
                                'fieldtype': field.fieldtype,
                                'label': field.label,
                                'options': field.options
                            }
                            
                            # Categorize fields - YOUR EXISTING CATEGORIZATION
                            if field.fieldtype in ['Data', 'Text', 'Small Text', 'Long Text']:
                                searchable_fields.append(field.fieldname)

                            elif field.fieldtype == 'Link':
                                link_fields[field.fieldname] = field.options

                            elif field.fieldtype in ['Int', 'Float', 'Percent', 'Currency', 'Date', 'Datetime', 'Time', 'Duration']:
                                numeric_fields.append(field.fieldname)

                            elif field.fieldtype in ['Select']:
                                select_fields.append(field.fieldname)

                            elif field.fieldtype in ['Check']:
                                check_fields.append(field.fieldname)

                            elif field.fieldtype in ['Table', 'Table MultiSelect']:
                                table_fields.append(field.fieldname)

                            elif field.fieldtype in ['HTML', 'Text Editor']:
                                richtext_fields.append(field.fieldname)

                            elif field.fieldtype in ['Attach', 'Attach Image']:
                                attachment_fields.append(field.fieldname)

                            elif field.fieldtype in ['Code']:
                                code_fields.append(field.fieldname)
                    
                    # ENHANCEMENT: Add record count for better relevance scoring
                    try:
                        record_count = frappe.db.count(dt_name)
                    except:
                        record_count = 0
                    
                    # Store schema - ENHANCED WITH RECORD COUNT
                    schemas[dt_name] = {
                        'fields': fields,
                        'searchable_fields': searchable_fields,
                        'link_fields': link_fields,
                        'numeric_fields': numeric_fields,
                        'select_fields': select_fields,
                        'check_fields': check_fields,
                        'table_fields': table_fields,
                        'richtext_fields': richtext_fields,
                        'attachment_fields': attachment_fields,
                        'code_fields': code_fields,
                        'record_count': record_count  # NEW: Track record count
                    }
                    
                    print(f"   ✅ {dt_name}: {record_count} records, {len(searchable_fields)} searchable fields")
                    
                except Exception as e:
                    logger.error(f"Error analyzing {dt_name}: {e}")
                    continue
                
            print(f"✅ Schema discovery complete: {len(schemas)} DocTypes")
            return schemas
            
        except Exception as e:
            logger.error(f"Schema discovery failed: {e}")
            return {}
    
    def _build_relationships(self) -> Dict[str, Dict]:
        """Build relationship map between DocTypes - YOUR EXISTING LOGIC"""
        relationships = {}
        
        for doctype, schema in self.doctype_schemas.items():
            relationships[doctype] = {
                'links_to': [],
                'linked_from': []
            }
            
            # Find what this DocType links to
            for field, linked_doctype in schema['link_fields'].items():
                relationships[doctype]['links_to'].append({
                    'field': field,
                    'doctype': linked_doctype
                })
        
        # Find reverse relationships
        for doctype, schema in self.doctype_schemas.items():
            for field, linked_doctype in schema['link_fields'].items():
                if linked_doctype in relationships:
                    relationships[linked_doctype]['linked_from'].append({
                        'field': field,
                        'source_doctype': doctype
                    })
        
        return relationships
    
    def load_for_query(self, query: str, limit: int = 1000) -> Dict[str, List[Dict]]:
        """Load relevant data based on query content - ENHANCED VERSION"""
        
        # Determine which DocTypes are relevant for the query
        relevant_doctypes = self._identify_relevant_doctypes(query)
        
        loaded_data = {}
        
        for doctype in relevant_doctypes:
            try:
                # Get important fields for the DocType - ENHANCED
                fields = self._get_important_fields(doctype)
                
                # ENHANCEMENT: Prioritize DocTypes with more records
                schema = self.doctype_schemas.get(doctype, {})
                record_count = schema.get('record_count', 0)
                
                # Adjust limit based on record count and DocType importance
                adjusted_limit = min(limit, max(100, record_count // 10)) if record_count > 0 else limit
                
                # Load data with relationships
                data = frappe.get_all(doctype, 
                    fields=fields,
                    limit=adjusted_limit)
                
                # Enhance with related data for complex queries - YOUR EXISTING LOGIC
                if data and self._needs_relationship_data(query, doctype):
                    data = self._enhance_with_relationships(doctype, data[:10])  # Limit for performance
                
                loaded_data[doctype] = data
                print(f"   📊 Loaded {len(data)}/{record_count} {doctype} records")
                
            except Exception as e:
                logger.error(f"Error loading {doctype}: {e}")
                continue
        
        return loaded_data
    
    def _identify_relevant_doctypes(self, query: str) -> List[str]:
        """Identify which DocTypes are relevant for a query - ENHANCED VERSION"""
        query_lower = query.lower()
        relevant = []
        
        # ENHANCEMENT: Score-based relevance instead of simple keyword matching
        doctype_scores = {}
        
        # Keyword-based DocType identification - YOUR EXISTING LOGIC BUT ENHANCED
        doctype_keywords = {
            'Student': ['student', 'students', 'learner', 'pupil'],
            'Teacher': ['teacher', 'teachers', 'instructor', 'faculty'],
            'School': ['school', 'schools', 'institution'],
            'Course': ['course', 'courses', 'subject', 'coding', 'science', 'arts'],
            'Activities': ['activity', 'activities', 'assignment', 'task'],
            'Performance': ['performance', 'progress', 'improvement', 'score', 'rate'],
            'Batch': ['batch', 'class', 'group'],
            'Enrollment': ['enrollment', 'enrolled', 'registration']
        }
        
        # Score each DocType
        for doctype, keywords in doctype_keywords.items():
            if doctype in self.doctype_schemas:
                score = 0
                
                # Keyword matching
                for keyword in keywords:
                    if keyword in query_lower:
                        score += 3
                
                # Record count bonus (popular DocTypes are often relevant)
                record_count = self.doctype_schemas[doctype].get('record_count', 0)
                if record_count > 100:
                    score += 2
                elif record_count > 10:
                    score += 1
                
                # Always include core DocTypes with some score
                if doctype in ['Student', 'Performance', 'Activities', 'School']:
                    score += 1
                
                doctype_scores[doctype] = score
        
        # Sort by score and include top DocTypes
        sorted_doctypes = sorted(doctype_scores.items(), key=lambda x: x[1], reverse=True)
        
        for doctype, score in sorted_doctypes:
            if score > 0:
                relevant.append(doctype)
        
        # ENHANCEMENT: Always include Performance for improvement/analytics queries
        if any(word in query_lower for word in ['improve', 'progress', 'analytics', 'how many']):
            if 'Performance' not in relevant and 'Performance' in self.doctype_schemas:
                relevant.append('Performance')
        
        # Fallback to core DocTypes if nothing relevant found
        if not relevant:
            core_doctypes = ['Student', 'Performance', 'Activities', 'Course', 'School', 'Teacher', 'Batch', 'Enrollment']
            relevant = [dt for dt in core_doctypes if dt in self.doctype_schemas]
        
        print(f"   🎯 Selected relevant DocTypes: {relevant}")
        return list(set(relevant))  # Remove duplicates
    
    def _get_important_fields(self, doctype: str) -> List[str]:
        """Get important fields for a DocType - YOUR EXISTING LOGIC"""
        base_fields = ['name']
        
        schema = self.doctype_schemas.get(doctype, {})
        
        # Add searchable fields
        base_fields.extend(schema.get('searchable_fields', []))
        
        # Add numeric fields for analytics
        base_fields.extend(schema.get('numeric_fields', []))
        
        # Add link fields for relationships
        base_fields.extend(list(schema.get('link_fields', {}).keys()))

        # Add select fields for options
        base_fields.extend(schema.get('select_fields', []))
        # Add check fields for boolean values
        base_fields.extend(schema.get('check_fields', []))
        # Add table fields for structured data
        base_fields.extend(schema.get('table_fields', []))
        # Add rich text fields for content
        base_fields.extend(schema.get('richtext_fields', []))
        # Add attachment fields for files
        base_fields.extend(schema.get('attachment_fields', []))
        # Add code fields for programming content
        base_fields.extend(schema.get('code_fields', []))
        
        # Remove duplicates and return
        return list(set(base_fields))
    
    def _needs_relationship_data(self, query: str, doctype: str) -> bool:
        """Determine if we need to load relationship data - YOUR EXISTING LOGIC"""
        relationship_indicators = [
            'school', 'teacher', 'course', 'activity', 
            'which', 'what', 'breakdown', 'by grade', 'by school'
        ]
        
        return any(indicator in query.lower() for indicator in relationship_indicators)
    
    def _enhance_with_relationships(self, doctype: str, data: List[Dict]) -> List[Dict]:
        """Enhance data with relationship information - YOUR EXISTING LOGIC"""
        
        schema = self.doctype_schemas.get(doctype, {})
        link_fields = schema.get('link_fields', {})
        
        for item in data:
            # Add linked document data
            for field, linked_doctype in link_fields.items():
                if item.get(field):
                    try:
                        linked_doc = frappe.get_doc(linked_doctype, item[field])
                        # Add key info from linked document
                        if hasattr(linked_doc, 'name1'):
                            item[f'{field}_name'] = linked_doc.name1
                        elif hasattr(linked_doc, 'title'):
                            item[f'{field}_name'] = linked_doc.title
                    except:
                        pass
        
        return data
    
    # NEW: Add some utility methods for monitoring
    def get_schema_summary(self) -> Dict[str, Any]:
        """Get a summary of discovered schemas"""
        total_records = sum(schema.get('record_count', 0) for schema in self.doctype_schemas.values())
        
        return {
            'total_doctypes': len(self.doctype_schemas),
            'total_records': total_records,
            'doctypes': list(self.doctype_schemas.keys()),
            'record_counts': {dt: schema.get('record_count', 0) for dt, schema in self.doctype_schemas.items()},
            'cached': True
        }
    
    def refresh_schemas(self) -> None:
        """Manually refresh schemas if needed"""
        # Clear the cache
        self._get_cached_schemas.cache_clear()
        # Reload schemas
        self.doctype_schemas = self._get_cached_schemas()
        self.relationship_map = self._build_relationships()
        print("✅ Schemas refreshed")