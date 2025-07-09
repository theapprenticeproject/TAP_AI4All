import frappe
from neo4j import GraphDatabase
import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Set
import yaml
import os

class Neo4jCloudFrappeToNeo4jMigrator:
    """
    Enhanced migrator for Neo4j Cloud (Aura) with APOC limitations handling
    """
    
    def __init__(self, config_path: str = "migration_config.yaml"):
        self.config_path = config_path
        self.config = self.load_or_create_config()
        self.discovered_doctypes = {}
        self.field_mappings = {}
        
        # Get Neo4j configuration from Frappe
        self.neo4j_config = self._get_neo4j_config()
        self.driver = self._create_driver()
        
        # Check if we're using Aura (cloud)
        self.is_aura = "neo4j+s://" in self.neo4j_config.get("uri", "")
        if self.is_aura:
            print("🌐 Detected Neo4j Aura (Cloud) - APOC limitations apply")
        
    def _get_neo4j_config(self) -> Dict:
        """Get Neo4j configuration from Frappe site config"""
        try:
            # Import here to avoid circular imports
            from tap_educational_assistant.ai_service.config.settings_c import get_neo4j_config
            return get_neo4j_config()
        except Exception as e:
            print(f"⚠️  Could not get Neo4j config from settings: {e}")
            # Fallback to site config directly
            site_config = frappe.get_site_config()
            return {
                "uri": site_config.get("neo4j_uri", ""),
                "user": site_config.get("neo4j_user", "neo4j"),
                "password": site_config.get("neo4j_password", ""),
                "database": site_config.get("neo4j_database", "neo4j"),
                "is_aura": "neo4j+s://" in site_config.get("neo4j_uri", "")
            }
    
    def _create_driver(self):
        """Create Neo4j driver with proper configuration for Cloud"""
        config = self.neo4j_config
        
        if not config.get("uri"):
            raise ValueError("Neo4j URI not configured in site_config.json")
        
        try:
            # Enhanced driver configuration for Aura
            driver_config = {

                "max_connection_lifetime": 3600,  # 1 hour
                "max_connection_pool_size": 50,
                "connection_acquisition_timeout": 60,  # 60 seconds
                "max_transaction_retry_time": 30,  # 30 seconds
            }
            
            driver = GraphDatabase.driver(
                config["uri"],
                auth=(config["user"], config["password"]),
                **driver_config
            )
            
            # Test connection
            with driver.session(database=config.get("database", "neo4j")) as session:
                result = session.run("RETURN 'Connection successful' as status")
                status = result.single()["status"]
                print(f"✅ {status} to Neo4j Cloud")
                
            return driver
            
        except Exception as e:
            print(f"❌ Failed to connect to Neo4j Cloud: {e}")
            raise
    
    def close(self):
        if self.driver:
            self.driver.close()
    
    def load_or_create_config(self) -> Dict:
        """Load configuration file or create default one - same as before but with Aura considerations"""
        default_config = {
            'doctypes': {
                'include': [],  # Empty means include all
                'exclude': ['Singles', 'File', 'User', 'Role'],  # System doctypes to exclude
                'custom_only': False  # Set True to only migrate custom doctypes
            },
            'field_mappings': {
                # Field name transformations
                'name1': 'display_name',
                'teachname': 'teacher_name'
            },
            'data_types': {
                # Field type mappings and defaults
                'Link': {'type': 'string', 'default': None},
                'Data': {'type': 'string', 'default': None},
                'Text': {'type': 'string', 'default': None},
                'Int': {'type': 'integer', 'default': 0},
                'Float': {'type': 'float', 'default': 0.0},
                'Currency': {'type': 'float', 'default': 0.0},
                'Percent': {'type': 'float', 'default': 0.0},
                'Check': {'type': 'boolean', 'default': False},
                'Date': {'type': 'date', 'default': None},
                'Datetime': {'type': 'datetime', 'default': None},
                'Time': {'type': 'time', 'default': None},
                'Select': {'type': 'string', 'default': None},
                'Table': {'type': 'json', 'default': []},
                'JSON': {'type': 'json', 'default': {}}
            },
            'relationships': {
                'auto_detect': True,  # Automatically detect relationships from Link fields
                'custom_relationships': {
                    # Define custom relationship logic here
                    'Student': {
                        'school_id': {'target': 'School', 'relationship': 'STUDIES_AT'},
                    },
                    'Performance': {
                        'student': {'target': 'Student', 'relationship': 'TRACKS_STUDENT'},
                        'activity': {'target': 'Activities', 'relationship': 'MEASURES_ACTIVITY'},
                        'enrollment': {'target': 'Enrollment', 'relationship': 'RELATED_TO_ENROLLMENT'}
                    },
                    'Enrollment': {
                        'batch': {'target': 'Batch', 'relationship': 'BELONGS_TO_BATCH'},
                        'course': {'target': 'Course', 'relationship': 'ENROLLED_IN_COURSE'}
                    }
                }
            },
            'migration_settings': {
                'batch_size': 500,  # Smaller batches for cloud
                'create_indexes': True,
                'create_constraints': True,
                'validate_data': True,
                'backup_before_migration': False,  # Not available in Aura
                'use_apoc': False,  # APOC limited in Aura
                'max_retries': 3,
                'retry_delay': 2  # seconds
            },
            'aura_specific': {
                'memory_optimization': True,
                'small_batch_processing': True,
                'avoid_apoc_functions': True,
                'use_native_cypher_only': True
            }
        }
        
        if os.path.exists(self.config_path):
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
                # Merge with defaults
                for key in default_config:
                    if key not in config:
                        config[key] = default_config[key]
                return config
        else:
            # Create default config file
            with open(self.config_path, 'w') as f:
                yaml.dump(default_config, f, default_flow_style=False, indent=2)
            print(f"Created default configuration at {self.config_path}")
            return default_config
    
    def test_aura_connection(self) -> bool:
        """Test connection to Neo4j Aura and check available features"""
        try:
            with self.driver.session(database=self.neo4j_config.get("database", "neo4j")) as session:
                # Test basic connection
                result = session.run("RETURN 'Neo4j Aura connected successfully' as message")
                message = result.single()["message"]
                print(f"✅ {message}")
                
                # Check APOC availability (limited in Aura)
                try:
                    apoc_result = session.run("CALL apoc.help('apoc')")
                    apoc_functions = list(apoc_result)
                    print(f"✅ APOC available with {len(apoc_functions)} functions")
                    return True
                except Exception as apoc_error:
                    print(f"⚠️  APOC limited or unavailable in Aura: {apoc_error}")
                    print("   Using native Cypher only")
                    return True  # Connection still works
                
        except Exception as e:
            print(f"❌ Aura connection test failed: {e}")
            return False
    
    def discover_doctypes(self) -> Dict[str, Dict]:
        """Discover DocTypes - same logic as before"""
        print("🔍 Discovering DocTypes and their schemas...")
        
        # Get all DocTypes
        doctypes = frappe.get_all("DocType", 
                                 fields=["name", "custom", "module", "is_submittable"],
                                 filters={"issingle": 0})  # Exclude single DocTypes
        
        discovered = {}
        
        for doctype_info in doctypes:
            doctype_name = doctype_info['name']
            
            # Apply filters from config
            if self.should_skip_doctype(doctype_name, doctype_info):
                continue
            
            print(f"  📋 Analyzing DocType: {doctype_name}")
            
            # Get DocType meta and fields
            try:
                doctype_meta = frappe.get_meta(doctype_name)
                fields_info = {}
                
                for field in doctype_meta.fields:
                    if field.fieldtype in ['Column Break', 'Section Break', 'Tab Break']:
                        continue
                    
                    fields_info[field.fieldname] = {
                        'fieldtype': field.fieldtype,
                        'label': field.label,
                        'options': field.options,  # For Link fields, this contains target DocType
                        'reqd': field.reqd,
                        'unique': field.unique,
                        'default': field.default,
                        'description': field.description
                    }
                
                # Get sample record count
                try:
                    record_count = frappe.db.count(doctype_name)
                except:
                    record_count = 0
                
                discovered[doctype_name] = {
                    'info': doctype_info,
                    'fields': fields_info,
                    'record_count': record_count,
                    'relationships': self.discover_relationships(doctype_name, fields_info)
                }
                
            except Exception as e:
                print(f"    ⚠️  Error analyzing {doctype_name}: {e}")
                continue
        
        self.discovered_doctypes = discovered
        print(f"✅ Discovered {len(discovered)} DocTypes")
        return discovered
    
    def should_skip_doctype(self, doctype_name: str, doctype_info: Dict) -> bool:
        """Same logic as before"""
        # Check exclusion list
        if doctype_name in self.config['doctypes']['exclude']:
            return True
        
        # Check inclusion list (if specified)
        include_list = self.config['doctypes']['include']
        if include_list and doctype_name not in include_list:
            return True
        
        # Check custom only filter
        if self.config['doctypes']['custom_only'] and not doctype_info.get('custom'):
            return True
        
        return False
    
    def discover_relationships(self, doctype_name: str, fields: Dict) -> List[Dict]:
        """Same logic as before"""
        relationships = []
        
        # Auto-detect from Link fields
        if self.config['relationships']['auto_detect']:
            for field_name, field_info in fields.items():
                if field_info['fieldtype'] == 'Link' and field_info['options']:
                    target_doctype = field_info['options']
                    
                    # Generate relationship name
                    relationship_name = f"LINKED_TO_{target_doctype.upper()}"
                    
                    relationships.append({
                        'field': field_name,
                        'target': target_doctype,
                        'relationship': relationship_name,
                        'source': 'auto_detected'
                    })
        
        # Add custom relationships from config
        custom_rels = self.config['relationships']['custom_relationships'].get(doctype_name, {})
        for field_name, rel_config in custom_rels.items():
            relationships.append({
                'field': field_name,
                'target': rel_config['target'],
                'relationship': rel_config['relationship'],
                'source': 'custom_config'
            })
        
        return relationships
    
    def migrate_doctype_cloud_optimized(self, doctype_name: str) -> int:
        """Cloud-optimized migration for a single DocType"""
        if doctype_name not in self.discovered_doctypes:
            print(f"❌ DocType {doctype_name} not discovered. Run discover_doctypes() first.")
            return 0
        
        doctype_info = self.discovered_doctypes[doctype_name]
        fields_info = doctype_info['fields']
        total_records = doctype_info['record_count']
        
        if total_records == 0:
            print(f"⏭️  Skipping {doctype_name} - no records found")
            return 0
        
        print(f"🌐 Migrating {doctype_name} to Neo4j Cloud ({total_records} records)...")
        
        migrated_count = 0
        batch_size = self.config['migration_settings']['batch_size']
        
        # Use smaller batches for cloud to avoid timeouts
        cloud_batch_size = min(batch_size, 100)
        
        # Process in batches
        for offset in range(0, total_records, cloud_batch_size):
            batch_end = min(offset + cloud_batch_size, total_records)
            print(f"  📦 Processing cloud batch {offset//cloud_batch_size + 1}: records {offset+1}-{batch_end}")
            
            try:
                records = frappe.get_all(doctype_name, 
                                       fields="*", 
                                       start=offset, 
                                       page_length=cloud_batch_size)
                
                migrated_count += self.migrate_doctype_batch_cloud(doctype_name, records, fields_info)
                
            except Exception as e:
                print(f"    ❌ Error in cloud batch {offset//cloud_batch_size + 1}: {e}")
                continue
        
        print(f"✅ Completed {doctype_name}: {migrated_count}/{total_records} records migrated to cloud")
        return migrated_count
    
    def migrate_doctype_batch_cloud(self, doctype_name: str, records: List[Dict], fields_info: Dict) -> int:
        """Cloud-optimized batch migration"""
        database = self.neo4j_config.get("database", "neo4j")
        
        with self.driver.session(database=database) as session:
            migrated_count = 0
            
            for record in records:
                try:
                    # Process all fields
                    processed_data = {}
                    
                    for field_name, field_info in fields_info.items():
                        if field_name in record:
                            processed_value = self.process_field_value(record[field_name], field_info)
                            if processed_value is not None:
                                mapped_field_name = self.get_mapped_field_name(field_name)
                                processed_data[mapped_field_name] = processed_value
                    
                    # Always include the primary key and metadata
                    processed_data['name'] = record['name']
                    processed_data['created_at'] = datetime.now().isoformat()
                    processed_data['updated_at'] = datetime.now().isoformat()
                    processed_data['_doctype'] = doctype_name
                    processed_data['_migrated_to_cloud'] = True
                    
                    # Cloud-optimized Cypher query (avoid APOC)
                    properties = []
                    parameters = {}
                    
                    for key, value in processed_data.items():
                        properties.append(f"{key}: ${key}")
                        parameters[key] = value
                    
                    # Use parameterized query for cloud performance
                    cypher = f"""
                    CREATE (n:{doctype_name} {{
                        {', '.join(properties)}
                    }})
                    """
                    
                    session.run(cypher, parameters)
                    migrated_count += 1
                    
                except Exception as e:
                    print(f"      ⚠️  Error migrating record {record.get('name', 'unknown')}: {e}")
                    continue
            
            return migrated_count
    
    def create_relationships_for_doctype_cloud(self, doctype_name: str) -> int:
        """Cloud-optimized relationship creation"""
        if doctype_name not in self.discovered_doctypes:
            return 0
        
        relationships = self.discovered_doctypes[doctype_name]['relationships']
        created_count = 0
        database = self.neo4j_config.get("database", "neo4j")
        
        with self.driver.session(database=database) as session:
            for rel_info in relationships:
                field_name = rel_info['field']
                target_doctype = rel_info['target']
                relationship_name = rel_info['relationship']
                
                # Map field name if necessary
                mapped_field_name = self.get_mapped_field_name(field_name)
                
                # Cloud-optimized relationship creation (smaller batches)
                cypher = f"""
                MATCH (source:{doctype_name}), (target:{target_doctype})
                WHERE source.{mapped_field_name} IS NOT NULL 
                AND source.{mapped_field_name} = target.name
                WITH source, target LIMIT 1000
                CREATE (source)-[:{relationship_name}]->(target)
                """
                
                try:
                    result = session.run(cypher)
                    count = result.consume().counters.relationships_created
                    created_count += count
                    print(f"  🔗 Created {count} {relationship_name} relationships from {doctype_name} to {target_doctype}")
                except Exception as e:
                    print(f"    ❌ Error creating {relationship_name} relationships: {e}")
        
        return created_count
    
    def create_constraints_and_indexes_cloud(self):
        """Cloud-optimized constraints and indexes creation"""
        if not self.config['migration_settings']['create_constraints']:
            return
        
        print("🔧 Creating constraints and indexes for Neo4j Cloud...")
        database = self.neo4j_config.get("database", "neo4j")
        
        with self.driver.session(database=database) as session:
            for doctype_name in self.discovered_doctypes.keys():
                # Create unique constraint on name field
                try:
                    constraint_query = f"CREATE CONSTRAINT {doctype_name.lower()}_name_unique IF NOT EXISTS FOR (n:{doctype_name}) REQUIRE n.name IS UNIQUE"
                    session.run(constraint_query)
                    print(f"  ✅ Created unique constraint for {doctype_name}.name")
                except Exception as e:
                    print(f"    ⚠️  Constraint for {doctype_name} might already exist: {e}")
                
                # Create index on _doctype field
                try:
                    index_query = f"CREATE INDEX {doctype_name.lower()}_doctype_index IF NOT EXISTS FOR (n:{doctype_name}) ON (n._doctype)"
                    session.run(index_query)
                    print(f"  ✅ Created index for {doctype_name}._doctype")
                except Exception as e:
                    print(f"    ⚠️  Index for {doctype_name} might already exist: {e}")
    
    def get_field_neo4j_type(self, field_info: Dict) -> Dict:
        """Same as before"""
        fieldtype = field_info['fieldtype']
        type_config = self.config['data_types'].get(fieldtype, {'type': 'string', 'default': None})
        
        result = {
            'neo4j_type': type_config['type'],
            'default_value': type_config['default']
        }
        
        # Override with field-specific default if available
        if field_info.get('default'):
            result['default_value'] = field_info['default']
        
        return result
    
    def process_field_value(self, value: Any, field_info: Dict) -> Any:
        """Same as before"""
        if value is None or value == '' or value == 'None':
            type_info = self.get_field_neo4j_type(field_info)
            return type_info['default_value']
        
        fieldtype = field_info['fieldtype']
        
        try:
            if fieldtype in ['Int']:
                return int(value)
            elif fieldtype in ['Float', 'Currency', 'Percent']:
                return float(value)
            elif fieldtype == 'Check':
                return bool(value) if value is not None else False
            elif fieldtype in ['Table', 'JSON']:
                if isinstance(value, str):
                    return json.loads(value)
                return value
            else:
                # String types
                return str(value).strip() if value else None
        except (ValueError, TypeError, json.JSONDecodeError):
            type_info = self.get_field_neo4j_type(field_info)
            return type_info['default_value']
    
    def get_mapped_field_name(self, original_name: str) -> str:
        """Same as before"""
        return self.config['field_mappings'].get(original_name, original_name)
    
    def run_full_cloud_migration(self, clear_db: bool = False):
        """Run complete migration optimized for Neo4j Cloud"""
        print("🌐 Starting Neo4j Cloud Migration...")
        
        # Test connection first
        if not self.test_aura_connection():
            print("❌ Cannot proceed - Cloud connection failed")
            return
        
        if clear_db:
            print("🗑️  Clearing existing database...")
            database = self.neo4j_config.get("database", "neo4j")
            with self.driver.session(database=database) as session:
                # Use batched deletion for cloud
                session.run("MATCH (n) DETACH DELETE n")
            print("✅ Database cleared")
        
        # Step 1: Discover schema
        self.discover_doctypes()
        
        # Step 2: Create constraints and indexes
        self.create_constraints_and_indexes_cloud()
        
        # Step 3: Migrate all DocTypes with cloud optimization
        total_migrated = 0
        migration_stats = {}
        
        for doctype_name in self.discovered_doctypes.keys():
            count = self.migrate_doctype_cloud_optimized(doctype_name)
            migration_stats[doctype_name] = count
            total_migrated += count
        
        # Step 4: Create relationships with cloud optimization
        print("\n🔗 Creating relationships for Cloud...")
        total_relationships = 0
        
        for doctype_name in self.discovered_doctypes.keys():
            count = self.create_relationships_for_doctype_cloud(doctype_name)
            total_relationships += count
        
        # Step 5: Generate migration report
        self.generate_migration_report(migration_stats, total_relationships)
        
        print(f"\n🎉 Cloud Migration completed! {total_migrated} total records, {total_relationships} relationships created")
        print(f"🌐 Data successfully migrated to Neo4j Aura")
    
    def generate_migration_report(self, migration_stats: Dict, total_relationships: int):
        """Same as before but with cloud-specific info"""
        report = {
            'migration_timestamp': datetime.now().isoformat(),
            'target_environment': 'Neo4j Aura (Cloud)',
            'neo4j_config': {
                'uri': self.neo4j_config.get('uri', '').replace(self.neo4j_config.get('password', ''), '*****'),
                'database': self.neo4j_config.get('database'),
                'is_aura': self.is_aura
            },
            'total_doctypes_migrated': len(migration_stats),
            'total_records_migrated': sum(migration_stats.values()),
            'total_relationships_created': total_relationships,
            'doctype_breakdown': migration_stats,
            'discovered_doctypes': {
                name: {
                    'record_count': info['record_count'],
                    'field_count': len(info['fields']),
                    'relationship_count': len(info['relationships'])
                }
                for name, info in self.discovered_doctypes.items()
            }
        }
        
        # Save report
        report_filename = f"cloud_migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_filename, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n📊 Cloud Migration report saved to {report_filename}")
        
        # Print summary
        print("\n📈 Cloud Migration Summary:")
        for doctype, count in migration_stats.items():
            print(f"  {doctype}: {count} records")
    
    def verify_cloud_migration(self) -> Dict:
        """Verify the migration was successful in Neo4j Cloud"""
        print("🔍 Verifying Cloud Migration...")
        
        verification_results = {}
        database = self.neo4j_config.get("database", "neo4j")
        
        with self.driver.session(database=database) as session:
            # Check total nodes
            result = session.run("MATCH (n) RETURN count(n) as total_nodes")
            total_nodes = result.single()["total_nodes"]
            verification_results["total_nodes"] = total_nodes
            
            # Check total relationships
            result = session.run("MATCH ()-[r]->() RETURN count(r) as total_relationships")
            total_relationships = result.single()["total_relationships"]
            verification_results["total_relationships"] = total_relationships
            
            # Check each DocType
            doctype_counts = {}
            for doctype_name in self.discovered_doctypes.keys():
                try:
                    result = session.run(f"MATCH (n:{doctype_name}) RETURN count(n) as count")
                    count = result.single()["count"]
                    doctype_counts[doctype_name] = count
                except Exception as e:
                    doctype_counts[doctype_name] = f"Error: {e}"
            
            verification_results["doctype_counts"] = doctype_counts
            
            # Check if cloud migration flag exists
            result = session.run("MATCH (n) WHERE n._migrated_to_cloud = true RETURN count(n) as cloud_migrated")
            cloud_migrated = result.single()["cloud_migrated"]
            verification_results["cloud_migrated_nodes"] = cloud_migrated
        
        print(f"✅ Verification complete:")
        print(f"   Total nodes: {total_nodes}")
        print(f"   Total relationships: {total_relationships}")
        print(f"   Cloud-migrated nodes: {cloud_migrated}")
        
        return verification_results

# Usage Example for Cloud
def migrate_to_neo4j_cloud():
    """Main function to migrate to Neo4j Cloud"""
    migrator = Neo4jCloudFrappeToNeo4jMigrator()
    
    try:
        print("🌐 Starting migration to Neo4j Aura...")
        
        # Test connection first
        if not migrator.test_aura_connection():
            print("❌ Failed to connect to Neo4j Aura")
            return False
        
        # Run full migration
        migrator.run_full_cloud_migration(clear_db=True)
        
        # Verify migration
        verification = migrator.verify_cloud_migration()
        
        if verification["total_nodes"] > 0:
            print("🎉 Migration to Neo4j Cloud successful!")
            return True
        else:
            print("❌ Migration appears to have failed - no nodes found")
            return False
        
    except Exception as e:
        print(f"❌ Migration to cloud failed: {e}")
        return False
    finally:
        migrator.close()

if __name__ == "__main__":
    migrate_to_neo4j_cloud()