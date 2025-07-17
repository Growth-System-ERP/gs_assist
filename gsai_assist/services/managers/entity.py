# gsai_assist/services/managers/entity.py

import frappe
import chromadb
from typing import Dict, List, Optional
from sentence_transformers import SentenceTransformer
from chromadb.config import Settings
from chromadb import PersistentClient
import os

class EntityManager:
    def __init__(self, model_name="all-MiniLM-L6-v2", persist_path=None):
        # Default path if none provided
        if persist_path is None:
            persist_path = frappe.get_site_path("private", "entity_vectors")
            os.makedirs(persist_path, exist_ok=True)

        # Persistent Chroma client so data survives restarts
        self.client = PersistentClient(path=persist_path, settings=Settings(anonymized_telemetry=False))
        self.embedder = SentenceTransformer(model_name)

    def _get_collection(self):
        return self.client.get_or_create_collection("all_entities")

    def _clean_metadata(self, metadata: Dict) -> Dict:
        """
        Clean metadata to ensure ChromaDB compatibility
        ChromaDB only accepts: str, int, float, bool (no None values)
        """
        cleaned = {}
        
        for key, value in metadata.items():
            if value is None:
                # Convert None to empty string
                cleaned[key] = ""
            elif isinstance(value, (str, int, float, bool)):
                # Keep valid types as-is
                cleaned[key] = value
            elif isinstance(value, list):
                # Convert lists to comma-separated strings
                cleaned[key] = ",".join(str(item) for item in value if item is not None)
            else:
                # Convert other types to string
                cleaned[key] = str(value)
        
        return cleaned

    def _prepare_entity_data(self, entity: dict) -> tuple:
        """
        Prepare entity data for ChromaDB insertion with proper validation
        """
        canon = entity.get("canonical_name", "")
        if not canon:
            raise ValueError("Entity must have a canonical_name")

        # Get aliases safely
        aliases_raw = entity.get("aliases", "")
        if aliases_raw:
            aliases = [a.strip() for a in str(aliases_raw).split(",") if a.strip()]
        else:
            aliases = []
        
        # Always include canonical name in aliases
        canonical_lower = canon.lower()
        if canonical_lower not in [a.lower() for a in aliases]:
            aliases.append(canonical_lower)

        # Get groups safely
        groups = entity.get("groups", [])
        if not groups:
            groups = []
        
        # Extract group names
        group_names = []
        for group in groups:
            if isinstance(group, dict):
                group_name = group.get("entity_group", "")
            else:
                group_name = str(group)
            
            if group_name:
                group_names.append(group_name)
        
        if not group_names:
            group_names = ["Default"]  # Fallback group
        
        return canon, aliases, group_names

    def sync_entity(self, entity: dict, old_entity: dict = None):
        """
        Sync entity with improved error handling and metadata cleaning
        """
        try:
            collection = self._get_collection()
            
            # Prepare entity data
            canon, aliases, group_names = self._prepare_entity_data(entity)
            
            # Delete all existing entries for this entity
            try:
                stale = collection.get(where={"canonical": canon})
                if stale["ids"]:
                    collection.delete(ids=stale["ids"])
            except Exception as e:
                frappe.log_error(f"Error deleting stale entity entries: {e}")
                # Continue even if deletion fails

            # Prepare new entries
            if aliases:
                docs = []
                ids = []
                metadatas = []

                for group in group_names:
                    for alias in aliases:
                        docs.append(alias)
                        ids.append(f"{group}::{canon}::{alias}")
                        
                        # Create metadata with None value handling
                        raw_metadata = {
                            "canonical": canon,
                            "alias": alias,
                            "entity_group": group,
                            "doc_type": entity.get("doc_type"),
                            "related_doctypes": ",".join(entity.get("related_doctypes") or [])
                        }
                        
                        # Clean metadata to remove None values
                        clean_metadata = self._clean_metadata(raw_metadata)
                        metadatas.append(clean_metadata)

                # Generate embeddings
                try:
                    embeddings = self.embedder.encode(docs).tolist()
                except Exception as e:
                    frappe.log_error(f"Error generating embeddings: {e}")
                    return

                # Add to collection
                try:
                    collection.add(
                        embeddings=embeddings,
                        documents=docs,
                        metadatas=metadatas,
                        ids=ids
                    )
                    print(f"✅ Synced entity '{canon}' with {len(aliases)} aliases across {len(group_names)} groups")
                    
                except Exception as e:
                    frappe.log_error(f"Error adding to ChromaDB: {e}")
                    # Log detailed debug info
                    frappe.log_error(f"Debug - Entity: {canon}")
                    frappe.log_error(f"Debug - Aliases: {aliases}")
                    frappe.log_error(f"Debug - Groups: {group_names}")
                    frappe.log_error(f"Debug - Metadata sample: {metadatas[0] if metadatas else 'None'}")
                    raise

        except Exception as e:
            frappe.log_error(f"Error in sync_entity for {entity.get('canonical_name', 'unknown')}: {str(e)}")
            # Re-raise to show user the error
            frappe.throw(f"Failed to sync entity: {str(e)}")

    def delete_entity(self, entity: dict):
        """
        Delete entity with improved error handling
        """
        try:
            coll = self._get_collection()
            canon = entity.get("canonical_name", "")
            
            if not canon:
                frappe.log_error("Cannot delete entity without canonical_name")
                return
            
            stale = coll.get(where={"canonical": canon})
            if stale["ids"]:
                coll.delete(ids=stale["ids"])
                print(f"✅ Deleted entity '{canon}' ({len(stale['ids'])} entries)")
            else:
                print(f"ℹ️  Entity '{canon}' not found in vector store")
                
        except Exception as e:
            frappe.log_error(f"Error deleting entity {entity.get('canonical_name', 'unknown')}: {str(e)}")
            # Don't re-raise for deletion errors as it's not critical
    
    def test_connection(self) -> bool:
        """
        Test ChromaDB connection and collection access
        """
        try:
            collection = self._get_collection()
            # Try a simple operation
            count = collection.count()
            print(f"✅ ChromaDB connection successful. Collection has {count} items.")
            return True
        except Exception as e:
            frappe.log_error(f"ChromaDB connection test failed: {e}")
            print(f"❌ ChromaDB connection failed: {e}")
            return False
    
    def get_collection_stats(self) -> Dict:
        """
        Get collection statistics for debugging
        """
        try:
            collection = self._get_collection()
            
            # Get basic stats
            count = collection.count()
            
            # Get sample data for inspection
            sample = collection.get(limit=5, include=["metadatas", "documents"])
            
            return {
                "total_count": count,
                "sample_ids": sample.get("ids", [])[:3],
                "sample_documents": sample.get("documents", [])[:3],
                "sample_metadata": sample.get("metadatas", [])[:3] if sample.get("metadatas") else []
            }
        except Exception as e:
            frappe.log_error(f"Error getting collection stats: {e}")
            return {"error": str(e)}

# Test function to verify the fix
def test_entity_manager():
    """
    Test function to verify ChromaDB integration works
    """
    print("🧪 Testing Entity Manager...")
    
    manager = EntityManager()
    
    # Test connection
    if not manager.test_connection():
        return False
    
    # Test with sample entity (including None values that caused the error)
    test_entity = {
        "canonical_name": "Test Customer",
        "aliases": "test corp, test company",
        "groups": [{"entity_group": "Industry"}],
        "doc_type": "Customer",
        "related_doctypes": None,  # This was causing the error
        "description": None        # This too
    }
    
    try:
        print("Testing entity sync...")
        manager.sync_entity(test_entity)
        
        print("Testing collection stats...")
        stats = manager.get_collection_stats()
        print(f"Collection stats: {stats}")
        
        print("Testing entity deletion...")
        manager.delete_entity(test_entity)
        
        print("✅ All tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

# Debug helper function
def debug_entity_metadata(entity_name: str):
    """
    Debug helper to inspect entity metadata
    """
    try:
        entity_doc = frappe.get_doc("Entity", entity_name)
        entity_dict = entity_doc.as_dict()
        
        print(f"🔍 Debugging entity: {entity_name}")
        print(f"Raw entity data: {entity_dict}")
        
        # Test metadata cleaning
        manager = EntityManager()
        canon, aliases, groups = manager._prepare_entity_data(entity_dict)
        
        print(f"Canonical: {canon}")
        print(f"Aliases: {aliases}")
        print(f"Groups: {groups}")
        
        # Test metadata preparation
        sample_metadata = {
            "canonical": canon,
            "alias": aliases[0] if aliases else "",
            "entity_group": groups[0] if groups else "",
            "doc_type": entity_dict.get("doc_type"),
            "related_doctypes": ",".join(entity_dict.get("related_doctypes") or [])
        }
        
        cleaned = manager._clean_metadata(sample_metadata)
        print(f"Sample cleaned metadata: {cleaned}")
        
    except Exception as e:
        print(f"Debug failed: {e}")

if __name__ == "__main__":
    test_entity_manager()