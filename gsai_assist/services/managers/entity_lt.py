import frappe
import numpy as np
import json
import os
import time
import threading
from typing import List, Dict, Any, Set, Optional, Tuple

class LightweightEntityStore:
    """A minimal in-memory vector store optimized for entity matching"""
    
    # Singleton instance
    _instance = None
    _lock = threading.RLock()
    _initialized = False
    
    # Data storage
    _entities = []  # List of entity texts
    _embeddings = None  # NumPy array of embeddings
    _metadata = []  # List of metadata dicts
    _group_index = {}  # Group name -> list of entity indices
    _text_index = {}  # Lowercase text -> entity index
    
    def __new__(cls):
        """Implement singleton pattern"""
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(LightweightEntityStore, cls).__new__(cls)
            return cls._instance
    
    def __init__(self):
        """Initialize the store if needed"""
        self._ensure_initialized()
    
    def _ensure_initialized(self):
        """Initialize entity store if not already done"""
        if not LightweightEntityStore._initialized:
            with LightweightEntityStore._lock:
                if not LightweightEntityStore._initialized:
                    self._initialize_store()
    
    def _initialize_store(self):
        """Load entities and embeddings from file or database"""
        start_time = time.time()
        
        # Load entities from database or file
        try:
            self._load_entities()
            print(f"Entity store initialized with {len(self._entities)} entities in {time.time() - start_time:.2f}s")
            LightweightEntityStore._initialized = True
        except Exception as e:
            print(f"Error initializing entity store: {str(e)}")
            # Create empty store on failure
            self._entities = []
            self._embeddings = np.array([]).reshape(0, 384)  # Adjust dimension based on your embedder
            self._metadata = []
            self._group_index = {}
            self._text_index = {}
            LightweightEntityStore._initialized = True
    
    def _load_entities(self):
        """Load entities from file or database"""
        # Check if we have a cached entity file first (faster load)
        cache_path = os.path.join(".", "entity_cache.npz")
        if os.path.exists(cache_path):
            try:
                data = np.load(cache_path, allow_pickle=True)
                self._entities = data["entities"].tolist()
                self._embeddings = data["embeddings"]
                self._metadata = data["metadata"].tolist()
                self._build_indices()
                return
            except Exception as e:
                print(f"Failed to load entity cache file: {str(e)}")
        
        # Fall back to loading from database
        self._load_from_database()
        
        # Save to cache file for faster loading next time
        self._save_to_cache()
    
    def _load_from_database(self):
        """Load entities from database"""
        # Load entities from your database - customize this for your schema
        entities = frappe.db.sql("""
            SELECT 
                name,
                canonical_form,
                doc_type,
                entity_group,
                related_doctypes
            FROM `tabEntity` 
            WHERE is_active = 1
        """, as_dict=True)
        
        if not entities:
            # Create empty arrays
            self._entities = []
            self._embeddings = np.array([]).reshape(0, 384)  # Adjust dimension based on your embedder
            self._metadata = []
            return
        
        # Process entities
        self._entities = []
        self._metadata = []
        embeddings_list = []
        
        # Get embedder
        embedder = self._get_embedder()
        
        # Process in batches for efficiency
        batch_size = 100
        for i in range(0, len(entities), batch_size):
            batch = entities[i:i+batch_size]
            
            # Extract texts for embedding
            texts = [entity.name for entity in batch]
            
            # Embed batch
            batch_embeddings = embedder.encode(texts)
            
            # Process each entity
            for j, entity in enumerate(batch):
                self._entities.append(entity.name)
                
                related_doctypes = []
                try:
                    if entity.related_doctypes:
                        related_doctypes = json.loads(entity.related_doctypes)
                except:
                    pass
                
                self._metadata.append({
                    "canonical": entity.canonical_form or entity.name,
                    "doc_type": entity.doc_type or "",
                    "entity_group": entity.entity_group or "",
                    "related_doctypes": related_doctypes or []
                })
                
                embeddings_list.append(batch_embeddings[j])
        
        # Convert embeddings to numpy array
        self._embeddings = np.array(embeddings_list)
        
        # Build indices for fast lookup
        self._build_indices()
    
    def _build_indices(self):
        """Build lookup indices for fast entity retrieval"""
        # Text index for exact matches
        self._text_index = {text.lower(): i for i, text in enumerate(self._entities)}
        
        # Group index for filtering
        self._group_index = {}
        for i, meta in enumerate(self._metadata):
            groups = meta.get("entity_group", "").split(",")
            for group in groups:
                if group:
                    self._group_index.setdefault(group, []).append(i)
    
    def _save_to_cache(self):
        """Save entities to cache file for faster loading"""
        try:
            cache_path = os.path.join(".", "entity_cache.npz")
            np.savez_compressed(
                cache_path,
                entities=np.array(self._entities, dtype=object),
                embeddings=self._embeddings,
                metadata=np.array(self._metadata, dtype=object)
            )
            print(f"Saved {len(self._entities)} entities to cache file")
        except Exception as e:
            print(f"Failed to save entity cache: {str(e)}")
    
    def _get_embedder(self):
        """Get the text embedder"""
        # Replace with your actual embedder import
        from your_module import embedder
        return embedder
    
    def search(self, queries: List[str], include_groups: List[str], 
              top_k: int = 5, max_distance: float = 0.25) -> List[Dict]:
        """
        Search for entities matching the queries
        
        Args:
            queries: List of query strings
            include_groups: List of entity groups to include
            top_k: Number of results per query
            max_distance: Maximum cosine distance for matches
            
        Returns:
            List of match dictionaries for each query
        """
        self._ensure_initialized()
        
        # Handle empty cases
        if not queries or not self._entities:
            return [{"matches": []} for _ in queries]
        
        # Convert include_groups to set for faster lookups
        include_groups_set = set(include_groups)
        
        # Create filter mask based on groups
        filter_indices = set()
        for group in include_groups:
            if group in self._group_index:
                filter_indices.update(self._group_index[group])
        
        # If no entities match the groups, return empty results
        if not filter_indices:
            return [{"matches": []} for _ in queries]
        
        # Create filtered arrays
        filter_indices = sorted(filter_indices)
        filtered_entities = [self._entities[i] for i in filter_indices]
        filtered_metadata = [self._metadata[i] for i in filter_indices]
        filtered_embeddings = self._embeddings[filter_indices]
        
        # Check for exact matches first
        results = []
        queries_for_vector = []
        query_indices = []
        
        for i, query in enumerate(queries):
            query_lower = query.lower()
            
            # Check for exact match
            if query_lower in self._text_index:
                idx = self._text_index[query_lower]
                meta = self._metadata[idx]
                
                # Check if this entity is in the requested groups
                entity_groups = set(meta.get("entity_group", "").split(","))
                if entity_groups & include_groups_set:
                    results.append({
                        "id": i,
                        "matches": [{
                            "text": self._entities[idx],
                            "canonical": meta.get("canonical", self._entities[idx]),
                            "doc_type": meta.get("doc_type", ""),
                            "entity_groups": list(entity_groups),
                            "related_doctypes": meta.get("related_doctypes", []),
                            "distance": 0.0
                        }]
                    })
                    continue
            
            # No exact match, add to vector search list
            queries_for_vector.append(query)
            query_indices.append(i)
        
        # If all queries matched exactly, return results
        if not queries_for_vector:
            return results
        
        # Get embedder
        embedder = self._get_embedder()
        
        # Encode queries
        query_embeddings = embedder.encode(queries_for_vector)
        
        # For very small entity sets, use brute force comparison
        if len(filtered_entities) < 1000:
            # Compute cosine similarity for each query
            for q_idx, query_embedding in enumerate(query_embeddings):
                original_idx = query_indices[q_idx]
                similarities = np.dot(filtered_embeddings, query_embedding)
                distances = 1.0 - similarities
                
                # Find top-k matches
                top_indices = np.argsort(distances)[:top_k]
                matches = []
                
                for idx in top_indices:
                    if distances[idx] <= max_distance:
                        meta = filtered_metadata[idx]
                        matches.append({
                            "text": filtered_entities[idx],
                            "canonical": meta.get("canonical", filtered_entities[idx]),
                            "doc_type": meta.get("doc_type", ""),
                            "entity_groups": meta.get("entity_group", "").split(","),
                            "related_doctypes": meta.get("related_doctypes", []),
                            "distance": float(distances[idx])
                        })
                
                results.append({
                    "id": original_idx,
                    "matches": matches
                })
        else:
            # For larger sets, use approximate nearest neighbors
            try:
                import faiss
                
                # Normalize embeddings for cosine similarity
                filtered_norm = np.copy(filtered_embeddings)
                query_norm = np.copy(query_embeddings)
                
                faiss.normalize_L2(filtered_norm)
                faiss.normalize_L2(query_norm)
                
                # Create FAISS index
                dim = filtered_norm.shape[1]
                index = faiss.IndexFlatIP(dim)
                index.add(filtered_norm)
                
                # Search
                similarities, indices = index.search(query_norm, top_k)
                
                # Process results
                for q_idx, (sims, idxs) in enumerate(zip(similarities, indices)):
                    original_idx = query_indices[q_idx]
                    matches = []
                    
                    for sim, idx in zip(sims, idxs):
                        if idx >= 0 and sim >= (1.0 - max_distance):  # Convert similarity to distance
                            meta = filtered_metadata[idx]
                            matches.append({
                                "text": filtered_entities[idx],
                                "canonical": meta.get("canonical", filtered_entities[idx]),
                                "doc_type": meta.get("doc_type", ""),
                                "entity_groups": meta.get("entity_group", "").split(","),
                                "related_doctypes": meta.get("related_doctypes", []),
                                "distance": float(1.0 - sim)
                            })
                    
                    results.append({
                        "id": original_idx,
                        "matches": matches
                    })
            except ImportError:
                # Fall back to numpy if FAISS not available
                for q_idx, query_embedding in enumerate(query_embeddings):
                    original_idx = query_indices[q_idx]
                    similarities = np.dot(filtered_embeddings, query_embedding)
                    distances = 1.0 - similarities
                    
                    # Find top-k matches
                    top_indices = np.argsort(distances)[:top_k]
                    matches = []
                    
                    for idx in top_indices:
                        if distances[idx] <= max_distance:
                            meta = filtered_metadata[idx]
                            matches.append({
                                "text": filtered_entities[idx],
                                "canonical": meta.get("canonical", filtered_entities[idx]),
                                "doc_type": meta.get("doc_type", ""),
                                "entity_groups": meta.get("entity_group", "").split(","),
                                "related_doctypes": meta.get("related_doctypes", []),
                                "distance": float(distances[idx])
                            })
                    
                    results.append({
                        "id": original_idx,
                        "matches": matches
                    })
        
        # Sort results by original query order
        results.sort(key=lambda x: x["id"])
        return results
    
    def refresh(self):
        """Reload entities from database"""
        with LightweightEntityStore._lock:
            LightweightEntityStore._initialized = False
            self._initialize_store()
    
    def get_stats(self):
        """Get statistics about the entity store"""
        self._ensure_initialized()
        
        group_stats = {group: len(indices) for group, indices in self._group_index.items()}
        
        return {
            "total_entities": len(self._entities),
            "embedding_dimensions": self._embeddings.shape[1] if len(self._embeddings) > 0 else 0,
            "memory_usage_mb": self._embeddings.nbytes / (1024 * 1024) if hasattr(self._embeddings, 'nbytes') else 0,
            "groups": group_stats
        }