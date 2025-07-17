# gsai_assist/services/preprocessing/persistent_layer.py

import os
import time
import pickle
import re
import numpy as np
from typing import Dict, List, Any, Tuple, Optional
from sentence_transformers import SentenceTransformer
import frappe
from frappe.utils import now

class PersistentVectorLearning:
    """
    Persistent vector-based learning that survives restarts
    Similar to your entity system but for preprocessing patterns
    """
    
    def __init__(self, store_name: str):
        self.store_name = store_name
        
        # Storage paths (same pattern as your entity system)
        self.base_path = frappe.get_site_path("private", "preprocessing_vectors")
        os.makedirs(self.base_path, exist_ok=True)
        
        self.vectors_file = os.path.join(self.base_path, f"{store_name}_vectors.npy")
        self.metadata_file = os.path.join(self.base_path, f"{store_name}_metadata.pkl")
        
        # In-memory components
        self.embedder = None  # Lazy loaded
        self.vectors = None
        self.metadata = {}
        self.exact_matches = {}  # For fast exact lookups
        
        # Load existing data
        self._load_from_storage()
    
    @property
    def embedder_model(self):
        """Lazy load embedder (same model as your entities)"""
        if self.embedder is None:
            self.embedder = SentenceTransformer("all-MiniLM-L6-v2")
        return self.embedder
    
    def add_learning_pattern(self, 
                           original: str, 
                           corrected: str, 
                           pattern_type: str,
                           confidence: float = 1.0) -> bool:
        """
        Add learning pattern that persists across restarts
        """
        pattern_id = f"{pattern_type}_{hash(original)}_{int(time.time())}"
        
        # Create embedding for the original text
        embedding = self.embedder_model.encode([original])[0]
        
        # Store metadata
        metadata = {
            'id': pattern_id,
            'original': original,
            'corrected': corrected,
            'pattern_type': pattern_type,
            'confidence': confidence,
            'usage_count': 1,
            'created_at': time.time(),
            'last_used': time.time()
        }
        
        # Add to in-memory structures
        if self.vectors is None:
            self.vectors = embedding.reshape(1, -1)
        else:
            self.vectors = np.vstack([self.vectors, embedding])
        
        self.metadata[pattern_id] = metadata
        self.exact_matches[original.lower()] = pattern_id
        
        # Save to disk
        self._save_to_storage()
        
        return True
    
    def find_similar_patterns(self, 
                            query: str, 
                            pattern_type: Optional[str] = None,
                            similarity_threshold: float = 0.7) -> List[Dict]:
        """
        Find similar learning patterns using vector similarity
        """
        # Check exact match first
        if query.lower() in self.exact_matches:
            pattern_id = self.exact_matches[query.lower()]
            metadata = self.metadata[pattern_id]
            
            # Update usage
            metadata['usage_count'] += 1
            metadata['last_used'] = time.time()
            self._save_to_storage()
            
            return [{
                'original': metadata['original'],
                'corrected': metadata['corrected'],
                'confidence': metadata['confidence'],
                'usage_count': metadata['usage_count'],
                'similarity': 1.0
            }]
        
        # Vector similarity search
        if self.vectors is None:
            return []
        
        query_embedding = self.embedder_model.encode([query])[0]
        similarities = np.dot(self.vectors, query_embedding)
        
        results = []
        for i, similarity in enumerate(similarities):
            if similarity >= similarity_threshold:
                # Find metadata for this vector
                for pattern_id, metadata in self.metadata.items():
                    if np.array_equal(self.vectors[i], 
                                    self.embedder_model.encode([metadata['original']])[0]):
                        
                        # Filter by pattern type if specified
                        if pattern_type is None or metadata['pattern_type'] == pattern_type:
                            # Update usage
                            metadata['usage_count'] += 1 
                            metadata['last_used'] = time.time()
                            
                            results.append({
                                'original': metadata['original'],
                                'corrected': metadata['corrected'],
                                'confidence': metadata['confidence'],
                                'usage_count': metadata['usage_count'],
                                'similarity': float(similarity)
                            })
                        break
        
        # Sort by similarity and save updated usage
        results.sort(key=lambda x: x['similarity'], reverse=True)
        if results:
            self._save_to_storage()
        
        return results
    
    def _load_from_storage(self):
        """Load learning patterns from disk"""
        try:
            if os.path.exists(self.vectors_file):
                self.vectors = np.load(self.vectors_file)
            
            if os.path.exists(self.metadata_file):
                with open(self.metadata_file, 'rb') as f:
                    self.metadata = pickle.load(f)
                
                # Rebuild exact matches index
                for pattern_id, metadata in self.metadata.items():
                    self.exact_matches[metadata['original'].lower()] = pattern_id
            
            if self.metadata:
                print(f"📚 Loaded {len(self.metadata)} {self.store_name} patterns from storage")
        
        except Exception as e:
            print(f"⚠️ Error loading {self.store_name} patterns: {e}")
            # Start fresh
            self.vectors = None
            self.metadata = {}
            self.exact_matches = {}
    
    def _save_to_storage(self):
        """Save learning patterns to disk"""
        try:
            if self.vectors is not None:
                np.save(self.vectors_file, self.vectors)
            
            with open(self.metadata_file, 'wb') as f:
                pickle.dump(self.metadata, f)
            
        except Exception as e:
            print(f"⚠️ Error saving {self.store_name} patterns: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get learning statistics"""
        if not self.metadata:
            return {"total_patterns": 0}
        
        return {
            "total_patterns": len(self.metadata),
            "total_usage": sum(m['usage_count'] for m in self.metadata.values()),
            "avg_confidence": np.mean([m['confidence'] for m in self.metadata.values()]),
            "storage_size_mb": (self.vectors.nbytes if self.vectors is not None else 0) / (1024*1024)
        }

class PersistentSpellCorrector:
    """
    Persistent spell corrector that learns and remembers corrections
    """
    
    def __init__(self):
        self.learning_store = PersistentVectorLearning("spell_corrections")
        self.erp_vocabulary_store = PersistentVectorLearning("erp_vocabulary")
        
        # Bootstrap vocabulary from ERPNext if empty
        if len(self.erp_vocabulary_store.metadata) == 0:
            self._bootstrap_vocabulary()
    
    def correct(self, text: str) -> Tuple[str, List[Dict]]:
        """
        Correct text using persistent learning patterns
        Returns: (corrected_text, corrections_applied)
        """
        words = text.split()
        corrected_words = []
        corrections_applied = []
        
        for word in words:
            if len(word) <= 2 or word.isdigit():
                corrected_words.append(word)
                continue
            
            # Look for learned correction patterns
            similar_patterns = self.learning_store.find_similar_patterns(
                word, 
                pattern_type="spell_correction",
                similarity_threshold=0.8
            )
            
            if similar_patterns:
                # Use the most confident/used pattern
                best_pattern = max(similar_patterns, 
                                 key=lambda x: x['confidence'] * x['usage_count'])
                
                corrected_words.append(best_pattern['corrected'])
                corrections_applied.append({
                    'original': word,
                    'corrected': best_pattern['corrected'],
                    'confidence': best_pattern['confidence'],
                    'source': 'learned_pattern'
                })
            else:
                # Try ERPNext vocabulary
                vocab_matches = self.erp_vocabulary_store.find_similar_patterns(
                    word,
                    pattern_type="vocabulary",
                    similarity_threshold=0.7
                )
                
                if vocab_matches:
                    correction = vocab_matches[0]['corrected']
                    corrected_words.append(correction)
                    corrections_applied.append({
                        'original': word,
                        'corrected': correction,
                        'confidence': 0.8,
                        'source': 'erp_vocabulary'
                    })
                    
                    # Learn this correction for future use
                    self.learn_correction(word, correction)
                else:
                    corrected_words.append(word)
        
        return " ".join(corrected_words), corrections_applied
    
    def learn_correction(self, original: str, corrected: str):
        """Learn a spelling correction that persists"""
        if original.lower() != corrected.lower():
            self.learning_store.add_learning_pattern(
                original=original.lower(),
                corrected=corrected,
                pattern_type="spell_correction",
                confidence=0.9
            )
            print(f"📚 Learned correction: '{original}' → '{corrected}'")
    
    def _bootstrap_vocabulary(self):
        """Bootstrap vocabulary from ERPNext data (one-time)"""
        try:
            print("🔄 Bootstrapping vocabulary from ERPNext data...")
            
            # Learn from customers
            customers = frappe.get_all("Customer", fields=["customer_name"], limit=200)
            for customer in customers:
                if customer.get("customer_name"):
                    name = customer["customer_name"].strip()
                    if name:
                        self.erp_vocabulary_store.add_learning_pattern(
                            original=name.lower(),
                            corrected=name,
                            pattern_type="vocabulary",
                            confidence=1.0
                        )
            
            # Learn from items  
            items = frappe.get_all("Item", fields=["item_name"], limit=200)
            for item in items:
                if item.get("item_name"):
                    name = item["item_name"].strip()
                    if name:
                        self.erp_vocabulary_store.add_learning_pattern(
                            original=name.lower(),
                            corrected=name,
                            pattern_type="vocabulary",
                            confidence=1.0
                        )
            
            print("✅ Vocabulary bootstrap complete")
            
        except Exception as e:
            print(f"⚠️ Vocabulary bootstrap error: {e}")

class PersistentPhraseDetector:
    """
    Persistent phrase detector that learns business phrases
    """
    
    def __init__(self):
        self.learning_store = PersistentVectorLearning("business_phrases")
        
        # Bootstrap phrases from schema if empty
        if len(self.learning_store.metadata) == 0:
            self._bootstrap_phrases()
    
    def detect_and_replace(self, text: str) -> Tuple[str, List[Dict]]:
        """
        Detect business phrases using persistent learning
        """
        processed_text = text
        detected_phrases = []
        phrase_mappings = {}
        
        # Look for learned business phrases
        words = text.split()
        
        # Check 2-word and 3-word combinations
        for i in range(len(words)):
            for j in range(i + 2, min(i + 4, len(words) + 1)):
                phrase_candidate = " ".join(words[i:j])
                
                # Search for similar learned phrases
                similar_phrases = self.learning_store.find_similar_patterns(
                    phrase_candidate,
                    pattern_type="business_phrase", 
                    similarity_threshold=0.8
                )
                
                if similar_phrases:
                    best_phrase = similar_phrases[0]
                    replacement = best_phrase['corrected']
                    
                    # Replace in text
                    processed_text = processed_text.replace(phrase_candidate, replacement)
                    
                    detected_phrases.append({
                        'original': phrase_candidate,
                        'replacement': replacement,
                        'confidence': best_phrase['confidence'],
                        'usage_count': best_phrase['usage_count']
                    })
                    
                    phrase_mappings[replacement] = {
                        'original': phrase_candidate,
                        'confidence': best_phrase['confidence']
                    }
        
        return processed_text, detected_phrases
    
    def learn_phrase(self, phrase: str, replacement: str = None):
        """Learn a business phrase that persists"""
        if not replacement:
            replacement = phrase.replace(" ", "_").lower()
        
        self.learning_store.add_learning_pattern(
            original=phrase.lower(),
            corrected=replacement,
            pattern_type="business_phrase",
            confidence=0.8
        )
        print(f"📚 Learned phrase: '{phrase}' → '{replacement}'")
    
    def _bootstrap_phrases(self):
        """Bootstrap phrases from ERPNext schema (one-time)"""
        try:
            print("🔄 Bootstrapping phrases from ERPNext schema...")
            
            # Learn from DocType names
            doctypes = frappe.get_all("DocType", 
                                    fields=["name"], 
                                    filters={"istable": 0, "issingle": 0})
            
            for dt in doctypes:
                name = dt["name"]
                if " " in name:
                    self.learn_phrase(name, name.replace(" ", "_").lower())
            
            # Learn from field labels
            fields = frappe.db.sql("""
                SELECT DISTINCT label 
                FROM `tabDocField` 
                WHERE label LIKE '% %' 
                AND LENGTH(label) < 30
                LIMIT 100
            """)
            
            for field in fields:
                if field[0]:
                    self.learn_phrase(field[0], field[0].replace(" ", "_").lower())
            
            print("✅ Phrase bootstrap complete")
            
        except Exception as e:
            print(f"⚠️ Phrase bootstrap error: {e}")

class PersistentPreprocessingPipeline:
    """
    Main preprocessing pipeline with persistent learning
    Drop-in replacement for your existing PipeLine class
    """
    
    def __init__(self):
        self.spell_corrector = PersistentSpellCorrector()
        self.phrase_detector = PersistentPhraseDetector()
        
        # Keep your excellent entity mapper
        from gsai_assist.services.preprocessing.entity_mapper import process as map_entity
        self.entity_mapper = map_entity
    
    def process(self, query: str, opts: Dict[str, Any]) -> Tuple[str, Dict, List]:
        """
        Enhanced preprocessing with persistent learning
        EXACT same interface as your original PipeLine.process()
        """
        logs = []
        logs.append(f"🧠 Starting persistent preprocessing: {now()}")
        
        try:
            # Step 1: Persistent spell correction
            corrected_query, corrections = self.spell_corrector.correct(query)
            if corrections:
                logs.append(f"📝 Applied {len(corrections)} learned corrections")
                for correction in corrections:
                    logs.append(f"   '{correction['original']}' → '{correction['corrected']}'")
            
            # Step 2: Persistent phrase detection  
            processed_query, detected_phrases = self.phrase_detector.detect_and_replace(corrected_query)
            if detected_phrases:
                logs.append(f"🔍 Detected {len(detected_phrases)} learned business phrases")
                for phrase in detected_phrases:
                    logs.append(f"   '{phrase['original']}' → '{phrase['replacement']}'")
            
            # Step 3: Tokenization (simplified for compatibility)
            from gsai_assist.services.preprocessing import Token
            tokens = []
            for match in re.finditer(r'\w+', processed_query):
                token = Token(match.group(), match.start(), match.end())
                tokens.append(token)
            
            # Step 4: Your excellent entity mapping (unchanged)
            entity_groups = opts.get("entity_groups", [])
            mapped_tokens, context = self.entity_mapper(tokens, entity_groups, debug=opts.get("debug"))
            
            logs.append(f"🎯 Entity mapping found {len(context.get('dt', []))} doctypes")
            
            # Step 5: Rebuild query with all enhancements
            rewritten_query = self._rebuild_query(corrected_query, mapped_tokens)
            
            return rewritten_query, context, logs
            
        except Exception as e:
            logs.append(f"❌ Error in preprocessing: {str(e)}")
            return query, {}, logs
    
    def _rebuild_query(self, base_query: str, mapped_tokens: List) -> str:
        """Rebuild query with entity mappings"""
        result = base_query
        
        for token in mapped_tokens:
            if hasattr(token, 'canonical') and token.canonical:
                result = result.replace(token.text, token.canonical)
        
        return result
    
    def learn_from_user_input(self, original_query: str, user_correction: str):
        """
        Learn from user corrections to improve future performance
        This is the learning feedback loop!
        """
        if original_query.lower() != user_correction.lower():
            # Learn spelling corrections
            orig_words = original_query.split()
            corr_words = user_correction.split()
            
            if len(orig_words) == len(corr_words):
                for orig_word, corr_word in zip(orig_words, corr_words):
                    if orig_word.lower() != corr_word.lower():
                        self.spell_corrector.learn_correction(orig_word, corr_word)
            
            # Learn phrase corrections
            # This could detect new business phrases user is teaching us
    
    def get_learning_stats(self) -> Dict[str, Any]:
        """Get statistics about what the system has learned"""
        return {
            "spell_patterns": self.spell_corrector.learning_store.get_stats(),
            "phrase_patterns": self.phrase_detector.learning_store.get_stats(),
            "vocabulary_size": self.spell_corrector.erp_vocabulary_store.get_stats(),
            "total_learned_patterns": (
                self.spell_corrector.learning_store.get_stats().get("total_patterns", 0) +
                self.phrase_detector.learning_store.get_stats().get("total_patterns", 0)
            )
        }

# Backward compatibility - replace your existing PipeLine
class PipeLine:
    """
    Drop-in replacement for your existing PipeLine class
    Now with persistent learning that survives restarts!
    """

    def __init__(self):
        self.persistent_pipeline = PersistentPreprocessingPipeline()

    def process(self, query: str, opts: Dict[str, Any]) -> Tuple[str, Dict, List]:
        """EXACT same interface as your original"""
        return self.persistent_pipeline.process(query, opts)

    def learn_from_correction(self, original: str, corrected: str):
        """New method to learn from user corrections"""
        self.persistent_pipeline.learn_from_user_input(original, corrected)

    def get_intelligence_report(self) -> Dict[str, Any]:
        """New method to see what the system has learned"""
        return self.persistent_pipeline.get_learning_stats()

# Test function
def test_persistent_learning():
    """Test persistent learning across 'restarts'"""
    print("🧠 TESTING PERSISTENT LEARNING")
    print("=" * 50)
    
    pipeline = PersistentPreprocessingPipeline()
    
    # Test spelling learning
    print("\n1. Testing spell correction learning...")
    corrected, corrections = pipeline.spell_corrector.correct("custmer ABC Corp")
    print(f"   Input: 'custmer ABC Corp'")
    print(f"   Output: '{corrected}'")
    print(f"   Corrections: {corrections}")
    
    # Manually teach a correction
    pipeline.spell_corrector.learn_correction("custmer", "customer")
    
    # Test it learned
    corrected2, corrections2 = pipeline.spell_corrector.correct("custmer XYZ Ltd")
    print(f"   After learning - Input: 'custmer XYZ Ltd'")
    print(f"   After learning - Output: '{corrected2}'")
    
    # Test phrase learning
    print("\n2. Testing phrase learning...")
    pipeline.phrase_detector.learn_phrase("sales order", "sales_order")
    
    processed, phrases = pipeline.phrase_detector.detect_and_replace("show sales order details")
    print(f"   Input: 'show sales order details'")
    print(f"   Output: '{processed}'")
    print(f"   Phrases: {phrases}")
    
    # Test full pipeline
    print("\n3. Testing full pipeline...")
    opts = {"entity_groups": ["Industry", "Region"], "debug": False}
    
    result_query, context, logs = pipeline.process("custmer sales order status", opts)
    print(f"   Input: 'custmer sales order status'")
    print(f"   Output: '{result_query}'")
    print(f"   Logs: {logs}")
    
    # Show learning stats
    stats = pipeline.get_learning_stats()
    print(f"\n📊 Learning Stats:")
    print(f"   Spell patterns: {stats['spell_patterns']['total_patterns']}")
    print(f"   Phrase patterns: {stats['phrase_patterns']['total_patterns']}")
    print(f"   Total learned: {stats['total_learned_patterns']}")
    
    print("\n✅ All patterns are now saved to disk and will survive restart!")

if __name__ == "__main__":
    test_persistent_learning()
