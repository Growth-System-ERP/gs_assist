import re
from frappe.utils import now
from gsai_assist.services.preprocessing.stop_words import STOP_WORDS
from gsai_assist.services.preprocessing.entity_mapper import process as map_entity

class EntityCandidate:
    """Simple candidate with position tracking"""
    def __init__(self, text, start, end, candidate_type, priority):
        self.text = text
        self.start = start
        self.end = end
        self.candidate_type = candidate_type
        self.priority = priority
        self.entity = None
        self.canonical = None
        self.confidence = 0.0

    def entatise(self, entity, confidence=0.0):
        self.entity = entity
        self.canonical = entity
        self.confidence = confidence

class PipeLine:
    def process(self, query, opts):
        self.query = query
        self.opts = opts
        self.logs = []
        
        # Single pass processing - no separate reset/preprocess steps
        return self._process_linear()

    def _process_linear(self):
        """Linear processing - single pass through all steps"""
        self.logs.append(f"starting linear processing: {now()}")
        
        # Step 1: Clean query once
        self.original_query = self.query
        cleaned_query = re.sub(r'[^\w\s\'-]', ' ', self.query.lower())
        cleaned_query = re.sub(r'\s+', ' ', cleaned_query.strip())
        
        # Step 2: Process words and build candidates in one pass
        words = cleaned_query.split()
        candidates = []
        meaningful_words = []
        expanded_terms = {}
        
        # Single loop through words with position tracking
        current_pos = 0
        keep_question_words = {'who', 'what', 'where', 'when', 'why', 'how', 'which'}
        
        # Build word info and chunks simultaneously
        word_infos = []
        for word in words:
            word_start = cleaned_query.find(word, current_pos)
            word_end = word_start + len(word)
            current_pos = word_end
            
            # Filter meaningful words inline
            is_meaningful = (
                word not in STOP_WORDS or 
                (word in keep_question_words and len(word_infos) == 0)
            ) and len(word) >= 3
            
            word_info = {
                'word': word,
                'start': word_start,
                'end': word_end,
                'meaningful': is_meaningful
            }
            word_infos.append(word_info)
            
            if is_meaningful:
                meaningful_words.append(word)
        
        # Step 3: Build all candidate types in single pass
        self._build_candidates_linear(word_infos, candidates, meaningful_words, expanded_terms)
        
        # Step 4: Entity mapping (unchanged - already optimized)
        context = {'dt': set(), 'rdt': set()}
        if candidates:
            mapped_entities, entity_context = map_entity(
                candidates, 
                self.opts.get("entity_groups"), 
                debug=self.opts.get("debug")
            )
            context.update(entity_context)
        else:
            mapped_entities = candidates
        
        # Step 5: Build entity mappings and schema context in parallel
        entity_mappings, schema_context = self._build_final_results(mapped_entities, context)
        
        return {
            'original_query': self.original_query,
            'meaningful_words': meaningful_words,
            'expanded_terms': expanded_terms,
            'entity_mappings': entity_mappings,
            'context': {
                'dt': list(context.get('dt', set())),
                'rdt': list(context.get('rdt', set()))
            },
            'schema_context': schema_context,
            'candidates_processed': len(candidates),
            'entities_found': len(entity_mappings),
            'logs': self.logs
        }

    def _build_candidates_linear(self, word_infos, candidates, meaningful_words, expanded_terms):
        """Build all candidate types in single optimized pass"""
        
        # Pre-compute meaningful word positions for faster lookup
        meaningful_positions = {
            i: word_info for i, word_info in enumerate(word_infos) 
            if word_info['meaningful']
        }
        
        # Get business expansions once upfront
        if meaningful_words:
            from gsai_assist.services.preprocessing.business_vocabulary import BusinessVocabularyExpander
            expander = BusinessVocabularyExpander()
            business_domain = self.opts.get('business_domain', 'general')
            
            # Batch expand all meaningful words at once
            for word in meaningful_words:
                expansions = expander.expand_word(word, business_domain)
                if len(expansions) > 1:
                    expanded_terms[word] = expansions[1:]
        
        # Build candidates in order of priority (most important first)
        # This allows early termination if we hit limits
        
        # Priority 1 & 2: Multi-word phrases (chunks and sub-phrases)
        self._add_phrase_candidates(meaningful_positions, candidates)
        
        # Priority 3: Individual meaningful words
        for pos, word_info in meaningful_positions.items():
            candidates.append(EntityCandidate(
                text=word_info['word'],
                start=word_info['start'],
                end=word_info['end'],
                candidate_type='word',
                priority=3
            ))
        
        # Priority 4: Expanded business terms (reuse positions)
        for original_word, expansions in expanded_terms.items():
            # Find first occurrence position for this word
            word_pos = next(
                (info for info in meaningful_positions.values() 
                 if info['word'] == original_word), 
                None
            )
            
            if word_pos:
                for expanded_term in expansions:
                    candidates.append(EntityCandidate(
                        text=expanded_term,
                        start=word_pos['start'],
                        end=word_pos['end'],
                        candidate_type='expanded_term',
                        priority=4
                    ))
        
        self.logs.append(f"generated {len(candidates)} candidates linearly")

    def _add_phrase_candidates(self, meaningful_positions, candidates):
        """Add phrase candidates efficiently"""
        positions = list(meaningful_positions.keys())
        
        if len(positions) < 2:
            return
        
        # Look for consecutive meaningful words to form phrases
        i = 0
        while i < len(positions):
            phrase_start = i
            phrase_end = i
            
            # Extend phrase as far as possible with consecutive or near-consecutive words
            while (phrase_end + 1 < len(positions) and 
                   positions[phrase_end + 1] - positions[phrase_end] <= 2):  # Allow 1 stop word gap
                phrase_end += 1
            
            # If we have a multi-word phrase
            if phrase_end > phrase_start:
                start_pos = meaningful_positions[positions[phrase_start]]['start']
                end_pos = meaningful_positions[positions[phrase_end]]['end']
                
                # Get the actual phrase text
                phrase_text = self.query[start_pos:end_pos].lower()
                phrase_text = re.sub(r'\s+', ' ', phrase_text.strip())
                
                # Add full phrase
                candidates.append(EntityCandidate(
                    text=phrase_text,
                    start=start_pos,
                    end=end_pos,
                    candidate_type='chunk',
                    priority=1
                ))
                
                # Add 2-word sub-phrases if phrase is longer
                if phrase_end - phrase_start >= 1:
                    for j in range(phrase_start, phrase_end):
                        sub_start = meaningful_positions[positions[j]]['start']
                        sub_end = meaningful_positions[positions[j + 1]]['end']
                        sub_phrase = self.query[sub_start:sub_end].lower()
                        sub_phrase = re.sub(r'\s+', ' ', sub_phrase.strip())
                        
                        candidates.append(EntityCandidate(
                            text=sub_phrase,
                            start=sub_start,
                            end=sub_end,
                            candidate_type='sub_phrase',
                            priority=2
                        ))
            
            i = phrase_end + 1

    def _build_final_results(self, mapped_entities, context):
        """Build final results efficiently"""
        entity_mappings = []
        found_doctypes = []
        
        # Single pass through mapped entities
        for candidate in mapped_entities:
            if hasattr(candidate, 'canonical') and candidate.canonical:
                entity_mappings.append({
                    'text': candidate.text,
                    'start': candidate.start,
                    'end': candidate.end,
                    'entity': candidate.canonical,
                    'candidate_type': candidate.candidate_type,
                    'priority': candidate.priority,
                    'confidence': getattr(candidate, 'confidence', 0.0)
                })
                found_doctypes.append(candidate.canonical)
        
        # Sort by confidence once
        entity_mappings.sort(key=lambda x: x['confidence'], reverse=True)
        
        # Build schema context only if we have entities
        schema_context = {}
        if found_doctypes:
            try:
                from gsai_assist.services.preprocessing.schema_mapper import get_doctypes_with_multiple_links
                related_doctypes = get_doctypes_with_multiple_links(list(set(found_doctypes)))
                
                schema_context = {
                    'found_doctypes': list(set(found_doctypes)),
                    'related_doctypes': related_doctypes[:10],
                    'entity_count': len(set(found_doctypes)),
                    'related_count': len(related_doctypes)
                }
                
                self.logs.append(f"found {len(related_doctypes)} related doctypes")
                
            except Exception as e:
                schema_context = {'found_doctypes': list(set(found_doctypes))}
                self.logs.append(f"schema context error: {str(e)}")
        
        return entity_mappings, schema_context

def benchmark_preprocessing():
    """Compare old vs new preprocessing performance"""
    import time
    
    test_queries = [
        "show me all customers with high sales this month",
        "what employees worked on project alpha last quarter",
        "total revenue from manufacturing items sold to corporate clients",
        "purchase orders pending approval from suppliers in asia region"
    ]
    
    test_opts = {
        "entity_groups": ["Sales", "HR", "Manufacturing", "Purchase"], 
        "debug": False,
        "business_domain": "general"
    }
    
    print("🚀 Performance Benchmark: Linear vs Original")
    print("=" * 50)
    
    for query in test_queries:
        print(f"\nQuery: '{query[:50]}...'")
        
        # Test linear version
        start_time = time.time()
        pipeline = PipeLine()
        result = pipeline.process(query, test_opts)
        linear_time = time.time() - start_time
        
        print(f"Linear processing: {linear_time*1000:.2f}ms")
        print(f"Entities found: {result['entities_found']}")
        print(f"Candidates processed: {result['candidates_processed']}")
    
    return "Benchmark completed"