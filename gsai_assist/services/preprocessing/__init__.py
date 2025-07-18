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

    def entatise(self, entity):
        self.entity = entity
        self.canonical = entity

class PipeLine:
    def process(self, query, opts):
        self.query = query
        self.opts = opts
        self.logs = []

        self.reset()
        self.preprocess()

        return self.build_result()

    def reset(self):
        self.context = {'dt': set(), 'rdt': set()}
        self.entity_mappings = []
        self.candidates = []

    def preprocess(self):
        self.logs.append(f"starting preprocessing: {now()}")
        
        self._basic_clean()
        self._replace_stopwords_with_positions()
        self._extract_meaningful_words()
        self._expand_with_business_terms()
        self._generate_candidates_with_expansions()
        self._entity_mapping()
        self._build_schema_context()

    def _basic_clean(self):
        """Clean the query"""
        self.original_query = self.query
        self.lq = self.query.lower()
        self.lq = re.sub(r'[^\w\s\'-]', ' ', self.lq)
        self.lq = re.sub(r'\s+', ' ', self.lq.strip())
        self.logs.append(f"cleaned query: '{self.lq}'")

    def _replace_stopwords_with_positions(self):
        """Replace stop words and track chunk positions"""
        words = self.lq.split()
        chunks = []
        current_chunk = []
        current_pos = 0
        
        keep_question_words = {'who', 'what', 'where', 'when', 'why', 'how', 'which'}
        
        for i, word in enumerate(words):
            word_start = self.lq.find(word, current_pos)
            word_end = word_start + len(word)
            current_pos = word_end
            
            should_keep = False
            if word in keep_question_words and len(chunks) == 0 and len(current_chunk) == 0:
                should_keep = True
            elif word not in STOP_WORDS:
                should_keep = True
            
            if should_keep:
                current_chunk.append({
                    'word': word,
                    'start': word_start,
                    'end': word_end
                })
            else:
                if current_chunk:
                    chunk_start = current_chunk[0]['start']
                    chunk_end = current_chunk[-1]['end']
                    chunk_text = self.lq[chunk_start:chunk_end]
                    
                    chunks.append({
                        'text': chunk_text,
                        'start': chunk_start,
                        'end': chunk_end,
                        'words': current_chunk[:]
                    })
                    current_chunk = []
        
        if current_chunk:
            chunk_start = current_chunk[0]['start']
            chunk_end = current_chunk[-1]['end']
            chunk_text = self.lq[chunk_start:chunk_end]
            
            chunks.append({
                'text': chunk_text,
                'start': chunk_start,
                'end': chunk_end,
                'words': current_chunk
            })
        
        self.chunks = chunks
        self.logs.append(f"created {len(chunks)} chunks: {[c['text'] for c in chunks]}")

    def _extract_meaningful_words(self):
        """Extract meaningful words from all chunks"""
        meaningful_words = []
        
        for chunk in self.chunks:
            for word_info in chunk['words']:
                word = word_info['word']
                if self._is_meaningful_word(word):
                    meaningful_words.append(word)
        
        self.meaningful_words = list(set(meaningful_words))  # Remove duplicates
        self.logs.append(f"extracted {len(self.meaningful_words)} meaningful words: {self.meaningful_words}")

    def _expand_with_business_terms(self):
        """Use business expander to get expanded terms"""
        from gsai_assist.services.preprocessing.business_vocabulary import BusinessVocabularyExpander
        
        expander = BusinessVocabularyExpander()
        
        # Get business domain (could be passed in opts)
        business_domain = self.opts.get('business_domain', 'general')
        
        # Expand all meaningful words
        expanded_terms = {}
        for word in self.meaningful_words:
            expansions = expander.expand_word(word, business_domain)
            if len(expansions) > 1:  # Has expansions beyond original word
                expanded_terms[word] = expansions[1:]  # Exclude original word
        
        self.expanded_terms = expanded_terms
        self.logs.append(f"expanded {len(expanded_terms)} words with business terms")
        
        if self.opts.get("debug") and expanded_terms:
            for word, expansions in expanded_terms.items():
                self.logs.append(f"'{word}' expanded to: {expansions}")

    def _generate_candidates_with_expansions(self):
        """Generate candidates including original phrases + expanded terms"""
        candidates = []
        
        # Priority 1: Multi-word chunks
        for chunk in self.chunks:
            if len(chunk['words']) > 1:
                candidates.append(EntityCandidate(
                    text=chunk['text'],
                    start=chunk['start'],
                    end=chunk['end'],
                    candidate_type='chunk',
                    priority=1
                ))
        
        # Priority 2: Sub-phrases from chunks
        for chunk in self.chunks:
            chunk_words = chunk['words']
            if len(chunk_words) >= 2:
                # 2-word combinations
                for i in range(len(chunk_words) - 1):
                    word1 = chunk_words[i]
                    word2 = chunk_words[i + 1]
                    phrase_start = word1['start']
                    phrase_end = word2['end']
                    phrase_text = self.lq[phrase_start:phrase_end]
                    
                    candidates.append(EntityCandidate(
                        text=phrase_text,
                        start=phrase_start,
                        end=phrase_end,
                        candidate_type='sub_phrase',
                        priority=2
                    ))
        
        # Priority 3: Original meaningful words
        for chunk in self.chunks:
            for word_info in chunk['words']:
                if self._is_meaningful_word(word_info['word']):
                    candidates.append(EntityCandidate(
                        text=word_info['word'],
                        start=word_info['start'],
                        end=word_info['end'],
                        candidate_type='word',
                        priority=3
                    ))
        
        # Priority 4: Expanded business terms
        for original_word, expansions in self.expanded_terms.items():
            # Find position of original word to use for expansions
            word_position = None
            for chunk in self.chunks:
                for word_info in chunk['words']:
                    if word_info['word'] == original_word:
                        word_position = (word_info['start'], word_info['end'])
                        break
                if word_position:
                    break
            
            if word_position:
                start, end = word_position
                for expanded_term in expansions:
                    candidates.append(EntityCandidate(
                        text=expanded_term,
                        start=start,  # Same position as original word
                        end=end,      # Same position as original word
                        candidate_type='expanded_term',
                        priority=4
                    ))
        
        # Filter: Keep phrases + all individual terms
        filtered_candidates = []
        phrase_ranges = []
        
        # Add phrases first
        for candidate in candidates:
            if candidate.candidate_type in ['chunk', 'sub_phrase']:
                overlaps = False
                for used_start, used_end in phrase_ranges:
                    if not (candidate.end <= used_start or candidate.start >= used_end):
                        overlaps = True
                        break
                
                if not overlaps:
                    filtered_candidates.append(candidate)
                    phrase_ranges.append((candidate.start, candidate.end))
        
        # Add all individual words and expanded terms
        for candidate in candidates:
            if candidate.candidate_type in ['word', 'expanded_term']:
                filtered_candidates.append(candidate)
        
        self.candidates = filtered_candidates
        self.logs.append(f"generated {len(self.candidates)} total candidates")

    def _entity_mapping(self):
        """Map candidates to entities"""
        self.logs.append(f"mapping entities: {now()}")
        
        if not self.candidates:
            return
        
        # Use existing entity mapper
        mapped_entities, context = map_entity(
            self.candidates, 
            self.opts.get("entity_groups"), 
            debug=self.opts.get("debug")
        )
        
        # Update context
        self.context.update(context)
        
        # Build entity mappings
        for candidate, mapped in zip(self.candidates, mapped_entities):
            if hasattr(mapped, 'canonical') and mapped.canonical:
                self.entity_mappings.append({
                    'text': candidate.text,
                    'start': candidate.start,
                    'end': candidate.end,
                    'entity': mapped.canonical,
                    'candidate_type': candidate.candidate_type,
                    'priority': candidate.priority
                })
        
        self.logs.append(f"found {len(self.entity_mappings)} entity mappings")

    def _build_schema_context(self):
        """Use schema_mapper to build relationships and context"""
        self.logs.append(f"building schema context: {now()}")
        
        # Get unique doctypes from entity mappings
        found_doctypes = list(set([em['entity'] for em in self.entity_mappings]))
        
        if not found_doctypes:
            self.schema_context = {}
            return
        
        try:
            # Use your existing schema_mapper
            from gsai_assist.services.preprocessing.schema_mapper import get_doctypes_with_multiple_links
            
            related_doctypes = get_doctypes_with_multiple_links(found_doctypes)
            
            # Build clean schema context
            self.schema_context = {
                'found_doctypes': found_doctypes,
                'related_doctypes': [
                    {
                        'doctype': rel['doctype'],
                        'linked_entities': rel['linked_entities'],
                        'connection_strength': len(rel['linked_entities'])
                    }
                    for rel in related_doctypes[:10]  # Top 10
                ],
                'entity_count': len(found_doctypes),
                'related_count': len(related_doctypes)
            }
            
            self.logs.append(f"found {len(related_doctypes)} related doctypes")
            
        except Exception as e:
            frappe.log_error(f"Schema context building failed: {str(e)}")
            self.schema_context = {'found_doctypes': found_doctypes}

    def build_result(self):
        """Build final clean result"""
        return {
            'original_query': self.original_query,
            'meaningful_words': getattr(self, 'meaningful_words', []),
            'expanded_terms': getattr(self, 'expanded_terms', {}),
            'entity_mappings': self.entity_mappings,
            'context': {
                'dt': list(self.context.get('dt', set())),
                'rdt': list(self.context.get('rdt', set()))
            },
            'schema_context': getattr(self, 'schema_context', {}),
            'candidates_processed': len(self.candidates),
            'entities_found': len(self.entity_mappings),
            'logs': self.logs
        }

    def _is_meaningful_word(self, word: str) -> bool:
        """Check if a word is meaningful for entity matching"""
        if len(word) < 3:
            return False

        return True

# Test function
def test_clean_pipeline():
    """Test the clean pipeline"""
    pipeline = CleanPipeLine()
    
    test_query = "most sold product in orders?"
    test_opts = {
        "entity_groups": ["Sales", "Inventory", "Finance"], 
        "debug": True,
        "business_domain": "general"
    }
    
    result = pipeline.process(test_query, test_opts)
    
    return {
        "test_query": test_query,
        "result": result,
        "flow_summary": {
            "meaningful_words": result.get('meaningful_words'),
            "expanded_terms": result.get('expanded_terms'),
            "entities_found": [em['entity'] for em in result.get('entity_mappings', [])],
            "related_doctypes": [rd['doctype'] for rd in result.get('schema_context', {}).get('related_doctypes', [])]
        }
    }