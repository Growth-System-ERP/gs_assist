# gsai_assist/services/business_vocabulary.py

class BusinessVocabularyExpander:
    """
    Comprehensive business vocabulary expansion
    Pre-built with extensive business terms
    """
    
    def __init__(self):
        # Comprehensive business term mappings
        self.business_expansions = {
            
            # SALES DOMAIN
            'sold': ['sales', 'sales order', 'sales invoice', 'revenue', 'selling'],
            'selling': ['sales', 'sales order', 'sales invoice'],
            'sale': ['sales', 'sales order', 'sales invoice'],
            'revenue': ['sales', 'sales invoice', 'income'],
            'income': ['sales', 'sales invoice', 'revenue'],
            'earnings': ['sales', 'sales invoice', 'revenue'],
            'turnover': ['sales', 'sales invoice', 'revenue'],
            'billed': ['sales invoice', 'invoice', 'billing'],
            'invoiced': ['sales invoice', 'invoice', 'billing'],
            'billing': ['sales invoice', 'invoice'],
            'quoted': ['quotation', 'quote', 'sales quote'],
            'ordered': ['sales order', 'order', 'purchase order'],
            
            # PURCHASE DOMAIN  
            'bought': ['purchase', 'purchase order', 'purchase invoice', 'procurement'],
            'purchasing': ['purchase', 'purchase order', 'purchase invoice'],
            'procurement': ['purchase', 'purchase order', 'supplier'],
            'sourcing': ['purchase', 'supplier', 'procurement'],
            'acquired': ['purchase', 'purchase order', 'asset'],
            'vendor': ['supplier', 'vendor master'],
            'outsourced': ['supplier', 'purchase order'],
            
            # PAYMENT & FINANCE
            'paid': ['payment', 'payment entry', 'receipt'],
            'payment': ['payment entry', 'receipt', 'transaction'],
            'receipt': ['payment entry', 'sales invoice', 'receipt'],
            'transaction': ['payment entry', 'journal entry'],
            'expense': ['expense claim', 'purchase invoice', 'journal entry'],
            'cost': ['expense', 'purchase invoice', 'cost center'],
            'spent': ['expense', 'payment', 'purchase invoice'],
            'charged': ['expense', 'sales invoice', 'fee'],
            'refunded': ['payment entry', 'credit note', 'return'],
            'credited': ['credit note', 'payment entry', 'journal entry'],
            'debited': ['journal entry', 'payment entry', 'invoice'],
            
            # INVENTORY & STOCK
            'stocked': ['stock', 'stock entry', 'inventory'],
            'inventory': ['stock', 'stock entry', 'item'],
            'stored': ['stock', 'warehouse', 'stock entry'],
            'warehouse': ['stock', 'stock entry', 'location'],
            'shipped': ['delivery', 'delivery note', 'shipment'],
            'delivered': ['delivery', 'delivery note', 'fulfillment'],
            'dispatched': ['delivery', 'delivery note', 'shipment'],
            'received': ['purchase receipt', 'stock entry', 'delivery'],
            'transferred': ['stock entry', 'material transfer', 'stock transfer'],
            'manufactured': ['work order', 'manufacturing', 'production'],
            'produced': ['work order', 'manufacturing', 'production'],
            'assembled': ['work order', 'bom', 'manufacturing'],
            
            # CUSTOMER RELATIONS
            'client': ['customer', 'client', 'account'],
            'account': ['customer', 'account', 'client'],
            'prospect': ['lead', 'opportunity', 'prospect'],
            'lead': ['lead', 'opportunity', 'prospect'],
            'opportunity': ['opportunity', 'lead', 'deal'],
            'deal': ['opportunity', 'sales order', 'contract'],
            'contract': ['contract', 'agreement', 'sales order'],
            
            # HR & EMPLOYEE
            'employee': ['employee', 'staff', 'personnel'],
            'staff': ['employee', 'staff', 'personnel'],
            'personnel': ['employee', 'staff', 'hr'],
            'worker': ['employee', 'staff', 'labor'],
            'contractor': ['employee', 'supplier', 'contractor'],
            'payroll': ['salary', 'payroll entry', 'employee'],
            'salary': ['salary slip', 'payroll', 'compensation'],
            'wage': ['salary slip', 'payroll', 'hourly rate'],
            'attended': ['attendance', 'employee', 'timesheet'],
            'worked': ['timesheet', 'attendance', 'employee'],
            
            # PROJECTS & TASKS
            'project': ['project', 'task', 'milestone'],
            'task': ['task', 'project', 'activity'],
            'activity': ['task', 'timesheet', 'project'],
            'milestone': ['project', 'task', 'deadline'],
            'assigned': ['task', 'project', 'employee'],
            'completed': ['task', 'project', 'status'],
            
            # ASSETS & EQUIPMENT
            'asset': ['asset', 'fixed asset', 'equipment'],
            'equipment': ['asset', 'fixed asset', 'machinery'],
            'machinery': ['asset', 'equipment', 'manufacturing'],
            'depreciated': ['asset', 'depreciation', 'fixed asset'],
            'maintained': ['asset', 'maintenance', 'equipment'],
            
            # COMMUNICATION & SUPPORT
            'communicated': ['communication', 'email', 'contact'],
            'contacted': ['communication', 'contact', 'call log'],
            'supported': ['support ticket', 'issue', 'help desk'],
            'resolved': ['support ticket', 'issue', 'task'],
            'escalated': ['issue', 'support ticket', 'escalation'],
            
            # QUALITY & COMPLIANCE
            'inspected': ['quality inspection', 'inspection', 'quality'],
            'quality': ['quality inspection', 'qc', 'standards'],
            'approved': ['approval', 'workflow', 'authorization'],
            'rejected': ['rejection', 'quality inspection', 'approval'],
            'compliant': ['compliance', 'standard', 'regulation'],
            
            # GENERAL BUSINESS ACTIONS
            'created': ['new', 'creation', 'setup'],
            'updated': ['modification', 'change', 'edit'],
            'modified': ['update', 'change', 'edit'],
            'deleted': ['removal', 'cancellation', 'void'],
            'cancelled': ['cancellation', 'void', 'deletion'],
            'submitted': ['submission', 'approval', 'finalization'],
            'drafted': ['draft', 'preparation', 'initial'],
            'processed': ['processing', 'execution', 'completion'],
            'pending': ['waiting', 'queue', 'backlog'],
            'overdue': ['late', 'delayed', 'past due'],
            
            # MEASUREMENTS & ANALYTICS
            'total': ['sum', 'aggregate', 'overall'],
            'average': ['mean', 'avg', 'typical'],
            'maximum': ['max', 'highest', 'peak'],
            'minimum': ['min', 'lowest', 'least'],
            'count': ['number', 'quantity', 'tally'],
            'percentage': ['percent', 'ratio', 'proportion'],
            'growth': ['increase', 'expansion', 'improvement'],
            'decline': ['decrease', 'reduction', 'drop'],
            
            # TIME PERIODS
            'daily': ['day', 'per day', 'everyday'],
            'weekly': ['week', 'per week', 'weekly'],
            'monthly': ['month', 'per month', 'monthly'],
            'quarterly': ['quarter', 'q1', 'q2', 'q3', 'q4'],
            'yearly': ['year', 'annual', 'per year'],
            'current': ['present', 'now', 'today'],
            'recent': ['latest', 'new', 'fresh'],
            'historical': ['past', 'previous', 'old'],
        }
        
        # Industry-specific expansions
        self.industry_expansions = {
            'manufacturing': {
                'produced': ['manufactured', 'work order', 'production order'],
                'assembled': ['work order', 'bom', 'manufacturing'],
                'quality': ['quality inspection', 'qc check', 'testing'],
            },
            'retail': {
                'sold': ['pos invoice', 'retail sale', 'transaction'],
                'returned': ['sales return', 'refund', 'exchange'],
                'discount': ['pricing rule', 'promotion', 'offer'],
            },
            'services': {
                'delivered': ['service delivery', 'project delivery', 'completion'],
                'billed': ['timesheet', 'service invoice', 'billing'],
                'consulted': ['consultation', 'advisory', 'service'],
            }
        }
    
    def expand_word(self, word, business_domain='general'):
        """
        Expand a word to include related business terms
        
        Args:
            word: The word to expand
            business_domain: Industry context (manufacturing, retail, services, etc.)
        
        Returns:
            List of related business terms
        """
        word_lower = word.lower()
        expanded_terms = [word]  # Always include original
        
        # Get general business expansions
        if word_lower in self.business_expansions:
            expanded_terms.extend(self.business_expansions[word_lower])
        
        # Get industry-specific expansions
        if business_domain in self.industry_expansions:
            industry_terms = self.industry_expansions[business_domain]
            if word_lower in industry_terms:
                expanded_terms.extend(industry_terms[word_lower])
        
        # Remove duplicates while preserving order
        return list(dict.fromkeys(expanded_terms))
    
    def expand_query_terms(self, words, business_domain='general'):
        """
        Expand all meaningful words in a query
        
        Args:
            words: List of words to expand
            business_domain: Industry context
            
        Returns:
            Dict mapping original words to expanded terms
        """
        expansions = {}
        
        for word in words:
            expanded = self.expand_word(word, business_domain)
            if len(expanded) > 1:  # Only include if we found expansions
                expansions[word] = expanded
        
        return expansions
    
    def get_all_business_terms(self):
        """Get all business terms for reference"""
        all_terms = set()
        
        for expansions in self.business_expansions.values():
            all_terms.update(expansions)
        
        for industry in self.industry_expansions.values():
            for expansions in industry.values():
                all_terms.update(expansions)
        
        return sorted(list(all_terms))

# Integration function
def expand_candidates_with_business_terms(candidates, business_domain='general'):
    """
    Expand candidate words with business vocabulary
    
    Args:
        candidates: List of candidate words/phrases
        business_domain: Industry context
        
    Returns:
        Enhanced list of candidates with business term expansions
    """
    expander = BusinessVocabularyExpander()
    enhanced_candidates = []
    
    # Keep original candidates
    enhanced_candidates.extend(candidates)
    
    # Add expanded terms
    for candidate in candidates:
        if isinstance(candidate, dict):
            word = candidate.get('text', '')
        else:
            word = str(candidate)
        
        # Only expand single words (not phrases)
        if len(word.split()) == 1:
            expanded_terms = expander.expand_word(word, business_domain)
            
            # Add expanded terms as new candidates
            for term in expanded_terms[1:]:  # Skip original word
                if isinstance(candidate, dict):
                    # Create new candidate dict with expanded term
                    new_candidate = candidate.copy()
                    new_candidate['text'] = term
                    new_candidate['candidate_type'] = 'expanded'
                    new_candidate['original_word'] = word
                    enhanced_candidates.append(new_candidate)
                else:
                    enhanced_candidates.append(term)
    
    return enhanced_candidates

# Test function
def test_business_vocabulary():
    """Test the business vocabulary expansion"""
    expander = BusinessVocabularyExpander()
    
    test_words = ['sold', 'bought', 'paid', 'delivered', 'employee', 'quality', 'produced']
    
    results = {}
    for word in test_words:
        results[word] = expander.expand_word(word)
    
    return {
        'expansions': results,
        'total_business_terms': len(expander.get_all_business_terms()),
        'sample_terms': expander.get_all_business_terms()[:20]
    }