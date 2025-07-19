from rapidfuzz import fuzz
from gsai_assist.services.managers.entity import EntityManager

def process(tokens, include_groups, max_dist=1.3, fuzz_thresh=80, debug=False):
    mgr = EntityManager()
    token_embs = mgr.embedder.encode([t.text for t in tokens])
    collection = mgr._get_collection()

    top_k = len(tokens)*2

    res = collection.query(
        query_embeddings=token_embs,
        n_results=top_k,
        include=["documents", "metadatas", "distances"]
    )

    context = {"dt": set(), "rdt": set()}
    include_groups = set(include_groups)

    for token_idx, (docs, metas, dists) in enumerate(zip(res["documents"], res["metadatas"], res["distances"])):
        token = tokens[token_idx]
        token_txt = token.text
        
        best_confidence = 0.0
        best_match = None
        
        for alias, meta, dist in zip(docs, metas, dists):
            entity_groups = set(meta.get("entity_group", "").split(","))

            if not include_groups & entity_groups:
                continue

            if dist > max_dist:
                continue

            # FIXED: Better confidence calculation for expanded terms
            if alias.lower() == token_txt.lower():
                confidence = 1.0  # Exact match
            else:
                fuzzy_score = fuzz.ratio(alias, token_txt)
                vector_sim = max(0, (2.0 - dist) / 2.0)
                
                if fuzzy_score >= fuzz_thresh:
                    # Fuzzy match - good but not perfect
                    confidence = (fuzzy_score/100.0) * 0.85 + vector_sim * 0.15
                else:
                    # Semantic match only
                    confidence = vector_sim * 0.7  # Lower max for semantic
                
                # PENALTY for expanded terms (they should rank lower)
                if hasattr(token, 'candidate_type') and token.candidate_type == 'expanded_term':
                    confidence *= 0.8  # 20% penalty for being expanded
                
                # Ensure minimum confidence for good matches
                if confidence < 0.5:
                    confidence = 0.0

            # Keep the best match for this token
            if confidence > best_confidence:
                best_confidence = confidence
                best_match = (meta["canonical"], meta)

        # Apply best match if found
        if best_match:
            canonical, meta = best_match
            token.entatise(canonical, best_confidence)
            
            context["dt"].add(meta["doc_type"])
            for rd in meta.get("related_doctypes", []):
                context["rdt"].add(rd)

    return tokens, context