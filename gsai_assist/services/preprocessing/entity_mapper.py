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

    # print(res["documents"])

    context = {"dt": set(), "rdt": set()}
    found_aliases = set()

    include_groups = set(include_groups)

    for token_idx, (docs, metas, dists) in enumerate(zip(res["documents"], res["metadatas"], res["distances"])):
        token = tokens[token_idx]
        token_txt = token.text
        for alias, meta, dist in zip(docs, metas, dists):
            entity_groups = set(meta.get("entity_group", "").split(","))

            if not include_groups & entity_groups:
                continue

            if dist > max_dist:
                continue

            if alias.lower() == token_txt.lower() or fuzz.ratio(alias, token_txt) >= fuzz_thresh:
                token.entatise(meta["canonical"])

                context["dt"].add(meta["doc_type"])
                for rd in meta.get("related_doctypes", []):
                    context["rdt"].add(rd)
                break

    return tokens, context