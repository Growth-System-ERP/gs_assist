# app/services/entity_matcher.py

import frappe

from .preprocessing import PipeLine

from gsai_assist.services.managers.entity import EntityManager

@frappe.whitelist()
def match_query(query, include_groups, debug=False):
    if isinstance(include_groups, str):
        include_groups = json.loads(include_groups)

    return PipeLine().process(query=query, opts={"entity_groups": include_groups, "debug": debug})

@frappe.whitelist()
def inspect():
    coll = EntityManager()._get_collection()
    recs = coll.get(include=["uris", "documents", "metadatas"])

    for _id, alias, meta in zip(recs["uris"], recs["documents"], recs["metadatas"]):
        print(f"ID: {_id}, al: {alias}", meta["entity_group"])