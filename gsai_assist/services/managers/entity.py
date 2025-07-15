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
            # os.chmod(persist_path, 0o777)

        # Persistent Chroma client so data survives restarts
        self.client = PersistentClient(path=persist_path, settings=Settings(anonymized_telemetry=False))
        self.embedder = SentenceTransformer(model_name)

    def _get_collection(self):
        return self.client.get_or_create_collection("all_entities")

    def sync_entity(self, entity:dict, old_entity:dict=None):
        collection = self._get_collection()
        canon = entity["canonical_name"]

        aliases = [a.strip() for a in entity["aliases"].split(",") if a.strip()]
        aliases.append(canon.lower())

        # Delete all existing entries for this entity
        stale = collection.get(where={"canonical": canon})
        if stale["ids"]:
            collection.delete(ids=stale["ids"])

        if aliases:
            docs = []
            ids = []
            metadatas = []

            for group in entity["groups"]:
                group = group["entity_group"]

                for alias in aliases:
                    docs.append(alias)
                    ids.append(f"{group}::{canon}::{alias}")
                    metadatas.append({
                        "canonical": canon,
                        "alias": alias,
                        "entity_group": group,
                        "doc_type": entity["doc_type"],
                        "related_doctypes": ",".join(entity.get("related_doctypes") or [])
                    })

            embeddings = self.embedder.encode(docs).tolist()
            collection.add(
                embeddings=embeddings,
                documents=docs,
                metadatas=metadatas,
                ids=ids
            )

    def delete_entity(self, entity:dict):
        coll = self._get_collection()
        stale = coll.get(where={"canonical": entity["canonical_name"]})
        if stale["ids"]:
            coll.delete(ids=stale["ids"])
