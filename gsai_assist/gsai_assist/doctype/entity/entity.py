# gsai_assist/gsai_assist/doctype/entity/entity.py

# Copyright (c) 2025, GWS and contributors
# For license information, please see license.txt

import re
import frappe
from gsai_assist.services.managers.entity import EntityManager
from frappe.model.document import Document

class Entity(Document):
    def validate(self):
        """Validate entity data before saving"""
        # Ensure canonical_name is provided
        if not self.canonical_name:
            frappe.throw("Canonical Name is required")

        # Clean up canonical name
        self.canonical_name = self.canonical_name.strip()

        # Validate aliases
        if self.aliases:
            # Clean up aliases - remove empty entries
            aliases = [alias.strip() for alias in self.aliases.split(",") if alias.strip()]
            self.aliases = ", ".join(aliases)

        # Ensure at least one group is selected
        if not self.groups:
            frappe.throw("At least one Entity Group must be selected")

        # Validate related_doctypes if provided
        if self.get("related_doctypes"):
            try:
                # Ensure it's a valid list
                if isinstance(self.related_doctypes, str):
                    import json
                    related_list = json.loads(self.related_doctypes)
                    if not isinstance(related_list, list):
                        frappe.throw("Related DocTypes must be a list")
            except (json.JSONDecodeError, TypeError):
                frappe.throw("Invalid Related DocTypes format")

    def on_update(self):
        """Sync entity to vector store on update"""
        try:
            # Get previous version for comparison
            before = self.get_doc_before_save()
            before_doc = before.as_dict() if before else None

            # Prepare current entity data
            current_entity = self.as_dict()

            # Ensure groups is properly formatted
            if current_entity.get("groups"):
                # Extract group names from child table
                group_names = []
                for group_row in current_entity["groups"]:
                    if isinstance(group_row, dict) and group_row.get("entity_group"):
                        group_names.append(group_row["entity_group"])
                current_entity["group_names"] = group_names

            # Initialize EntityManager and sync
            manager = EntityManager()
            manager.sync_entity(current_entity, old_entity=before_doc)

            # Update last_indexed timestamp
            self.db_set("last_indexed", frappe.utils.now(), notify=False)

        except Exception as e:
            frappe.log_error(f"Entity sync error for {self.canonical_name}: {str(e)}")

            # Show user-friendly error message
            error_msg = str(e)
            if "MetadataValue" in error_msg:
                error_msg = "Vector store metadata error. Please check that all fields have valid values."
            elif "embedding" in error_msg.lower():
                error_msg = "Text embedding error. Please check your aliases and canonical name."

            frappe.msgprint(
                f"Entity saved but vector sync failed: {error_msg}",
                title="Vector Sync Warning",
                indicator="orange"
            )

    def on_trash(self):
        """Remove entity from vector store on deletion"""
        try:
            current_entity = self.as_dict()
            manager = EntityManager()
            manager.delete_entity(current_entity)

        except Exception as e:
            frappe.log_error(f"Entity deletion error for {self.canonical_name}: {str(e)}")
            # Don't block deletion for vector store errors
            frappe.msgprint(
                f"Entity deleted but vector cleanup failed: {str(e)}",
                title="Vector Cleanup Warning",
                indicator="orange"
            )

    def test_vector_sync(self):
        """Test method to manually sync this entity to vector store"""
        try:
            manager = EntityManager()
            current_entity = self.as_dict()

            # Test the sync
            manager.sync_entity(current_entity)

            return {
                "success": True,
                "message": f"Successfully synced '{self.canonical_name}' to vector store"
            }

        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to sync '{self.canonical_name}': {str(e)}"
            }

# Utility functions for Entity management

@frappe.whitelist()
def test_entity_sync(entity_name):
    """Test entity sync from client side"""
    try:
        entity_doc = frappe.get_doc("Entity", entity_name)
        result = entity_doc.test_vector_sync()
        return result

    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@frappe.whitelist()
def bulk_sync_entities():
    """Bulk sync all entities to vector store"""
    try:
        entities = frappe.get_all("Entity", fields=["name"])
        results = {
            "total": len(entities),
            "successful": 0,
            "failed": 0,
            "errors": []
        }

        manager = EntityManager()

        for entity_meta in entities:
            try:
                entity_doc = frappe.get_doc("Entity", entity_meta.name)
                entity_dict = entity_doc.as_dict()

                manager.sync_entity(entity_dict)
                results["successful"] += 1

            except Exception as e:
                results["failed"] += 1
                results["errors"].append({
                    "entity": entity_meta.name,
                    "error": str(e)
                })

        return results

    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@frappe.whitelist()
def get_vector_store_stats():
    """Get vector store statistics"""
    try:
        manager = EntityManager()
        stats = manager.get_collection_stats()

        # Add ERPNext entity counts
        entity_count = frappe.db.count("Entity")
        active_entity_count = frappe.db.count("Entity", {"is_active": 1})

        stats.update({
            "erp_entity_count": entity_count,
            "erp_active_count": active_entity_count,
            "sync_ratio": stats.get("total_count", 0) / max(active_entity_count, 1)
        })

        return stats

    except Exception as e:
        return {"error": str(e)}

@frappe.whitelist()
def fix_entity_metadata_issues():
    """Fix common entity metadata issues"""
    try:
        # Find entities with potential issues
        entities_with_issues = frappe.db.sql("""
            SELECT name, canonical_name, aliases, doc_type
            FROM `tabEntity`
            WHERE is_active = 1
            AND (
                canonical_name IS NULL
                OR canonical_name = ''
                OR aliases IS NULL
            )
        """, as_dict=True)

        fixed_count = 0

        for entity_data in entities_with_issues:
            try:
                entity_doc = frappe.get_doc("Entity", entity_data.name)

                # Fix canonical_name if empty
                if not entity_doc.canonical_name:
                    entity_doc.canonical_name = entity_data.name

                # Fix aliases if empty
                if not entity_doc.aliases:
                    entity_doc.aliases = entity_doc.canonical_name.lower()

                # Save without triggering sync
                entity_doc.save(ignore_permissions=True)
                fixed_count += 1

            except Exception as e:
                frappe.log_error(f"Error fixing entity {entity_data.name}: {e}")

        return {
            "success": True,
            "total_issues": len(entities_with_issues),
            "fixed_count": fixed_count
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }
