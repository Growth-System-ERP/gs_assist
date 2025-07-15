# Copyright (c) 2025, GWS and contributors
# For license information, please see license.txt


import re
import frappe

from gsai_assist.services.managers.entity import EntityManager
from frappe.model.document import Document

class Entity(Document):
	def on_update(self):
		before = self.get_doc_before_save()
		before_doc = before and before.as_dict() or None

		EntityManager().sync_entity(self.as_dict(), old_entity=before_doc)

	def on_trash(self):
		EntityManager().delete_entity(self.as_dict())