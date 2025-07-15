import os
import frappe

def before_llmware():
	llmware_path = os.path.join(frappe.get_site_path(), "llmware")

	if not os.path.exists(llmware_path):
		os.makedirs(llmware_path)

	if not os.environ.get("HOME"):
		os.environ["HOME"] = llmware_path

	if not os.environ.get("HOME"):
		os.environ["USERPROFILE"] = os.environ["HOME"]



	from llmware.configs import LLMWareConfig

	LLMWareConfig().set_vector_db("milvus")
