import frappe

@frappe.whitelist()
def get_doctypes_with_multiple_links(entity_list):
    """
    Get all doctypes that have links to two or more doctypes from the entity_list.
    Uses an integrated query approach for better efficiency.
    
    Args:
        entity_list (list): List of doctypes to check relations against
        
    Returns:
        list: List of doctypes that have links to two or more doctypes from entity_list
    """
    # Convert entity_list to tuple for SQL query
    entity_tuple = tuple(entity_list)
    
    doctype_links = {}
    ignore_modules = ["Core", "Custom", "Desk", "Workflow", "Print"]

    # 1. Direct Links: Find all direct link fields pointing to our entities
    direct_links_query = f"""
        SELECT df.parent, df.options
        FROM `tabDocField` df
        JOIN `tabDocType` dt ON df.parent = dt.name
        WHERE df.fieldtype = 'Link'
        AND df.options IN %s
        AND dt.istable = 0
        AND dt.issingle = 0
        AND dt.module not in %s
    """
    direct_links = frappe.db.sql(direct_links_query, (entity_tuple, ignore_modules), as_dict=1)

    # Process direct links
    for link in direct_links:
        parent = link.parent
        doctype_links.setdefault(parent, set()).add(link.options)
    
    # 2. Child Table Links: Find all child tables and their link fields to our entities
    # This is a complex query that:
    # a) Finds all Table fields and their parent DocTypes
    # b) Joins with DocFields in those child tables that link to our entities
    child_table_links_query = """
        SELECT 
            parent_df.parent as parent_doctype,
            child_df.options as linked_entity
        FROM 
            `tabDocField` parent_df
        JOIN 
            `tabDocType` parent_dt ON parent_df.parent = parent_dt.name
        JOIN 
            `tabDocField` child_df ON parent_df.options = child_df.parent
        WHERE 
            parent_df.fieldtype IN ('Table', 'Table MultiSelect')
            AND child_df.fieldtype = 'Link'
            AND child_df.options IN %s
            AND parent_dt.istable = 0
            AND parent_dt.issingle = 0
            AND parent_dt.module not in %s
    """
    child_links = frappe.db.sql(child_table_links_query, (entity_tuple, ignore_modules), as_dict=1)
    
    # Process child table links
    for link in child_links:
        parent = link.parent_doctype
        doctype_links.setdefault(parent, set()).add(link.linked_entity)
    
    # 3. Dynamic Links - This is more complex and requires checking actual data
    # First, find all Dynamic Link fields and their link_doctype fields
    dynamic_links_query = """
        SELECT
            df_dyn.parent,
            df_dyn.fieldname as link_field,
            df_dyn.options as doctype_field
        FROM
            `tabDocField` df_dyn
        JOIN
            `tabDocType` dt ON df_dyn.parent = dt.name
        WHERE
            df_dyn.fieldtype = 'Dynamic Link'
            AND dt.module not in %s
            AND dt.issingle = 0
            AND dt.istable = 0
            AND EXISTS (
                SELECT 1
                FROM `tabDocField` df_link
                WHERE df_link.parent = df_dyn.parent
                AND df_link.fieldname = df_dyn.options
            )
    """
    dynamic_links = frappe.db.sql(dynamic_links_query, (ignore_modules, ), as_dict=1)
    
    # For each dynamic link field, check if it links to our entities
    for dyn_link in dynamic_links:
        parent = dyn_link.parent
            
        has_links_query = f"""
            SELECT DISTINCT `{dyn_link.doctype_field}` as entity_type
            FROM `tab{parent}`
            WHERE `{dyn_link.doctype_field}` IN %s
        """
        try:
            entity_links = frappe.db.sql(has_links_query, (entity_list, ignore_modules), as_dict=1)
            for link in entity_links:
                doctype_links.setdefault(parent, set()).add(link.entity_type)
        except:
            continue
    
    # 4. Child Table Dynamic Links
    # First find all child tables with dynamic link fields
    child_dynamic_query = """
        SELECT
            parent_df.parent as parent_doctype,
            parent_df.options as child_table,
            child_df.fieldname as link_field,
            child_df.options as doctype_field
        FROM 
            `tabDocField` parent_df
        JOIN 
            `tabDocType` parent_dt ON parent_df.parent = parent_dt.name
        JOIN 
            `tabDocField` child_df ON parent_df.options = child_df.parent
        WHERE 
            parent_df.fieldtype IN ('Table', 'Table MultiSelect')
            AND child_df.fieldtype = 'Dynamic Link'
            AND parent_dt.istable = 0
            AND parent_dt.issingle = 0
            AND parent_dt.module not in %s
            AND EXISTS (
                SELECT 1 
                FROM `tabDocField` df_link
                WHERE df_link.parent = child_df.parent
                AND df_link.fieldname = child_df.options
            )
    """
    child_dynamic_links = frappe.db.sql(child_dynamic_query, (ignore_modules, ), as_dict=1)
    
    # For each child table with dynamic link, check actual data
    for dyn_link in child_dynamic_links:
        parent = dyn_link.parent_doctype
        child_table = dyn_link.child_table

        # Check actual records for links to our entities
        has_links_query = f"""
            SELECT DISTINCT `{dyn_link.doctype_field}` as entity_type
            FROM `tab{child_table}`
            WHERE `{dyn_link.doctype_field}` IN ('{"', '".join(entity_list)}')
        """
        try:
            entity_links = frappe.db.sql(has_links_query, as_dict=1)
            for link in entity_links:
                doctype_links.setdefault(parent, set()).add(link.entity_type)
        except:
            continue

    result = []
    for doctype, linked_entities in doctype_links.items():
        if len(linked_entities) >= 2:
            result.append({
                "doctype": doctype,
                "linked_entities": list(linked_entities)
            })
    
    return result