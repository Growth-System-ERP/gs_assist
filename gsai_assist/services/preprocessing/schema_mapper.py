import frappe

@frappe.whitelist()
def get_doctypes_with_multiple_links(entity_list):
    """
    Enhanced version: Get all doctypes with links + detailed metadata
    Now returns comprehensive DocType information in single pass
    """
    entity_tuple = tuple(entity_list)
    doctype_links = {}
    ignore_modules = ["Core", "Custom", "Desk", "Workflow", "Print"]

    # OPTIMIZATION: Batch get all DocType metadata upfront
    all_doctype_metas = {}
    
    # 1. Direct Links Query (enhanced to get more info)
    direct_links_query = f"""
        SELECT 
            df.parent, 
            df.options,
            df.fieldname,
            df.label,
            df.reqd,
            df.in_list_view,
            dt.module,
            dt.is_submittable,
            dt.istable,
            dt.issingle,
            dt.autoname,
            dt.title_field,
            dt.search_fields,
            dt.sort_field,
            dt.sort_order
        FROM `tabDocField` df
        JOIN `tabDocType` dt ON df.parent = dt.name
        WHERE df.fieldtype = 'Link'
        AND df.options IN %s
        AND dt.istable = 0
        AND dt.issingle = 0
        AND dt.module not in %s
    """
    direct_links = frappe.db.sql(direct_links_query, (entity_tuple, ignore_modules), as_dict=1)

    # Process direct links and collect DocType info
    for link in direct_links:
        parent = link.parent
        
        # Initialize with DocType metadata
        if parent not in doctype_links:
            doctype_links[parent] = {
                "entities": set(),
                "doctype_info": {
                    "name": parent,
                    "module": link.module,
                    "is_submittable": bool(link.is_submittable),
                    "is_table": bool(link.istable),
                    "is_single": bool(link.issingle),
                    "autoname": link.autoname or '',
                    "title_field": link.title_field or '',
                    "search_fields": link.search_fields or '',
                    "sort_field": link.sort_field or 'modified',
                    "sort_order": link.sort_order or 'DESC'
                },
                "link_fields": [],
                "child_doctypes": set()
            }
        
        doctype_links[parent]["entities"].add(link.options)
        doctype_links[parent]["link_fields"].append({
            "fieldname": link.fieldname,
            "label": link.label,
            "linked_doctype": link.options,
            "required": bool(link.reqd),
            "in_list_view": bool(link.in_list_view)
        })
    
    # 2. Child Table Links (enhanced query)
    child_table_links_query = """
        SELECT 
            parent_df.parent as parent_doctype,
            child_df.options as linked_entity,
            child_df.parent as child_doctype,
            child_df.fieldname as child_fieldname,
            child_df.label as child_label,
            parent_df.fieldname as parent_fieldname,
            parent_df.label as parent_label,
            parent_dt.module,
            parent_dt.is_submittable,
            parent_dt.autoname,
            parent_dt.title_field
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
        
        # Initialize if not exists
        if parent not in doctype_links:
            doctype_links[parent] = {
                "entities": set(),
                "doctype_info": {
                    "name": parent,
                    "module": link.module,
                    "is_submittable": bool(link.is_submittable),
                    "autoname": link.autoname or '',
                    "title_field": link.title_field or ''
                },
                "link_fields": [],
                "child_doctypes": set()
            }
        
        doctype_links[parent]["entities"].add(link.linked_entity)
        doctype_links[parent]["entities"].add(link.child_doctype)
        doctype_links[parent]["child_doctypes"].add(link.child_doctype)
    
    # 3. Dynamic Links - Original logic preserved with enhancements
    dynamic_links_query = """
        SELECT
            df_dyn.parent,
            df_dyn.fieldname as link_field,
            df_dyn.options as doctype_field,
            dt.module,
            dt.is_submittable,
            dt.autoname
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
    dynamic_links = frappe.db.sql(dynamic_links_query, (ignore_modules,), as_dict=1)
    
    # For each dynamic link field, check if it links to our entities
    for dyn_link in dynamic_links:
        parent = dyn_link.parent
            
        has_links_query = f"""
            SELECT DISTINCT `{dyn_link.doctype_field}` as entity_type
            FROM `tab{parent}`
            WHERE `{dyn_link.doctype_field}` IN %s
        """
        try:
            entity_links = frappe.db.sql(has_links_query, (entity_tuple,), as_dict=1)
            
            if entity_links:
                if parent not in doctype_links:
                    doctype_links[parent] = {
                        "entities": set(),
                        "doctype_info": {
                            "name": parent,
                            "module": dyn_link.module,
                            "is_submittable": bool(dyn_link.is_submittable),
                            "autoname": dyn_link.autoname or ''
                        },
                        "link_fields": [],
                        "child_doctypes": set()
                    }
                
                for link in entity_links:
                    doctype_links[parent]["entities"].add(link.entity_type)
                    
        except:
            continue

    # 4. Child Table Dynamic Links - Original logic preserved
    child_dynamic_query = """
        SELECT
            parent_df.parent as parent_doctype,
            parent_df.options as child_table,
            child_df.fieldname as link_field,
            child_df.options as doctype_field,
            child_df.parent as child_doctype,
            parent_dt.module,
            parent_dt.is_submittable,
            parent_dt.autoname
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
    child_dynamic_links = frappe.db.sql(child_dynamic_query, (ignore_modules,), as_dict=1)
    
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
            
            if entity_links:
                if parent not in doctype_links:
                    doctype_links[parent] = {
                        "entities": set(),
                        "doctype_info": {
                            "name": parent,
                            "module": dyn_link.module,
                            "is_submittable": bool(dyn_link.is_submittable),
                            "autoname": dyn_link.autoname or ''
                        },
                        "link_fields": [],
                        "child_doctypes": set()
                    }
                
                for link in entity_links:
                    doctype_links[parent]["entities"].add(link.entity_type)
                    doctype_links[parent]["entities"].add(dyn_link.child_doctype)
                    doctype_links[parent]["child_doctypes"].add(dyn_link.child_doctype)
        except:
            continue

    # 4. ENHANCED: Get practical field information in batch
    all_parents = list(doctype_links.keys())
    if all_parents:
        # Get all important fields with practical flags
        fields_query = f"""
            SELECT 
                parent,
                fieldname,
                label,
                fieldtype,
                options,
                reqd,
                in_list_view,
                in_standard_filter,
                search_index,
                read_only,
                hidden
            FROM `tabDocField`
            WHERE parent IN ({','.join(['%s'] * len(all_parents))})
            AND (
                reqd = 1 OR 
                in_list_view = 1 OR 
                in_standard_filter = 1 OR
                fieldname IN ('name', 'title', 'status', 'customer', 'supplier', 'item_code') OR
                fieldname like '%date%' OR
                fieldname like '%total%'
            )
            AND hidden = 0
            AND fieldtype NOT IN ('Section Break', 'Column Break', 'HTML', 'Heading')
            ORDER BY parent, reqd DESC, in_list_view DESC, in_standard_filter DESC
        """
        
        all_fields = frappe.db.sql(fields_query, all_parents, as_dict=1)
        
        for field in all_fields:            
            field_info = {
                'fieldname': field.fieldname,
                'label': field.label,
                'fieldtype': field.fieldtype,
                'reqd': bool(field.reqd),
                'in_list_view': bool(field.in_list_view),
                'in_standard_filter': bool(field.in_standard_filter),
                'search_index': bool(field.search_index),
                'read_only': bool(field.read_only)
            }
            
            # Add field type specific info
            if field.fieldtype == 'Link':
                field_info['linked_doctype'] = field.options
            elif field.fieldtype in ['Select', 'Table MultiSelect']:
                field_info['options'] = field.options.split('\n') if field.options else []
            elif field.fieldtype in ['Table']:
                field_info['child_table'] = field.options
            
            doctype_links.setdefault(field.parent, {}).setdefault("fields", []).append(field_info)

    # 5. Build enhanced result
    result = []
    for doctype, details in doctype_links.items():
        linked_entities = details["entities"]
        
        if len(linked_entities) >= 2:
            enhanced_doctype = {
                "doctype": doctype,
                "linked_entities": list(linked_entities),
                "child_doctypes": list(details["child_doctypes"]),
                "connection_strength": len(linked_entities),
                
                # Enhanced metadata
                "metadata": details["doctype_info"],
                "link_fields": details.get("link_fields", []),
                "fields": details.get("fields", {}),
            }
            result.append(enhanced_doctype)
    
    # Sort by connection strength (most connected first)
    result.sort(key=lambda x: x["connection_strength"], reverse=True)
    
    return result