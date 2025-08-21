import os
import json

# Update this path to point to your doctype folder
doctype_folder = r"\\wsl.localhost\Ubuntu-22.04\home\frappe\frappe-bench\apps\tap_lms\tap_lms\tap_lms\doctype"

# Dictionary to store mapping: source_doctype: { fieldname: target_doctype }
link_mapping = {}

for root, dirs, files in os.walk(doctype_folder):
    for file in files:
        if file.endswith(".json"):
            file_path = os.path.join(root, file)
            try:
                with open(file_path, "r") as f:
                    data = json.load(f)
                # Verify it's a DocType definition
                if data.get("doctype") != "DocType":
                    continue
                source_doctype = data.get("name")
                fields = data.get("fields", [])
                for field in fields:
                    if field.get("fieldtype") == "Link":
                        target = field.get("options")
                        if source_doctype not in link_mapping:
                            link_mapping[source_doctype] = {}
                        # Note the field that creates the link and the target DocType
                        link_mapping[source_doctype][field.get("fieldname")] = target
            except Exception as e:
                print(f"Error processing {file_path}: {e}")

# Print out the link mapping
for source, links in link_mapping.items():
    if links:
        print(f"DocType '{source}' has Link fields:")
        for fieldname, target in links.items():
            print(f"  - Field '{fieldname}' links to DocType '{target}'")
    else:
        print(f"DocType '{source}' has no Link fields.")