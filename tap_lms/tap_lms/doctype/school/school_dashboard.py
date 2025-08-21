from frappe import _

def get_data():
    return {
        "fieldname": "school",  # The field linking to this doctype
        "transactions": [
            {
                "label": _("Batch onboarding "),
                "items": ["Batch onboarding"]
            }
        ],
    }
