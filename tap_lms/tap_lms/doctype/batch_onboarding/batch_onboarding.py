# Copyright (c) 2024, Techt4dev and contributors
# For license information, please see license.txt

# import frappe
#from frappe.model.document import Document

#class Batchonboarding(Document):
#	pass


import frappe
from frappe.model.document import Document
from tap_lms.batch_onboarding_utils import generate_unique_batch_keyword

class Batchonboarding(Document):
    def before_insert(self):
        if not self.batch_skeyword:
            self.batch_skeyword = generate_unique_batch_keyword(self)

