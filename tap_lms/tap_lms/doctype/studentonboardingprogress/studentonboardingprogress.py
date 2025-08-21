# Copyright (c) 2023, Techt4dev and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from frappe.utils import now_datetime

class StudentOnboardingProgress(Document):
    pass

def update_student_progress(doc=None, method=None):
    """
    Update student onboarding progress when a StudentStageProgress record is created or modified
    
    Args:
        doc: The StudentStageProgress document
        method: The trigger method (after_insert, on_update, etc.)
    """
    try:
        # Skip if no document or not an OnboardingStage
        if not doc or doc.stage_type != "OnboardingStage":
            return
            
        # Get or create StudentOnboardingProgress record
        onboarding_records = frappe.get_all(
            "StudentOnboardingProgress",
            filters={"student": doc.student},
            fields=["name"]
        )
        
        if onboarding_records:
            onboarding = frappe.get_doc("StudentOnboardingProgress", onboarding_records[0].name)
        else:
            # Create new onboarding progress record
            onboarding = frappe.new_doc("StudentOnboardingProgress")
            onboarding.student = doc.student
            onboarding.status = "not_started"
            
            # Try to determine batch from student enrollments
            student = frappe.get_doc("Student", doc.student)
            if hasattr(student, 'enrollment') and student.enrollment:
                for enrollment in student.enrollment:
                    if enrollment.batch:
                        onboarding.batch = enrollment.batch
                        break
        
        # Update current stage if this is the newest progress record
        stage_progress = frappe.get_all(
            "StudentStageProgress",
            filters={"student": doc.student, "stage_type": "OnboardingStage"},
            fields=["name", "stage", "status", "start_timestamp"],
            order_by="start_timestamp desc",
            limit=1
        )
        
        if stage_progress and stage_progress[0].name == doc.name:
            onboarding.current_stage = doc.stage
            
            # Update status based on stage progress status
            if doc.status == "completed" and frappe.db.exists("OnboardingStage", doc.stage):
                stage = frappe.get_doc("OnboardingStage", doc.stage)
                if hasattr(stage, 'is_final') and stage.is_final:
                    onboarding.status = "completed"
                    if hasattr(doc, 'completion_timestamp') and doc.completion_timestamp:
                        onboarding.completion_timestamp = doc.completion_timestamp
                else:
                    onboarding.status = "in_progress"
            elif doc.status == "in_progress":
                onboarding.status = "in_progress"
        
        # Update timestamps
        if hasattr(doc, 'last_activity_timestamp') and doc.last_activity_timestamp:
            onboarding.last_activity_timestamp = doc.last_activity_timestamp
        if not onboarding.start_timestamp and hasattr(doc, 'start_timestamp') and doc.start_timestamp:
            onboarding.start_timestamp = doc.start_timestamp
        
        onboarding.save()
        
    except Exception as e:
        frappe.log_error(f"Error updating StudentOnboardingProgress: {str(e)}", 
                        "StudentOnboardingProgress Update Error")
