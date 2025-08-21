import frappe
from frappe import _
import json
from frappe.utils import nowdate, nowtime, now
from tap_lms.glific_integration import create_or_get_glific_group_for_batch, add_student_to_glific_for_onboarding, get_contact_by_phone
from tap_lms.api import get_course_level

def normalize_phone_number(phone):
    """
    Normalize phone number to handle both 10-digit and 12-digit formats
    Returns both normalized 12-digit format and 10-digit format for comparison
    """
    if not phone:
        return None, None
    
    phone = phone.strip().replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
    
    # Remove any non-digit characters
    phone = ''.join(filter(str.isdigit, phone))
    
    if len(phone) == 10:
        # 10-digit number, add country code
        phone_12 = f"91{phone}"
        phone_10 = phone
    elif len(phone) == 12 and phone.startswith('91'):
        # 12-digit number with country code
        phone_12 = phone
        phone_10 = phone[2:] # Remove 91 prefix
    elif len(phone) == 11 and phone.startswith('1'):
        # Sometimes numbers come as 1XXXXXXXXXX, treat as 91XXXXXXXXXX
        phone_12 = f"9{phone}"
        phone_10 = phone[1:]
    else:
        # Invalid format
        return None, None
    
    return phone_12, phone_10

def find_existing_student_by_phone_and_name(phone, name):
    """
    Find existing student by phone and name, handling both 10-digit and 12-digit phone formats
    """
    if not phone or not name:
        return None
    
    phone_12, phone_10 = normalize_phone_number(phone)
    
    if not phone_12 or not phone_10:
        return None
    
    # Search for existing students with either phone format
    existing_students = frappe.db.sql("""
        SELECT name, phone, name1
        FROM `tabStudent`
        WHERE name1 = %s 
        AND (phone = %s OR phone = %s)
        LIMIT 1
    """, (name, phone_10, phone_12), as_dict=True)
    
    if existing_students:
        return existing_students[0]
    
    return None

@frappe.whitelist()
def get_onboarding_batches():
    print("get_onboarding_batches called")
    # Return all draft backend onboarding batches
    return frappe.get_all("Backend Student Onboarding", 
                         filters={"status": ["in", ["Draft", "Processing", "Failed"]]},
                         fields=["name", "set_name", "upload_date", "uploaded_by", 
                                "student_count", "processed_student_count"])

@frappe.whitelist()
def get_batch_details(batch_id):
    # Get the details of a specific batch
    batch = frappe.get_doc("Backend Student Onboarding", batch_id)
    # Only request fields that exist in the database
    students = frappe.get_all("Backend Students", 
                             filters={"parent": batch_id},
                             fields=["name", "student_name", "phone", "gender", 
                                    "batch", "course_vertical", "grade", "school",
                                    "language", "processing_status", "student_id"])
    
    # Add validation flags
    for student in students:
        student["validation"] = validate_student(student)
    
    # Get Glific group for this batch if exists
    glific_group = frappe.get_all("GlificContactGroup", 
                                 filters={"backend_onboarding_set": batch_id},
                                 fields=["group_id", "label"])
    
    return {
        "batch": batch,
        "students": students,
        "glific_group": glific_group[0] if glific_group else None
    }

def validate_student(student):
    validation = {}
    
    # Check for empty required fields
    required_fields = ["student_name", "phone", "school", "grade", "language", "batch"]
    for field in required_fields:
        if not student.get(field):
            validation[field] = "missing"
    
    # Check for duplicate phone numbers with normalized phone comparison
    if student.get("phone"):
        existing = find_existing_student_by_phone_and_name(student.get("phone"), student.get("student_name"))
        if existing:
            validation["duplicate"] = {
                "student_id": existing.name,
                "student_name": existing.name1
            }
    
    return validation

@frappe.whitelist()
def get_onboarding_stages():
    try:
        # Check if the DocType exists
        if not frappe.db.table_exists("OnboardingStage"):
            return []
        
        # Get all onboarding stages ordered by the order field
        return frappe.get_all("OnboardingStage", 
                             fields=["name", "description", "order"],
                             order_by="`order`") # Using backticks to escape the reserved keyword
    except Exception as e:
        frappe.log_error(f"Error fetching OnboardingStage: {str(e)}")
        return []

def get_initial_stage():
    """Get the initial onboarding stage (with order=0)"""
    try:
        stages = frappe.get_all("OnboardingStage", 
                               filters={"order": 0},
                               fields=["name"])
        if stages:
            return stages[0].name
        else:
            # If no stage with order 0, get the stage with minimum order
            stages = frappe.get_all("OnboardingStage", 
                                   fields=["name", "order"],
                                   order_by="order ASC",
                                   limit=1)
            if stages:
                return stages[0].name
    except Exception as e:
        frappe.log_error(f"Error getting initial stage: {str(e)}")
    
    return None

@frappe.whitelist()
def process_batch(batch_id, use_background_job=False):
    """
    Process the batch by creating students and Glific contacts
    
    Args:
        batch_id: ID of the Backend Student Onboarding document
        use_background_job: Whether to process in the background
    
    Returns:
        If background job is used, returns the job ID
        Otherwise, returns processing results
    """
    use_background_job = json.loads(use_background_job) if isinstance(use_background_job, str) else use_background_job
    
    # Update batch status to Processing
    batch = frappe.get_doc("Backend Student Onboarding", batch_id)
    batch.status = "Processing"
    batch.save()
    
    if use_background_job:
        # Enqueue the processing job
        job = frappe.enqueue(
            process_batch_job,
            queue='long',
            timeout=1800, # 30 minutes
            job_name=f"student_onboarding_{batch_id}",
            batch_id=batch_id
        )
        return {"job_id": job.id}
    else:
        # Process immediately
        return process_batch_job(batch_id)

def process_batch_job(set_id):
    """Background job function to process the batch"""
    try:
        frappe.db.commit() # Commit any pending changes before starting job
        
        batch = frappe.get_doc("Backend Student Onboarding", set_id)
        
        # Get students to process (only pending or failed)
        students = frappe.get_all("Backend Students", 
                                 filters={"parent": set_id, "processing_status": ["in", ["Pending", "Failed"]]},
                                 fields=["name","batch_skeyword"])
        
        success_count = 0
        failure_count = 0
        results = {
            "success": [],
            "failed": []
        }
        
        # Get or create Glific group for this batch
        try:
            glific_group = create_or_get_glific_group_for_batch(set_id)
        except Exception as e:
            frappe.log_error(f"Error creating Glific group: {str(e)}", "Backend Student Onboarding")
            glific_group = None
        
        # Get initial stage
        initial_stage = get_initial_stage()
        
        # Process students in batches for better performance
        total_students = len(students)
        batch_size = 50  # Process 50 students at a time
        commit_interval = 10  # Commit every 10 students
        
        for batch_start in range(0, total_students, batch_size):
            batch_end = min(batch_start + batch_size, total_students)
            batch_students = students[batch_start:batch_end]
            
            # Pre-fetch batch onboarding data for this batch
            batch_keywords = list(set([
                s.get('batch_skeyword') for s in batch_students 
                if hasattr(s, 'batch_skeyword') and s.batch_skeyword
            ]))
            
            batch_onboarding_cache = {}
            if batch_keywords:
                batch_onboardings = frappe.get_all(
                    "Batch onboarding",
                    filters={"batch_skeyword": ["in", batch_keywords]},
                    fields=["batch_skeyword", "name", "kit_less"]
                )
                batch_onboarding_cache = {b.batch_skeyword: b for b in batch_onboardings}
            
            for index, student_entry in enumerate(batch_students):
                try:
                    # Update job progress
                    actual_index = batch_start + index
                    update_job_progress(actual_index, total_students)
                    
                    student = frappe.get_doc("Backend Students", student_entry.name)
                    
                    # 1. Handle Glific contact creation/retrieval
                    try:
                        course_level_for_glific = None
                        if hasattr(student, 'batch_skeyword') and student.batch_skeyword and student.course_vertical and student.grade:
                            # Use cached batch onboarding data
                            batch_onboarding = batch_onboarding_cache.get(student.batch_skeyword)
                            
                            if batch_onboarding:
                                kitless = batch_onboarding.kit_less
                                course_level_for_glific = get_course_level_with_validation_backend(
                                    student.course_vertical,
                                    student.grade,
                                    student.phone,
                                    student.student_name,
                                    kitless
                                )
                        
                        glific_contact = process_glific_contact(student, glific_group, course_level_for_glific)
                    except Exception as e:
                        frappe.log_error(f"Error processing Glific contact for {student.student_name}: {str(e)}", 
                                       "Backend Student Onboarding")
                        glific_contact = None
                    
                    # 2. Create/update student record
                    student_doc = process_student_record(student, glific_contact, set_id, initial_stage, course_level_for_glific)
                    
                    # 3. Update Backend Students record
                    update_backend_student_status(student, "Success", student_doc)
                    
                    success_count += 1
                    success_data = {
                        "backend_id": student.name,
                        "student_id": student_doc.name,
                        "student_name": student_doc.name1,
                        "phone": student.phone
                    }
                    if glific_contact and 'id' in glific_contact:
                        success_data["glific_id"] = glific_contact['id']
                    
                    results["success"].append(success_data)
                    
                    # Commit every 10 students instead of every student
                    if (actual_index + 1) % commit_interval == 0:
                        frappe.db.commit()
                        frappe.log_error(f"Committed batch at student {actual_index + 1}/{total_students}", "Backend Batch Progress")
                    
                except Exception as e:
                    frappe.db.rollback()
                    
                    failure_count += 1
                    try:
                        student = frappe.get_doc("Backend Students", student_entry.name)
                        update_backend_student_status(student, "Failed", error=str(e))
                        
                        results["failed"].append({
                            "backend_id": student.name,
                            "student_name": student.student_name,
                            "error": str(e)
                        })
                        
                        frappe.db.commit()
                    except Exception as inner_e:
                        frappe.log_error(f"Error updating failed status for student {student_entry.name}: {str(inner_e)}", 
                                       "Backend Student Onboarding")
                        
                        results["failed"].append({
                            "backend_id": student_entry.name,
                            "student_name": "Unknown",
                            "error": f"Original error: {str(e)}. Status update error: {str(inner_e)}"
                        })
            
            # Commit at end of each batch
            frappe.db.commit()
            frappe.log_error(f"Completed batch {batch_start//batch_size + 1}/{(total_students + batch_size - 1)//batch_size}", "Backend Batch Complete")
        
        # Final commit and update batch status
        try:
            batch = frappe.get_doc("Backend Student Onboarding", set_id)
            if failure_count == 0:
                batch.status = "Processed"
            elif success_count == 0:
                batch.status = "Failed"
            else:
                batch.status = "Processing" # Since "Partially Processed" might not be an allowed status value
            
            # Update processed_student_count field if it exists
            processed_count = frappe.db.count("Backend Students", 
                                             filters={"parent": set_id, "processing_status": "Success"})
            if hasattr(batch, 'processed_student_count'):
                batch.processed_student_count = processed_count
            
            batch.save()
            frappe.db.commit() # Final commit
        except Exception as e:
            frappe.log_error(f"Error updating batch status: {str(e)}", "Backend Student Onboarding")
        
        return {
            "success_count": success_count,
            "failure_count": failure_count,
            "results": results
        }
    except Exception as e:
        frappe.db.rollback()
        try:
            # Update batch status to Failed
            batch = frappe.get_doc("Backend Student Onboarding", set_id)
            batch.status = "Failed"
            # Add processing_notes if the field exists
            if hasattr(batch, 'processing_notes'):
                # Get the field's max length
                meta = frappe.get_meta("Backend Student Onboarding")
                field = meta.get_field("processing_notes")
                max_length = field.length if field and hasattr(field, 'length') else 140
                
                batch.processing_notes = str(e)[:max_length]
            batch.save()
            frappe.db.commit()
        except:
            pass # If this fails too, just continue
        
        frappe.log_error(f"Error in batch processing job: {str(e)}", "Backend Student Onboarding")
        raise

def update_job_progress(current, total):
    """Update the background job progress"""
    if total > 0:
        try:
            # Try without user parameter first (for older Frappe versions)
            frappe.publish_progress(
                percent=(current+1) * 100 / total,
                title=_("Processing Students"),
                description=_("Processing student {0} of {1}").format(current + 1, total)
            )
        except Exception:
            # Fall back to basic approach if publish_progress fails
            if (current+1) % 10 == 0 or (current+1) == total: # Update every 10 items
                frappe.db.commit()
                print(f"Processed {current+1} of {total} students")





def process_glific_contact(student, glific_group, course_level=None):
    """
    Process Glific contact creation or retrieval
    FIXED: Shorter log messages to avoid 140-char limit
    
    Args:
        student: Backend Students document
        glific_group: Glific group information
        course_level: Optional course level name for Glific
    
    Returns:
        Glific contact information if successful, None otherwise
    """
    # Format phone number 
    phone = format_phone_number(student.phone)
    if not phone:
        raise ValueError(f"Invalid phone number format: {student.phone}")
    
    # Get school name for Glific
    school_name = ""
    if student.school:
        school_name = frappe.get_value("School", student.school, "name1") or ""
    
    # Get batch name for Glific
    batch_name = ""
    if student.batch:
        batch_id = frappe.get_value("Batch", student.batch, "name") or ""
    
    # Get language ID for Glific from TAP Language
    language_id = None
    if student.language:
        try:
            language_id = frappe.get_value("TAP Language", student.language, "glific_language_id")
            if not language_id:
                frappe.logger().warning(f"No glific_language_id found for language {student.language}, will use default")
        except Exception as e:
            frappe.logger().warning(f"Error getting glific_language_id: {str(e)}")
    
    # Get course level name for Glific
    course_level_name = ""
    if course_level:
        try:
            course_level_name = frappe.get_value("Course Level", course_level, "name1") or ""
            # SHORTENED LOG
            print(f"Course level: {course_level} -> '{course_level_name}'")
        except Exception as e:
            print(f"Course level error: {str(e)}")
            course_level_name = ""
    else:
        print(f"No course level provided for {student.student_name}")
    
    # Get course vertical name for Glific
    course_vertical_name = ""
    if student.course_vertical:
        course_vertical_name = frappe.get_value("Course Verticals", student.course_vertical, "name2") or ""
    
    # Check if contact already exists in Glific
    existing_contact = get_contact_by_phone(phone)
    
    if existing_contact and 'id' in existing_contact:
        # Contact exists, add to group if needed
        if glific_group and glific_group.get("group_id"):
            from tap_lms.glific_integration import add_contact_to_group
            add_contact_to_group(existing_contact['id'], glific_group.get("group_id"))
        
        # Update fields to ensure they're current
        fields_to_update = {
            "buddy_name": student.student_name,
            "batch_id": student.batch
        }
        
        if school_name:
            fields_to_update["school"] = school_name
        if course_level_name:
            fields_to_update["course_level"] = course_level_name
            print(f"Adding course_level: '{course_level_name}'")
        if course_vertical_name:
            fields_to_update["course"] = course_vertical_name
        if student.grade:
            fields_to_update["grade"] = student.grade
        
        # Update the contact fields
        from tap_lms.glific_integration import update_contact_fields
        update_result = update_contact_fields(existing_contact['id'], fields_to_update)
        
        # SHORTENED LOG - just print, don't use frappe.log_error
        print(f"Updated {student.student_name}: {len(fields_to_update)} fields")
        
        return existing_contact
    else:
        # Create new contact and add to group
        contact = add_student_to_glific_for_onboarding(
            student.student_name,
            phone,
            school_name,
            batch_id,
            glific_group.get("group_id") if glific_group else None,
            language_id,
            course_level_name,
            course_vertical_name,
            student.grade
        )
        
        if not contact or 'id' not in contact:
            frappe.log_error(
                f"Failed to create Glific contact for {student.student_name}",
                "Glific Contact Error"
            )
        else:
            print(f"Created contact: {student.student_name}")
        
        return contact






def determine_student_type_backend(phone_number, student_name, course_vertical):
    """
    Determine if student is New or Old based on comprehensive enrollment analysis
    
    Logic:
    - IF student has enrollments in SAME vertical (valid links) → OLD
    - ELSE IF student has enrollments with BROKEN course links → OLD  
    - ELSE IF student has enrollments in DIFFERENT verticals → NEW
    - ELSE IF student has enrollments with NULL course → OLD
    - ELSE IF student has ANY enrollments but can't determine vertical → OLD
    - ELSE → NEW
    
    Args:
        phone_number: Student's phone number (can be 10 or 12 digits)
        student_name: Student's name (name1 field)
        course_vertical: Course vertical name/ID for comparison
    
    Returns:
        "Old" or "New" based on enrollment analysis
    """
    try:
        phone_12, phone_10 = normalize_phone_number(phone_number)
        
        if not phone_12 or not phone_10:
            frappe.log_error(f"Invalid phone format for student type check: {phone_number}", "Backend Student Type Error")
            return "New"
        
        # Find existing student
        existing_students = frappe.db.sql("""
            SELECT name, phone, name1
            FROM `tabStudent`
            WHERE name1 = %s 
            AND (phone = %s OR phone = %s)
            LIMIT 1
        """, (student_name, phone_10, phone_12), as_dict=True)
        
        if not existing_students:
            frappe.log_error(
                f"Backend: No existing student found: phone={phone_number}, name={student_name} → NEW",
                "Backend Student Type Classification"
            )
            return "New"
        
        student_id = existing_students[0].name
        
        # Get all enrollments for this student
        enrollments = frappe.db.sql("""
            SELECT name, course, batch, grade, school
            FROM `tabEnrollment` 
            WHERE parent = %s
        """, (student_id,), as_dict=True)
        
        if not enrollments:
            frappe.log_error(
                f"Backend: Student exists but no enrollments: {student_name} → NEW",
                "Backend Student Type Classification"
            )
            return "New"
        
        # Analyze each enrollment
        same_vertical_count = 0
        different_vertical_count = 0
        broken_course_count = 0
        null_course_count = 0
        undetermined_count = 0
        
        enrollment_details = []
        
        for enrollment in enrollments:
            detail = {
                "enrollment": enrollment.name,
                "course": enrollment.course,
                "status": "",
                "vertical": ""
            }
            
            if not enrollment.course:
                # NULL course
                null_course_count += 1
                detail["status"] = "NULL_COURSE"
                detail["vertical"] = "N/A"
            else:
                # Check if course exists
                course_exists = frappe.db.exists("Course Level", enrollment.course)
                if not course_exists:
                    # BROKEN course link
                    broken_course_count += 1
                    detail["status"] = "BROKEN_COURSE"
                    detail["vertical"] = "BROKEN"
                else:
                    # Valid course - check vertical
                    course_vertical_data = frappe.db.sql("""
                        SELECT cv.name as vertical_name
                        FROM `tabCourse Level` cl
                        INNER JOIN `tabCourse Verticals` cv ON cv.name = cl.vertical
                        WHERE cl.name = %s
                    """, (enrollment.course,), as_dict=True)
                    
                    if course_vertical_data:
                        enrollment_vertical = course_vertical_data[0].vertical_name
                        detail["vertical"] = enrollment_vertical
                        
                        if enrollment_vertical == course_vertical:
                            same_vertical_count += 1
                            detail["status"] = "SAME_VERTICAL"
                        else:
                            different_vertical_count += 1
                            detail["status"] = "DIFFERENT_VERTICAL"
                    else:
                        # Course exists but can't determine vertical
                        undetermined_count += 1
                        detail["status"] = "UNDETERMINED_VERTICAL"
                        detail["vertical"] = "UNKNOWN"
            
            enrollment_details.append(detail)
        
        # Apply decision logic in priority order
        student_type = "New"  # Default
        reason = ""
        
        if same_vertical_count > 0:
            # Rule 1: Has enrollments in SAME vertical (valid links) → OLD
            student_type = "Old"
            reason = f"Has {same_vertical_count} enrollments in same vertical '{course_vertical}'"
        elif broken_course_count > 0:
            # Rule 2: Has enrollments with BROKEN course links → OLD
            student_type = "Old"
            reason = f"Has {broken_course_count} enrollments with broken course links"
        elif different_vertical_count > 0 and null_course_count == 0 and undetermined_count == 0:
            # Rule 3: Has enrollments ONLY in DIFFERENT verticals → NEW
            student_type = "New"
            reason = f"Has {different_vertical_count} enrollments only in different verticals"
        elif null_course_count > 0:
            # Rule 4: Has enrollments with NULL course → OLD
            student_type = "Old"
            reason = f"Has {null_course_count} enrollments with NULL course"
        elif undetermined_count > 0:
            # Rule 5: Has enrollments but can't determine vertical → OLD
            student_type = "Old"
            reason = f"Has {undetermined_count} enrollments with undetermined vertical"
        else:
            # Rule 6: Fallback → NEW (shouldn't reach here if logic is correct)
            student_type = "New"
            reason = "Fallback case - no clear enrollment pattern"
        
        # Detailed logging for debugging
        frappe.log_error(
            f"Backend: Student type analysis for {student_name} (phone={phone_10}/{phone_12}):\n"
            f"Target vertical: {course_vertical}\n"
            f"Total enrollments: {len(enrollments)}\n"
            f"Same vertical: {same_vertical_count}\n"
            f"Different vertical: {different_vertical_count}\n"
            f"Broken courses: {broken_course_count}\n"
            f"NULL courses: {null_course_count}\n"
            f"Undetermined: {undetermined_count}\n"
            f"Decision: {student_type} - {reason}\n"
            f"Enrollment details: {enrollment_details}",
            "Backend Student Type Classification"
        )
        
        return student_type
        
    except Exception as e:
        frappe.log_error(f"Backend: Error determining student type: {str(e)}", "Backend Student Type Error")
        return "New"  # Default to New on error


@frappe.whitelist()
def fix_broken_course_links(student_id=None):
    """
    Fix broken course links in enrollments by setting them to NULL
    This allows the student type logic to handle them properly
    """
    try:
        result = []
        
        if student_id:
            # Fix specific student
            students_to_check = [{"name": student_id}]
            result.append(f"Checking student: {student_id}")
        else:
            # Check all students
            students_to_check = frappe.get_all("Student", fields=["name"])
            result.append(f"Checking all {len(students_to_check)} students")
        
        total_fixed = 0
        
        for student in students_to_check:
            # Get enrollments with broken course links
            broken_enrollments = frappe.db.sql("""
                SELECT e.name, e.course
                FROM `tabEnrollment` e
                LEFT JOIN `tabCourse Level` cl ON cl.name = e.course
                WHERE e.parent = %s 
                AND e.course IS NOT NULL 
                AND cl.name IS NULL
            """, (student["name"],), as_dict=True)
            
            if broken_enrollments:
                result.append(f"Student {student['name']}: {len(broken_enrollments)} broken links")
                
                for enrollment in broken_enrollments:
                    # Set course to NULL instead of broken link
                    frappe.db.set_value("Enrollment", enrollment.name, "course", None)
                    result.append(f"  Fixed: {enrollment.name} (was: {enrollment.course})")
                    total_fixed += 1
        
        if total_fixed > 0:
            frappe.db.commit()
            result.append(f"\nTotal fixed: {total_fixed} broken course links")
        else:
            result.append("No broken course links found")
        
        return "\n".join(result)
        
    except Exception as e:
        return f"ERROR fixing broken links: {str(e)}"


@frappe.whitelist()
def debug_student_type_analysis(student_name, phone_number, course_vertical):
    """
    Debug function to analyze student type determination in detail
    """
    try:
        result = []
        result.append(f"\n=== STUDENT TYPE ANALYSIS: {student_name} ({phone_number}) ===")
        
        # Get normalized phone
        phone_12, phone_10 = normalize_phone_number(phone_number)
        result.append(f"Normalized phone: {phone_number} -> {phone_10} / {phone_12}")
        result.append(f"Target vertical: {course_vertical}")
        
        # Find existing student
        existing_students = frappe.db.sql("""
            SELECT name, phone, name1
            FROM `tabStudent`
            WHERE name1 = %s 
            AND (phone = %s OR phone = %s)
            LIMIT 1
        """, (student_name, phone_10, phone_12), as_dict=True)
        
        if not existing_students:
            result.append("No existing student found → NEW")
            return "\n".join(result)
        
        student_id = existing_students[0].name
        result.append(f"Found student: {student_id}")
        
        # Get all enrollments
        enrollments = frappe.db.sql("""
            SELECT name, course, batch, grade, school
            FROM `tabEnrollment` 
            WHERE parent = %s
        """, (student_id,), as_dict=True)
        
        result.append(f"Total enrollments: {len(enrollments)}")
        
        if not enrollments:
            result.append("No enrollments found → NEW")
            return "\n".join(result)
        
        # Analyze each enrollment
        for i, enrollment in enumerate(enrollments, 1):
            result.append(f"\nEnrollment {i}: {enrollment.name}")
            result.append(f"  Course: {enrollment.course}")
            result.append(f"  Batch: {enrollment.batch}")
            
            if not enrollment.course:
                result.append("  Status: NULL COURSE → contributes to OLD")
                continue
            
            # Check if course exists
            course_exists = frappe.db.exists("Course Level", enrollment.course)
            if not course_exists:
                result.append("  Status: BROKEN COURSE LINK → contributes to OLD")
                continue
            
            # Get course vertical
            course_vertical_data = frappe.db.sql("""
                SELECT cv.name as vertical_name
                FROM `tabCourse Level` cl
                INNER JOIN `tabCourse Verticals` cv ON cv.name = cl.vertical
                WHERE cl.name = %s
            """, (enrollment.course,), as_dict=True)
            
            if course_vertical_data:
                enrollment_vertical = course_vertical_data[0].vertical_name
                result.append(f"  Vertical: {enrollment_vertical}")
                
                if enrollment_vertical == course_vertical:
                    result.append("  Status: SAME VERTICAL → contributes to OLD")
                else:
                    result.append("  Status: DIFFERENT VERTICAL → contributes to NEW")
            else:
                result.append("  Status: UNDETERMINED VERTICAL → contributes to OLD")
        
        # Get final determination
        student_type = determine_student_type_backend(phone_number, student_name, course_vertical)
        result.append(f"\nFINAL DETERMINATION: {student_type}")
        
        result.append(f"\n=== END ANALYSIS ===\n")
        
        return "\n".join(result)
        
    except Exception as e:
        return f"ANALYSIS ERROR: {str(e)}"


def get_current_academic_year_backend():
    """
    Get current academic year based on current date
    Academic year runs from April to March
    (Same logic as API version)
    
    Returns:
        Academic year string in format "YYYY-YY" (e.g., "2025-26")
    """
    try:
        current_date = frappe.utils.getdate()
        
        if current_date.month >= 4: # April onwards = new academic year
            academic_year = f"{current_date.year}-{str(current_date.year + 1)[-2:]}"
        else:
            academic_year = f"{current_date.year - 1}-{str(current_date.year)[-2:]}"
        
        frappe.log_error(f"Backend: Current academic year determined: {academic_year}", "Backend Academic Year Calculation")
        
        return academic_year
        
    except Exception as e:
        frappe.log_error(f"Backend: Error calculating academic year: {str(e)}", "Backend Academic Year Error")
        return None

def validate_enrollment_data(student_name, phone_number):
    """
    Validate enrollment data WITHOUT making any repairs - only detection
    
    Args:
        student_name: Student's name
        phone_number: Student's phone number
        
    Returns:
        dict: Summary of validation results
    """
    try:
        phone_12, phone_10 = normalize_phone_number(phone_number)
        
        if not phone_12 or not phone_10:
            return {"error": "Invalid phone number format"}
        
        # Find all enrollments for this student
        enrollments = frappe.db.sql("""
            SELECT s.name as student_id, e.name as enrollment_id, e.course, e.batch, e.grade
            FROM `tabStudent` s
            INNER JOIN `tabEnrollment` e ON e.parent = s.name 
            WHERE (s.phone = %s OR s.phone = %s) AND s.name1 = %s
        """, (phone_10, phone_12, student_name), as_dict=True)
        
        validation_results = {
            "total_enrollments": len(enrollments),
            "valid_enrollments": 0,
            "broken_enrollments": 0,
            "broken_details": []
        }
        
        for enrollment in enrollments:
            if enrollment.course:
                # Check if course_level exists
                course_level_exists = frappe.db.exists("Course Level", enrollment.course)
                
                if course_level_exists:
                    validation_results["valid_enrollments"] += 1
                else:
                    validation_results["broken_enrollments"] += 1
                    validation_results["broken_details"].append({
                        "enrollment_id": enrollment.enrollment_id,
                        "invalid_course": enrollment.course,
                        "batch": enrollment.batch,
                        "grade": enrollment.grade
                    })
                    
                    # Only log the broken data, don't fix it
                    frappe.log_error(
                        f"Detected broken course_level link: enrollment={enrollment.enrollment_id}, invalid_course={enrollment.course}, student={student_name}",
                        "Backend Broken Enrollment Data"
                    )
        
        return validation_results
        
    except Exception as e:
        frappe.log_error(f"Error validating enrollment data: {str(e)}", "Backend Enrollment Validation Error")
        return {"error": str(e)}

def get_course_level_with_mapping_backend(course_vertical, grade, phone_number, student_name, kitless):
    """
    Get course level using Grade Course Level Mapping with fallback to Stage Grades logic
    UPDATED: Now handles both 10-digit and 12-digit phone number formats
    
    Args:
        course_vertical: Course vertical name/ID
        grade: Student grade
        phone_number: Student phone number (can be 10 or 12 digits)
        student_name: Student name (for unique identification with phone)
        kitless: School's kit capability (for fallback logic)
    
    Returns:
        Course level name or raises exception
    """
    try:
        # Step 1: Normalize phone number and determine student type
        student_type = determine_student_type_backend(phone_number, student_name, course_vertical)
        
        # Step 2: Get current academic year
        academic_year = get_current_academic_year_backend()
        
        phone_12, phone_10 = normalize_phone_number(phone_number)
        
        frappe.log_error(
            f"Backend: Course level mapping lookup: vertical={course_vertical}, grade={grade}, type={student_type}, year={academic_year}, phone={phone_10}/{phone_12}",
            "Backend Course Level Mapping Lookup"
        )
        
        # Step 3: Try manual mapping with current academic year
        if academic_year:
            mapping = frappe.get_all(
                "Grade Course Level Mapping",
                filters={
                    "academic_year": academic_year,
                    "course_vertical": course_vertical,
                    "grade": grade,
                    "student_type": student_type,
                    "is_active": 1
                },
                fields=["assigned_course_level", "mapping_name"],
                order_by="modified desc", # Last modified takes priority
                limit=1
            )
            
            if mapping:
                frappe.log_error(
                    f"Backend: Found mapping: {mapping[0].mapping_name} -> {mapping[0].assigned_course_level}",
                    "Backend Course Level Mapping Found"
                )
                return mapping[0].assigned_course_level
        
        # Step 4: Try mapping with academic_year = null (flexible mappings)
        mapping_null = frappe.get_all(
            "Grade Course Level Mapping",
            filters={
                "academic_year": ["is", "not set"], # Null academic year
                "course_vertical": course_vertical,
                "grade": grade,
                "student_type": student_type,
                "is_active": 1
            },
            fields=["assigned_course_level", "mapping_name"],
            order_by="modified desc",
            limit=1
        )
        
        if mapping_null:
            frappe.log_error(
                f"Backend: Found flexible mapping: {mapping_null[0].mapping_name} -> {mapping_null[0].assigned_course_level}",
                "Backend Course Level Flexible Mapping Found"
            )
            return mapping_null[0].assigned_course_level
        
        # Step 5: Log that no mapping was found, falling back
        frappe.log_error(
            f"Backend: No mapping found for vertical={course_vertical}, grade={grade}, type={student_type}, year={academic_year}. Using Stage Grades fallback.",
            "Backend Course Level Mapping Fallback"
        )
        
        # Step 6: Fallback to current Stage Grades logic
        return get_course_level(course_vertical, grade, kitless)
        
    except Exception as e:
        frappe.log_error(f"Backend: Error in course level mapping: {str(e)}", "Backend Course Level Mapping Error")
        # On any error, fallback to original logic
        return get_course_level(course_vertical, grade, kitless)

def get_course_level_with_validation_backend(course_vertical, grade, phone_number, student_name, kitless):
    """
    Enhanced version of get_course_level_with_mapping_backend with data validation (NO REPAIRS)
    
    Args:
        course_vertical: Course vertical name/ID
        grade: Student grade
        phone_number: Student phone number
        student_name: Student name
        kitless: School's kit capability
        
    Returns:
        Course level name or None if not found
    """
    try:
        # First validate existing enrollment data (detection only, no repairs)
        validation_results = validate_enrollment_data(student_name, phone_number)
        
        if validation_results.get("broken_enrollments", 0) > 0:
            frappe.log_error(
                f"Backend: Detected {validation_results['broken_enrollments']} broken enrollments for {student_name}, continuing without repair",
                "Backend Data Validation"
            )
        
        # Now proceed with the original logic
        return get_course_level_with_mapping_backend(course_vertical, grade, phone_number, student_name, kitless)
        
    except Exception as e:
        frappe.log_error(f"Backend: Error in course level selection with validation: {str(e)}", "Backend Course Level Validation Error")
        # Fallback to basic course level selection
        try:
            return get_course_level(course_vertical, grade, kitless)
        except Exception as fallback_error:
            frappe.log_error(f"Backend: Fallback course level selection also failed: {str(fallback_error)}", "Backend Course Level Fallback Error")
            return None


def process_student_record(student, glific_contact, batch_id, initial_stage, course_level=None):
    """
    Create or update student record based on duplicate handling logic
    UPDATED: Enhanced error handling for broken enrollment data
    
    Args:
        student: Backend Students document
        glific_contact: Glific contact information
        batch_id: Backend onboarding batch ID
        initial_stage: Initial onboarding stage
        course_level: Pre-determined course level (optional)
    """
    try:
        # Check for duplicate using normalized phone number comparison
        existing_student_data = find_existing_student_by_phone_and_name(student.phone, student.student_name)
        
        if existing_student_data:
            # Phone and name match - update existing student
            existing_student = frappe.get_doc("Student", existing_student_data.name)
            
            # Update phone number to normalized 12-digit format if needed
            phone_12, phone_10 = normalize_phone_number(student.phone)
            if phone_12 and existing_student.phone != phone_12:
                # Update to the 12-digit format for consistency
                existing_student.phone = phone_12
                frappe.log_error(
                    f"Updated phone format for existing student {existing_student.name}: {existing_student_data.phone} -> {phone_12}",
                    "Backend Phone Format Update"
                )
            
            # SHORTENED LOG MESSAGE
            frappe.log_error(
                f"Existing: {student.student_name} | Grade: {existing_student.grade}→{student.grade}",
                "Backend Student Found"
            )
            
            # Update student fields including grade
            updated_fields = []
            
            # Update grade (allow both upgrade and downgrade)
            if student.grade and str(student.grade) != str(existing_student.grade):
                frappe.log_error(
                    f"Grade update: {student.student_name} | {existing_student.grade}→{student.grade}",
                    "Backend Grade Update"
                )
                existing_student.grade = student.grade
                updated_fields.append(f"grade: {existing_student.grade}→{student.grade}")
            
            # Update school if changed
            if student.school and student.school != existing_student.school_id:
                frappe.log_error(
                    f"School update: {student.student_name} | {existing_student.school_id}→{student.school}",
                    "Backend School Update"
                )
                existing_student.school_id = student.school
                updated_fields.append(f"school: {existing_student.school_id}→{student.school}")
            
            # Update language if changed
            if student.language and student.language != existing_student.language:
                frappe.log_error(
                    f"Language update: {student.student_name} | {existing_student.language}→{student.language}",
                    "Backend Language Update"
                )
                existing_student.language = student.language
                updated_fields.append(f"language: {existing_student.language}→{student.language}")
            
            # Update gender if missing or changed
            if student.gender and (not existing_student.gender or student.gender != existing_student.gender):
                old_gender = existing_student.gender or "Not Set"
                existing_student.gender = student.gender
                updated_fields.append(f"gender: {old_gender}→{student.gender}")
            
            # Log all updates with shortened message
            if updated_fields:
                update_msg = f"Updated fields: {student.student_name} | {', '.join(updated_fields)}"
                frappe.log_error(
                    update_msg[:140], # Truncate to 140 chars
                    "Backend Fields Updated"
                )
            
            # ALWAYS ADD NEW ENROLLMENT (regardless of existing enrollments)
            if student.batch:
                # Use pre-determined course level if available, otherwise determine it
                if course_level is None:
                    try:
                        if hasattr(student, 'batch_skeyword') and student.batch_skeyword and student.course_vertical and student.grade:
                            # Get batch onboarding details using batch_skeyword
                            batch_onboarding = frappe.get_all(
                                "Batch onboarding",
                                filters={"batch_skeyword": student.batch_skeyword},
                                fields=["name", "kit_less"]
                            )
                            
                            if batch_onboarding:
                                kitless = batch_onboarding[0].kit_less
                                
                                # Use enhanced course level selection that handles broken data
                                course_level = get_course_level_with_validation_backend(
                                    student.course_vertical,
                                    student.grade,
                                    phone_12 or student.phone, # Use normalized phone
                                    student.student_name, # Student name for unique identification
                                    kitless # For fallback logic
                                )
                                
                                # SHORTENED LOG MESSAGE
                                frappe.log_error(
                                    f"Course selected: {student.student_name} | {course_level or 'None'}",
                                    "Backend Course Selection"
                                )
                        
                        # If course_level is still None, try basic fallback
                        if not course_level and student.course_vertical and student.grade:
                            try:
                                # Direct fallback to get_course_level without mapping
                                course_level = get_course_level(student.course_vertical, student.grade, False)
                                frappe.log_error(
                                    f"Fallback course selected: {student.student_name} | {course_level or 'None'}",
                                    "Backend Course Fallback"
                                )
                            except Exception as fallback_error:
                                frappe.log_error(f"Fallback course selection failed: {str(fallback_error)}", "Backend Course Fallback Error")
                                course_level = None
                                
                    except Exception as e:
                        frappe.log_error(f"Course selection error: {str(e)}", "Backend Course Error")
                        course_level = None
                else:
                    # Use the pre-determined course level
                    frappe.log_error(
                        f"Using pre-determined course level: {student.student_name} | {course_level}",
                        "Backend Course Reuse"
                    )
                
                # Create new enrollment (always) - with enhanced error handling
                try:
                    enrollment = {
                        "doctype": "Enrollments",
                        "batch": student.batch,
                        "grade": student.grade, # Use the updated grade
                        "date_joining": nowdate(),
                        "school": student.school
                    }
                    
                    # Add course level if we found one (can be None)
                    if course_level:
                        enrollment["course"] = course_level
                    
                    existing_student.append("enrollment", enrollment)
                    
                    # SHORTENED LOG MESSAGE
                    enrollment_msg = f"Enrollment added: {student.student_name} | Batch: {student.batch} | Grade: {student.grade} | Course: {course_level or 'None'}"
                    frappe.log_error(
                        enrollment_msg[:140], # Truncate to 140 chars
                        "Backend Enrollment Added"
                    )
                    
                except Exception as enrollment_error:
                    frappe.log_error(f"Error creating enrollment: {str(enrollment_error)}", "Backend Enrollment Error")
                    # Continue without enrollment if there's an error
            
            # Update Glific ID if we have it and student doesn't
            if glific_contact and 'id' in glific_contact and not existing_student.glific_id:
                existing_student.glific_id = glific_contact['id']
                frappe.log_error(
                    f"Glific ID added: {student.student_name} | ID: {glific_contact['id']}",
                    "Backend Glific Added"
                )
            
            # Update backend onboarding reference
            existing_student.backend_onboarding = batch_id
            
            # Save the existing student with all updates - with error handling
            try:
                existing_student.save()
                
                # SHORTENED LOG MESSAGE
                frappe.log_error(
                    f"Student updated: {student.student_name} (ID: {existing_student.name})",
                    "Backend Update Complete"
                )
                
            except Exception as save_error:
                frappe.log_error(f"Error saving existing student: {str(save_error)}", "Backend Save Error")
                raise save_error
            
            student_doc = existing_student
            
        else:
            # Create new student with normalized phone number
            phone_12, phone_10 = normalize_phone_number(student.phone)
            
            frappe.log_error(
                f"Creating new: {student.student_name} | Grade: {student.grade}",
                "Backend New Student"
            )
            
            student_doc = frappe.new_doc("Student")
            student_doc.name1 = student.student_name
            student_doc.phone = phone_12 or student.phone # Use normalized 12-digit format
            student_doc.gender = student.gender
            student_doc.school_id = student.school
            student_doc.grade = student.grade
            student_doc.language = student.language
            student_doc.backend_onboarding = batch_id
            student_doc.joined_on = nowdate()
            student_doc.status = "active"
            
            # Add Glific ID if available
            if glific_contact and 'id' in glific_contact:
                student_doc.glific_id = glific_contact['id']
            
            # Add enrollment with course level for new student
            if student.batch:
                # Use pre-determined course level if available, otherwise determine it
                if course_level is None:
                    try:
                        if hasattr(student, 'batch_skeyword') and student.batch_skeyword and student.course_vertical and student.grade:
                            # Get batch onboarding details using batch_skeyword
                            batch_onboarding = frappe.get_all(
                                "Batch onboarding",
                                filters={"batch_skeyword": student.batch_skeyword},
                                fields=["name", "kit_less"]
                            )
                            
                            if batch_onboarding:
                                kitless = batch_onboarding[0].kit_less
                                
                                # Use enhanced course level selection
                                course_level = get_course_level_with_validation_backend(
                                    student.course_vertical,
                                    student.grade,
                                    phone_12 or student.phone, # Use normalized phone
                                    student.student_name, # Student name for unique identification
                                    kitless # For fallback logic
                                )
                                
                                # SHORTENED LOG MESSAGE
                                frappe.log_error(
                                    f"Course selected: {student.student_name} | {course_level or 'None'}",
                                    "Backend Course Selection"
                                )
                        
                        # If course_level is still None, try basic fallback
                        if not course_level and student.course_vertical and student.grade:
                            try:
                                course_level = get_course_level(student.course_vertical, student.grade, False)
                                frappe.log_error(
                                    f"Fallback course selected: {student.student_name} | {course_level or 'None'}",
                                    "Backend Course Fallback"
                                )
                            except Exception as fallback_error:
                                frappe.log_error(f"Fallback course selection failed: {str(fallback_error)}", "Backend Course Fallback Error")
                                course_level = None
                                
                    except Exception as e:
                        frappe.log_error(f"Course selection error: {str(e)}", "Backend Course Error")
                        course_level = None
                else:
                    # Use the pre-determined course level
                    frappe.log_error(
                        f"Using pre-determined course level for new student: {student.student_name} | {course_level}",
                        "Backend Course Reuse"
                    )
                
                # Create enrollment with enhanced error handling
                try:
                    enrollment = {
                        "doctype": "Enrollments",
                        "batch": student.batch,
                        "grade": student.grade,
                        "date_joining": nowdate(),
                        "school": student.school
                    }
                    
                    # Add course level if we found one (can be None)
                    if course_level:
                        enrollment["course"] = course_level
                    
                    student_doc.append("enrollment", enrollment)
                    
                    # SHORTENED LOG MESSAGE
                    enrollment_msg = f"New enrollment: {student.student_name} | Batch: {student.batch} | Grade: {student.grade} | Course: {course_level or 'None'}"
                    frappe.log_error(
                        enrollment_msg[:140], # Truncate to 140 chars
                        "Backend New Enrollment"
                    )
                    
                except Exception as enrollment_error:
                    frappe.log_error(f"Error creating new enrollment: {str(enrollment_error)}", "Backend New Enrollment Error")
                    # Continue without enrollment if there's an error
            
            # Insert new student with error handling
            try:
                student_doc.insert()
                
                # SHORTENED LOG MESSAGE
                frappe.log_error(
                    f"Created: {student.student_name} (ID: {student_doc.name})",
                    "Backend Creation Complete"
                )
                
            except Exception as insert_error:
                frappe.log_error(f"Error inserting new student: {str(insert_error)}", "Backend Insert Error")
                raise insert_error
            
            # Initialize LearningState if it doesn't exist
            if not frappe.db.exists("LearningState", {"student": student_doc.name}):
                try:
                    learning_state = frappe.new_doc("LearningState")
                    learning_state.student = student_doc.name
                    learning_state.insert()
                except Exception as e:
                    frappe.log_error(f"Error creating LearningState for student {student_doc.name}: {str(e)}", 
                                   "Backend Student Onboarding")
                    # Continue without creating LearningState if there's an error
            
            # Initialize EngagementState if it doesn't exist
            if not frappe.db.exists("EngagementState", {"student": student_doc.name}):
                try:
                    engagement_state = frappe.new_doc("EngagementState")
                    engagement_state.student = student_doc.name
                    
                    # Set default values for required fields
                    engagement_state.average_response_time = "0" # Based on error, this is a required field
                    engagement_state.completion_rate = "0"
                    engagement_state.session_frequency = 0
                    engagement_state.current_streak = 0
                    engagement_state.last_activity_date = nowdate()
                    engagement_state.engagement_trend = "Stable"
                    engagement_state.re_engagement_attempts = "0"
                    engagement_state.sentiment_analysis = "Neutral"
                    
                    engagement_state.insert()
                except Exception as e:
                    frappe.log_error(f"Error creating EngagementState for student {student_doc.name}: {str(e)}", 
                                   "Backend Student Onboarding")
                    # Continue without creating EngagementState if there's an error
            
            # Create first StudentStageProgress for onboarding if it doesn't exist
            if initial_stage and not frappe.db.exists("StudentStageProgress", 
                                                     {"student": student_doc.name, "stage_type": "OnboardingStage"}):
                try:
                    stage_progress = frappe.new_doc("StudentStageProgress")
                    stage_progress.student = student_doc.name
                    stage_progress.stage_type = "OnboardingStage"
                    stage_progress.stage = initial_stage
                    stage_progress.status = "not_started"
                    stage_progress.start_timestamp = now()
                    stage_progress.insert()
                except Exception as e:
                    frappe.log_error(f"Error creating StudentStageProgress for student {student_doc.name}: {str(e)}", 
                                   "Backend Student Onboarding")
                    # Continue without creating StudentStageProgress if there's an error
        
        return student_doc
        
    except Exception as main_error:
        frappe.log_error(f"Critical error in process_student_record for {student.student_name}: {str(main_error)}", "Backend Student Processing Critical Error")
        raise main_error

def update_backend_student_status(student, status, student_doc=None, error=None):
    """
    Update the status of a Backend Students record
    
    Args:
        student: Backend Students document
        status: New status ("Success" or "Failed")
        student_doc: Optional Student document (for Success status)
        error: Optional error message (for Failed status)
    """
    student.processing_status = status
    
    if status == "Success" and student_doc:
        student.student_id = student_doc.name
        # If we have a glific_id field, update it
        if hasattr(student, 'glific_id') and student_doc.glific_id:
            student.glific_id = student_doc.glific_id
    
    # Handle processing_notes with proper truncation for not-null constraint
    if error and hasattr(student, 'processing_notes'):
        # Get the field's max length from metadata or default to 140
        try:
            meta = frappe.get_meta("Backend Students")
            field = meta.get_field("processing_notes")
            max_length = field.length if field and hasattr(field, 'length') else 140
        except:
            max_length = 140 # Fallback if metadata can't be accessed
        
        # Truncate error message to max length
        student.processing_notes = str(error)[:max_length]
    
    student.save()

def format_phone_number(phone):
    """Format phone number for Glific (must be 12 digits with 91 prefix for India)"""
    phone_12, phone_10 = normalize_phone_number(phone)
    return phone_12

@frappe.whitelist()
def get_job_status(job_id):
    """Get the status of a background job using compatibility methods for different Frappe versions"""
    try:
        # First try to get the job directly from the database instead of using get_doc
        result = {
            "status": "Unknown"
        }
        
        # Try different table names that might exist in different Frappe versions
        tables_to_try = ["tabBackground Job", "tabRQ Job"]
        
        for table in tables_to_try:
            # Check if table exists
            if frappe.db.table_exists(table.replace("tab", "")):
                try:
                    # Get job data directly from the table
                    job_data = frappe.db.get_value(
                        table, 
                        job_id, 
                        ["status", "progress_data", "result"], 
                        as_dict=True
                    )
                    
                    if job_data:
                        result["status"] = job_data.status
                        
                        # If job is running or queued, check progress
                        if job_data.status == "started" or job_data.status == "Started":
                            if job_data.progress_data:
                                try:
                                    progress = json.loads(job_data.progress_data)
                                    result["progress"] = progress
                                except:
                                    pass
                        
                        # If job is completed, check result
                        if job_data.status == "finished" or job_data.status == "Finished":
                            result["status"] = "Completed"
                            if job_data.result:
                                try:
                                    result["result"] = json.loads(job_data.result)
                                except:
                                    pass
                        
                        # If job failed, update status
                        if job_data.status == "failed" or job_data.status == "Failed":
                            result["status"] = "Failed"
                        
                        return result
                except Exception as e:
                    frappe.logger().warning(f"Error getting job data from {table}: {str(e)}")
                    continue
        
        # If we reach here, try using frappe's queue functions directly
        try:
            from frappe.utils.background_jobs import get_job_status as get_rq_job_status
            status = get_rq_job_status(job_id)
            if status:
                result["status"] = status
        except Exception as e:
            frappe.logger().warning(f"Error getting job status via RQ: {str(e)}")
        
        return result
    except Exception as e:
        frappe.logger().error(f"Error in get_job_status: {str(e)}")
        # Return a fallback response that won't break the UI
        return {
            "status": "Unknown",
            "message": "Unable to determine job status. The job may still be running or have completed."
        }

@frappe.whitelist()
def debug_student_processing(student_name, phone_number):
    """
    Debug function to identify why student processing is failing
    """
    try:
        result = []
        result.append(f"\n=== DEBUGGING STUDENT: {student_name} ({phone_number}) ===")
        
        # 1. Check phone number normalization
        phone_12, phone_10 = normalize_phone_number(phone_number)
        result.append(f"1. Phone normalization: {phone_number} -> {phone_10} / {phone_12}")
        
        # 2. Check if student exists
        existing_student = find_existing_student_by_phone_and_name(phone_number, student_name)
        if existing_student:
            result.append(f"2. Student EXISTS: {existing_student}")
            
            # Get full student record
            student_doc = frappe.get_doc("Student", existing_student.name)
            result.append(f"   - Current Grade: {student_doc.grade}")
            result.append(f"   - Current School: {student_doc.school_id}")
            result.append(f"   - Current Language: {student_doc.language}")
            result.append(f"   - Glific ID: {student_doc.glific_id}")
            
            # Check enrollments
            enrollments = frappe.get_all("Enrollment", 
                                       filters={"parent": student_doc.name},
                                       fields=["name", "course", "batch", "grade", "school"])
            result.append(f"   - Existing Enrollments: {len(enrollments)}")
            for enrollment in enrollments:
                result.append(f"     * {enrollment}")
                
                # Check if course exists
                if enrollment.course:
                    course_exists = frappe.db.exists("Course Level", enrollment.course)
                    result.append(f"       Course '{enrollment.course}' exists: {course_exists}")
                    if not course_exists:
                        result.append(f"       *** BROKEN COURSE LINK DETECTED ***")
        else:
            result.append("2. Student DOES NOT EXIST - will create new")
        
        # 3. Check backend student record
        backend_students = frappe.get_all("Backend Students",
                                        filters={"student_name": student_name, "phone": phone_number},
                                        fields=["name", "batch", "course_vertical", "grade", "school", 
                                               "language", "batch_skeyword", "processing_status"])
        
        if backend_students:
            backend_student = backend_students[0]
            result.append(f"3. Backend Student Record: {backend_student}")
            
            # 4. Check batch keyword mapping
            if backend_student.batch_skeyword:
                batch_onboarding = frappe.get_all("Batch onboarding",
                                                filters={"batch_skeyword": backend_student.batch_skeyword},
                                                fields=["name", "batch", "school", "kit_less"])
                result.append(f"4. Batch Onboarding for keyword '{backend_student.batch_skeyword}': {batch_onboarding}")
                
                if not batch_onboarding:
                    result.append(f"   *** ERROR: No Batch onboarding found for keyword '{backend_student.batch_skeyword}' ***")
            
            # 5. Check batch exists
            if backend_student.batch:
                batch_exists = frappe.db.exists("Batch", backend_student.batch)
                result.append(f"5. Batch '{backend_student.batch}' exists: {batch_exists}")
                if not batch_exists:
                    result.append(f"   *** ERROR: Batch '{backend_student.batch}' does not exist ***")
            
            # 6. Check school exists
            if backend_student.school:
                school_exists = frappe.db.exists("School", backend_student.school)
                result.append(f"6. School '{backend_student.school}' exists: {school_exists}")
                if not school_exists:
                    result.append(f"   *** ERROR: School '{backend_student.school}' does not exist ***")
            
            # 7. Check course vertical exists
            if backend_student.course_vertical:
                vertical_exists = frappe.db.exists("Course Verticals", backend_student.course_vertical)
                result.append(f"7. Course Vertical '{backend_student.course_vertical}' exists: {vertical_exists}")
                if not vertical_exists:
                    result.append(f"   *** ERROR: Course Vertical '{backend_student.course_vertical}' does not exist ***")
            
            # 8. Check language exists
            if backend_student.language:
                language_exists = frappe.db.exists("TAP Language", backend_student.language)
                result.append(f"8. Language '{backend_student.language}' exists: {language_exists}")
                if not language_exists:
                    result.append(f"   *** ERROR: Language '{backend_student.language}' does not exist ***")
            
            # 9. Test course level selection
            try:
                if backend_student.batch_skeyword and backend_student.course_vertical and backend_student.grade:
                    batch_onboarding = frappe.get_all("Batch onboarding",
                                                    filters={"batch_skeyword": backend_student.batch_skeyword},
                                                    fields=["name", "kit_less"])
                    
                    if batch_onboarding:
                        kitless = batch_onboarding[0].kit_less
                        result.append(f"9. Testing course level selection with kitless={kitless}")
                        
                        # Test student type determination
                        student_type = determine_student_type_backend(phone_number, student_name, backend_student.course_vertical)
                        result.append(f"   - Student Type: {student_type}")
                        
                        # Test course level selection
                        course_level = get_course_level_with_validation_backend(
                            backend_student.course_vertical,
                            backend_student.grade,
                            phone_number,
                            student_name,
                            kitless
                        )
                        result.append(f"   - Selected Course Level: {course_level}")
                        
                        if course_level:
                            course_exists = frappe.db.exists("Course Level", course_level)
                            result.append(f"   - Course Level exists: {course_exists}")
                        
            except Exception as course_error:
                result.append(f"9. Course level selection ERROR: {str(course_error)}")
            
            # 10. Test basic enrollment creation (without course)
            try:
                result.append("10. Testing basic enrollment structure:")
                test_enrollment = {
                    "doctype": "Enrollments",
                    "batch": backend_student.batch,
                    "grade": backend_student.grade,
                    "date_joining": nowdate(),
                    "school": backend_student.school
                }
                result.append(f"    Basic enrollment structure: {test_enrollment}")
                
                # Check if all referenced records exist
                refs_exist = {
                    "batch": frappe.db.exists("Batch", backend_student.batch) if backend_student.batch else False,
                    "school": frappe.db.exists("School", backend_student.school) if backend_student.school else False
                }
                result.append(f"    Referenced records exist: {refs_exist}")
                
            except Exception as enrollment_error:
                result.append(f"10. Enrollment test ERROR: {str(enrollment_error)}")
        
        else:
            result.append("3. *** ERROR: No Backend Student record found ***")
        
        result.append(f"\n=== END DEBUG FOR {student_name} ===\n")
        
        return "\n".join(result)
        
    except Exception as e:
        return f"DEBUG ERROR: {str(e)}"

@frappe.whitelist()
def test_basic_student_creation():
    """
    Test creating a minimal student record to identify basic issues
    """
    try:
        result = []
        result.append("=== TESTING BASIC STUDENT CREATION ===")
        
        # Create a minimal test student
        test_student = frappe.new_doc("Student")
        test_student.name1 = "Test Student Debug"
        test_student.phone = "919999999999"
        test_student.gender = "Male"
        test_student.grade = "5"
        test_student.status = "active"
        test_student.joined_on = nowdate()
        
        # Try to insert without any enrollments
        test_student.insert()
        result.append(f"Basic student created successfully: {test_student.name}")
        
        # Now try to add a simple enrollment
        enrollment = {
            "doctype": "Enrollments",
            "batch": "BT00000015",  # From your data
            "grade": "5",
            "date_joining": nowdate()
        }
        
        test_student.append("enrollment", enrollment)
        test_student.save()
        result.append("Enrollment added successfully")
        
        # Clean up
        frappe.delete_doc("Student", test_student.name)
        result.append("Test student deleted successfully")
        
        result.append("=== BASIC TEST PASSED ===")
        
        return "\n".join(result)
        
    except Exception as e:
        return f"BASIC TEST FAILED: {str(e)}"
