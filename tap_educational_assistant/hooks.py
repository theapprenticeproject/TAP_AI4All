app_name = "tap_educational_assistant"
app_title = "TAP Educational Assistant"
app_publisher = "TAP Organization"
app_description = "AI-powered educational management system with WhatsApp integration"
app_email = "admin@tap.edu"
app_license = "mit"

# Apps
# ------------------

# required_apps = []

# Each item in the list will be shown as an app in the apps page
# add_to_apps_screen = [
# 	{
# 		"name": "tap_educational_assistant",
# 		"logo": "/assets/tap_educational_assistant/logo.png",
# 		"title": "TAP Educational Assistant",
# 		"route": "/tap_educational_assistant",
# 		"has_permission": "tap_educational_assistant.api.permission.has_app_permission"
# 	}
# ]

# Includes in <head>
# ------------------

# include js, css files in header of desk.html
# app_include_css = "/assets/tap_educational_assistant/css/tap_educational_assistant.css"
# app_include_js = "/assets/tap_educational_assistant/js/tap_educational_assistant.js"

# include js, css files in header of web template
# web_include_css = "/assets/tap_educational_assistant/css/tap_educational_assistant.css"
# web_include_js = "/assets/tap_educational_assistant/js/tap_educational_assistant.js"

# include custom scss in every website theme (without file extension ".scss")
# website_theme_scss = "tap_educational_assistant/public/scss/website"

# include js, css files in header of web form
# webform_include_js = {"doctype": "public/js/doctype.js"}
# webform_include_css = {"doctype": "public/css/doctype.css"}

# include js in page
# page_js = {"page" : "public/js/file.js"}

# include js in doctype views
# doctype_js = {"doctype" : "public/js/doctype.js"}
# doctype_list_js = {"doctype" : "public/js/doctype_list.js"}
# doctype_tree_js = {"doctype" : "public/js/doctype_tree.js"}
# doctype_calendar_js = {"doctype" : "public/js/doctype_calendar.js"}

# Svg Icons
# ------------------
# include app icons in desk
# app_include_icons = "tap_educational_assistant/public/icons.svg"

# Home Pages
# ----------

# application home page (will override Website Settings)
# home_page = "login"

# website user home page (by Role)
# role_home_page = {
# 	"Role": "home_page"
# }

# Generators
# ----------

# automatically create page for each record of this doctype
# website_generators = ["Web Page"]

# Jinja
# ----------

# add methods and filters to jinja environment
# jinja = {
# 	"methods": "tap_educational_assistant.utils.jinja_methods",
# 	"filters": "tap_educational_assistant.utils.jinja_filters"
# }

# Installation
# ------------

# before_install = "tap_educational_assistant.install.before_install"
# after_install = "tap_educational_assistant.install.after_install"

# Uninstallation
# ------------

# before_uninstall = "tap_educational_assistant.uninstall.before_uninstall"
# after_uninstall = "tap_educational_assistant.uninstall.after_uninstall"

# Integration Setup
# ------------------
# To set up dependencies/integrations with other apps
# Name of the app being installed is passed as an argument

# before_app_install = "tap_educational_assistant.utils.before_app_install"
# after_app_install = "tap_educational_assistant.utils.after_app_install"

# Integration Cleanup
# -------------------
# To clean up dependencies/integrations with other apps
# Name of the app being uninstalled is passed as an argument

# before_app_uninstall = "tap_educational_assistant.utils.before_app_uninstall"
# after_app_uninstall = "tap_educational_assistant.utils.after_app_uninstall"

# Desk Notifications
# ------------------
# See frappe.core.notifications.get_notification_config

# notification_config = "tap_educational_assistant.notifications.get_notification_config"

# Permissions
# -----------
# Permissions evaluated in scripted ways

# permission_query_conditions = {
# 	"Event": "frappe.desk.doctype.event.event.get_permission_query_conditions",
# }
#
# has_permission = {
# 	"Event": "frappe.desk.doctype.event.event.has_permission",
# }

# DocType Class
# ---------------
# Override standard doctype classes

# override_doctype_class = {
# 	"ToDo": "custom_app.overrides.CustomToDo"
# }

# Document Events
# ---------------
# Hook on document methods and events

# doc_events = {
# 	"*": {
# 		"on_update": "method",
# 		"on_cancel": "method",
# 		"on_trash": "method"
# 	}
# }

# Scheduled Tasks
# ---------------

# scheduler_events = {
# 	"all": [
# 		"tap_educational_assistant.tasks.all"
# 	],
# 	"daily": [
# 		"tap_educational_assistant.tasks.daily"
# 	],
# 	"hourly": [
# 		"tap_educational_assistant.tasks.hourly"
# 	],
# 	"weekly": [
# 		"tap_educational_assistant.tasks.weekly"
# 	],
# 	"monthly": [
# 		"tap_educational_assistant.tasks.monthly"
# 	],
# }

# Testing
# -------

# before_tests = "tap_educational_assistant.install.before_tests"

# Overriding Methods
# ------------------------------
#
# override_whitelisted_methods = {
# 	"frappe.desk.doctype.event.event.get_events": "tap_educational_assistant.event.get_events"
# }
#
# each overriding function accepts a `data` argument;
# generated from the base implementation of the doctype dashboard,
# along with any modifications made in other Frappe apps
# override_doctype_dashboards = {
# 	"Task": "tap_educational_assistant.task.get_dashboard_data"
# }

# exempt linked doctypes from being automatically cancelled
#
# auto_cancel_exempted_doctypes = ["Auto Repeat"]

# Ignore links to specified DocTypes when deleting documents
# -----------------------------------------------------------

# ignore_links_on_delete = ["Communication", "ToDo"]

# Request Events
# ----------------
# before_request = ["tap_educational_assistant.utils.before_request"]
# after_request = ["tap_educational_assistant.utils.after_request"]

# Job Events
# ----------
# before_job = ["tap_educational_assistant.utils.before_job"]
# after_job = ["tap_educational_assistant.utils.after_job"]

# User Data Protection
# --------------------

# user_data_fields = [
# 	{
# 		"doctype": "{doctype_1}",
# 		"filter_by": "{filter_by}",
# 		"redact_fields": ["{field_1}", "{field_2}"],
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_2}",
# 		"filter_by": "{filter_by}",
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_3}",
# 		"strict": False,
# 	},
# 	{
# 		"doctype": "{doctype_4}"
# 	}
# ]

# Authentication and authorization
# --------------------------------

# auth_hooks = [
# 	"tap_educational_assistant.auth.validate"
# ]

# Automatically update python controller files with type annotations for this app.
# export_python_type_annotations = True

# default_log_clearing_doctypes = {
# 	"Logging DocType Name": 30  # days to retain logs
# }



# File: ~/frappe-bench/apps/tap_educational_assistant/tap_educational_assistant/hooks.py

# Add these hooks to your existing hooks.py file

import frappe
from typing import Dict, Any

# ==============================================
# CACHE INVALIDATION HOOKS
# ==============================================

# Document event hooks for cache invalidation
doc_events = {
    # Student DocType hooks
    "Student": {
        "after_insert": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change",
        "on_update": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change",
        "on_trash": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change"
    },
    
    # School DocType hooks
    "School": {
        "after_insert": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change",
        "on_update": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change",
        "on_trash": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change"
    },
    
    # Course DocType hooks
    "Course": {
        "after_insert": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change",
        "on_update": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change",
        "on_trash": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change"
    },
    
    # Activities DocType hooks
    "Activities": {
        "after_insert": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change",
        "on_update": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change",
        "on_trash": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change"
    },
    
    # Performance DocType hooks (most critical for cache invalidation)
    "Performance": {
        "after_insert": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change",
        "on_update": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change",
        "on_trash": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change"
    },
    
    # Enrollment DocType hooks
    "Enrollment": {
        "after_insert": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change",
        "on_update": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change",
        "on_trash": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change"
    },
    
    # Batch DocType hooks
    "Batch": {
        "after_insert": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change",
        "on_update": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change",
        "on_trash": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change"
    },
    
    # Teacher DocType hooks
    "Teacher": {
        "after_insert": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change",
        "on_update": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change",
        "on_trash": "tap_educational_assistant.ai_service.core.redis_cache_manager.invalidate_cache_on_doctype_change"
    }
}

# ==============================================
# SCHEDULED JOBS FOR CACHE MANAGEMENT
# ==============================================

# Scheduler events for cache maintenance
scheduler_events = {
    # Cache warming - run every hour during active hours
    "hourly": [
        "tap_educational_assistant.ai_service.cache.scheduled_cache_warming"
    ],
    
    # Cache cleanup - run daily at night
    "daily": [
        "tap_educational_assistant.ai_service.cache.daily_cache_cleanup"
    ],
    
    # Cache statistics - run weekly
    "weekly": [
        "tap_educational_assistant.ai_service.cache.weekly_cache_report"
    ]
}

# ==============================================
# BOOT SESSION HOOKS
# ==============================================

# Boot session hook to initialize cache on startup
boot_session = [
    "tap_educational_assistant.ai_service.cache.initialize_cache_on_boot"
]

