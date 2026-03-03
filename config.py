"""
MoEngage Dashboard Configuration
All credentials, event names, segment definitions, and constants
"""
import os
from typing import Dict, List, Any

# ============================================================================
# API CREDENTIALS (from environment variables with fallbacks)
# ============================================================================
API_BASE = os.getenv("MOENGAGE_API_BASE", "https://api-01.moengage.com")
WORKSPACE_ID = os.getenv("MOENGAGE_WORKSPACE_ID", "95PNUHBSYSLLJZ22PEOFMKF2")
DATA_API_KEY = os.getenv("MOENGAGE_DATA_API_KEY", "Mj5JSGKcwYum9NKAGmGHJG_E")
CAMPAIGN_API_KEY = os.getenv("MOENGAGE_CAMPAIGN_API_KEY", "3XMHJ83D2X4V")

# ============================================================================
# API ENDPOINTS
# ============================================================================
SEGMENTATION_API_ENDPOINT = f"{API_BASE}/v3/custom-segments"
CAMPAIGN_META_API_ENDPOINT = f"{API_BASE}/core-services/v1/campaigns/meta"
CAMPAIGN_STATS_API_ENDPOINT = f"{API_BASE}/core-services/v1/campaign-stats"

# ============================================================================
# API RATE LIMITS
# ============================================================================
SEGMENT_API_RATE_LIMIT = 1.0  # seconds between requests
SEGMENT_POLL_TIMEOUT = 90  # seconds to wait for segment count computation
SEGMENT_POLL_INTERVAL = 5  # seconds between poll attempts
CAMPAIGN_META_LIMIT = 15  # max campaigns per page
STATS_API_BATCH_SIZE = 10  # max campaign IDs per batch request

# ============================================================================
# COUNTRIES
# ============================================================================
COUNTRIES = {
    "GB": "UK",
    "AE": "UAE",
}
COUNTRY_CODES = list(COUNTRIES.keys())

# ============================================================================
# EVENT NAMES (EXACT case-sensitive names from MoEngage)
# ============================================================================
EVENT_NAMES = {
    "ORDER": "ORDER",
    "ORDER_SUB_EVENT_COMPLETED": "COMPLETED",  # sub_event of ORDER
    "ORDER_SUB_EVENT_PAYMENT_COMPLETED": "PAYMENT_COMPLETED",  # sub_event of ORDER
    # Push received events — PLATFORM-SPECIFIC (verified from MoEngage EventList.csv)
    "NOTIFICATION_RECEIVED_ANDROID": "NOTIFICATION_RECEIVED_MOE",  # Android only, 1.8M/month
    "NOTIFICATION_RECEIVED_IOS": "NOTIFICATION_RECEIVED_IOS_MOE",  # iOS only, 49K/month
    "NOTIFICATION_SENT_IOS": "n_i_s",  # iOS "Notification Sent", 1.3M/month
    # Email events
    "MOE_EMAIL_SENT": "MOE_EMAIL_SENT",  # 7.5M/month
    # Unsubscribe events
    "MOE_PUSH_PERMISSION_STATE_BLOCKED": "MOE_PUSH_PERMISSION_STATE_BLOCKED",  # 7.9K/month
    "MOE_EMAIL_UNSUBSCRIBE": "MOE_EMAIL_UNSUBSCRIBE",  # 42K/month
}

# Push notification events that together represent "user received push" across all platforms
PUSH_RECEIVED_EVENTS = [
    "NOTIFICATION_RECEIVED_MOE",       # Android — 1.8M/month
    "NOTIFICATION_RECEIVED_IOS_MOE",   # iOS (received) — 49K/month
    "n_i_s",                           # iOS (sent/impression) — 1.3M/month
]

# ============================================================================
# SEGMENT DEFINITIONS
# ============================================================================
SEGMENT_TYPES = {
    "TOTAL_USERS": "Total Users",
    "ACTIVE_USERS_60D": "Active Users (60d)",
    "TRANSACTED_USERS_PERIOD": "Transacted Users (Period)",
    "RECEIVED_PUSH_PERIOD": "Received Push",
    "RECEIVED_EMAIL_PERIOD": "Received Email",
    "ACTIVE_PUSH_PERIOD": "Active + Received Push",
    "ACTIVE_EMAIL_PERIOD": "Active + Received Email",
    "UNSUBSCRIBED_PUSH_PERIOD": "Unsubscribed Push",
    "UNSUBSCRIBED_EMAIL_PERIOD": "Unsubscribed Email",
}

# All 9 segment types
ALL_SEGMENT_TYPES = list(SEGMENT_TYPES.keys())

# ============================================================================
# CAMPAIGN METRICS CONFIGURATION
# ============================================================================
CAMPAIGN_TYPES = {
    "PROMOTIONAL": "ONE_TIME",
    "TRANSACTIONAL": "EVENT_TRIGGERED",
}

CHANNELS = ["push", "email"]

CAMPAIGN_CATEGORIES = [
    "UK_PUSH_PROMO",
    "UK_PUSH_TXN",
    "UK_EMAIL_PROMO",
    "UK_EMAIL_TXN",
    "AE_PUSH_PROMO",
    "AE_PUSH_TXN",
    "AE_EMAIL_PROMO",
    "AE_EMAIL_TXN",
]

# ============================================================================
# METRIC FORMULAS (for documentation)
# ============================================================================
METRIC_FORMULAS = {
    "pct_receiving_comms_total": "push_received / total_users OR email_received / total_users",
    "unsubscribe_rate": "push_unsubscribed / total_users OR email_unsubscribed / total_users",
    "pct_receiving_comms_active": "active_push_received / active_users OR active_email_received / active_users",
    "comms_per_user": {
        "transactional": "tx_pn_sent / transacted_users OR tx_email_sent / transacted_users",
        "promotional": "pr_pn_sent / total_users OR pr_email_sent / total_users",
    },
    "pn_ctr": "total_pn_clicks / total_pn_sent (both promo + txn)",
    "email_open_rate": "total_email_opens / total_email_sent (both promo + txn)",
}

# ============================================================================
# CONSTANTS FROM PDF
# ============================================================================
PN_SENT_TO_IMPRESSION_RATIO = {
    "GB": 2.831,
    "AE": 1.817,
}

# ============================================================================
# DATABASE
# ============================================================================
DATABASE_PATH = os.getenv("MOENGAGE_DB_PATH", "moengage_metrics.db")

# ============================================================================
# LOGGING
# ============================================================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "moengage_dashboard.log")

# ============================================================================
# TRANSACTIONAL CAMPAIGNS CONFIG
# ============================================================================
TRANSACTIONAL_CAMPAIGNS_FILE = "transactional_campaigns.json"
