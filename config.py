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
APP_KEY_ID = os.getenv("MOENGAGE_APP_KEY_ID", "95PNUHBSYSLLJZ22PEOFMKF2")

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


# MoEngage Dashboard API Token (for fetching segment user counts)
MOENGAGE_DASHBOARD_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6IjE1NzI1MTI0NTciLCJ0eXAiOiJKV1QifQ.eyJhdXRoIjoidjIiLCJkYXRhIjoiUWVxcnV2RGpXUGJrVFZnaW9ld0t0b1o3OEgzb0RycS95TGdRbWNJdklKTmY3aXlKR0s1R2JwMmM2WWYxQUVHMEFUdWxadnZ1dkF0bnZCMlJId21jL2VMM2h3djVtQUdFbjEyam50TWptdGw5OXRMY0ppZCtLdWlhNWtRdmJwSkFrWHVYZURmVElWNWNvVTJqRTJvTnlBcVNGRFRoT2tpSW5NaStPVkovUmdvTVVYM1UzV1daVkpMekdqMklOeTJEVXBRYjdRR08wNTN3SGI1NzgvMjgwZklkRzlweVZRd21hakN1Zk5MRm1WdmFlTC92QURrOGpONlA5TWNLcEt0VWRZaVdNV0xkWERWcHhCM0x6aFJrb2RtNnJmTno2THNWblpXRERBcnk2enJTUlJpbGdpZG1haUFRZzBwVHFIclRxM0svMlhzcTR3MUhWdVQ3d1lyWEIva2lVYzlzQjRZSXZSZmJUa1ZiNDVXSnBRR25EcHVzdFNlM2ZsWmY2S0Q5U2hwUW56OGlXWkQ3VDdaN25VM3Nxc0Faek9QVkU4amk4dVZKVWlQL2Z2UDJpSXZaTnZtNXlMV0ZTcjdicFRvYjhqclh0R3V0V2dpbFNQbk1PdldTS0FNOE9NcGNLRFVGTjdUN3JmTklKaWJrL1FnVk1CdjZPQ1p5dzczMXBsSTNlQUU5S2htTUcrNzZYYW81TkcrV1lwcWxueXQ0N0ZZb1drMEFEK3NHTFBUd0p3TGM0Qmhuc0szd1JYQXZqM0NCRW84REM4Qzl1d0IvNkcxZHJUSmhFejdScmludmFMREZTd1pzT1MyK0hlWFdsTUNWWWtaWkEwc25YWkZTT0wxckhNREg0L3VRZnJ6WnBNRXBIaHF2VE9uY29VMm5oc0tJVWtEUGdacFdRaWs9IiwiZXhwIjoxNzcyNjE4ODk1LCJpYXQiOjE3NzI2MTE2OTUsImlzcyI6ImFwcC5tb2VuZ2FnZS5jb20iLCJuYmYiOjE3NzI2MTE2OTV9.j_QbCPJ1-n_yAWeCQL1RjM-RBK5rnTp-xfaZ9iAX6Z0aw9q009B93FqNUi4TXxGZ-F1kgmAcTh1QD3KMRR765pKoavqBdwwu7VaFH9p7tPjKS8dR5bdnCwmy2ZfTX8WyIbU8Izp0i4ddg97mEiXcCylUX4jiiaO8mFtYf3HEffXdocCKbQhk-qaIFpEAi-9Fd11_BYyYfvZUmTU3F1A1jV7F5tZ9YpPbHNjzRTiML-0gT7lQe6BwsU6u6nzNv-pZjOJpS2nxWA-SlWzxsZrxZB7C6maazV078HLQT-4jPwVbHXpLNVfqqm22Ysc9CKs0amj0MAgg2c28k6ggKMQvlQ"
MOENGAGE_DASHBOARD_BASE_URL = "https://dashboard-01.moengage.com"
