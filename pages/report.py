"""
Weekly Report - Full MoEngage report with all 13 sections.
Pulls campaign data from API, manual/auto-fetched segment counts,
auto-computes all derived metrics, supports period comparison, CSV export.
Completely independent from dashboard.py - no database involved.
"""
import streamlit as st
import time
import traceback
import pandas as pd
from datetime import datetime, timedelta

st.set_page_config(page_title="Weekly Report", layout="wide")
st.title("MoEngage Weekly Report")

from data_puller import DataPuller, SegmentBuilder
from moengage_api import MoEngageAPIClient
from config import EVENT_NAMES, PUSH_RECEIVED_EVENTS

# ==========================================================================
# Constants
# ==========================================================================
COUNTRIES = {"GB": "UK", "AE": "UAE"}
BUCKET_PFX = {"GB": "UK", "AE": "AE"}  # bucket name prefix per country code

SEGMENT_KEYS = [
    "TOTAL_USERS", "ACTIVE_USERS_60D", "TRANSACTED_USERS_PERIOD",
    "RECEIVED_PUSH_PERIOD", "RECEIVED_EMAIL_PERIOD",
    "ACTIVE_PUSH_PERIOD", "ACTIVE_EMAIL_PERIOD",
    "UNSUBSCRIBED_PUSH_PERIOD", "UNSUBSCRIBED_EMAIL_PERIOD",
]
SEGMENT_LABELS = {
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
ALL_BUCKETS = [
    "UK_PUSH_PROMO", "UK_PUSH_TXN", "UK_EMAIL_PROMO", "UK_EMAIL_TXN",
    "AE_PUSH_PROMO", "AE_PUSH_TXN", "AE_EMAIL_PROMO", "AE_EMAIL_TXN",
]
BUCKET_DISPLAY = {
    "UK_PUSH_PROMO": "UK Push Promotional", "UK_PUSH_TXN": "UK Push Transactional",
    "UK_EMAIL_PROMO": "UK Email Promotional", "UK_EMAIL_TXN": "UK Email Transactional",
    "AE_PUSH_PROMO": "UAE Push Promotional", "AE_PUSH_TXN": "UAE Push Transactional",
    "AE_EMAIL_PROMO": "UAE Email Promotional", "AE_EMAIL_TXN": "UAE Email Transactional",
}

# ==========================================================================
# Helpers
# ==========================================================================
def safe_pct(n, d):
    return (n / d * 100) if d else 0.0

def safe_div(n, d):
    return (n / d) if d else 0.0

def fmt_pct(v):
    return f"{v:.1f}%"

def fmt_num(v):
    if v is None:
        return "-"
    return f"{v:,}"

def fmt_dec(v):
    return f"{v:.2f}"

def seg_val(pfx, cc, key):
    """Read a segment count from session state."""
    return st.session_state.get(f"{pfx}_{cc}_{key}", 0) or 0

def agg_bucket(campaigns, stats):
    """Sum campaign stats for a list of campaigns."""
    t = {k: 0 for k in ["sent", "delivered", "open", "click", "unsubscribe", "bounced", "failed"]}
    for c in campaigns:
        cid = c.get("campaign_id") or c.get("id")
        if cid and cid in stats:
            for k in t:
                t[k] += stats[cid].get(k, 0)
    return t

def delta_str(a, b):
    """Format change between two values."""
    diff = a - b
    if diff == 0:
        return "-"
    sign = "+" if diff > 0 else ""
    if isinstance(a, float) or isinstance(b, float):
        return f"{sign}{diff:.1f}pp"
    return f"{sign}{diff:,}"


# ==========================================================================
# Auto-fetch segments via Segmentation API
# ==========================================================================
def auto_fetch_segments(cc, start_iso, end_iso, pfx, progress):
    """
    Create segments via Segmentation API, poll for user counts, delete segments.
    Stores results in session_state. Dates in YYYY-MM-DD format.
    WARNING: Slow - each segment takes ~10-30s (create, poll, delete).
    """
    api = MoEngageAPIClient()
    sb = SegmentBuilder()
    tr_b = {"type": "between", "start": start_iso, "end": end_iso}
    tr_60 = {"type": "relative", "days": 60}
    ts = int(time.time())
    cn = COUNTRIES[cc]

    # Define all 9 segment payloads for this country
    defs = [
        ("TOTAL_USERS", [sb.build_country_filter(cc)]),
        ("ACTIVE_USERS_60D", [
            sb.build_country_filter(cc),
            sb.build_event_filter(EVENT_NAMES["ORDER"], "atleast", 1,
                                  tr_60, EVENT_NAMES["ORDER_SUB_EVENT_COMPLETED"]),
        ]),
        ("TRANSACTED_USERS_PERIOD", [
            sb.build_country_filter(cc),
            sb.build_event_filter(EVENT_NAMES["ORDER"], "atleast", 1,
                                  tr_b, EVENT_NAMES["ORDER_SUB_EVENT_COMPLETED"]),
        ]),
        ("RECEIVED_PUSH_PERIOD", [
            sb.build_country_filter(cc),
            sb.build_push_received_filters(tr_b),
        ]),
        ("RECEIVED_EMAIL_PERIOD", [
            sb.build_country_filter(cc),
            sb.build_event_filter(EVENT_NAMES["MOE_EMAIL_SENT"], "atleast", 1, tr_b),
        ]),
        ("ACTIVE_PUSH_PERIOD", [
            sb.build_country_filter(cc),
            sb.build_push_received_filters(tr_b),
            sb.build_event_filter(EVENT_NAMES["ORDER"], "atleast", 1,
                                  tr_60, EVENT_NAMES["ORDER_SUB_EVENT_COMPLETED"]),
        ]),
        ("ACTIVE_EMAIL_PERIOD", [
            sb.build_country_filter(cc),
            sb.build_event_filter(EVENT_NAMES["MOE_EMAIL_SENT"], "atleast", 1, tr_b),
            sb.build_event_filter(EVENT_NAMES["ORDER"], "atleast", 1,
                                  tr_60, EVENT_NAMES["ORDER_SUB_EVENT_COMPLETED"]),
        ]),
        ("UNSUBSCRIBED_PUSH_PERIOD", [
            sb.build_country_filter(cc),
            sb.build_event_filter(EVENT_NAMES["MOE_PUSH_PERMISSION_STATE_BLOCKED"],
                                  "atleast", 1, tr_b),
        ]),
        ("UNSUBSCRIBED_EMAIL_PERIOD", [
            sb.build_country_filter(cc),
            sb.build_event_filter(EVENT_NAMES["MOE_EMAIL_UNSUBSCRIBE"],
                                  "atleast", 1, tr_b),
        ]),
    ]

    fetched = 0
    for i, (seg_key, filters) in enumerate(defs):
        progress.progress((i + 1) / len(defs),
                          f"({i+1}/{len(defs)}) {SEGMENT_LABELS[seg_key]} for {cn}...")
        try:
            payload = sb.build_segment_payload(
                f"rpt_{seg_key[:12]}_{cc}_{ts}_{i}",
                f"Report: {SEGMENT_LABELS[seg_key]} {cn}",
                filters,
            )
            count = api.query_user_count(payload)
            if count is not None:
                st.session_state[f"{pfx}_{cc}_{seg_key}"] = count
                fetched += 1
        except Exception as e:
            st.warning(f"Failed: {SEGMENT_LABELS[seg_key]} ({cn}): {e}")

    return fetched


# ==========================================================================
# Pull campaign data from API
# ==========================================================================
def pull_campaigns(start_mm, end_mm, state_key):
    """Pull campaign meta + stats. Dates in MM/DD/YYYY format."""
    puller = DataPuller()
    all_campaigns, buckets = puller.fetch_campaigns(start_mm, end_mm)

    relevant = []
    for b in ALL_BUCKETS:
        relevant.extend(buckets.get(b, []))

    stats = puller.fetch_campaign_stats(relevant, start_mm, end_mm) if relevant else {}

    bucket_stats = {}
    for b in ALL_BUCKETS + ["UNCATEGORIZED"]:
        bucket_stats[b] = agg_bucket(buckets.get(b, []), stats)

    st.session_state[state_key] = {
        "total": len(all_campaigns),
        "bucket_counts": {k: len(v) for k, v in buckets.items()},
        "bucket_stats": bucket_stats,
    }


# ==========================================================================
# Compute all metrics for a period
# ==========================================================================
def compute(pfx, cdata):
    """Compute all segment-based and campaign-based metrics. Returns {cc: {...}}."""
    m = {}
    for cc in COUNTRIES:
        s = {k: seg_val(pfx, cc, k) for k in SEGMENT_KEYS}
        bp = BUCKET_PFX[cc]
        bs = cdata.get("bucket_stats", {}) if cdata else {}
        pp = bs.get(f"{bp}_PUSH_PROMO", {})
        pt = bs.get(f"{bp}_PUSH_TXN", {})
        ep = bs.get(f"{bp}_EMAIL_PROMO", {})
        et = bs.get(f"{bp}_EMAIL_TXN", {})

        tpn_sent = pp.get("sent", 0) + pt.get("sent", 0)
        tpn_click = pp.get("click", 0) + pt.get("click", 0)
        tem_sent = ep.get("sent", 0) + et.get("sent", 0)
        tem_open = ep.get("open", 0) + et.get("open", 0)

        m[cc] = {
            # Raw segments
            "seg": s,
            # Per-bucket campaign stats
            "push_promo": pp, "push_txn": pt, "email_promo": ep, "email_txn": et,
            # Totals across promo+txn
            "tpn_sent": tpn_sent, "tpn_click": tpn_click,
            "tem_sent": tem_sent, "tem_open": tem_open,
            # ----- Computed Metrics -----
            # Section 10: % Receiving Comms (Total) - Reachable %
            "rcv_push_total": safe_pct(s["RECEIVED_PUSH_PERIOD"], s["TOTAL_USERS"]),
            "rcv_email_total": safe_pct(s["RECEIVED_EMAIL_PERIOD"], s["TOTAL_USERS"]),
            # Section 11: Unsubscribe Rate
            "unsub_push": safe_pct(s["UNSUBSCRIBED_PUSH_PERIOD"], s["TOTAL_USERS"]),
            "unsub_email": safe_pct(s["UNSUBSCRIBED_EMAIL_PERIOD"], s["TOTAL_USERS"]),
            # Section 12: % Receiving Comms (Active) - Reached %
            "rcv_push_active": safe_pct(s["ACTIVE_PUSH_PERIOD"], s["ACTIVE_USERS_60D"]),
            "rcv_email_active": safe_pct(s["ACTIVE_EMAIL_PERIOD"], s["ACTIVE_USERS_60D"]),
            # Section 13: Comms per User
            "cpu_txn_push": safe_div(pt.get("sent", 0), s["TRANSACTED_USERS_PERIOD"]),
            "cpu_txn_email": safe_div(et.get("sent", 0), s["TRANSACTED_USERS_PERIOD"]),
            "cpu_promo_push": safe_div(pp.get("sent", 0), s["TOTAL_USERS"]),
            "cpu_promo_email": safe_div(ep.get("sent", 0), s["TOTAL_USERS"]),
            # PN CTR & Email Open Rate
            "pn_ctr": safe_pct(tpn_click, tpn_sent),
            "email_open_rate": safe_pct(tem_open, tem_sent),
        }
    return m


# ==========================================================================
# Build export DataFrame
# ==========================================================================
def build_csv_df(metrics_a, metrics_b=None):
    """Build a DataFrame for CSV export."""
    rows = []

    def add_seg_row(label, key):
        row = {"Section": "User Segments", "Metric": label}
        for cc, cn in COUNTRIES.items():
            row[f"{cn} (A)"] = metrics_a[cc]["seg"][key]
            if metrics_b:
                row[f"{cn} (B)"] = metrics_b[cc]["seg"][key]
                row[f"{cn} Chg"] = metrics_a[cc]["seg"][key] - metrics_b[cc]["seg"][key]
        rows.append(row)

    def add_camp_row(label, bucket_key, stat_key):
        row = {"Section": "Campaign Stats", "Metric": f"{label} - {stat_key.title()}"}
        for cc, cn in COUNTRIES.items():
            bp = BUCKET_PFX[cc]
            bk = f"{bp}_{bucket_key}"
            bs_a = st.session_state.get("campaign_data_a", {}).get("bucket_stats", {})
            val_a = bs_a.get(bk, {}).get(stat_key, 0)
            row[f"{cn} (A)"] = val_a
            if metrics_b:
                bs_b = st.session_state.get("campaign_data_b", {}).get("bucket_stats", {})
                val_b = bs_b.get(bk, {}).get(stat_key, 0)
                row[f"{cn} (B)"] = val_b
                row[f"{cn} Chg"] = val_a - val_b
        rows.append(row)

    def add_pct_row(section, label, key_a, key_b_name=None):
        row = {"Section": section, "Metric": label}
        for cc, cn in COUNTRIES.items():
            row[f"{cn} (A)"] = f"{metrics_a[cc][key_a]:.1f}%"
            if metrics_b:
                row[f"{cn} (B)"] = f"{metrics_b[cc][key_a]:.1f}%"
                row[f"{cn} Chg"] = f"{metrics_a[cc][key_a] - metrics_b[cc][key_a]:+.1f}pp"
        rows.append(row)

    def add_dec_row(section, label, key_a):
        row = {"Section": section, "Metric": label}
        for cc, cn in COUNTRIES.items():
            row[f"{cn} (A)"] = f"{metrics_a[cc][key_a]:.2f}"
            if metrics_b:
                row[f"{cn} (B)"] = f"{metrics_b[cc][key_a]:.2f}"
                row[f"{cn} Chg"] = f"{metrics_a[cc][key_a] - metrics_b[cc][key_a]:+.2f}"
        rows.append(row)

    # Sections 1-9: Segments
    for key in SEGMENT_KEYS:
        add_seg_row(SEGMENT_LABELS[key], key)

    # Campaign stats per bucket
    for bucket_suffix in ["PUSH_PROMO", "PUSH_TXN", "EMAIL_PROMO", "EMAIL_TXN"]:
        for stat in ["sent", "delivered", "open", "click"]:
            add_camp_row(bucket_suffix.replace("_", " ").title(), bucket_suffix, stat)

    # Section 10: Reachable %
    add_pct_row("Reachable %", "Push - % Receiving Comms (Total)", "rcv_push_total")
    add_pct_row("Reachable %", "Email - % Receiving Comms (Total)", "rcv_email_total")

    # Section 11: Unsub Rate
    add_pct_row("Unsub Rate", "Push Unsubscribe Rate", "unsub_push")
    add_pct_row("Unsub Rate", "Email Unsubscribe Rate", "unsub_email")

    # Section 12: Reached %
    add_pct_row("Reached %", "Push - % Receiving Comms (Active)", "rcv_push_active")
    add_pct_row("Reached %", "Email - % Receiving Comms (Active)", "rcv_email_active")

    # Section 13: Comms per User
    add_dec_row("Comms per User", "TXN Push Comms/User", "cpu_txn_push")
    add_dec_row("Comms per User", "TXN Email Comms/User", "cpu_txn_email")
    add_dec_row("Comms per User", "PROMO Push Comms/User", "cpu_promo_push")
    add_dec_row("Comms per User", "PROMO Email Comms/User", "cpu_promo_email")

    # CTR & Open Rate
    add_pct_row("CTR / Open Rate", "PN CTR", "pn_ctr")
    add_pct_row("CTR / Open Rate", "Email Open Rate", "email_open_rate")

    return pd.DataFrame(rows)


# **************************************************************************
# UI
# **************************************************************************

# ---------- Date Ranges ----------
st.header("Date Ranges")
compare = st.checkbox("Enable period comparison (side-by-side)", value=False)

if compare:
    ca, cb = st.columns(2)
    with ca:
        st.subheader("Period A (Current)")
        a_start = st.date_input("Start", value=datetime.now() - timedelta(days=7), key="ds_a_s")
        a_end = st.date_input("End", value=datetime.now(), key="ds_a_e")
    with cb:
        st.subheader("Period B (Previous)")
        b_start = st.date_input("Start", value=datetime.now() - timedelta(days=14), key="ds_b_s")
        b_end = st.date_input("End", value=datetime.now() - timedelta(days=7), key="ds_b_e")
else:
    st.subheader("Report Period")
    c1, c2, _ = st.columns([1, 1, 2])
    a_start = c1.date_input("Start", value=datetime.now() - timedelta(days=7), key="ds_a_s")
    a_end = c2.date_input("End", value=datetime.now(), key="ds_a_e")
    b_start = b_end = None

a_mm = (a_start.strftime("%m/%d/%Y"), a_end.strftime("%m/%d/%Y"))
a_iso = (a_start.strftime("%Y-%m-%d"), a_end.strftime("%Y-%m-%d"))
if compare and b_start:
    b_mm = (b_start.strftime("%m/%d/%Y"), b_end.strftime("%m/%d/%Y"))
    b_iso = (b_start.strftime("%Y-%m-%d"), b_end.strftime("%Y-%m-%d"))

st.divider()

# ---------- Pull Campaign Data ----------
st.header("Step 1 - Pull Campaign Data")
st.caption("Fetches campaigns and stats from MoEngage Campaign APIs. No database involved.")

pcols = st.columns(2 if compare else 1)
with pcols[0]:
    if st.button("Pull Period A Campaigns", type="primary", key="btn_pull_a"):
        with st.spinner(f"Pulling campaigns for {a_mm[0]} - {a_mm[1]}..."):
            try:
                pull_campaigns(a_mm[0], a_mm[1], "campaign_data_a")
                d = st.session_state["campaign_data_a"]
                st.success(f"Period A: {d['total']} campaigns fetched, stats aggregated.")
            except Exception as e:
                st.error(f"Failed: {e}")
                st.code(traceback.format_exc())
if compare and len(pcols) > 1:
    with pcols[1]:
        if st.button("Pull Period B Campaigns", type="primary", key="btn_pull_b"):
            with st.spinner(f"Pulling campaigns for {b_mm[0]} - {b_mm[1]}..."):
                try:
                    pull_campaigns(b_mm[0], b_mm[1], "campaign_data_b")
                    d = st.session_state["campaign_data_b"]
                    st.success(f"Period B: {d['total']} campaigns fetched, stats aggregated.")
                except Exception as e:
                    st.error(f"Failed: {e}")
                    st.code(traceback.format_exc())

# Show bucket counts preview
for label, key in [("Period A", "campaign_data_a")] + ([("Period B", "campaign_data_b")] if compare else []):
    if key in st.session_state:
        with st.expander(f"{label} - bucket counts"):
            bc = st.session_state[key]["bucket_counts"]
            for b in ALL_BUCKETS + ["UNCATEGORIZED"]:
                cnt = bc.get(b, 0)
                st.write(f"**{BUCKET_DISPLAY.get(b, b)}**: {cnt}")

st.divider()

# ---------- Segment Counts ----------
st.header("Step 2 - Segment User Counts")
st.caption(
    "Enter counts manually OR click Auto-fetch to pull from MoEngage Segmentation API. "
    "Auto-fetch is slow (~2-5 min per country: creates segment, polls count, deletes)."
)

periods_to_show = [("Period A", "seg_a", a_iso)]
if compare and b_start:
    periods_to_show.append(("Period B", "seg_b", b_iso))

for plabel, pfx, (iso_s, iso_e) in periods_to_show:
    st.subheader(plabel)
    tabs = st.tabs([f"UK (GB)", f"UAE (AE)"])
    for tab, (cc, cn) in zip(tabs, COUNTRIES.items()):
        with tab:
            # Auto-fetch button
            if st.button(
                f"Auto-fetch {cn} segments via API",
                key=f"af_{pfx}_{cc}",
                help=f"Creates 9 segments via MoEngage Segmentation API for {cn}, polls counts, then deletes. Takes ~2-5 min.",
            ):
                prog = st.progress(0, "Starting...")
                with st.spinner(f"Fetching segments for {cn}..."):
                    n = auto_fetch_segments(cc, iso_s, iso_e, pfx, prog)
                st.success(f"Fetched {n}/9 segment counts for {cn}.")
                st.rerun()

            # Manual input grid (3 columns)
            c1, c2, c3 = st.columns(3)
            for i, sk in enumerate(SEGMENT_KEYS):
                col = [c1, c2, c3][i % 3]
                state_key = f"{pfx}_{cc}_{sk}"
                col.number_input(
                    SEGMENT_LABELS[sk],
                    min_value=0,
                    value=st.session_state.get(state_key, 0) or 0,
                    step=1,
                    key=state_key,
                    help=f"{SEGMENT_LABELS[sk]} for {cn}",
                )

st.divider()

# **************************************************************************
# REPORT OUTPUT
# **************************************************************************
st.header("Report")

cdata_a = st.session_state.get("campaign_data_a")
cdata_b = st.session_state.get("campaign_data_b") if compare else None

ma = compute("seg_a", cdata_a)
mb = compute("seg_b", cdata_b) if compare else None


def render_report(m, cdata, period_label):
    """Render all 13 report sections for one period."""

    # ---- Sections 1-9: User Segment Counts ----
    st.markdown(f"#### Sections 1-9: User Segment Counts")
    seg_rows = []
    for sk in SEGMENT_KEYS:
        row = {"Metric": SEGMENT_LABELS[sk]}
        for cc, cn in COUNTRIES.items():
            row[cn] = fmt_num(m[cc]["seg"][sk])
        seg_rows.append(row)
    st.dataframe(
        pd.DataFrame(seg_rows).set_index("Metric"),
        use_container_width=True,
    )

    # ---- Section 10: Campaign Performance Summary ----
    st.markdown("#### Section 10: Campaign Stats per Bucket")
    if cdata:
        camp_rows = []
        for cc, cn in COUNTRIES.items():
            bp = BUCKET_PFX[cc]
            for suffix, short in [("PUSH_PROMO", "Push Promo"), ("PUSH_TXN", "Push TXN"),
                                  ("EMAIL_PROMO", "Email Promo"), ("EMAIL_TXN", "Email TXN")]:
                bk = f"{bp}_{suffix}"
                bs = cdata.get("bucket_stats", {}).get(bk, {})
                camp_rows.append({
                    "Country": cn, "Bucket": short,
                    "Sent": fmt_num(bs.get("sent", 0)),
                    "Delivered": fmt_num(bs.get("delivered", 0)),
                    "Opens": fmt_num(bs.get("open", 0)),
                    "Clicks": fmt_num(bs.get("click", 0)),
                    "Unsubs": fmt_num(bs.get("unsubscribe", 0)),
                    "Bounced": fmt_num(bs.get("bounced", 0)),
                })
        st.dataframe(
            pd.DataFrame(camp_rows).set_index(["Country", "Bucket"]),
            use_container_width=True,
        )
    else:
        st.info("No campaign data pulled yet. Click 'Pull Period Campaigns' above.")

    # ---- Section 11: % Receiving Comms (Total) - Reachable % ----
    st.markdown("#### Section 11: % Receiving Comms (Total) - Reachable %")
    st.caption("received_push / total_users, received_email / total_users")
    rcv_rows = [
        {"Channel": "Push", **{cn: fmt_pct(m[cc]["rcv_push_total"]) for cc, cn in COUNTRIES.items()}},
        {"Channel": "Email", **{cn: fmt_pct(m[cc]["rcv_email_total"]) for cc, cn in COUNTRIES.items()}},
    ]
    st.dataframe(pd.DataFrame(rcv_rows).set_index("Channel"), use_container_width=True)

    # ---- Section 12: Unsubscribe Rate ----
    st.markdown("#### Section 12: Unsubscribe Rate")
    st.caption("unsubscribed_push / total_users, unsubscribed_email / total_users")
    unsub_rows = [
        {"Channel": "Push", **{cn: fmt_pct(m[cc]["unsub_push"]) for cc, cn in COUNTRIES.items()}},
        {"Channel": "Email", **{cn: fmt_pct(m[cc]["unsub_email"]) for cc, cn in COUNTRIES.items()}},
    ]
    st.dataframe(pd.DataFrame(unsub_rows).set_index("Channel"), use_container_width=True)

    # ---- Section 13: % Receiving Comms (Active) - Reached % ----
    st.markdown("#### Section 13: % Receiving Comms (Active) - Reached %")
    st.caption("active_push / active_users_60d, active_email / active_users_60d")
    reached_rows = [
        {"Channel": "Push", **{cn: fmt_pct(m[cc]["rcv_push_active"]) for cc, cn in COUNTRIES.items()}},
        {"Channel": "Email", **{cn: fmt_pct(m[cc]["rcv_email_active"]) for cc, cn in COUNTRIES.items()}},
    ]
    st.dataframe(pd.DataFrame(reached_rows).set_index("Channel"), use_container_width=True)

    # ---- Comms per User ----
    st.markdown("#### Comms per User")
    st.caption("TXN: sent / transacted_users | PROMO: sent / total_users")
    cpu_rows = [
        {"Metric": "TXN Push Comms/User",
         **{cn: fmt_dec(m[cc]["cpu_txn_push"]) for cc, cn in COUNTRIES.items()}},
        {"Metric": "TXN Email Comms/User",
         **{cn: fmt_dec(m[cc]["cpu_txn_email"]) for cc, cn in COUNTRIES.items()}},
        {"Metric": "PROMO Push Comms/User",
         **{cn: fmt_dec(m[cc]["cpu_promo_push"]) for cc, cn in COUNTRIES.items()}},
        {"Metric": "PROMO Email Comms/User",
         **{cn: fmt_dec(m[cc]["cpu_promo_email"]) for cc, cn in COUNTRIES.items()}},
    ]
    st.dataframe(pd.DataFrame(cpu_rows).set_index("Metric"), use_container_width=True)

    # ---- PN CTR & Email Open Rate ----
    st.markdown("#### PN CTR & Email Open Rate")
    st.caption("PN CTR = total_pn_clicks / total_pn_sent | Email Open Rate = total_email_opens / total_email_sent")
    ctr_rows = [
        {"Metric": "PN CTR",
         **{cn: fmt_pct(m[cc]["pn_ctr"]) for cc, cn in COUNTRIES.items()}},
        {"Metric": "Email Open Rate",
         **{cn: fmt_pct(m[cc]["email_open_rate"]) for cc, cn in COUNTRIES.items()}},
    ]
    st.dataframe(pd.DataFrame(ctr_rows).set_index("Metric"), use_container_width=True)

    # Raw totals for reference
    with st.expander("Raw campaign totals"):
        for cc, cn in COUNTRIES.items():
            st.write(f"**{cn}**: PN Sent={fmt_num(m[cc]['tpn_sent'])}, "
                     f"PN Clicks={fmt_num(m[cc]['tpn_click'])}, "
                     f"Email Sent={fmt_num(m[cc]['tem_sent'])}, "
                     f"Email Opens={fmt_num(m[cc]['tem_open'])}")


# ---------- Render report(s) ----------
if compare and mb:
    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader(f"Period A: {a_mm[0]} - {a_mm[1]}")
        render_report(ma, cdata_a, "Period A")
    with col_b:
        st.subheader(f"Period B: {b_mm[0]} - {b_mm[1]}")
        render_report(mb, cdata_b, "Period B")

    # ---------- Comparison table ----------
    st.divider()
    st.header("Period Comparison (A vs B)")
    comp_rows = []
    # Segments
    for sk in SEGMENT_KEYS:
        row = {"Metric": SEGMENT_LABELS[sk]}
        for cc, cn in COUNTRIES.items():
            va = ma[cc]["seg"][sk]
            vb = mb[cc]["seg"][sk]
            row[f"{cn} (A)"] = fmt_num(va)
            row[f"{cn} (B)"] = fmt_num(vb)
            row[f"{cn} Chg"] = delta_str(va, vb)
        comp_rows.append(row)
    # Computed %
    pct_metrics = [
        ("% Receiving Comms (Total) - Push", "rcv_push_total"),
        ("% Receiving Comms (Total) - Email", "rcv_email_total"),
        ("Unsubscribe Rate - Push", "unsub_push"),
        ("Unsubscribe Rate - Email", "unsub_email"),
        ("% Receiving Comms (Active) - Push", "rcv_push_active"),
        ("% Receiving Comms (Active) - Email", "rcv_email_active"),
        ("PN CTR", "pn_ctr"),
        ("Email Open Rate", "email_open_rate"),
    ]
    for label, key in pct_metrics:
        row = {"Metric": label}
        for cc, cn in COUNTRIES.items():
            va = ma[cc][key]
            vb = mb[cc][key]
            row[f"{cn} (A)"] = fmt_pct(va)
            row[f"{cn} (B)"] = fmt_pct(vb)
            row[f"{cn} Chg"] = f"{va - vb:+.1f}pp"
        comp_rows.append(row)
    # Comms per user
    dec_metrics = [
        ("TXN Push Comms/User", "cpu_txn_push"),
        ("TXN Email Comms/User", "cpu_txn_email"),
        ("PROMO Push Comms/User", "cpu_promo_push"),
        ("PROMO Email Comms/User", "cpu_promo_email"),
    ]
    for label, key in dec_metrics:
        row = {"Metric": label}
        for cc, cn in COUNTRIES.items():
            va = ma[cc][key]
            vb = mb[cc][key]
            row[f"{cn} (A)"] = fmt_dec(va)
            row[f"{cn} (B)"] = fmt_dec(vb)
            row[f"{cn} Chg"] = f"{va - vb:+.2f}"
        comp_rows.append(row)

    st.dataframe(
        pd.DataFrame(comp_rows).set_index("Metric"),
        use_container_width=True,
        height=700,
    )
else:
    render_report(ma, cdata_a, "Period A")

# ---------- CSV Download ----------
st.divider()
st.header("Export")

csv_df = build_csv_df(ma, mb)
csv_data = csv_df.to_csv(index=False)
st.download_button(
    label="Download Report CSV",
    data=csv_data,
    file_name=f"moengage_report_{a_start.strftime('%Y%m%d')}_{a_end.strftime('%Y%m%d')}.csv",
    mime="text/csv",
    type="primary",
)
with st.expander("Preview CSV data"):
    st.dataframe(csv_df, use_container_width=True, height=400)
