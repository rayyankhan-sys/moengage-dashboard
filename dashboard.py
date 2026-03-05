"""
MoEngage Automation Dashboard
Streamlit-based dashboard for viewing and pulling MoEngage metrics
Clean flow: Pick dates ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Pull Data ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ View ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Export
"""
import json
import base64
import logging
import os
from datetime import datetime, timedelta

import streamlit as st
import pandas as pd
from pathlib import Path

from data_puller import DataPuller
from database import MoEngageDatabase
from report_generator import ReportGenerator
from config import (
    TRANSACTIONAL_CAMPAIGNS_FILE,
    DATABASE_PATH,
    COUNTRIES,
    COUNTRY_CODES,
    PN_SENT_TO_IMPRESSION_RATIO,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Streamlit page configuration
st.set_page_config(
    page_title="MoEngage Dashboard ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Aspora",
    page_icon="ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for cleaner look
st.markdown("""
<style>
    .block-container { padding-top: 1rem; }
    div[data-testid="stMetric"] {
        background-color: #1A1F2E;
        border: 1px solid #2D3348;
        padding: 12px 16px;
        border-radius: 8px;
    }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        padding: 8px 20px;
        border-radius: 4px 4px 0 0;
    }
    .section-header {
        background-color: #1A1F2E;
        padding: 12px 16px;
        border-radius: 6px;
        margin: 20px 0 12px 0;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if "data_pulled" not in st.session_state:
    st.session_state.data_pulled = False
if "pull_summary" not in st.session_state:
    st.session_state.pull_summary = None
if "report_bytes" not in st.session_state:
    st.session_state.report_bytes = None


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def load_transactional_campaigns() -> list:
    """Load transactional campaign IDs from config file"""
    if os.path.exists(TRANSACTIONAL_CAMPAIGNS_FILE):
        try:
            with open(TRANSACTIONAL_CAMPAIGNS_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading transactional campaigns: {e}")
    return []


def save_transactional_campaigns(campaigns: list) -> None:
    """Save transactional campaign IDs to config file"""
    try:
        with open(TRANSACTIONAL_CAMPAIGNS_FILE, "w") as f:
            json.dump(campaigns, f, indent=2)
        logger.info(f"Saved {len(campaigns)} transactional campaigns")
    except Exception as e:
        logger.error(f"Error saving transactional campaigns: {e}")


def fmt_pct(value: float) -> str:
    """Format percentage value"""
    if value is None:
        return "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ"
    return f"{value:.2f}%"


def fmt_num(value) -> str:
    """Format number with thousands separator"""
    if value is None:
        return "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ"
    return f"{int(value):,}"


def safe_div(numerator, denominator, as_pct=False):
    """Safe division ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ returns None if denominator is 0 or None"""
    if not denominator or numerator is None:
        return None
    result = numerator / denominator
    return result * 100 if as_pct else result


# ============================================================================
# MAIN DASHBOARD
# ============================================================================


def safe_sub(a, b):
    """Safely subtract two values, treating None as 0."""
    a = a if a is not None else 0
    b = b if b is not None else 0
    return a - b

def page_dashboard():
    """Main dashboard page"""
    st.title("ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ MoEngage Comms Dashboard")
    st.caption("Aspora ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ UK & UAE Communications Metrics")

    # --- Bookmarklet count receiver ---
    try:
        qp = st.experimental_get_query_params()
        if "update_counts" in qp:
            raw_b64 = qp["update_counts"][0]
            count_json = json.loads(base64.b64decode(raw_b64).decode("utf-8"))
            _type_map = {
                "GB_TOTAL_USERS": ("TOTAL_USERS", "GB"),
                "GB_ACTIVE_USERS_60D": ("ACTIVE_USERS_60D", "GB"),
                "GB_TRANSACTED_USERS_PERIOD": ("TRANSACTED_USERS_PERIOD", "GB"),
                "GB_RECEIVED_PUSH_PERIOD": ("RECEIVED_PUSH_PERIOD", "GB"),
                "GB_RECEIVED_EMAIL_PERIOD": ("RECEIVED_EMAIL_PERIOD", "GB"),
                "GB_ACTIVE_PUSH_PERIOD": ("ACTIVE_PUSH_PERIOD", "GB"),
                "GB_ACTIVE_EMAIL_PERIOD": ("ACTIVE_EMAIL_PERIOD", "GB"),
                "GB_UNSUBSCRIBED_PUSH_PERIOD": ("UNSUBSCRIBED_PUSH_PERIOD", "GB"),
                "GB_UNSUBSCRIBED_EMAIL_PERIOD": ("UNSUBSCRIBED_EMAIL_PERIOD", "GB"),
                "AE_TOTAL_USERS": ("TOTAL_USERS", "AE"),
                "AE_ACTIVE_USERS_60D": ("ACTIVE_USERS_60D", "AE"),
                "AE_TRANSACTED_USERS_PERIOD": ("TRANSACTED_USERS_PERIOD", "AE"),
                "AE_RECEIVED_PUSH_PERIOD": ("RECEIVED_PUSH_PERIOD", "AE"),
                "AE_RECEIVED_EMAIL_PERIOD": ("RECEIVED_EMAIL_PERIOD", "AE"),
                "AE_ACTIVE_PUSH_PERIOD": ("ACTIVE_PUSH_PERIOD", "AE"),
                "AE_ACTIVE_EMAIL_PERIOD": ("ACTIVE_EMAIL_PERIOD", "AE"),
                "AE_UNSUBSCRIBED_PUSH_PERIOD": ("UNSUBSCRIBED_PUSH_PERIOD", "AE"),
                "AE_UNSUBSCRIBED_EMAIL_PERIOD": ("UNSUBSCRIBED_EMAIL_PERIOD", "AE"),
            }
            db = MoEngageDatabase()
            today = datetime.now()
            period_end = today.strftime("%Y-%m-%d")
            period_start = today.replace(day=1).strftime("%Y-%m-%d")
            saved = 0
            for seg_id, info in count_json.items():
                bm_type = info.get("type", "")
                mapped = _type_map.get(bm_type)
                if not mapped:
                    continue
                seg_type, country = mapped
                try:
                    db.upsert_segment_metric(
                        segment_type=seg_type,
                        country=country,
                        user_count=info.get("count", 0),
                        segment_id=seg_id,
                        period_start=period_start,
                        period_end=period_end,
                        raw_json=json.dumps(info),
                    )
                    saved += 1
                except Exception as e:
                    logger.error(f"Failed to save {bm_type}: {e}")
            st.success(f"Bookmarklet: saved {saved} segment counts!")
            st.experimental_set_query_params()
    except Exception as e:
        if "update_counts" in str(e):
            logger.error(f"Bookmarklet handler error: {e}")


    db = MoEngageDatabase()

    # ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Auto-Import from Bookmarklet ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ
    qp = st.experimental_get_query_params()
    if qp.get("auto_import", [None])[0] == "1":
        _fields = [
            ("uk_total", "TOTAL_USERS", "GB"),
            ("uk_active", "ACTIVE_USERS_60D", "GB"),
            ("uk_transacted", "TRANSACTED_USERS_PERIOD", "GB"),
            ("uk_recv_push", "RECEIVED_PUSH_PERIOD", "GB"),
            ("uk_recv_email", "RECEIVED_EMAIL_PERIOD", "GB"),
            ("uk_active_push", "ACTIVE_PUSH_PERIOD", "GB"),
            ("uk_active_email", "ACTIVE_EMAIL_PERIOD", "GB"),
            ("uk_unsub_push", "UNSUBSCRIBED_PUSH_PERIOD", "GB"),
            ("uk_unsub_email", "UNSUBSCRIBED_EMAIL_PERIOD", "GB"),
            ("ae_total", "TOTAL_USERS", "AE"),
            ("ae_active", "ACTIVE_USERS_60D", "AE"),
            ("ae_transacted", "TRANSACTED_USERS_PERIOD", "AE"),
            ("ae_recv_push", "RECEIVED_PUSH_PERIOD", "AE"),
            ("ae_recv_email", "RECEIVED_EMAIL_PERIOD", "AE"),
            ("ae_active_push", "ACTIVE_PUSH_PERIOD", "AE"),
            ("ae_active_email", "ACTIVE_EMAIL_PERIOD", "AE"),
            ("ae_unsub_push", "UNSUBSCRIBED_PUSH_PERIOD", "AE"),
            ("ae_unsub_email", "UNSUBSCRIBED_EMAIL_PERIOD", "AE"),
        ]
        _ps = qp.get("ps", [""])[0]
        _pe = qp.get("pe", [""])[0]
        if _ps and _pe:
            _saved = 0
            for _param, _seg_type, _country in _fields:
                _val = int(qp.get(_param, ["0"])[0] or "0")
                if _val > 0:
                    db.upsert_segment_metric(
                        segment_type=_seg_type,
                        country=_country,
                        user_count=_val,
                        segment_id="auto_import",
                        period_start=_ps,
                        period_end=_pe,
                    )
                    _saved += 1
            st.success(f"Auto-imported {_saved} segment counts for {_ps} to {_pe}")
            st.experimental_set_query_params()
        else:
            st.warning("Auto-import needs ps (period start) and pe (period end) params.")

    # ==================================================================
    # TOP CONTROL BAR ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Date Pickers + Actions
    # ==================================================================
    st.markdown("---")

    # Row 1: Current period + Comparison period
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown("**Current Period**")
        period_start = st.date_input(
            "From",
            value=datetime.now().replace(day=1),
            key="period_start",
        )
        period_start_str = period_start.strftime("%Y-%m-%d")

    with col2:
        st.markdown("** **")
        period_end = st.date_input(
            "To",
            value=datetime.now(),
            key="period_end",
        )
        period_end_str = period_end.strftime("%Y-%m-%d")

    with col3:
        st.markdown("**Comparison Period** *(optional)*")
        # Default comparison = previous month same range
        default_comp_start = (period_start - timedelta(days=30)).replace(day=1)
        comp_start = st.date_input(
            "From ",
            value=default_comp_start,
            key="comp_start",
        )
        comp_start_str = comp_start.strftime("%Y-%m-%d") if comp_start else None

    with col4:
        st.markdown("** **")
        default_comp_end = period_start - timedelta(days=1)
        comp_end = st.date_input(
            "To ",
            value=default_comp_end,
            key="comp_end",
        )
        comp_end_str = comp_end.strftime("%Y-%m-%d") if comp_end else None

    # Row 2: Action buttons
    st.markdown("")
    btn_col1, btn_col2, btn_col3, btn_col4, spacer = st.columns([1.5, 1.5, 1.5, 1.8, 3.7])

    with btn_col1:
        pull_clicked = st.button(
            "ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Pull Data",
            key="pull_data",
            use_container_width=True,
            type="primary",
        )

    with btn_col2:
        export_clicked = st.button(
            "ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¥ Export Report",
            key="export_report",
            use_container_width=True,
        )

    with btn_col3:
        dry_run_clicked = st.button(
            "ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ§ÃÂÃÂÃÂÃÂª Dry Run",
            key="dry_run",
            use_container_width=True,
            help="Test the pull without making real API calls",
        )
    with btn_col4:
        camp_only_clicked = st.button(
            "\U0001f4e8 Pull Campaigns",
            key="pull_campaigns_only",
            use_container_width=True,
            help="Fetch campaign data only (skip slow segment creation)",
        )

    # Handle Pull Data
    if pull_clicked:
        progress_bar = st.progress(0, text="Initializing...")

        def update_progress(current, total, description):
            pct = min(current / max(total, 1), 1.0)
            progress_bar.progress(pct, text=description)

        with st.spinner("Pulling data from MoEngage..."):
            try:
                puller = DataPuller()
                summary = puller.pull_all_data(
                    period_start_str,
                    period_end_str,
                    progress_callback=update_progress,
                )
                # Fetch real counts from MoEngage dashboard API
                progress_bar.progress(0.5, text="Fetching real user counts from dashboard...")
                try:
                    count_results = puller.fetch_dashboard_counts(
                        period_start_str, period_end_str,
                        progress_callback=update_progress,
                    )
                    if count_results:
                        st.success(f"Fetched {len(count_results)} segment counts from dashboard API")
                    else:
                        st.warning("Could not fetch counts from dashboard API (token may be expired)")
                except Exception as e:
                    logger.warning(f"Dashboard count fetch failed: {e}")
                    st.warning(f"Count fetch from dashboard API failed: {e}")
                st.session_state.pull_summary = summary
                st.session_state.data_pulled = True
                progress_bar.progress(1.0, text="Done!")

                # Show quick summary
                seg_count = len(summary.get("segments", {}))
                camp_count = summary.get("campaigns", {}).get("fetched", 0)
                err_count = len(summary.get("errors", []))
                total_time = summary.get("total_time_seconds", 0)

                if err_count == 0:
                    st.success(
                        f"Pull complete ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ {seg_count} segments, {camp_count} campaigns "
                        f"in {total_time:.1f}s"
                    )
                else:
                    st.warning(
                        f"Pull complete with {err_count} error(s) ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ "
                        f"{seg_count} segments, {camp_count} campaigns"
                    )
                st.rerun()
            except Exception as e:
                st.error(f"Error pulling data: {e}")
                logger.error(f"Data pull error: {e}", exc_info=True)

    # Handle Pull Campaigns Only
    if camp_only_clicked:
        progress_bar = st.progress(0, text="Initializing campaign pull...")
        with st.spinner("Fetching campaigns from MoEngage..."):
            try:
                import time as _time
                puller = DataPuller()
                pull_id = f"camp_{int(_time.time())}"
                # Step 1: Fetch campaigns
                progress_bar.progress(0.2, text="Fetching campaign list...")
                campaigns, category_buckets = puller.fetch_campaigns(period_start_str, period_end_str)
                camp_count = len(campaigns)
                categorized = sum(len(v) for k, v in category_buckets.items() if k != "UNCATEGORIZED")
                progress_bar.progress(0.4, text=f"Found {camp_count} campaigns. Fetching stats...")
                # Step 2: Fetch stats
                all_stats = puller.fetch_campaign_stats(campaigns, period_start_str, period_end_str)
                progress_bar.progress(0.7, text=f"Got stats for {len(all_stats)} campaigns. Storing...")
                # Step 3: Store in database
                db = MoEngageDatabase()
                stored = 0
                for campaign in campaigns:
                    campaign_id = campaign.get("id") or campaign.get("campaign_id")
                    campaign_name = campaign.get("name")
                    country = puller._detect_country(campaign)
                    channel = puller._detect_channel(campaign)
                    campaign_type = puller._detect_campaign_type(campaign)
                    created_date = campaign.get("created_date")
                    stats = all_stats.get(campaign_id, {})
                    try:
                        db.upsert_campaign_metric(
                            campaign_id=campaign_id,
                            campaign_name=campaign_name,
                            country=country,
                            channel=channel,
                            campaign_type=campaign_type,
                            sent=stats.get("sent", 0),
                            delivered=stats.get("delivered", 0),
                            open=stats.get("open", 0),
                            click=stats.get("click", 0),
                            unsubscribe=stats.get("unsubscribe", 0),
                            bounced=stats.get("bounced", 0),
                            failed=stats.get("failed", 0),
                            created_date=created_date,
                            period_start=period_start_str,
                            period_end=period_end_str,
                            raw_json=json.dumps(campaign),
                        )
                        stored += 1
                    except Exception as e:
                        logger.error(f"Error storing campaign {campaign_id}: {e}")
                progress_bar.progress(1.0, text="Done!")
                st.success(f"Campaigns pulled: {stored} stored, {len(all_stats)} with stats, {categorized} categorized")
                st.rerun()
            except Exception as e:
                st.error(f"Error fetching campaigns: {e}")
                logger.error(f"Campaign-only pull error: {e}", exc_info=True)

    # Handle Dry Run
    if dry_run_clicked:
        with st.spinner("Running dry run..."):
            try:
                puller = DataPuller(dry_run=True)
                summary = puller.pull_all_data(period_start_str, period_end_str)
                st.info(
                    f"Dry run complete ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ would create {len(summary.get('segments', {}))} segments. "
                    f"Check logs for payload details."
                )
            except Exception as e:
                st.error(f"Dry run error: {e}")

    # Handle Export
    if export_clicked:
        segment_metrics = db.get_all_segment_metrics(period_start_str, period_end_str)
        campaign_metrics = db.get_all_campaign_metrics(period_start_str, period_end_str)

        if not segment_metrics and not campaign_metrics:
            st.warning("No data available for the selected period. Pull data first.")
        else:
            with st.spinner("Generating Excel report..."):
                try:
                    report_gen = ReportGenerator()
                    report_bytes = report_gen.generate_report(
                        period_start_str,
                        period_end_str,
                        comp_start_str,
                        comp_end_str,
                    )
                    st.session_state.report_bytes = report_bytes
                except Exception as e:
                    st.error(f"Error generating report: {e}")

    # Show download button if report is ready
    if st.session_state.report_bytes:
        st.download_button(
            label="ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂ¬ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¯ÃÂÃÂÃÂÃÂ¸ÃÂÃÂÃÂÃÂ Download Weekly Report (.xlsx)",
            data=st.session_state.report_bytes,
            file_name=f"MoEngage_Report_{period_start_str}_to_{period_end_str}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=False,
        )

    st.markdown("---")

    # ==================================================================
    # TABS ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Metrics, Comparison, Campaigns, Pull History, Settings
    # ==================================================================
    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        ["ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Metrics", "ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Comparison", "ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Campaigns", "ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Pull History", "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¯ÃÂÃÂÃÂÃÂ¸ÃÂÃÂÃÂÃÂ Settings"]
    )

    # Fetch current data from DB
    segment_metrics = db.get_all_segment_metrics(period_start_str, period_end_str)
    campaign_metrics = db.get_all_campaign_metrics(period_start_str, period_end_str)

    # Build lookup dicts
    seg = {}
    if segment_metrics:
        seg = {
            (s["segment_type"], s["country"]): (s.get("user_count") or 0)
            for s in segment_metrics
        }

    # Also fetch comparison data
    comp_seg = {}
    comp_campaign_metrics = []
    if comp_start_str and comp_end_str:
        comp_segment_metrics = db.get_all_segment_metrics(comp_start_str, comp_end_str)
        comp_campaign_metrics = db.get_all_campaign_metrics(comp_start_str, comp_end_str)
        if comp_segment_metrics:
            comp_seg = {
                (s["segment_type"], s["country"]): (s.get("user_count") or 0)
                for s in comp_segment_metrics
            }

    # ==================================================================
    # TAB 1: METRICS
    # ==================================================================
    with tab1:
        if not segment_metrics:
            st.info("No data for the selected period. Use **Pull Data** above to fetch metrics.")
        else:
            # --- UK SECTION ---
            st.subheader("ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¬ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ§ United Kingdom")
            _render_country_metrics("GB", seg, campaign_metrics)

            st.markdown("---")

            # --- UAE SECTION ---
            st.subheader("ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂª United Arab Emirates")
            _render_country_metrics("AE", seg, campaign_metrics)

    # ==================================================================
    # TAB 2: COMPARISON
    # ==================================================================
    with tab2:
        _render_comparison_tab(db)

    # ==================================================================
    # TAB 3: CAMPAIGNS
    # ==================================================================
    with tab3:
        if not campaign_metrics:
            st.info("No campaign data available. Pull data first.")
        else:
            campaign_df = pd.DataFrame(campaign_metrics)

            # Filters
            fcol1, fcol2, fcol3 = st.columns(3)
            with fcol1:
                sel_country = st.multiselect(
                    "Country",
                    options=sorted(campaign_df["country"].dropna().unique().tolist()),
                    default=sorted(campaign_df["country"].dropna().unique().tolist()),
                )
            with fcol2:
                sel_channel = st.multiselect(
                    "Channel",
                    options=sorted(campaign_df["channel"].dropna().unique().tolist()),
                    default=sorted(campaign_df["channel"].dropna().unique().tolist()),
                )
            with fcol3:
                sel_type = st.multiselect(
                    "Type",
                    options=sorted(campaign_df["campaign_type"].dropna().unique().tolist()),
                    default=sorted(campaign_df["campaign_type"].dropna().unique().tolist()),
                )

            filtered = campaign_df[
                (campaign_df["country"].isin(sel_country))
                & (campaign_df["channel"].isin(sel_channel))
                & (campaign_df["campaign_type"].isin(sel_type))
            ]

            display_cols = [
                "campaign_name", "country", "channel", "campaign_type",
                "sent", "delivered", "open", "click", "unsubscribe", "bounced",
            ]
            available_cols = [c for c in display_cols if c in filtered.columns]

            st.dataframe(filtered[available_cols], use_container_width=True, height=400)

            # Aggregate stats
            st.markdown("---")
            mcol1, mcol2, mcol3, mcol4, mcol5 = st.columns(5)
            with mcol1:
                st.metric("Total Sent", fmt_num(filtered["sent"].sum()))
            with mcol2:
                st.metric("Total Delivered", fmt_num(filtered["delivered"].sum()))
            with mcol3:
                st.metric("Total Opens", fmt_num(filtered["open"].sum()))
            with mcol4:
                st.metric("Total Clicks", fmt_num(filtered["click"].sum()))
            with mcol5:
                ctr = safe_div(filtered["click"].sum(), filtered["sent"].sum(), as_pct=True)
                st.metric("CTR", fmt_pct(ctr))

    # ==================================================================
    # TAB 4: PULL HISTORY
    # ==================================================================
    with tab4:
        history = db.get_pull_history(limit=10)
        if history:
            hist_df = pd.DataFrame(history)
            display_cols = [c for c in ["pull_id", "period_start", "period_end",
                                         "status", "segments_fetched", "campaigns_fetched",
                                         "started_at", "completed_at"] if c in hist_df.columns]
            st.dataframe(hist_df[display_cols], use_container_width=True)
        else:
            st.info("No pull history yet.")

        # Show last pull summary if available
        if st.session_state.pull_summary:
            summary = st.session_state.pull_summary
            with st.expander("Last Pull Details", expanded=False):
                st.json(summary)

    # ==================================================================
    # TAB 5: SETTINGS
    # ==================================================================
    with tab5:
        _render_settings(db)


def _render_country_metrics(
    country_code: str,
    seg: dict,
    campaign_metrics: list,
):
    """
    Render comprehensive metrics for a single country.
    Includes raw data, computed metrics, and performance metrics.
    """
    country_name = COUNTRIES.get(country_code, country_code)
    pn_ratio = PN_SENT_TO_IMPRESSION_RATIO.get(country_code, 1.0)

    # ==================================================================
    # RAW DATA ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ User Base
    # ==================================================================
    st.markdown('<div class="section-header">ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¦ RAW DATA ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ User Base</div>', unsafe_allow_html=True)

    total_users = seg.get(("TOTAL_USERS", country_code), 0)
    active_users = seg.get(("ACTIVE_USERS_60D", country_code), 0)
    transacted_users = seg.get(("TRANSACTED_USERS_PERIOD", country_code), 0)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Total Users", fmt_num(total_users))
    with c2:
        st.metric("Active Users (60d)", fmt_num(active_users))
    with c3:
        st.metric("Transacted Users", fmt_num(transacted_users))

    # ==================================================================
    # RAW DATA ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Segment Counts
    # ==================================================================
    st.markdown('<div class="section-header">ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¦ RAW DATA ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Segment Counts</div>', unsafe_allow_html=True)

    recv_push = seg.get(("RECEIVED_PUSH_PERIOD", country_code), 0)
    recv_email = seg.get(("RECEIVED_EMAIL_PERIOD", country_code), 0)
    active_email = seg.get(("ACTIVE_EMAIL_PERIOD", country_code), 0)
    unsub_push = seg.get(("UNSUBSCRIBED_PUSH_PERIOD", country_code), 0)
    unsub_email = seg.get(("UNSUBSCRIBED_EMAIL_PERIOD", country_code), 0)

    # Computed reachability
    push_reachable = total_users - unsub_push
    email_reachable = total_users - unsub_email

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        st.metric("Unique Users Received Push", fmt_num(recv_push))
    with c2:
        st.metric("Unique Users Received Email", fmt_num(recv_email))
    with c3:
        st.metric("Active Users Received Email", fmt_num(active_email))
    with c4:
        st.metric("Push Reachable (Raw)", fmt_num(push_reachable))
    with c5:
        st.metric("Email Reachable (Raw)", fmt_num(email_reachable))
    with c6:
        st.metric("Unsub Rate ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Push", fmt_num(unsub_push))

    c7, c8 = st.columns(2)
    with c7:
        st.metric("Unsubscribed Email", fmt_num(unsub_email))

    # ==================================================================
    # RAW DATA ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Promotional & Transactional
    # ==================================================================
    st.markdown('<div class="section-header">ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¦ RAW DATA ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Campaign Metrics</div>', unsafe_allow_html=True)

    # Filter campaigns for this country
    country_campaigns = [c for c in campaign_metrics if c.get("country") == country_code]

    # Push campaigns
    push_promo = [c for c in country_campaigns if c.get("channel") == "push" and c.get("campaign_type") == "promotional"]
    push_txn = [c for c in country_campaigns if c.get("channel") == "push" and c.get("campaign_type") == "transactional"]

    promo_push_impressions = sum(c.get("sent", 0) for c in push_promo)
    promo_push_clicks = sum(c.get("click", 0) for c in push_promo)
    txn_push_sent = sum(c.get("sent", 0) for c in push_txn)
    push_campaign_count = len(push_promo)

    # Email campaigns
    email_promo = [c for c in country_campaigns if c.get("channel") == "email" and c.get("campaign_type") == "promotional"]
    email_txn = [c for c in country_campaigns if c.get("channel") == "email" and c.get("campaign_type") == "transactional"]

    promo_email_sent = sum(c.get("sent", 0) for c in email_promo)
    promo_email_opens = sum(c.get("open", 0) for c in email_promo)
    promo_email_clicks = sum(c.get("click", 0) for c in email_promo)
    txn_email_sent = sum(c.get("sent", 0) for c in email_txn)
    email_campaign_count = len(email_promo)

    # Estimated push sent
    est_promo_push_sent = promo_push_impressions * pn_ratio

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        st.metric("Promo Push Impressions", fmt_num(promo_push_impressions))
    with c2:
        st.metric("Promo Push Clicks", fmt_num(promo_push_clicks))
    with c3:
        st.metric("Promo Email Sent", fmt_num(promo_email_sent))
    with c4:
        st.metric("Promo Email Opens", fmt_num(promo_email_opens))
    with c5:
        st.metric("Promo Email Clicks", fmt_num(promo_email_clicks))
    with c6:
        st.metric("Push Campaigns (Promo)", fmt_num(push_campaign_count))

    c7, c8, c9, c10, c11 = st.columns(5)
    with c7:
        st.metric("Email Campaigns (Promo)", fmt_num(email_campaign_count))
    with c8:
        st.metric("PN Sent:Impression Ratio", f"{pn_ratio:.3f}")
    with c9:
        st.metric("Transactional Push Sent", fmt_num(txn_push_sent))
    with c10:
        st.metric("Transactional Email Sent", fmt_num(txn_email_sent))

    # ==================================================================
    # COMPUTED METRICS ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Reachability & Reach
    # ==================================================================
    st.markdown('<div class="section-header">ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ COMPUTED METRICS ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Reachability & Reach</div>', unsafe_allow_html=True)

    # Reachability percentages
    push_reachable_pct = safe_div(push_reachable, total_users, as_pct=True)
    email_reachable_pct = safe_div(email_reachable, total_users, as_pct=True)
    push_reached_pct = safe_div(recv_push, total_users, as_pct=True)
    email_reached_pct = safe_div(recv_email, total_users, as_pct=True)
    push_reachable_active_pct = safe_div(push_reachable, active_users, as_pct=True)
    email_reachable_active_pct = safe_div(active_email, active_users, as_pct=True)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        st.metric("Comms Reachable (% Total) ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Push", fmt_pct(push_reachable_pct))
    with c2:
        st.metric("Comms Reachable (% Total) ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Email", fmt_pct(email_reachable_pct))
    with c3:
        st.metric("Comms Reached (% Total) ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Push", fmt_pct(push_reached_pct))
    with c4:
        st.metric("Comms Reached (% Total) ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Email", fmt_pct(email_reached_pct))
    with c5:
        st.metric("Comms Reachable (% Active) ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Push", fmt_pct(push_reachable_active_pct))
    with c6:
        st.metric("Comms Reachable (% Active) ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Email", fmt_pct(email_reachable_active_pct))

    # ==================================================================
    # COMPUTED METRICS ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Unsubscribes
    # ==================================================================
    st.markdown('<div class="section-header">ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ COMPUTED METRICS ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Unsubscribe Rates</div>', unsafe_allow_html=True)

    unsub_rate_push = safe_div(unsub_push, total_users, as_pct=True)
    unsub_rate_email = safe_div(unsub_email, total_users, as_pct=True)

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Unsubscribe Rate ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Push", fmt_pct(unsub_rate_push))
    with c2:
        st.metric("Unsubscribe Rate ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Email", fmt_pct(unsub_rate_email))

    # ==================================================================
    # COMPUTED METRICS ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Est. Promo Push Sent
    # ==================================================================
    st.markdown('<div class="section-header">ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ COMPUTED METRICS ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Estimated Promo Push Sent</div>', unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Est. Promo Push Sent", fmt_num(est_promo_push_sent))

    # ==================================================================
    # COMPUTED METRICS ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Avg Comms per User
    # ==================================================================
    st.markdown('<div class="section-header">ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ COMPUTED METRICS ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Avg Comms per User</div>', unsafe_allow_html=True)

    avg_promo_push_per_user = safe_div(est_promo_push_sent, total_users)
    avg_promo_email_per_user = safe_div(promo_email_sent, total_users)
    avg_txn_push_per_user = safe_div(txn_push_sent, transacted_users)
    avg_txn_email_per_user = safe_div(txn_email_sent, transacted_users)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        val_str = f"{avg_promo_push_per_user:.2f}" if avg_promo_push_per_user else "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ"
        st.metric("Avg Promo Comms per User ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Push", val_str)
    with c2:
        val_str = f"{avg_promo_email_per_user:.2f}" if avg_promo_email_per_user else "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ"
        st.metric("Avg Promo Comms per User ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Email", val_str)
    with c3:
        val_str = f"{avg_txn_push_per_user:.2f}" if avg_txn_push_per_user else "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ"
        st.metric("Avg Txn Comms per User ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Push", val_str)
    with c4:
        val_str = f"{avg_txn_email_per_user:.2f}" if avg_txn_email_per_user else "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ"
        st.metric("Avg Txn Comms per User ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Email", val_str)

    # ==================================================================
    # COMPUTED METRICS ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Performance (Promotional)
    # ==================================================================
    st.markdown('<div class="section-header">ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ COMPUTED METRICS ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Performance (Promotional)</div>', unsafe_allow_html=True)

    promo_push_ctr_impression = safe_div(promo_push_clicks, promo_push_impressions, as_pct=True)
    promo_push_ctr_sent = safe_div(promo_push_clicks, est_promo_push_sent, as_pct=True)
    email_open_rate = safe_div(promo_email_opens, promo_email_sent, as_pct=True)
    email_ctr = safe_div(promo_email_clicks, promo_email_sent, as_pct=True)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Promo Push CTR (Impression basis)", fmt_pct(promo_push_ctr_impression))
    with c2:
        st.metric("Promo Push CTR (Est. Sent basis)", fmt_pct(promo_push_ctr_sent))
    with c3:
        st.metric("Email Open Rate", fmt_pct(email_open_rate))
    with c4:
        st.metric("Promo Email CTR", fmt_pct(email_ctr))

    # ==================================================================
    # COMPUTED METRICS ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ PNs & Emails per Reachable User
    # ==================================================================
    st.markdown('<div class="section-header">ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ COMPUTED METRICS ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Comms per Reachable User</div>', unsafe_allow_html=True)

    # FIX: Use est_promo_push_sent (not raw impressions) to match report_generator
    # PNs per reachable = (estimated promo push sent + txn push sent) / push reachable
    total_push_est_sent = est_promo_push_sent + txn_push_sent
    total_email = promo_email_sent + txn_email_sent

    pns_per_reachable = safe_div(total_push_est_sent, push_reachable)
    emails_per_reachable = safe_div(total_email, email_reachable)

    c1, c2 = st.columns(2)
    with c1:
        val_str = f"{pns_per_reachable:.2f}" if pns_per_reachable else "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ"
        st.metric("PNs per Push Reachable User", val_str)
    with c2:
        val_str = f"{emails_per_reachable:.2f}" if emails_per_reachable else "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ"
        st.metric("Emails per Email Reachable User", val_str)


def _render_comparison_tab(db):
    """Render the Comparison tab with side-by-side period analysis"""
    st.markdown("---")

    # Comparison date pickers
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown("**Period A**")
        period_a_start = st.date_input(
            "From",
            value=datetime.now().replace(day=1),
            key="comp_period_a_start",
        )
        period_a_start_str = period_a_start.strftime("%Y-%m-%d")

    with col2:
        st.markdown("** **")
        period_a_end = st.date_input(
            "To",
            value=datetime.now(),
            key="comp_period_a_end",
        )
        period_a_end_str = period_a_end.strftime("%Y-%m-%d")

    with col3:
        st.markdown("**Period B**")
        default_b_start = (period_a_start - timedelta(days=30)).replace(day=1)
        period_b_start = st.date_input(
            "From ",
            value=default_b_start,
            key="comp_period_b_start",
        )
        period_b_start_str = period_b_start.strftime("%Y-%m-%d")

    with col4:
        st.markdown("** **")
        default_b_end = period_a_start - timedelta(days=1)
        period_b_end = st.date_input(
            "To ",
            value=default_b_end,
            key="comp_period_b_end",
        )
        period_b_end_str = period_b_end.strftime("%Y-%m-%d")

    st.markdown("")
    load_col = st.columns([1.5, 10])[0]
    with load_col:
        load_comp = st.button(
            "ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Load Comparison",
            key="load_comparison",
            use_container_width=True,
            type="primary",
        )

    if load_comp:
        try:
            with st.spinner("Loading comparison data..."):
                seg_a = {}
                seg_b = {}
                camp_a = []
                camp_b = []

                seg_metrics_a = db.get_all_segment_metrics(period_a_start_str, period_a_end_str)
                seg_metrics_b = db.get_all_segment_metrics(period_b_start_str, period_b_end_str)
                camp_metrics_a = db.get_all_campaign_metrics(period_a_start_str, period_a_end_str)
                camp_metrics_b = db.get_all_campaign_metrics(period_b_start_str, period_b_end_str)

                if seg_metrics_a:
                    seg_a = {
                        (s["segment_type"], s["country"]): (s.get("user_count") or 0)
                        for s in seg_metrics_a
                    }
                if seg_metrics_b:
                    seg_b = {
                        (s["segment_type"], s["country"]): (s.get("user_count") or 0)
                        for s in seg_metrics_b
                    }

                camp_a = camp_metrics_a or []
                camp_b = camp_metrics_b or []

                if not seg_a and not seg_b:
                    st.warning("No data available for selected periods.")
                else:
                    st.session_state.comp_seg_a = seg_a
                    st.session_state.comp_seg_b = seg_b
                    st.session_state.comp_camp_a = camp_a
                    st.session_state.comp_camp_b = camp_b
                    st.session_state.comp_period_a = f"{period_a_start_str} to {period_a_end_str}"
                    st.session_state.comp_period_b = f"{period_b_start_str} to {period_b_end_str}"
                    st.success("Comparison data loaded!")

        except Exception as e:
            st.error(f"Error loading comparison: {e}")
            logger.error(f"Comparison error: {e}", exc_info=True)

    # Display comparison if loaded
    if "comp_seg_a" in st.session_state:
        st.markdown("---")

        seg_a = st.session_state.comp_seg_a
        seg_b = st.session_state.comp_seg_b
        camp_a = st.session_state.comp_camp_a
        camp_b = st.session_state.comp_camp_b
        period_a_label = st.session_state.comp_period_a
        period_b_label = st.session_state.comp_period_b

        # UK Comparison
        st.subheader("ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¬ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ§ United Kingdom ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Comparison")
        _render_comparison_country("GB", seg_a, seg_b, camp_a, camp_b, period_a_label, period_b_label)

        st.markdown("---")

        # UAE Comparison
        st.subheader("ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¦ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂª United Arab Emirates ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Comparison")
        _render_comparison_country("AE", seg_a, seg_b, camp_a, camp_b, period_a_label, period_b_label)


def _render_comparison_country(
    country_code: str,
    seg_a: dict,
    seg_b: dict,
    camp_a: list,
    camp_b: list,
    period_a_label: str,
    period_b_label: str,
):
    """
    Render comparison metrics for a single country across two periods.
    Shows Period A | Period B | Change (%)
    """
    pn_ratio = PN_SENT_TO_IMPRESSION_RATIO.get(country_code, 1.0)

    # ==================================================================
    # RAW DATA ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ User Base
    # ==================================================================
    st.markdown('<div class="section-header">ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¦ RAW DATA ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ User Base</div>', unsafe_allow_html=True)

    # Period A
    total_a = seg_a.get(("TOTAL_USERS", country_code), 0)
    active_a = seg_a.get(("ACTIVE_USERS_60D", country_code), 0)
    transacted_a = seg_a.get(("TRANSACTED_USERS_PERIOD", country_code), 0)

    # Period B
    total_b = seg_b.get(("TOTAL_USERS", country_code), 0)
    active_b = seg_b.get(("ACTIVE_USERS_60D", country_code), 0)
    transacted_b = seg_b.get(("TRANSACTED_USERS_PERIOD", country_code), 0)

    # Display with change
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.markdown(f"**{period_a_label}**")
        st.metric("Total Users", fmt_num(total_a))

    with c2:
        st.markdown(f"**{period_b_label}**")
        st.metric("Total Users", fmt_num(total_b))

    with c3:
        change = safe_div(safe_sub(total_a, total_b), total_b, as_pct=True) if total_b else None
        st.metric("Change", fmt_pct(change) if change else "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ")

    with c4:
        st.markdown("")

    # Active users
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Active (60d)", fmt_num(active_a))
    with c2:
        st.metric("Active (60d)", fmt_num(active_b))
    with c3:
        change = safe_div(safe_sub(active_a, active_b), active_b, as_pct=True) if active_b else None
        st.metric("Change", fmt_pct(change) if change else "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ")
    with c4:
        st.markdown("")

    # Transacted users
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Transacted", fmt_num(transacted_a))
    with c2:
        st.metric("Transacted", fmt_num(transacted_b))
    with c3:
        change = safe_div(safe_sub(transacted_a, transacted_b), transacted_b, as_pct=True) if transacted_b else None
        st.metric("Change", fmt_pct(change) if change else "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ")
    with c4:
        st.markdown("")

    # ==================================================================
    # RAW DATA ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Segment Counts
    # ==================================================================
    st.markdown('<div class="section-header">ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¦ RAW DATA ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Segment Counts</div>', unsafe_allow_html=True)

    recv_push_a = seg_a.get(("RECEIVED_PUSH_PERIOD", country_code), 0)
    recv_push_b = seg_b.get(("RECEIVED_PUSH_PERIOD", country_code), 0)
    recv_email_a = seg_a.get(("RECEIVED_EMAIL_PERIOD", country_code), 0)
    recv_email_b = seg_b.get(("RECEIVED_EMAIL_PERIOD", country_code), 0)
    unsub_push_a = seg_a.get(("UNSUBSCRIBED_PUSH_PERIOD", country_code), 0)
    unsub_push_b = seg_b.get(("UNSUBSCRIBED_PUSH_PERIOD", country_code), 0)
    unsub_email_a = seg_a.get(("UNSUBSCRIBED_EMAIL_PERIOD", country_code), 0)
    unsub_email_b = seg_b.get(("UNSUBSCRIBED_EMAIL_PERIOD", country_code), 0)

    push_reach_a = safe_sub(total_a, unsub_push_a)
    push_reach_b = safe_sub(total_b, unsub_push_b)
    email_reach_a = safe_sub(total_a, unsub_email_a)
    email_reach_b = safe_sub(total_b, unsub_email_b)

    # Received Push
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Recv Push", fmt_num(recv_push_a))
    with c2:
        st.metric("Recv Push", fmt_num(recv_push_b))
    with c3:
        change = safe_div(safe_sub(recv_push_a, recv_push_b), recv_push_b, as_pct=True) if recv_push_b else None
        st.metric("Change", fmt_pct(change) if change else "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ")
    with c4:
        st.markdown("")

    # Received Email
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Recv Email", fmt_num(recv_email_a))
    with c2:
        st.metric("Recv Email", fmt_num(recv_email_b))
    with c3:
        change = safe_div(safe_sub(recv_email_a, recv_email_b), recv_email_b, as_pct=True) if recv_email_b else None
        st.metric("Change", fmt_pct(change) if change else "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ")
    with c4:
        st.markdown("")

    # Push Reachable
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Push Reachable", fmt_num(push_reach_a))
    with c2:
        st.metric("Push Reachable", fmt_num(push_reach_b))
    with c3:
        change = safe_div(safe_sub(push_reach_a, push_reach_b), push_reach_b, as_pct=True) if push_reach_b else None
        st.metric("Change", fmt_pct(change) if change else "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ")
    with c4:
        st.markdown("")

    # Email Reachable
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Email Reachable", fmt_num(email_reach_a))
    with c2:
        st.metric("Email Reachable", fmt_num(email_reach_b))
    with c3:
        change = safe_div(safe_sub(email_reach_a, email_reach_b), email_reach_b, as_pct=True) if email_reach_b else None
        st.metric("Change", fmt_pct(change) if change else "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ")
    with c4:
        st.markdown("")

    # ==================================================================
    # RAW DATA ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Campaign Metrics
    # ==================================================================
    st.markdown('<div class="section-header">ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¦ RAW DATA ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Campaign Metrics</div>', unsafe_allow_html=True)

    camp_a_country = [c for c in camp_a if c.get("country") == country_code]
    camp_b_country = [c for c in camp_b if c.get("country") == country_code]

    push_promo_a = [c for c in camp_a_country if c.get("channel") == "push" and c.get("campaign_type") == "promotional"]
    push_promo_b = [c for c in camp_b_country if c.get("channel") == "push" and c.get("campaign_type") == "promotional"]
    push_txn_a = [c for c in camp_a_country if c.get("channel") == "push" and c.get("campaign_type") == "transactional"]
    push_txn_b = [c for c in camp_b_country if c.get("channel") == "push" and c.get("campaign_type") == "transactional"]
    email_promo_a = [c for c in camp_a_country if c.get("channel") == "email" and c.get("campaign_type") == "promotional"]
    email_promo_b = [c for c in camp_b_country if c.get("channel") == "email" and c.get("campaign_type") == "promotional"]
    email_txn_a = [c for c in camp_a_country if c.get("channel") == "email" and c.get("campaign_type") == "transactional"]
    email_txn_b = [c for c in camp_b_country if c.get("channel") == "email" and c.get("campaign_type") == "transactional"]

    promo_push_imp_a = sum(c.get("sent", 0) for c in push_promo_a)
    promo_push_imp_b = sum(c.get("sent", 0) for c in push_promo_b)
    promo_push_clicks_a = sum(c.get("click", 0) for c in push_promo_a)
    promo_push_clicks_b = sum(c.get("click", 0) for c in push_promo_b)
    promo_email_sent_a = sum(c.get("sent", 0) for c in email_promo_a)
    promo_email_sent_b = sum(c.get("sent", 0) for c in email_promo_b)
    promo_email_opens_a = sum(c.get("open", 0) for c in email_promo_a)
    promo_email_opens_b = sum(c.get("open", 0) for c in email_promo_b)
    promo_email_clicks_a = sum(c.get("click", 0) for c in email_promo_a)
    promo_email_clicks_b = sum(c.get("click", 0) for c in email_promo_b)
    txn_push_sent_a = sum(c.get("sent", 0) for c in push_txn_a)
    txn_push_sent_b = sum(c.get("sent", 0) for c in push_txn_b)
    txn_email_sent_a = sum(c.get("sent", 0) for c in email_txn_a)
    txn_email_sent_b = sum(c.get("sent", 0) for c in email_txn_b)

    est_promo_push_sent_a = promo_push_imp_a * pn_ratio
    est_promo_push_sent_b = promo_push_imp_b * pn_ratio

    # Promo Push Impressions
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Promo Push Impressions", fmt_num(promo_push_imp_a))
    with c2:
        st.metric("Promo Push Impressions", fmt_num(promo_push_imp_b))
    with c3:
        change = safe_div(safe_sub(promo_push_imp_a, promo_push_imp_b), promo_push_imp_b, as_pct=True) if promo_push_imp_b else None
        st.metric("Change", fmt_pct(change) if change else "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ")
    with c4:
        st.markdown("")

    # Promo Email Sent
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Promo Email Sent", fmt_num(promo_email_sent_a))
    with c2:
        st.metric("Promo Email Sent", fmt_num(promo_email_sent_b))
    with c3:
        change = safe_div(safe_sub(promo_email_sent_a, promo_email_sent_b), promo_email_sent_b, as_pct=True) if promo_email_sent_b else None
        st.metric("Change", fmt_pct(change) if change else "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ")
    with c4:
        st.markdown("")

    # ==================================================================
    # COMPUTED METRICS ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Reachability (sample)
    # ==================================================================
    st.markdown('<div class="section-header">ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ COMPUTED METRICS ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Reachability & Performance</div>', unsafe_allow_html=True)

    # Push reached %
    push_reached_pct_a = safe_div(recv_push_a, total_a, as_pct=True)
    push_reached_pct_b = safe_div(recv_push_b, total_b, as_pct=True)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Comms Reached (%) ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Push", fmt_pct(push_reached_pct_a))
    with c2:
        st.metric("Comms Reached (%) ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Push", fmt_pct(push_reached_pct_b))
    with c3:
        change = safe_div(safe_sub(push_reached_pct_a, push_reached_pct_b), 1, as_pct=False)
        change_str = f"{change:+.2f}pp" if change else "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ"
        st.metric("Change (pp)", change_str)
    with c4:
        st.markdown("")

    # Email reached %
    email_reached_pct_a = safe_div(recv_email_a, total_a, as_pct=True)
    email_reached_pct_b = safe_div(recv_email_b, total_b, as_pct=True)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Comms Reached (%) ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Email", fmt_pct(email_reached_pct_a))
    with c2:
        st.metric("Comms Reached (%) ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Email", fmt_pct(email_reached_pct_b))
    with c3:
        change = safe_div(safe_sub(email_reached_pct_a, email_reached_pct_b), 1, as_pct=False)
        change_str = f"{change:+.2f}pp" if change else "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ"
        st.metric("Change (pp)", change_str)
    with c4:
        st.markdown("")

    # Promo Push CTR
    push_ctr_a = safe_div(promo_push_clicks_a, promo_push_imp_a, as_pct=True)
    push_ctr_b = safe_div(promo_push_clicks_b, promo_push_imp_b, as_pct=True)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Promo Push CTR", fmt_pct(push_ctr_a))
    with c2:
        st.metric("Promo Push CTR", fmt_pct(push_ctr_b))
    with c3:
        change = safe_div(safe_sub(push_ctr_a, push_ctr_b), 1, as_pct=False)
        change_str = f"{change:+.2f}pp" if change else "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ"
        st.metric("Change (pp)", change_str)
    with c4:
        st.markdown("")

    # Email Open Rate
    email_open_a = safe_div(promo_email_opens_a, promo_email_sent_a, as_pct=True)
    email_open_b = safe_div(promo_email_opens_b, promo_email_sent_b, as_pct=True)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Email Open Rate", fmt_pct(email_open_a))
    with c2:
        st.metric("Email Open Rate", fmt_pct(email_open_b))
    with c3:
        change = safe_div(safe_sub(email_open_a, email_open_b), 1, as_pct=False)
        change_str = f"{change:+.2f}pp" if change else "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ"
        st.metric("Change (pp)", change_str)
    with c4:
        st.markdown("")


def _render_settings(db):
    """Render settings tab"""
    st.subheader("Transactional Campaigns")
    st.caption(
        "EVENT_TRIGGERED campaigns are classified as transactional if their ID "
        "appears in this list. Everything else is promotional."
    )

    txn_campaigns = load_transactional_campaigns()

    if txn_campaigns:
        for i, campaign in enumerate(txn_campaigns):
            col_id, col_name, col_ch, col_ctry, col_remove = st.columns([2, 3, 1, 1, 1])
            with col_id:
                st.text(campaign.get("campaign_id", "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ"))
            with col_name:
                st.text(campaign.get("campaign_name", "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ"))
            with col_ch:
                st.text(campaign.get("channel", "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ"))
            with col_ctry:
                st.text(campaign.get("country", "ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ"))
            with col_remove:
                if st.button("ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ", key=f"rm_{i}"):
                    txn_campaigns.pop(i)
                    save_transactional_campaigns(txn_campaigns)
                    st.rerun()
    else:
        st.info("No transactional campaigns configured yet.")

    st.markdown("---")
    st.markdown("**Add Transactional Campaign**")

    ac1, ac2, ac3, ac4, ac5 = st.columns([2, 3, 1, 1, 1])
    with ac1:
        new_id = st.text_input("Campaign ID", key="new_txn_id")
    with ac2:
        new_name = st.text_input("Campaign Name", key="new_txn_name")
    with ac3:
        new_ch = st.selectbox("Channel", ["push", "email"], key="new_txn_ch")
    with ac4:
        new_ctry = st.selectbox("Country", ["GB", "AE"], key="new_txn_ctry")
    with ac5:
        st.markdown("&nbsp;", unsafe_allow_html=True)
        if st.button("Add", key="add_txn", use_container_width=True):
            if new_id and new_name:
                db.add_transactional_campaign(new_id, new_name)
                txn_campaigns.append({
                    "campaign_id": new_id,
                    "campaign_name": new_name,
                    "channel": new_ch,
                    "country": new_ctry,
                })
                save_transactional_campaigns(txn_campaigns)
                st.success(f"Added: {new_name}")
                st.rerun()
            else:
                st.error("ID and Name are required")

    st.markdown("---")

    # Preflight check
    st.subheader("System Check")
    if st.button("ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ©ÃÂÃÂÃÂÃÂº Run Preflight Check", use_container_width=False):
        with st.spinner("Running preflight diagnostics..."):
            try:
                from preflight_check import run_preflight
                results = run_preflight(quick=True)

                for check in results.get("checks", []):
                    status = check.get("status", "UNKNOWN")
                    name = check.get("name", "Unknown Check")
                    if status == "PASS":
                        st.success(f"ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ {name}")
                    elif status == "FAIL":
                        st.error(f"ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ {name}: {check.get('message', '')}")
                    elif status == "SKIP":
                        st.info(f"ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ {name} (skipped)")

                passed = results.get("passed", 0)
                total = results.get("total", 0)
                if passed == total:
                    st.balloons()
            except ImportError:
                st.warning("preflight_check.py not found. Skipping.")
            except Exception as e:
                st.error(f"Preflight error: {e}")

    st.markdown("---")

    # Database management
    st.subheader("Database")
    st.text(f"Path: {DATABASE_PATH}")

    if st.button("ÃÂÃÂÃÂÃÂ°ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¯ÃÂÃÂÃÂÃÂ¸ÃÂÃÂÃÂÃÂ Clear All Data", use_container_width=False):
        st.session_state.confirm_clear = True

    if st.session_state.get("confirm_clear"):
        st.warning("This will delete ALL stored metrics. Are you sure?")
        ccol1, ccol2, _ = st.columns([1, 1, 4])
        with ccol1:
            if st.button("Yes, clear", type="primary"):
                if os.path.exists(DATABASE_PATH):
                    os.remove(DATABASE_PATH)
                st.session_state.confirm_clear = False
                st.session_state.pull_summary = None
                st.session_state.report_bytes = None
                st.success("Database cleared.")
                st.rerun()
        with ccol2:
            if st.button("Cancel"):
                st.session_state.confirm_clear = False
                st.rerun()



    # ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ Manual Entry for User-Count Metrics ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ
    st.markdown("---")
    st.subheader("\u270f Manual Entry \u2014 User-Count Metrics")
    st.caption(
        "These metrics are not available via the MoEngage API. "
        "Enter values manually from the MoEngage dashboard."
    )

    with st.form("manual_metrics_form"):
        me_col1, me_col2 = st.columns(2)
        with me_col1:
            me_start = st.date_input("Period Start", key="me_period_start")
        with me_col2:
            me_end = st.date_input("Period End", key="me_period_end")

        st.markdown("#### UK Metrics")
        uk_c1, uk_c2, uk_c3 = st.columns(3)
        with uk_c1:
            uk_total = st.number_input("Total Users", min_value=0, value=0, key="uk_total")
        with uk_c2:
            uk_active = st.number_input("Active Users (60d)", min_value=0, value=0, key="uk_active")
        with uk_c3:
            uk_transacted = st.number_input("Transacted Users", min_value=0, value=0, key="uk_transacted")

        uk_c4, uk_c5, uk_c6 = st.columns(3)
        with uk_c4:
            uk_recv_push = st.number_input("Received Push", min_value=0, value=0, key="uk_recv_push")
        with uk_c5:
            uk_recv_email = st.number_input("Received Email", min_value=0, value=0, key="uk_recv_email")
        with uk_c6:
            uk_active_push = st.number_input("Active Push", min_value=0, value=0, key="uk_active_push")

        uk_c7, uk_c8, uk_c9 = st.columns(3)
        with uk_c7:
            uk_active_email = st.number_input("Active Email", min_value=0, value=0, key="uk_active_email")
        with uk_c8:
            uk_unsub_push = st.number_input("Unsub Push", min_value=0, value=0, key="uk_unsub_push")
        with uk_c9:
            uk_unsub_email = st.number_input("Unsub Email", min_value=0, value=0, key="uk_unsub_email")

        st.markdown("#### UAE Metrics")
        ae_c1, ae_c2, ae_c3 = st.columns(3)
        with ae_c1:
            ae_total = st.number_input("Total Users", min_value=0, value=0, key="ae_total")
        with ae_c2:
            ae_active = st.number_input("Active Users (60d)", min_value=0, value=0, key="ae_active")
        with ae_c3:
            ae_transacted = st.number_input("Transacted Users", min_value=0, value=0, key="ae_transacted")

        ae_c4, ae_c5, ae_c6 = st.columns(3)
        with ae_c4:
            ae_recv_push = st.number_input("Received Push", min_value=0, value=0, key="ae_recv_push")
        with ae_c5:
            ae_recv_email = st.number_input("Received Email", min_value=0, value=0, key="ae_recv_email")
        with ae_c6:
            ae_active_push = st.number_input("Active Push", min_value=0, value=0, key="ae_active_push")

        ae_c7, ae_c8, ae_c9 = st.columns(3)
        with ae_c7:
            ae_active_email = st.number_input("Active Email", min_value=0, value=0, key="ae_active_email")
        with ae_c8:
            ae_unsub_push = st.number_input("Unsub Push", min_value=0, value=0, key="ae_unsub_push")
        with ae_c9:
            ae_unsub_email = st.number_input("Unsub Email", min_value=0, value=0, key="ae_unsub_email")

        submitted = st.form_submit_button("Save Metrics", type="primary")

        if submitted:
            ps = me_start.strftime("%Y-%m-%d")
            pe = me_end.strftime("%Y-%m-%d")
            entries = [
                ("TOTAL_USERS", "GB", uk_total),
                ("ACTIVE_USERS_60D", "GB", uk_active),
                ("TRANSACTED_USERS_PERIOD", "GB", uk_transacted),
                ("RECEIVED_PUSH_PERIOD", "GB", uk_recv_push),
                ("RECEIVED_EMAIL_PERIOD", "GB", uk_recv_email),
                ("ACTIVE_PUSH_PERIOD", "GB", uk_active_push),
                ("ACTIVE_EMAIL_PERIOD", "GB", uk_active_email),
                ("UNSUBSCRIBED_PUSH_PERIOD", "GB", uk_unsub_push),
                ("UNSUBSCRIBED_EMAIL_PERIOD", "GB", uk_unsub_email),
                ("TOTAL_USERS", "AE", ae_total),
                ("ACTIVE_USERS_60D", "AE", ae_active),
                ("TRANSACTED_USERS_PERIOD", "AE", ae_transacted),
                ("RECEIVED_PUSH_PERIOD", "AE", ae_recv_push),
                ("RECEIVED_EMAIL_PERIOD", "AE", ae_recv_email),
                ("ACTIVE_PUSH_PERIOD", "AE", ae_active_push),
                ("ACTIVE_EMAIL_PERIOD", "AE", ae_active_email),
                ("UNSUBSCRIBED_PUSH_PERIOD", "AE", ae_unsub_push),
                ("UNSUBSCRIBED_EMAIL_PERIOD", "AE", ae_unsub_email),
            ]
            saved = 0
            for seg_type, country, value in entries:
                if value > 0:
                    db.upsert_segment_metric(
                        segment_type=seg_type,
                        country=country,
                        user_count=value,
                        segment_id="manual",
                        period_start=ps,
                        period_end=pe,
                    )
                    saved += 1
            st.success(f"Saved {saved} metric(s) for {ps} to {pe}")


    # ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ One-Click Auto-Fetch Bookmarklet ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ
    st.markdown("---")
    st.subheader("One-Click Auto-Fetch from MoEngage")
    st.caption(
        "Drag the button below to your browser bookmarks bar. "
        "Then, while logged into MoEngage, click it to auto-fetch "
        "all 18 segment counts and import them into this dashboard."
    )

    # Segment IDs mapping (field_name -> segment_id)
    # These should match the segments created by the Metrics Calculator
    _seg_ids = st.session_state.get("segment_ids", {})

    with st.form("bookmarklet_config_form"):
        st.markdown("**Configure Segment IDs**")
        st.caption("Paste the MoEngage segment IDs from the Metrics Calculator.")
        bk_c1, bk_c2 = st.columns(2)
        with bk_c1:
            st.markdown("**UK Segments**")
            bk_uk_total = st.text_input("UK Total Users", value=_seg_ids.get("uk_total", ""), key="bk_uk_total")
            bk_uk_active = st.text_input("UK Active Users", value=_seg_ids.get("uk_active", ""), key="bk_uk_active")
            bk_uk_transacted = st.text_input("UK Transacted", value=_seg_ids.get("uk_transacted", ""), key="bk_uk_transacted")
            bk_uk_recv_push = st.text_input("UK Recv Push", value=_seg_ids.get("uk_recv_push", ""), key="bk_uk_recv_push")
            bk_uk_recv_email = st.text_input("UK Recv Email", value=_seg_ids.get("uk_recv_email", ""), key="bk_uk_recv_email")
            bk_uk_active_push = st.text_input("UK Active Push", value=_seg_ids.get("uk_active_push", ""), key="bk_uk_active_push")
            bk_uk_active_email = st.text_input("UK Active Email", value=_seg_ids.get("uk_active_email", ""), key="bk_uk_active_email")
            bk_uk_unsub_push = st.text_input("UK Unsub Push", value=_seg_ids.get("uk_unsub_push", ""), key="bk_uk_unsub_push")
            bk_uk_unsub_email = st.text_input("UK Unsub Email", value=_seg_ids.get("uk_unsub_email", ""), key="bk_uk_unsub_email")
        with bk_c2:
            st.markdown("**UAE Segments**")
            bk_ae_total = st.text_input("UAE Total Users", value=_seg_ids.get("ae_total", ""), key="bk_ae_total")
            bk_ae_active = st.text_input("UAE Active Users", value=_seg_ids.get("ae_active", ""), key="bk_ae_active")
            bk_ae_transacted = st.text_input("UAE Transacted", value=_seg_ids.get("ae_transacted", ""), key="bk_ae_transacted")
            bk_ae_recv_push = st.text_input("UAE Recv Push", value=_seg_ids.get("ae_recv_push", ""), key="bk_ae_recv_push")
            bk_ae_recv_email = st.text_input("UAE Recv Email", value=_seg_ids.get("ae_recv_email", ""), key="bk_ae_recv_email")
            bk_ae_active_push = st.text_input("UAE Active Push", value=_seg_ids.get("ae_active_push", ""), key="bk_ae_active_push")
            bk_ae_active_email = st.text_input("UAE Active Email", value=_seg_ids.get("ae_active_email", ""), key="bk_ae_active_email")
            bk_ae_unsub_push = st.text_input("UAE Unsub Push", value=_seg_ids.get("ae_unsub_push", ""), key="bk_ae_unsub_push")
            bk_ae_unsub_email = st.text_input("UAE Unsub Email", value=_seg_ids.get("ae_unsub_email", ""), key="bk_ae_unsub_email")

        bk_ps = st.date_input("Period Start", key="bk_period_start")
        bk_pe = st.date_input("Period End", key="bk_period_end")

        bk_submitted = st.form_submit_button("Generate Bookmarklet", type="primary")

    if bk_submitted:
        _ids = {
            "uk_total": bk_uk_total, "uk_active": bk_uk_active,
            "uk_transacted": bk_uk_transacted,
            "uk_recv_push": bk_uk_recv_push, "uk_recv_email": bk_uk_recv_email,
            "uk_active_push": bk_uk_active_push, "uk_active_email": bk_uk_active_email,
            "uk_unsub_push": bk_uk_unsub_push, "uk_unsub_email": bk_uk_unsub_email,
            "ae_total": bk_ae_total, "ae_active": bk_ae_active,
            "ae_transacted": bk_ae_transacted,
            "ae_recv_push": bk_ae_recv_push, "ae_recv_email": bk_ae_recv_email,
            "ae_active_push": bk_ae_active_push, "ae_active_email": bk_ae_active_email,
            "ae_unsub_push": bk_ae_unsub_push, "ae_unsub_email": bk_ae_unsub_email,
        }
        st.session_state["segment_ids"] = _ids

        # Build the IDs JSON for the bookmarklet
        _ids_json = json.dumps({k: v for k, v in _ids.items() if v})
        _ps_str = bk_ps.strftime("%Y-%m-%d")
        _pe_str = bk_pe.strftime("%Y-%m-%d")
        _dash_url = "https://web-production-233fc.up.railway.app"

        # Bookmarklet JS
        _bk_js = (
            "javascript:void((async function(){"
            "var S=" + _ids_json + ";"
            "var D='" + _dash_url + "';"
            "var ps='" + _ps_str + "';"
            "var pe='" + _pe_str + "';"
            "var tok=localStorage.getItem('bearer');"
            "var h={'Authorization':'Bearer '+tok,'Content-Type':'application/json'};"
            "var p={};"
            "var keys=Object.keys(S);"
            "var n=keys.length;"
            "var rqMap={};"
            "for(var i=0;i<n;i++){"
            "var k=keys[i];"
            "var sid=S[k];"
            "document.title='Triggering '+(i+1)+'/'+n+'...';"
            "try{"
            "var mr=await fetch('/v2/custom-segments/dashboard/'+sid+'/meta',{headers:h});"
            "var mj=await mr.json();"
            "var sn=mj.cs_details?mj.cs_details.name:k;"
            "var body={filters:{included_filters:{filter_operator:'and',filters:[{filter_type:'custom_segments',id:sid,name:sn}]}},reachability:{push:{platforms:['ANDROID','iOS','web'],aggregated_count_required:true},email:{aggregated_count_required:true},sms:{aggregated_count_required:true}},channel_source:'all',cs_id:sid};"
            "var cr=await fetch('/segmentation/recent_query/count?api=1',{method:'POST',headers:h,body:JSON.stringify(body)});"
            "var cj=await cr.json();"
            "if(cj.rq_id){rqMap[k]=cj.rq_id;}"
            "}catch(e){console.log(k+':'+e);}"
            "}"
            "document.title='Waiting for results...';"
            "var pending=Object.keys(rqMap);"
            "var results={};"
            "for(var poll=0;poll<30&&pending.length>0;poll++){"
            "await new Promise(r=>setTimeout(r,2000));"
            "document.title='Polling '+(poll+1)+'... ('+pending.length+' left)';"
            "try{"
            "var ids=pending.map(function(x){return rqMap[x];});"
            "var pr=await fetch('/segmentation/recent_query/get_bulk?api=1',{method:'POST',headers:h,body:JSON.stringify({ids:ids})});"
            "var pj=await pr.json();"
            "if(Array.isArray(pj.data)){"
            "var revMap={};for(var rk in rqMap){revMap[rqMap[rk]]=rk;}"
            "var matched={};"
            "for(var pi=0;pi<pj.data.length;pi++){"
            "var rd=pj.data[pi];"
            "var mk=revMap[rd._id];"
            "if(mk&&rd.status==='success'){results[mk]=rd;matched[mk]=1;}"
            "}"
            "pending=pending.filter(function(x){return !matched[x];});"
            "}"
            "}catch(e){console.log('poll:'+e);}"
            "}"
            "for(var k in results){"
            "var rd=results[k];"
            "if(k.indexOf('push')!==-1&&rd.reachability_count&&rd.reachability_count.push){"
            "p[k]=rd.reachability_count.push.unique_count||0;"
            "}else if(k.indexOf('email')!==-1&&rd.reachability_count&&rd.reachability_count.email){"
            "p[k]=rd.reachability_count.email.unique_count||0;"
            "}else{"
            "p[k]=rd.user_count||0;"
            "}"
            "}"
            "for(var k in S){if(!(k in p))p[k]=0;}"
            "var u=D+'/?auto_import=1&ps='+ps+'&pe='+pe;"
            "for(var k in p){u+='&'+k+'='+p[k];}"
            "document.title='Done! Redirecting...';"
            "window.open(u,'_blank');"
            "})())"
        )

        st.success("Bookmarklet generated! Copy the link below:")
        st.code(_bk_js, language=None)
        st.info(
            "**How to use:**\n"
            "1. Copy the code above\n"
            "2. Create a new bookmark in your browser\n"
            "3. Paste the code as the URL\n"
            "4. Go to any MoEngage page (while logged in)\n"
            "5. Click the bookmark ÃÂÃÂÃÂÃÂ¢ÃÂÃÂÃÂÃÂÃÂÃÂÃÂÃÂ it will fetch all counts and redirect here"
        )

# ============================================================================
# MAIN
# ============================================================================


if __name__ == "__main__":
    page_dashboard()
