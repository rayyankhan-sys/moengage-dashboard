"""
MoEngage Automation Dashboard
Streamlit-based dashboard for viewing and pulling MoEngage metrics
Clean flow: Pick dates → Pull Data → View → Export
"""
import json
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
    page_title="MoEngage Dashboard — Aspora",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for cleaner look
st.markdown("""
<style>
    .block-container { padding-top: 1rem; }
    div[data-testid="stMetric"] {
        background-color: #f8f9fa;
        border: 1px solid #e9ecef;
        padding: 12px 16px;
        border-radius: 8px;
    }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        padding: 8px 20px;
        border-radius: 4px 4px 0 0;
    }
    .section-header {
        background-color: #f0f2f6;
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
        return "—"
    return f"{value:.2f}%"


def fmt_num(value) -> str:
    """Format number with thousands separator"""
    if value is None:
        return "—"
    return f"{int(value):,}"


def safe_div(numerator, denominator, as_pct=False):
    """Safe division — returns None if denominator is 0 or None"""
    if not denominator or numerator is None:
        return None
    result = numerator / denominator
    return result * 100 if as_pct else result


# ============================================================================
# MAIN DASHBOARD
# ============================================================================


def page_dashboard():
    """Main dashboard page"""
    st.title("📊 MoEngage Comms Dashboard")
    st.caption("Aspora — UK & UAE Communications Metrics")

    db = MoEngageDatabase()

    # ==================================================================
    # TOP CONTROL BAR — Date Pickers + Actions
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
    btn_col1, btn_col2, btn_col3, spacer = st.columns([1.5, 1.5, 1.5, 5])

    with btn_col1:
        pull_clicked = st.button(
            "🔄 Pull Data",
            key="pull_data",
            use_container_width=True,
            type="primary",
        )

    with btn_col2:
        export_clicked = st.button(
            "📥 Export Report",
            key="export_report",
            use_container_width=True,
        )

    with btn_col3:
        dry_run_clicked = st.button(
            "🧪 Dry Run",
            key="dry_run",
            use_container_width=True,
            help="Test the pull without making real API calls",
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
                        f"Pull complete — {seg_count} segments, {camp_count} campaigns "
                        f"in {total_time:.1f}s"
                    )
                else:
                    st.warning(
                        f"Pull complete with {err_count} error(s) — "
                        f"{seg_count} segments, {camp_count} campaigns"
                    )
                st.rerun()
            except Exception as e:
                st.error(f"Error pulling data: {e}")
                logger.error(f"Data pull error: {e}", exc_info=True)

    # Handle Dry Run
    if dry_run_clicked:
        with st.spinner("Running dry run..."):
            try:
                puller = DataPuller(dry_run=True)
                summary = puller.pull_all_data(period_start_str, period_end_str)
                st.info(
                    f"Dry run complete — would create {len(summary.get('segments', {}))} segments. "
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
            label="⬇️ Download Weekly Report (.xlsx)",
            data=st.session_state.report_bytes,
            file_name=f"MoEngage_Report_{period_start_str}_to_{period_end_str}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=False,
        )

    st.markdown("---")

    # ==================================================================
    # TABS — Metrics, Comparison, Campaigns, Pull History, Settings
    # ==================================================================
    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        ["📈 Metrics", "🔄 Comparison", "📋 Campaigns", "🕐 Pull History", "⚙️ Settings"]
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
            st.subheader("🇬🇧 United Kingdom")
            _render_country_metrics("GB", seg, campaign_metrics)

            st.markdown("---")

            # --- UAE SECTION ---
            st.subheader("🇦🇪 United Arab Emirates")
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
    # RAW DATA — User Base
    # ==================================================================
    st.markdown('<div class="section-header">📦 RAW DATA — User Base</div>', unsafe_allow_html=True)

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
    # RAW DATA — Segment Counts
    # ==================================================================
    st.markdown('<div class="section-header">📦 RAW DATA — Segment Counts</div>', unsafe_allow_html=True)

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
        st.metric("Unsub Rate — Push", fmt_num(unsub_push))

    c7, c8 = st.columns(2)
    with c7:
        st.metric("Unsubscribed Email", fmt_num(unsub_email))

    # ==================================================================
    # RAW DATA — Promotional & Transactional
    # ==================================================================
    st.markdown('<div class="section-header">📦 RAW DATA — Campaign Metrics</div>', unsafe_allow_html=True)

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
    # COMPUTED METRICS — Reachability & Reach
    # ==================================================================
    st.markdown('<div class="section-header">📊 COMPUTED METRICS — Reachability & Reach</div>', unsafe_allow_html=True)

    # Reachability percentages
    push_reachable_pct = safe_div(push_reachable, total_users, as_pct=True)
    email_reachable_pct = safe_div(email_reachable, total_users, as_pct=True)
    push_reached_pct = safe_div(recv_push, total_users, as_pct=True)
    email_reached_pct = safe_div(recv_email, total_users, as_pct=True)
    push_reachable_active_pct = safe_div(push_reachable, active_users, as_pct=True)
    email_reachable_active_pct = safe_div(active_email, active_users, as_pct=True)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        st.metric("Comms Reachable (% Total) — Push", fmt_pct(push_reachable_pct))
    with c2:
        st.metric("Comms Reachable (% Total) — Email", fmt_pct(email_reachable_pct))
    with c3:
        st.metric("Comms Reached (% Total) — Push", fmt_pct(push_reached_pct))
    with c4:
        st.metric("Comms Reached (% Total) — Email", fmt_pct(email_reached_pct))
    with c5:
        st.metric("Comms Reachable (% Active) — Push", fmt_pct(push_reachable_active_pct))
    with c6:
        st.metric("Comms Reachable (% Active) — Email", fmt_pct(email_reachable_active_pct))

    # ==================================================================
    # COMPUTED METRICS — Unsubscribes
    # ==================================================================
    st.markdown('<div class="section-header">📊 COMPUTED METRICS — Unsubscribe Rates</div>', unsafe_allow_html=True)

    unsub_rate_push = safe_div(unsub_push, total_users, as_pct=True)
    unsub_rate_email = safe_div(unsub_email, total_users, as_pct=True)

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Unsubscribe Rate — Push", fmt_pct(unsub_rate_push))
    with c2:
        st.metric("Unsubscribe Rate — Email", fmt_pct(unsub_rate_email))

    # ==================================================================
    # COMPUTED METRICS — Est. Promo Push Sent
    # ==================================================================
    st.markdown('<div class="section-header">📊 COMPUTED METRICS — Estimated Promo Push Sent</div>', unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Est. Promo Push Sent", fmt_num(est_promo_push_sent))

    # ==================================================================
    # COMPUTED METRICS — Avg Comms per User
    # ==================================================================
    st.markdown('<div class="section-header">📊 COMPUTED METRICS — Avg Comms per User</div>', unsafe_allow_html=True)

    avg_promo_push_per_user = safe_div(est_promo_push_sent, total_users)
    avg_promo_email_per_user = safe_div(promo_email_sent, total_users)
    avg_txn_push_per_user = safe_div(txn_push_sent, transacted_users)
    avg_txn_email_per_user = safe_div(txn_email_sent, transacted_users)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        val_str = f"{avg_promo_push_per_user:.2f}" if avg_promo_push_per_user else "—"
        st.metric("Avg Promo Comms per User — Push", val_str)
    with c2:
        val_str = f"{avg_promo_email_per_user:.2f}" if avg_promo_email_per_user else "—"
        st.metric("Avg Promo Comms per User — Email", val_str)
    with c3:
        val_str = f"{avg_txn_push_per_user:.2f}" if avg_txn_push_per_user else "—"
        st.metric("Avg Txn Comms per User — Push", val_str)
    with c4:
        val_str = f"{avg_txn_email_per_user:.2f}" if avg_txn_email_per_user else "—"
        st.metric("Avg Txn Comms per User — Email", val_str)

    # ==================================================================
    # COMPUTED METRICS — Performance (Promotional)
    # ==================================================================
    st.markdown('<div class="section-header">📊 COMPUTED METRICS — Performance (Promotional)</div>', unsafe_allow_html=True)

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
    # COMPUTED METRICS — PNs & Emails per Reachable User
    # ==================================================================
    st.markdown('<div class="section-header">📊 COMPUTED METRICS — Comms per Reachable User</div>', unsafe_allow_html=True)

    # FIX: Use est_promo_push_sent (not raw impressions) to match report_generator
    # PNs per reachable = (estimated promo push sent + txn push sent) / push reachable
    total_push_est_sent = est_promo_push_sent + txn_push_sent
    total_email = promo_email_sent + txn_email_sent

    pns_per_reachable = safe_div(total_push_est_sent, push_reachable)
    emails_per_reachable = safe_div(total_email, email_reachable)

    c1, c2 = st.columns(2)
    with c1:
        val_str = f"{pns_per_reachable:.2f}" if pns_per_reachable else "—"
        st.metric("PNs per Push Reachable User", val_str)
    with c2:
        val_str = f"{emails_per_reachable:.2f}" if emails_per_reachable else "—"
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
            "📊 Load Comparison",
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
        st.subheader("🇬🇧 United Kingdom — Comparison")
        _render_comparison_country("GB", seg_a, seg_b, camp_a, camp_b, period_a_label, period_b_label)

        st.markdown("---")

        # UAE Comparison
        st.subheader("🇦🇪 United Arab Emirates — Comparison")
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
    # RAW DATA — User Base
    # ==================================================================
    st.markdown('<div class="section-header">📦 RAW DATA — User Base</div>', unsafe_allow_html=True)

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
        change = safe_div(total_a - total_b, total_b, as_pct=True) if total_b else None
        st.metric("Change", fmt_pct(change) if change else "—")

    with c4:
        st.markdown("")

    # Active users
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Active (60d)", fmt_num(active_a))
    with c2:
        st.metric("Active (60d)", fmt_num(active_b))
    with c3:
        change = safe_div(active_a - active_b, active_b, as_pct=True) if active_b else None
        st.metric("Change", fmt_pct(change) if change else "—")
    with c4:
        st.markdown("")

    # Transacted users
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Transacted", fmt_num(transacted_a))
    with c2:
        st.metric("Transacted", fmt_num(transacted_b))
    with c3:
        change = safe_div(transacted_a - transacted_b, transacted_b, as_pct=True) if transacted_b else None
        st.metric("Change", fmt_pct(change) if change else "—")
    with c4:
        st.markdown("")

    # ==================================================================
    # RAW DATA — Segment Counts
    # ==================================================================
    st.markdown('<div class="section-header">📦 RAW DATA — Segment Counts</div>', unsafe_allow_html=True)

    recv_push_a = seg_a.get(("RECEIVED_PUSH_PERIOD", country_code), 0)
    recv_push_b = seg_b.get(("RECEIVED_PUSH_PERIOD", country_code), 0)
    recv_email_a = seg_a.get(("RECEIVED_EMAIL_PERIOD", country_code), 0)
    recv_email_b = seg_b.get(("RECEIVED_EMAIL_PERIOD", country_code), 0)
    unsub_push_a = seg_a.get(("UNSUBSCRIBED_PUSH_PERIOD", country_code), 0)
    unsub_push_b = seg_b.get(("UNSUBSCRIBED_PUSH_PERIOD", country_code), 0)
    unsub_email_a = seg_a.get(("UNSUBSCRIBED_EMAIL_PERIOD", country_code), 0)
    unsub_email_b = seg_b.get(("UNSUBSCRIBED_EMAIL_PERIOD", country_code), 0)

    push_reach_a = total_a - unsub_push_a
    push_reach_b = total_b - unsub_push_b
    email_reach_a = total_a - unsub_email_a
    email_reach_b = total_b - unsub_email_b

    # Received Push
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Recv Push", fmt_num(recv_push_a))
    with c2:
        st.metric("Recv Push", fmt_num(recv_push_b))
    with c3:
        change = safe_div(recv_push_a - recv_push_b, recv_push_b, as_pct=True) if recv_push_b else None
        st.metric("Change", fmt_pct(change) if change else "—")
    with c4:
        st.markdown("")

    # Received Email
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Recv Email", fmt_num(recv_email_a))
    with c2:
        st.metric("Recv Email", fmt_num(recv_email_b))
    with c3:
        change = safe_div(recv_email_a - recv_email_b, recv_email_b, as_pct=True) if recv_email_b else None
        st.metric("Change", fmt_pct(change) if change else "—")
    with c4:
        st.markdown("")

    # Push Reachable
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Push Reachable", fmt_num(push_reach_a))
    with c2:
        st.metric("Push Reachable", fmt_num(push_reach_b))
    with c3:
        change = safe_div(push_reach_a - push_reach_b, push_reach_b, as_pct=True) if push_reach_b else None
        st.metric("Change", fmt_pct(change) if change else "—")
    with c4:
        st.markdown("")

    # Email Reachable
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Email Reachable", fmt_num(email_reach_a))
    with c2:
        st.metric("Email Reachable", fmt_num(email_reach_b))
    with c3:
        change = safe_div(email_reach_a - email_reach_b, email_reach_b, as_pct=True) if email_reach_b else None
        st.metric("Change", fmt_pct(change) if change else "—")
    with c4:
        st.markdown("")

    # ==================================================================
    # RAW DATA — Campaign Metrics
    # ==================================================================
    st.markdown('<div class="section-header">📦 RAW DATA — Campaign Metrics</div>', unsafe_allow_html=True)

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
        change = safe_div(promo_push_imp_a - promo_push_imp_b, promo_push_imp_b, as_pct=True) if promo_push_imp_b else None
        st.metric("Change", fmt_pct(change) if change else "—")
    with c4:
        st.markdown("")

    # Promo Email Sent
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Promo Email Sent", fmt_num(promo_email_sent_a))
    with c2:
        st.metric("Promo Email Sent", fmt_num(promo_email_sent_b))
    with c3:
        change = safe_div(promo_email_sent_a - promo_email_sent_b, promo_email_sent_b, as_pct=True) if promo_email_sent_b else None
        st.metric("Change", fmt_pct(change) if change else "—")
    with c4:
        st.markdown("")

    # ==================================================================
    # COMPUTED METRICS — Reachability (sample)
    # ==================================================================
    st.markdown('<div class="section-header">📊 COMPUTED METRICS — Reachability & Performance</div>', unsafe_allow_html=True)

    # Push reached %
    push_reached_pct_a = safe_div(recv_push_a, total_a, as_pct=True)
    push_reached_pct_b = safe_div(recv_push_b, total_b, as_pct=True)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Comms Reached (%) — Push", fmt_pct(push_reached_pct_a))
    with c2:
        st.metric("Comms Reached (%) — Push", fmt_pct(push_reached_pct_b))
    with c3:
        change = safe_div(push_reached_pct_a - push_reached_pct_b, 1, as_pct=False)
        change_str = f"{change:+.2f}pp" if change else "—"
        st.metric("Change (pp)", change_str)
    with c4:
        st.markdown("")

    # Email reached %
    email_reached_pct_a = safe_div(recv_email_a, total_a, as_pct=True)
    email_reached_pct_b = safe_div(recv_email_b, total_b, as_pct=True)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Comms Reached (%) — Email", fmt_pct(email_reached_pct_a))
    with c2:
        st.metric("Comms Reached (%) — Email", fmt_pct(email_reached_pct_b))
    with c3:
        change = safe_div(email_reached_pct_a - email_reached_pct_b, 1, as_pct=False)
        change_str = f"{change:+.2f}pp" if change else "—"
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
        change = safe_div(push_ctr_a - push_ctr_b, 1, as_pct=False)
        change_str = f"{change:+.2f}pp" if change else "—"
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
        change = safe_div(email_open_a - email_open_b, 1, as_pct=False)
        change_str = f"{change:+.2f}pp" if change else "—"
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
                st.text(campaign.get("campaign_id", "—"))
            with col_name:
                st.text(campaign.get("campaign_name", "—"))
            with col_ch:
                st.text(campaign.get("channel", "—"))
            with col_ctry:
                st.text(campaign.get("country", "—"))
            with col_remove:
                if st.button("✕", key=f"rm_{i}"):
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
    if st.button("🩺 Run Preflight Check", use_container_width=False):
        with st.spinner("Running preflight diagnostics..."):
            try:
                from preflight_check import run_preflight
                results = run_preflight(quick=True)

                for check in results.get("checks", []):
                    status = check.get("status", "UNKNOWN")
                    name = check.get("name", "Unknown Check")
                    if status == "PASS":
                        st.success(f"✓ {name}")
                    elif status == "FAIL":
                        st.error(f"✗ {name}: {check.get('message', '')}")
                    elif status == "SKIP":
                        st.info(f"⊘ {name} (skipped)")

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

    if st.button("🗑️ Clear All Data", use_container_width=False):
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


# ============================================================================
# MAIN
# ============================================================================


if __name__ == "__main__":
    page_dashboard()
