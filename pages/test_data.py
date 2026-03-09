"""
Test Data Page - Bypasses database, calls DataPuller directly
Proof that data flows correctly end-to-end from MoEngage APIs
"""
import streamlit as st
import traceback
from datetime import datetime, timedelta

st.set_page_config(page_title="Test Data", layout="wide")
st.title("Test Data - Direct API Pull")
st.caption("This page calls DataPuller directly. No database involved.")

# ---------- Date range picker ----------
col1, col2, col3 = st.columns([1, 1, 2])
with col1:
    start_date = st.date_input("Start date", value=datetime.now() - timedelta(days=30))
with col2:
    end_date = st.date_input("End date", value=datetime.now())

period_start = start_date.strftime("%m/%d/%Y")
period_end = end_date.strftime("%m/%d/%Y")

st.write(f"Period: **{period_start}** to **{period_end}**")

# ---------- Pull & Show button ----------
if st.button("Pull & Show", type="primary"):

    # ------------------------------------------------------------------
    # Step 1: Fetch and categorize campaigns
    # ------------------------------------------------------------------
    st.header("1. Fetching Campaigns")
    try:
        from data_puller import DataPuller

        puller = DataPuller()
        with st.spinner("Calling DataPuller.fetch_campaigns()..."):
            all_campaigns, buckets = puller.fetch_campaigns(period_start, period_end)

        st.success(f"Total campaigns returned: **{len(all_campaigns)}**")

        # Show bucket counts (UK and UAE only + UNCATEGORIZED)
        st.subheader("Bucket counts")
        display_buckets = [
            "UK_PUSH_PROMO", "UK_PUSH_TXN", "UK_EMAIL_PROMO", "UK_EMAIL_TXN",
            "AE_PUSH_PROMO", "AE_PUSH_TXN", "AE_EMAIL_PROMO", "AE_EMAIL_TXN",
            "UNCATEGORIZED",
        ]
        for bucket_name in display_buckets:
            campaigns_in_bucket = buckets.get(bucket_name, [])
            count = len(campaigns_in_bucket)
            if count > 0:
                st.write(f"  **{bucket_name}**: {count}")
            else:
                st.write(f"  {bucket_name}: 0")

    except Exception as e:
        st.error(f"fetch_campaigns failed: {e}")
        st.code(traceback.format_exc())
        st.stop()

    # ------------------------------------------------------------------
    # Step 2: Fetch stats per bucket (UK & UAE only)
    # ------------------------------------------------------------------
    st.header("2. Campaign Stats Per Bucket")

    stats_buckets = [
        "UK_PUSH_PROMO", "UK_PUSH_TXN", "UK_EMAIL_PROMO", "UK_EMAIL_TXN",
        "AE_PUSH_PROMO", "AE_PUSH_TXN", "AE_EMAIL_PROMO", "AE_EMAIL_TXN",
    ]

    for bucket_name in stats_buckets:
        campaigns_in_bucket = buckets.get(bucket_name, [])
        if not campaigns_in_bucket:
            continue

        st.subheader(f"{bucket_name} ({len(campaigns_in_bucket)} campaigns)")

        try:
            with st.spinner(f"Fetching stats for {bucket_name}..."):
                stats = puller.fetch_campaign_stats(
                    campaigns_in_bucket, period_start, period_end
                )

            if not stats:
                st.warning(f"No stats returned for {bucket_name}")
                continue

            # Aggregate stats across all campaigns in this bucket
            totals = {
                "sent": 0,
                "delivered": 0,
                "open": 0,
                "click": 0,
                "unsubscribe": 0,
                "bounced": 0,
                "failed": 0,
            }
            for cid, cstats in stats.items():
                for key in totals:
                    totals[key] += cstats.get(key, 0)

            # Display aggregated metrics
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Sent", f"{totals['sent']:,}")
            m2.metric("Delivered", f"{totals['delivered']:,}")
            m3.metric("Opens", f"{totals['open']:,}")
            m4.metric("Clicks", f"{totals['click']:,}")

            m5, m6, m7, _ = st.columns(4)
            m5.metric("Unsubscribe", f"{totals['unsubscribe']:,}")
            m6.metric("Bounced", f"{totals['bounced']:,}")
            m7.metric("Failed", f"{totals['failed']:,}")

            # Show per-campaign breakdown
            with st.expander(f"Per-campaign breakdown ({len(stats)} campaigns with stats)"):
                for cid, cstats in stats.items():
                    # Find campaign name
                    name = cid
                    for c in campaigns_in_bucket:
                        if (c.get("campaign_id") or c.get("id")) == cid:
                            name = c.get("campaign_name") or c.get("name") or cid
                            break
                    st.write(
                        f"**{name}** - "
                        f"Sent: {cstats.get('sent', 0):,} | "
                        f"Delivered: {cstats.get('delivered', 0):,} | "
                        f"Opens: {cstats.get('open', 0):,} | "
                        f"Clicks: {cstats.get('click', 0):,}"
                    )

        except Exception as e:
            st.error(f"Stats failed for {bucket_name}: {e}")
            st.code(traceback.format_exc())

    # ------------------------------------------------------------------
    # Step 3: UNCATEGORIZED campaigns info
    # ------------------------------------------------------------------
    uncategorized = buckets.get("UNCATEGORIZED", [])
    if uncategorized:
        st.header("3. UNCATEGORIZED Campaigns")
        st.write(f"**{len(uncategorized)}** campaigns could not be categorized.")
        with st.expander("Show uncategorized campaigns"):
            for c in uncategorized[:20]:
                cid = c.get("campaign_id") or c.get("id") or "?"
                cname = c.get("campaign_name") or c.get("name") or "?"
                ctype = c.get("campaign_delivery_type") or "?"
                channel = c.get("channel") or "?"
                tags = c.get("campaign_tags") or []
                st.write(
                    f"- **{cname}** | id={cid} | type={ctype} | "
                    f"channel={channel} | tags={tags}"
                )
            if len(uncategorized) > 20:
                st.write(f"... and {len(uncategorized) - 20} more")

    st.success("Done! If you see non-zero numbers above, the data pipeline works.")
