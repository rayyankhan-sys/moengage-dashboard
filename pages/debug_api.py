"""Debug API Test - Auto-runs MoEngage API calls on load"""
import streamlit as st
import json
import traceback
from datetime import datetime, timedelta

st.set_page_config(page_title="API Debug", layout="wide")
st.title("MoEngage API Debug Test")
st.info("This page auto-tests the MoEngage API on load.")

end_date = datetime.now()
start_date = end_date - timedelta(days=30)
period_start = start_date.strftime("%m/%d/%Y")
period_end = end_date.strftime("%m/%d/%Y")
st.write(f"Test period: {period_start} to {period_end}")

st.header("1. Config Check")
try:
    from config import CAMPAIGN_META_API_ENDPOINT, CAMPAIGN_STATS_API_ENDPOINT, APP_KEY_ID, CAMPAIGN_API_KEY
    st.success("Config loaded OK")
    st.write(f"Meta endpoint: {CAMPAIGN_META_API_ENDPOINT}")
    st.write(f"Stats endpoint: {CAMPAIGN_STATS_API_ENDPOINT}")
    st.write(f"App Key: {APP_KEY_ID[:8]}...")
    st.write(f"Campaign Key: {CAMPAIGN_API_KEY[:8]}...")
except Exception as e:
    st.error(f"Config error: {e}")

st.header("2. Raw Campaign Meta API Call")
try:
    import requests
    from requests.auth import HTTPBasicAuth
    url = CAMPAIGN_META_API_ENDPOINT
    auth = HTTPBasicAuth(APP_KEY_ID, CAMPAIGN_API_KEY)
    payload = {"start_date": period_start, "end_date": period_end, "page": 1, "page_size": 5}
    st.write(f"URL: {url}")
    st.json(payload)
    resp = requests.post(url, json=payload, auth=auth, timeout=30)
    st.write(f"Status: {resp.status_code}")
    data = resp.json()
    if isinstance(data, list):
        st.success(f"Got LIST with {len(data)} campaigns")
        if data:
            st.write("First campaign keys: " + str(list(data[0].keys())))
            st.json(data[0])
    elif isinstance(data, dict):
        st.success("Got DICT response")
        st.json(data)
except Exception as e:
    st.error(f"API call failed: {e}")
    st.code(traceback.format_exc())

st.header("3. DataPuller.fetch_campaigns()")
campaigns = []
try:
    from data_puller import DataPuller
    puller = DataPuller()
    campaigns, buckets = puller.fetch_campaigns(period_start, period_end)
    st.success(f"Got {len(campaigns)} campaigns")
    for k, v in buckets.items():
        st.write(f"  {k}: {len(v)}")
    if campaigns:
        st.json(campaigns[0] if isinstance(campaigns[0], dict) else str(campaigns[0]))
except Exception as e:
    st.error(f"fetch_campaigns failed: {e}")
    st.code(traceback.format_exc())

st.header("4. Campaign Stats (first 3)")
try:
    if campaigns:
        stats = puller.fetch_campaign_stats(campaigns[:3], period_start, period_end)
        st.success(f"Got stats for {len(stats)} campaigns")
        for cid, s in stats.items():
            st.write(f"Campaign {cid}:")
            st.json(s)
    else:
        st.warning("No campaigns to test")
except Exception as e:
    st.error(f"Stats failed: {e}")
    st.code(traceback.format_exc())

st.header("5. Database Check")
try:
    import sqlite3
    conn = sqlite3.connect("moengage_data.db")
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM campaign_metrics")
    count = cur.fetchone()[0]
    st.write(f"campaign_metrics rows: {count}")
    if count > 0:
        cur.execute("SELECT campaign_name, sent, delivered, open_count, click FROM campaign_metrics LIMIT 5")
        for row in cur.fetchall():
            st.write(f"  {row}")
    conn.close()
except Exception as e:
    st.error(f"DB error: {e}")
    st.code(traceback.format_exc())

st.caption("Debug page - remove after testing")
