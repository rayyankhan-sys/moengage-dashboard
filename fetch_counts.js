// fetch_counts.js - Bookmarklet script for fetching MoEngage segment counts
// Runs on the MoEngage dashboard, fetches counts for all 18 segments,
// then redirects to the Aspora dashboard with the count data.

(async function() {
  var DASHBOARD_URL = "https://web-production-233fc.up.railway.app/";
  var SEGMENTS = [
    ["gb","GB_TOTAL_USERS","697fd30caaa05331f7bb1ad6"],
    ["gb","GB_ACTIVE_USERS_60D","6988dff50bbb027d637981f0"],
    ["gb","GB_TRANSACTED_USERS_PERIOD","69a7e9fbccab5df1507ea366"],
    ["gb","GB_RECEIVED_PUSH_PERIOD","69a7e9f2d45906be5bc90083"],
    ["gb","GB_RECEIVED_EMAIL_PERIOD","69a7e9f42a0e61e9c0d450a6"],
    ["gb","GB_ACTIVE_PUSH_PERIOD","6988dffaed75baf6b728240c"],
    ["gb","GB_ACTIVE_EMAIL_PERIOD","69a7e9f7d45906be5bc90086"],
    ["gb","GB_UNSUBSCRIBED_PUSH_PERIOD","69a7e9f8d45906be5bc90089"],
    ["gb","GB_UNSUBSCRIBED_EMAIL_PERIOD","69a7e9fa17bb774154d2e7e4"],
    ["ae","AE_TOTAL_USERS","697fd315b9272646a2228751"],
    ["ae","AE_ACTIVE_USERS_60D","6988e001f85db5a33fcac777"],
    ["ae","AE_TRANSACTED_USERS_PERIOD","69a7ea09be3c37e97342fb68"],
    ["ae","AE_RECEIVED_PUSH_PERIOD","69a7ea0034ffb02bee622f26"],
    ["ae","AE_RECEIVED_EMAIL_PERIOD","69a7ea01be3c37e97342fb64"],
    ["ae","AE_ACTIVE_PUSH_PERIOD","6988e0057f1fbe4ddb7f12be"],
    ["ae","AE_ACTIVE_EMAIL_PERIOD","69a7ea0434ffb02bee622f29"],
    ["ae","AE_UNSUBSCRIBED_PUSH_PERIOD","69a7ea06e7560bad73430efa"],
    ["ae","AE_UNSUBSCRIBED_EMAIL_PERIOD","69a7ea07e7560bad73430efd"]
  ];

  var token = localStorage.getItem("bearer");
  if (!token) { alert("Not logged into MoEngage. Please log in first."); return; }

  var status = document.createElement("div");
  status.style.cssText = "position:fixed;top:0;left:0;right:0;padding:16px;background:#1a73e8;color:#fff;z-index:99999;font:16px sans-serif;text-align:center";
  status.textContent = "Fetching segment counts (0/" + SEGMENTS.length + ")...";
  document.body.appendChild(status);

  var base = "https://dashboard-01.moengage.com";
  var headers = {"Content-Type":"application/json","Authorization":"Bearer "+token};
  var rqMap = {};
  var results = {};

  for (var i = 0; i < SEGMENTS.length; i++) {
    var seg = SEGMENTS[i];
    status.textContent = "Triggering count (" + (i+1) + "/" + SEGMENTS.length + "): " + seg[1];
    try {
      var resp = await fetch(base + "/segmentation/recent_query/count?api=1", {
        method: "POST", credentials: "include", headers: headers,
        body: JSON.stringify({
          filters: {included_filters: {filter_operator:"and", filters:[{filter_type:"custom_segments",id:seg[2],name:seg[1]}]}},
          reachability: {push:{platforms:["ANDROID","iOS","web"],aggregated_count_required:true},email:{aggregated_count_required:true},sms:{aggregated_count_required:true}},
          channel_source:"all", cs_id:seg[2]
        })
      });
      var data = await resp.json();
      if (data.success && data.rq_id) { rqMap[data.rq_id] = seg; }
    } catch(e) { console.error("Trigger failed for " + seg[1], e); }
    await new Promise(function(r) { setTimeout(r, 300); });
  }

  var pendingIds = Object.keys(rqMap);
  for (var poll = 0; poll < 20 && pendingIds.length > 0; poll++) {
    var done = Object.keys(rqMap).length - pendingIds.length;
    status.textContent = "Polling for counts (" + done + "/" + Object.keys(rqMap).length + " done)...";
    await new Promise(function(r) { setTimeout(r, 3000); });
    try {
      var resp2 = await fetch(base + "/segmentation/recent_query/get_bulk?api=1", {
        method: "POST", credentials: "include", headers: headers,
        body: JSON.stringify({ids: pendingIds})
      });
      var pd = await resp2.json();
      if (pd.data) {
        var still = [];
        for (var j = 0; j < pd.data.length; j++) {
          var rd = pd.data[j];
          if (!rqMap[rd._id]) continue;
          if (rd.status === "success" || rd.status === "completed") {
            var s = rqMap[rd._id];
            var st = s[1].toLowerCase();
            var uc = rd.user_count || 0;
            var rc = rd.reachability_count || {};
            var count;
            if (st.indexOf("push") !== -1 && st.indexOf("unsub") === -1) {
              count = (rc.push && rc.push.unique_count) || uc;
            } else if (st.indexOf("email") !== -1 && st.indexOf("unsub") === -1) {
              count = (rc.email && rc.email.unique_count) || uc;
            } else {
              count = uc;
            }
            results[s[2]] = {type:s[1],country:s[0],count:count,user_count:uc,push_reach:(rc.push&&rc.push.unique_count)||0,email_reach:(rc.email&&rc.email.unique_count)||0};
          } else { still.push(rd._id); }
        }
        pendingIds = still;
      }
    } catch(e) { console.error("Poll error", e); }
  }

  var countData = btoa(JSON.stringify(results));
  var total = Object.keys(results).length;
  if (total === 0) {
    status.style.background = "#d32f2f";
    status.textContent = "Failed to fetch counts. Token may be expired - try refreshing MoEngage.";
    return;
  }
  status.style.background = "#2e7d32";
  status.textContent = "Got " + total + " counts! Redirecting to dashboard...";
  setTimeout(function() {
    window.open(DASHBOARD_URL + "?update_counts=" + encodeURIComponent(countData), "_blank");
  }, 1000);
})();
