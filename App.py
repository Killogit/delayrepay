import streamlit as st
import pandas as pd
import re
import time
from datetime import datetime, timedelta, date
from curl_cffi import requests
from bs4 import BeautifulSoup

# --- CONFIGURATION ---
st.set_page_config(
    page_title="Delay Repay Finder",
    page_icon="🚄",
    layout="centered"
)

# Apply custom CSS for the Dark Mode aesthetic matching your preferences
st.markdown("""
    <style>
    .stApp {
        background-color: #0e1117;
        color: #fafafa;
    }
    h1, h2, h3 {
        font-family: 'Segoe UI', sans-serif;
        color: #4fc3f7 !important;
    }
    /* Hide row indices in dataframe */
    thead tr th:first-child {display:none}
    tbody th {display:none}
    </style>
    """, unsafe_allow_html=True)

# ==========================================
# 1. LOGIC & SCRAPING
# ==========================================

def clean_time(t_str):
    try:
        if not t_str: return None
        if not re.fullmatch(r'\d{4}', t_str): return None
        return int(t_str[:2]) * 60 + int(t_str[2:])
    except: return None

def format_date_ordinal(d):
    day = d.day
    suffix = "th" if 11 <= day <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    return f"{day}{suffix} {d.strftime('%b')}"

def normalize_station_name(name):
    return name.lower().replace("london", "").replace(" ", "").replace("international", "").strip()

def fetch_detailed_departure(service_url, target_station_name):
    try:
        full_url = f"https://www.realtimetrains.co.uk{service_url}"
        if "?" not in full_url: full_url += "?detailed=true"
        else: full_url += "&detailed=true"
            
        resp = requests.get(full_url, impersonate="chrome110", timeout=10)
        if resp.status_code != 200: return "?"
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        locs = soup.find_all('div', class_='location')
        target_clean = normalize_station_name(target_station_name)
        
        for i, loc in enumerate(locs):
            name_div = loc.find('div', class_='name')
            if not name_div: continue
            row_name = normalize_station_name(name_div.get_text(strip=True))
            
            if target_clean in row_name or row_name in target_clean:
                dep_block = loc.find('div', class_='dep')
                if dep_block:
                    plan = dep_block.find('div', class_='plan')
                    if plan: return plan.get_text(strip=True)
                if i == 0:
                    plans = loc.find_all('div', class_='plan')
                    if plans: return plans[-1].get_text(strip=True)
        return "-"
    except: return "?"

def parse_row_text(text):
    sched_match = re.match(r'^(\d{4})', text)
    if not sched_match: return None, None, None, None
    sched_str = sched_match.group(1)
    
    clean_text = text[4:].strip() 
    origin = re.split(r'\s+(Arrived|On time|Expected|Cancel)', clean_text)[0].strip()

    act_str = None
    status = "UNKNOWN"

    if "Cancel" in text:
        status = "CANCELLED"
        act_str = None
    elif "On time" in text:
        status = "ON TIME"
        act_str = sched_str
    else:
        arrival_match = re.search(r'Arrived at\s+(\d{4})', text)
        if arrival_match:
            act_str = arrival_match.group(1)
            status = "LATE/EARLY"
        else:
            status = "NO REPORT"
    return origin, sched_str, act_str, status

def process_delays(trains_df):
    if trains_df.empty: return trains_df
    trains_df = trains_df.sort_values('sched_mins').reset_index(drop=True)
    results = []
    
    for i, row in trains_df.iterrows():
        delay = 0
        notes = row['status_raw']
        
        if row['direction'] == "To London":
            from_stn = "Sevenoaks"
            to_stn = "London Charing Cross" if "CHX" in row['dest_code'] else "London Cannon Street"
            lookup_station = "Sevenoaks" 
        else:
            from_stn = row['origin']
            to_stn = "Sevenoaks"
            lookup_station = row['origin']

        if row['status_raw'] == "CANCELLED":
            next_train = None
            for j in range(i + 1, len(trains_df)):
                candidate = trains_df.iloc[j]
                if candidate['status_raw'] != "CANCELLED":
                    next_train = candidate
                    break
            if next_train is not None and pd.notna(next_train['act_mins']) and next_train['act_mins'] > 0:
                diff = next_train['act_mins'] - row['sched_mins']
                if diff < -1000: diff += 1440
                delay = int(diff)
                notes = f"Cancelled (Next Arr: {next_train['act_str']})"
            else:
                delay = 999 
                notes = "Cancelled (No replacement)"
        elif pd.notna(row['act_mins']) and pd.notna(row['sched_mins']):
            diff = row['act_mins'] - row['sched_mins']
            if diff < -1000: diff += 1440
            delay = int(diff)
            if delay <= 0:
                notes = "On Time"
                delay = 0 
            else:
                notes = f"{delay}m Late"

        results.append({
            "dt_obj": row['dt_obj'], "From": from_stn, "To": to_stn,
            "Sched Dep": "?", "Sched Arr": row['sched_str'],
            "Actual Arr": row['act_str'] if row['act_str'] else "---",
            "Delay_Mins": delay, "Status": notes, "url": row['url'],
            "lookup_station": lookup_station
        })
    return pd.DataFrame(results)

# Cache data so resizing the window doesn't re-run the scrape
@st.cache_data(show_spinner=False)
def run_full_scrape(date_list, am_hours, pm_hours):
    all_raw_data = []
    
    # Progress bar logic handled in UI section loop
    for d in date_list:
        date_str = d.strftime("%Y-%m-%d")
        display_date = d.strftime("%d/%m/%Y")
        
        jobs = []
        for h in am_hours:
            for term in ["CHX", "CST"]:
                jobs.append({"url": f"https://www.realtimetrains.co.uk/search/simple/gb-nr:{term}/{date_str}/{h}/arrivals", 
                             "dir": "To London", "filter": "Sevenoaks", "dest_code": term})
        for h in pm_hours:
            jobs.append({"url": f"https://www.realtimetrains.co.uk/search/simple/gb-nr:SEV/{date_str}/{h}/arrivals", 
                         "dir": "To Home", "filter": ["London Charing Cross", "London Cannon Street"], "dest_code": "SEV"})

        for job in jobs:
            try:
                resp = requests.get(job['url'], impersonate="chrome110", timeout=10)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    for row in soup.find_all('a', class_='service'):
                        text = row.get_text(" ", strip=True)
                        origin, sched, act, status = parse_row_text(text)
                        if sched:
                            filters = job['filter'] if isinstance(job['filter'], list) else [job['filter']]
                            if any(f in origin for f in filters):
                                all_raw_data.append({
                                    "date": display_date, "dt_obj": d, "direction": job['dir'], "origin": origin,
                                    "dest_code": job['dest_code'], "sched_str": sched, "act_str": act,
                                    "sched_mins": clean_time(sched), "act_mins": clean_time(act),
                                    "status_raw": status, "url": row.get('href')
                                })
                time.sleep(0.05)
            except: pass

    if not all_raw_data: return pd.DataFrame()
    raw_df = pd.DataFrame(all_raw_data)
    final_dfs = []
    for (d, direct, dest), group in raw_df.groupby(['date', 'direction', 'dest_code']):
        final_dfs.append(process_delays(group.copy()))
    if not final_dfs: return pd.DataFrame()
    return pd.concat(final_dfs).drop_duplicates(subset=['dt_obj', 'From', 'To', 'Sched Arr']).reset_index(drop=True)

# ==========================================
# 3. UI LAYOUT
# ==========================================

st.title("🚄 Southeastern Delay Repay Finder")
st.caption("Sevenoaks ↔️ London Charing Cross / Cannon Street")

# --- CONTROLS ---
with st.container():
    col1, col2 = st.columns(2)
    with col1:
        mode = st.radio("Date Selection", ["Last N Days", "Date Range"], horizontal=True)
    with col2:
        weekends = st.checkbox("Exclude Weekends", value=False)

    if mode == "Last N Days":
        days = st.slider("Lookback Days", 1, 30, 7)
        date_list = []
        curr = date.today()
        for _ in range(days):
            if not weekends or curr.weekday() < 5: date_list.append(curr)
            curr -= timedelta(days=1)
    else:
        c1, c2 = st.columns(2)
        start = c1.date_input("Start Date", date.today() - timedelta(days=7))
        end = c2.date_input("End Date", date.today())
        date_list = []
        curr = start
        while curr <= end:
            if not weekends or curr.weekday() < 5: date_list.append(curr)
            curr += timedelta(days=1)
        date_list.sort(reverse=True)

    with st.expander("⏰ Select Hours", expanded=False):
        c1, c2 = st.columns(2)
        hour_opts = [f"{h:02d}00" for h in range(5, 24)]
        with c1:
            am_hours = st.multiselect("Morning (To London)", hour_opts, default=['0700', '0800', '0900'])
        with c2:
            pm_hours = st.multiselect("Evening (To Home)", hour_opts, default=['1700', '1800', '1900'])

# --- EXECUTION ---
if st.button("🔎 Check for Delays", type="primary", use_container_width=True):
    
    status_text = st.empty()
    progress_bar = st.progress(0)
    
    status_text.text("Scanning schedules...")
    
    # 1. Run Main Scrape
    df = run_full_scrape(date_list, am_hours, pm_hours)
    progress_bar.progress(50)
    
    if df.empty:
        st.warning("No data found for the selected dates/hours.")
    else:
        # 2. Filter & Enrich
        delayed = df[df['Delay_Mins'] >= 15].copy()
        
        # Limit Top 5 per day
        top_delays = []
        for d in date_list:
            day_subset = delayed[delayed['dt_obj'] == d]
            top_delays.append(day_subset.sort_values('Delay_Mins', ascending=False).head(5))
        
        target_df = pd.concat(top_delays) if top_delays else pd.DataFrame()
        
        # Enrich Sched Dep
        total_enrich = len(target_df)
        if total_enrich > 0:
            status_text.text(f"Found {total_enrich} delays. Fetching detailed departure times...")
            for i, idx in enumerate(target_df.index):
                row = df.loc[idx]
                df.at[idx, 'Sched Dep'] = fetch_detailed_departure(row['url'], row['lookup_station'])
                progress_bar.progress(50 + int((i / total_enrich) * 50))
                time.sleep(0.1)
        
        progress_bar.progress(100)
        status_text.empty()

        # 3. Prepare Display
        display_rows = []
        for d in date_list:
            formatted_date = format_date_ordinal(d)
            
            if not target_df.empty:
                day_data = df.loc[df.index.isin(target_df[target_df['dt_obj'] == d].index)].copy()
                if not day_data.empty:
                    day_data = day_data.sort_values("Delay_Mins", ascending=False)
                    day_data['Date'] = formatted_date
                    day_data['is_first_row'] = False
                    day_data.iloc[0, day_data.columns.get_loc('is_first_row')] = True
                    display_rows.extend(day_data.to_dict('records'))
                else:
                    display_rows.append({"Date": formatted_date, "From": "No delays >15mn", "Status": "-", "Delay_Mins": 0, "is_first_row": True})
            else:
                display_rows.append({"Date": formatted_date, "From": "No delays >15mn", "Status": "-", "Delay_Mins": 0, "is_first_row": True})

        final_display = pd.DataFrame(display_rows)
        
        # CSV Download
        if not target_df.empty:
            csv_data = df.loc[target_df.index].drop(columns=['url', 'lookup_station', 'dt_obj'], errors='ignore').to_csv(index=False)
            st.download_button("📥 Download CSV", csv_data, "delay_repay.csv", "text/csv", use_container_width=True)

        # 4. Styling & Rendering
        cols = ["Date", "From", "To", "Sched Dep", "Sched Arr", "Actual Arr", "Status", "Check"]
        
        def make_clickable(url):
            if isinstance(url, str) and len(url) > 5:
                return f"https://www.realtimetrains.co.uk{url}"
            return None

        # Streamlit allows LinkColumn, but for custom "View" text we use pandas styler or column config
        # Using Styler for coloring
        
        final_display['Check'] = final_display.get('url', '').apply(make_clickable)

        def style_dataframe(row):
            # Dark Mode Base
            bg = '#1e1e1e' # Dark Grey
            color = '#e0e0e0' # Soft White
            
            # Date Grouping (Subtle Divider)
            border = 'border-top: 1px solid #444;' if row.get('is_first_row') else 'border-top: 1px solid #2a2a2a;'
            
            base = f'background-color: {bg}; color: {color}; border-bottom: 1px solid #2a2a2a;'
            styles = [f'{base} {border}'] * len(row)
            
            # Hide Date text if not first row
            if not row.get('is_first_row'):
                styles[0] = f'{base} {border} color: {bg};' 
            else:
                styles[0] = f'{base} {border} font-weight: bold; color: #90caf9;' 

            # Status Column Coloring
            mins = row['Delay_Mins']
            stat_color = color
            
            if mins >= 60: stat_color = '#ef5350' # Soft Red
            elif mins >= 30: stat_color = '#ff8a65' # Coral
            elif mins >= 15: stat_color = '#ffe082' # Pastel Gold
            
            if "CANCELLED" in str(row['Status']):
                if mins == 0: stat_color = '#ffe082' 
                styles[6] = f'{base} {border} color: {stat_color}; font-style: italic;'
            elif "No delays" in str(row['From']):
                styles[1] = f'{base} {border} color: #757575; font-style: italic;'
            else:
                styles[6] = f'{base} {border} color: {stat_color};'

            return styles

        st.dataframe(
            final_display.style.apply(style_dataframe, axis=1),
            column_config={
                "Check": st.column_config.LinkColumn("Link", display_text="View"),
                "Delay_Mins": None, "is_first_row": None, "dt_obj": None, "url": None
            },
            hide_index=True,
            use_container_width=True,
            height=600
        )