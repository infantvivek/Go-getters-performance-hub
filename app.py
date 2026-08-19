import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import urllib.parse
import re
import base64
import time
from streamlit.components.v1 import iframe

# --- 1. CONFIGURATION & URLS ---
TEAM_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSU-KDmKs9i1EIEuIuJTuKKxG4nFZoPluRqOonP2BxRbQuVJunS8WQ9uJA6ayUCdoq043uFMH6u3UcM/pub?gid=0&single=true&output=csv"
KPI_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSU-KDmKs9i1EIEuIuJTuKKxG4nFZoPluRqOonP2BxRbQuVJunS8WQ9uJA6ayUCdoq043uFMH6u3UcM/pub?gid=1918948844&single=true&output=csv"
DSAT_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSU-KDmKs9i1EIEuIuJTuKKxG4nFZoPluRqOonP2BxRbQuVJunS8WQ9uJA6ayUCdoq043uFMH6u3UcM/pub?gid=367459010&single=true&output=csv"
LOGO_URL = "https://s3.amazonaws.com/cdn.freshdesk.com/data/helpdesk/attachments/production/48175265495/original/PTXBCP40UHx-8LCKsM1zqLX-pq8nndFHSw.png?1641235482"

# --- GOOGLE FORM CONFIGURATION ---
FORM_ID = "1FAIpQLSdu2gEmHPZBCoUZ1naQlGTeJtgTgB47YfCENCfeKAHU1OA76g"
ENTRY_KEY = "entry.1726897360"        
ENTRY_TYPE = "entry.1303252108"      
ENTRY_FEEDBACK = "entry.1754509958"  

# Set Menu Items to None to remove unwanted standard options natively where possible
st.set_page_config(
    layout="wide", 
    page_title="GoHighLevel Performance Hub", 
    page_icon="🚀", 
    menu_items={'Get Help': None, 'Report a bug': None, 'About': None}
)

# --- 2. SaaS/GHL THEME ENGINE ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    @import url('https://fonts.googleapis.com/icon?family=Material+Icons');
    @import url('https://fonts.googleapis.com/css2?family=Material+Symbols+Rounded:opsz,wght,FILL,GRAD@48,400,0,0');
    
    :root { --ghl-blue: #0052FF; --ghl-dark: #1E293B; }

    /* Apply Inter font, but strictly EXCLUDE icon classes so the password eye icon works! */
    html, body, [class*="st-"] { font-family: 'Inter', sans-serif; }
    .material-icons, .material-symbols-rounded, [class*="icon"] { font-family: 'Material Symbols Rounded', 'Material Icons', sans-serif !important; }
    
    /* --- STRICT CSS TO HIDE FORK, DEPLOY & UNWANTED MENU ITEMS --- */
    
    /* 1. Hide the Fork and Deploy buttons from the top toolbar */
    .stAppDeployButton, [data-testid="stAppDeployButton"] { display: none !important; }
    [data-testid="stToolbar"] a { display: none !important; }
    
    /* 2. Hide everything in the hamburger menu EXCEPT the theme toggle.
       The theme toggle sits in a div ABOVE the main list. We hide the ul list 
       and the "Made with Streamlit" div below it. */
    ul[data-testid="main-menu-list"] { display: none !important; }
    ul[data-testid="main-menu-list"] ~ div { display: none !important; }
    
    /* 3. Hide the global footer */
    footer { visibility: hidden !important; display: none !important; }
    
    /* 4. Hide the sidebar collapse control to lock it */
    [data-testid="collapsedControl"],
    [data-testid="stSidebarCollapseButton"] { display: none !important; }
    
    
    /* --- COZY INPUTS & SIDEBAR --- */
    .stTextInput input {
        border-radius: 12px !important;
        padding: 12px 16px !important;
        border: 1px solid rgba(0, 82, 255, 0.15) !important;
        box-shadow: inset 0 2px 4px rgba(0,0,0,0.02) !important;
        transition: all 0.2s ease;
    }
    .stTextInput input:focus {
        border-color: var(--ghl-blue) !important;
        box-shadow: 0 0 0 3px rgba(0, 82, 255, 0.15) !important;
    }
    
    [data-testid="stSidebarNav"]::before {
        content: ""; display: block; background-image: url('""" + LOGO_URL + """');
        background-size: contain; background-repeat: no-repeat;
        width: 170px; height: 50px; margin: 25px 0 10px 25px; filter: brightness(0) invert(1); 
    }
    
    /* --- MODERNIZED TABS --- */
    div[data-testid="stTabs"] button[role="tab"] {
        background-color: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 20px;
        padding: 8px 24px;
        margin-right: 10px;
        margin-bottom: 20px;
        color: var(--text-color);
        font-weight: 500;
        transition: all 0.3s ease;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    div[data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
        background: linear-gradient(135deg, #0052FF 0%, #0036A8 100%) !important;
        color: white !important;
        border: none !important;
        font-weight: 700;
        box-shadow: 0 6px 16px rgba(0, 82, 255, 0.3) !important;
        transform: translateY(-2px);
    }
    div[data-testid="stTabs"] button[role="tab"]:hover {
        border-color: #0052FF;
        transform: translateY(-1px);
    }
    
    /* --- SLEEK METRIC CARDS --- */
    .metric-card {
        background: linear-gradient(145deg, var(--secondary-background-color), rgba(255,255,255,0.02));
        padding: 24px; 
        border-radius: 20px; 
        box-shadow: 0 10px 25px rgba(0, 0, 0, 0.08); 
        margin-bottom: 1.2rem;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
        position: relative;
        overflow: hidden;
    }
    .metric-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 14px 30px rgba(0, 0, 0, 0.15);
    }
    .metric-title {
        color: var(--text-color); opacity: 0.7; font-size: 13px; margin-bottom: 8px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.8px;
    }
    .metric-value {
        margin-top: 0; margin-bottom: 0; font-size: 34px; font-weight: 700; letter-spacing: -0.5px;
    }
    .metric-sub {
        color: var(--text-color); opacity: 0.5; font-size: 12px; margin-top: 8px; font-weight: 500;
    }
    
    div.stInfo { background-color: rgba(0, 82, 255, 0.04); border-left: 5px solid #0052FF; color: var(--text-color); border-radius: 12px; padding: 18px; font-size: 15px; box-shadow: 0 4px 6px rgba(0,0,0,0.03); margin-bottom: 1rem; }
    .ghl-header-img { margin-bottom: 10px; filter: drop-shadow(0px 4px 6px rgba(0,0,0,0.1)); }
    .tab-logo { width: 35px; vertical-align: middle; margin-right: 10px; padding-bottom: 5px; }
    </style>
""", unsafe_allow_html=True)

# --- 3. ROBUST DATA PROCESSING ENGINE ---
def parse_duration(time_str):
    if pd.isna(time_str) or not isinstance(time_str, str): return 0
    try:
        h, m = 0, 0
        parts = str(time_str).lower().split()
        for p in parts:
            if 'h' in p: h = int(re.sub(r'\D', '', p))
            elif 'm' in p: m = int(re.sub(r'\D', '', p))
        return (h * 60) + m
    except: return 0

@st.cache_data(ttl=600, show_spinner="Connecting to Data Source...")
def load_and_standardize(url, sheet_type):
    try:
        fresh_url = f"{url}&_t={int(time.time())}" if "?" in url else f"{url}?_t={int(time.time())}"
        
        df = pd.read_csv(fresh_url)
        df.columns = [re.sub(r'[^a-zA-Z0-9]', '', str(c)).lower() for c in df.columns]
        
        rmap = {
            "advisorname": "name", "agentname": "name", "email": "email", "advisoremail": "email",
            "manager": "mgr", "managername": "mgr", "accesslevel": "level", "password": "pass",
            "ia": "ia_raw", "advisorcalltime": "call_raw", "sentrate": "sent_rate", 
            "satisfiedsurvey": "sat_rate", "obcalls": "ob", "qacalls": "qa", 
            "totalsurvey": "surveys", "timestamp": "ts_raw", "processed": "date_raw", "chatdsaturl": "link", "datelevelas": "date_raw",
            "freshcallsmade": "fresh_calls_made", "connectedfreshcalls": "connected_fresh_calls", "unresolved": "unresolved"
        }
        df = df.rename(columns=rmap)
        if 'email' in df.columns: df['email'] = df['email'].astype(str).str.strip().str.lower()
        
        if sheet_type == "KPI":
            for col in ['sent_rate', 'sat_rate']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace('%', ''), errors='coerce')
                    if df[col].max() <= 1.1: df[col] = df[col] * 100
            
            df['date_dt'] = pd.to_datetime(df['date_raw'], format="%b'%d'%y", errors='coerce')
            df['ia_min'] = df['ia_raw'].apply(parse_duration) if 'ia_raw' in df.columns else 0
            df['call_min'] = df['call_raw'].apply(parse_duration) if 'call_raw' in df.columns else 0
            df['shift_score'] = np.where(df['ia_min'] > 0, (df['call_min']/df['ia_min']*100), np.nan)
            
        if sheet_type == "DSAT":
            if 'createddate' in df.columns: df['date_raw'] = df['createddate']
            
            if 'resolvedbyteammember' in df.columns: 
                df['name'] = df['resolvedbyteammember']
            elif 'resolvedby' in df.columns: 
                df['name'] = df['resolvedby']
            
            if 'satisfactory' in df.columns:
                df['is_csat'] = df['satisfactory'].astype(str).str.strip().str.lower().isin(['true', '1', 'yes', 't'])
            else:
                df['is_csat'] = False 
                
            if 'conversationid' in df.columns:
                def clean_id(x):
                    x_str = str(x).strip()
                    if pd.isna(x) or x_str.lower() in ['nan', 'null', '']: return "-"
                    try:
                        return str(int(float(x))) 
                    except:
                        return x_str
                
                cleaned_ids = df['conversationid'].apply(clean_id)
                df['conv_id'] = cleaned_ids
                df['link'] = np.where(cleaned_ids == "-", "-", "https://highlevel-team.freshchat.com/a/309618592266199/inbox/0/254430/conversation/" + cleaned_ids)
            else:
                df['conv_id'] = "-"
                df['link'] = "-"
                
            if 'assignedgroup' in df.columns:
                def map_call_type(x):
                    x_str = str(x).strip()
                    if x_str == "[S]: Advisor QA": return "QA"
                    elif x_str in ["[S]: Advisor Onboarding", "[S]: Senior Onboarding"]: return "OB"
                    elif pd.isna(x) or x_str.lower() in ['nan', 'null', '']: return "-"
                    else: return x_str
                df['call_type'] = df['assignedgroup'].apply(map_call_type)
            else:
                df['call_type'] = "-"
                
            date_col = df['date_raw'] if 'date_raw' in df.columns else df['ts_raw'] if 'ts_raw' in df.columns else df.get('createddate', pd.Series())
            df['date_dt'] = pd.to_datetime(date_col, errors='coerce')
            df['date_dt'] = df['date_dt'].ffill()
            
        return df
    except Exception as e:
        return pd.DataFrame()

def create_metric_card(title, value, target_val=None, is_percent=True, exact_target_str=None):
    if target_val is not None:
        if value > target_val: color = "#22C55E"
        elif value >= target_val - 15: color = "#F59E0B"
        else: color = "#EF4444"
    else:
        color = "#0052FF" 

    val_str = f"{value:.2f}%" if is_percent else f"{int(value):,}"
    
    if exact_target_str: target_str = exact_target_str
    elif target_val: target_str = f"Target: {target_val}{'%' if is_percent else ''}"
    else: target_str = "Activity Metric"
    
    html = f"""
    <div class="metric-card" style="border-left: 6px solid {color};">
        <p class="metric-title">{title}</p>
        <h2 class="metric-value" style="color: {color};">{val_str}</h2>
        <p class="metric-sub">{target_str}</p>
    </div>
    """
    return html

def format_custom_card(title, val, color, sub_txt):
    return f"""
    <div class="metric-card" style="border-left: 6px solid {color};">
        <p class="metric-title">{title}</p>
        <h2 class="metric-value" style="color: {color};">{val}</h2>
        <p class="metric-sub">{sub_txt}</p>
    </div>
    """

def display_admin_discrepancies(kpi_df, dsat_df):
    st.markdown("##### 🔍 Daily Survey Volume Discrepancies")
    st.caption("Showing only dates where the KPI Sheet total surveys does not match the raw CSAT Sheet volume.")
    
    if not kpi_df.empty and not dsat_df.empty and 'surveys' in kpi_df.columns:
        kpi_daily = kpi_df.groupby(kpi_df['date_dt'].dt.date)['surveys'].sum().reset_index(name='kpi_surveys')
        dsat_daily = dsat_df.groupby(dsat_df['date_dt'].dt.date).size().reset_index(name='dsat_surveys')
        
        diff_df = pd.merge(kpi_daily, dsat_daily, on='date_dt', how='outer').fillna(0)
        diff_df['kpi_surveys'] = diff_df['kpi_surveys'].astype(int)
        diff_df['dsat_surveys'] = diff_df['dsat_surveys'].astype(int)
        
        discrepancies = diff_df[diff_df['kpi_surveys'] != diff_df['dsat_surveys']].copy()
        
        if not discrepancies.empty:
            discrepancies['Difference (KPI - CSAT)'] = discrepancies['kpi_surveys'] - discrepancies['dsat_surveys']
            discrepancies = discrepancies.rename(columns={
                'date_dt': 'Date',
                'kpi_surveys': 'KPI Sheet Volume',
                'dsat_surveys': 'CSAT Sheet Volume'
            }).sort_values('Date', ascending=False)
            
            st.dataframe(discrepancies, hide_index=True, use_container_width=True)
        else:
            st.success("No daily volume discrepancies detected in the selected period. Both sheets match perfectly!")
    else:
        st.info("Insufficient data available to compare daily discrepancies.")

@st.dialog("Update Feedback & Type", width="large")
def open_form_dialog(row):
    fb = row.get('feedback', '')
    tp = row.get('type', '')
    
    # Prepend apostrophe to force Google Sheets to read ID as Plain Text
    raw_conv_id = str(row.get('conv_id', '')).strip()
    safe_conv_id = f"'{raw_conv_id}" if raw_conv_id not in ["", "-", "nan"] else ""
    
    params = {
        ENTRY_KEY: safe_conv_id,  
        ENTRY_FEEDBACK: fb if str(fb) != "nan" and fb != "-" else "",
        ENTRY_TYPE: tp if str(tp) != "nan" and tp != "-" else ""
    }
    url = f"https://docs.google.com/forms/d/e/{FORM_ID}/viewform?usp=pp_url&{urllib.parse.urlencode(params)}"
    
    st.markdown("### Update Data Repository")
    st.caption("Submit updates below to push them directly to the Google Sheet backend.")
    iframe(url, height=550, scrolling=True)
    
    if st.button("Close & Sync Dashboard", use_container_width=True): 
        with st.spinner("Syncing data from Google Sheets..."):
            time.sleep(3.5)
            st.cache_data.clear()
        st.rerun()

# --- 4. AUTHENTICATION & SESSION TIMEOUT ---
if 'auth' not in st.session_state: st.session_state.auth = None
team_db = load_and_standardize(TEAM_URL, "TEAM")

if not st.session_state.auth and 'session' in st.query_params:
    try:
        decoded_email = base64.b64decode(st.query_params['session']).decode('utf-8')
        match = team_db[team_db['email'] == decoded_email]
        if not match.empty:
            st.session_state.auth = match.iloc[0].to_dict()
            st.session_state.last_active = time.time()
    except Exception:
        pass

if st.session_state.auth:
    current_time = time.time()
    if 'last_active' in st.session_state:
        if current_time - st.session_state.last_active > 900:
            st.session_state.auth = None
            st.query_params.clear()
            st.warning("⚠️ Your session has expired due to inactivity. Please log in again.")
            st.stop()
    st.session_state.last_active = current_time

if not st.session_state.auth:
    st.write("") # Spacer
    st.write("") # Spacer
    col_l, col_r = st.columns([1.2, 5])
    with col_l: st.image(LOGO_URL, width=220)
    with col_r: 
        st.write("") # Vertical alignment 
        st.title("HighLevel Performance Hub")
    
    with st.form("login"):
        u_email = st.text_input("Work Email").lower().strip()
        u_pass = st.text_input("Password", type="password")
        
        if st.form_submit_button("Sign In"):
            match = team_db[(team_db['email'] == u_email) & (team_db['pass'].astype(str) == str(u_pass))]
            if not match.empty:
                st.session_state.auth = match.iloc[0].to_dict()
                st.session_state.last_active = time.time()
                
                token = base64.b64encode(u_email.encode('utf-8')).decode('utf-8')
                st.query_params['session'] = token
                st.rerun()
            else: 
                st.error("Invalid credentials.")
    st.stop()

# --- 5. FREQUENCY & DATA FILTERING ---
user = st.session_state.auth
kpi_raw = load_and_standardize(KPI_URL, "KPI")
dsat_raw = load_and_standardize(DSAT_URL, "DSAT")

st.sidebar.title("Navigation Filters")

freq = st.sidebar.radio("Frequency", ["Daily", "Weekly", "Monthly", "Yearly", "Perf Cycle", "Custom"], horizontal=True)

if not kpi_raw.empty:
    if freq == "Daily":
        available = sorted(kpi_raw['date_dt'].dropna().unique(), reverse=True)
        if available:
            sel = st.sidebar.selectbox("Select Date", available, format_func=lambda x: x.strftime('%d-%m-%Y'))
            k_f = kpi_raw[kpi_raw['date_dt'] == sel]
            d_f = dsat_raw[dsat_raw['date_dt'].dt.date == sel.date()] if not dsat_raw.empty else dsat_raw.copy()
        else: 
            k_f, d_f = kpi_raw.copy(), dsat_raw.copy()
        
    elif freq == "Weekly":
        kpi_raw['wk'] = kpi_raw['date_dt'].dt.to_period('W-SAT').apply(lambda r: r.start_time if pd.notna(r) else pd.NaT)
        available = sorted(kpi_raw['wk'].dropna().unique(), reverse=True)
        if available:
            sel = st.sidebar.selectbox("Select Week", available, format_func=lambda x: x.strftime('%d-%m-%Y'))
            k_f = kpi_raw[kpi_raw['wk'] == sel]
            d_f = dsat_raw[(dsat_raw['date_dt'] >= sel) & (dsat_raw['date_dt'] < sel + pd.Timedelta(days=7))] if not dsat_raw.empty else dsat_raw.copy()
        else: 
            k_f, d_f = kpi_raw.copy(), dsat_raw.copy()
        
    elif freq in ["Monthly", "Yearly"]:
        kpi_raw['mo'] = kpi_raw['date_dt'].dt.strftime('%B %Y') if freq == "Monthly" else kpi_raw['date_dt'].dt.year
        available = kpi_raw.sort_values('date_dt', ascending=False)['mo'].dropna().unique()
        if len(available) > 0:
            sel = st.sidebar.selectbox(f"Select Period", available)
            k_f = kpi_raw[kpi_raw['mo'] == sel]
            if freq == "Monthly": 
                d_f = dsat_raw[dsat_raw['date_dt'].dt.strftime('%B %Y') == sel] if not dsat_raw.empty else dsat_raw.copy()
            else: 
                d_f = dsat_raw[dsat_raw['date_dt'].dt.year == sel] if not dsat_raw.empty else dsat_raw.copy()
        else: 
            k_f, d_f = kpi_raw.copy(), dsat_raw.copy()
        
    elif freq == "Perf Cycle":
        def get_perf_cycle(d):
            if pd.isna(d): return None
            y = d.year
            return f"{y}-{y+1} Cycle" if d.month >= 6 else f"{y-1}-{y} Cycle"
            
        kpi_raw['cycle'] = kpi_raw['date_dt'].apply(get_perf_cycle)
        available = sorted(kpi_raw['cycle'].dropna().unique(), reverse=True)
        
        if available:
            sel = st.sidebar.selectbox("Select Performance Cycle", available)
            k_f = kpi_raw[kpi_raw['cycle'] == sel]
            if not dsat_raw.empty:
                dsat_raw['cycle'] = dsat_raw['date_dt'].apply(get_perf_cycle)
                d_f = dsat_raw[dsat_raw['cycle'] == sel]
            else:
                d_f = dsat_raw.copy()
        else: 
            k_f, d_f = kpi_raw.copy(), dsat_raw.copy()
            
    elif freq == "Custom":
        min_date = kpi_raw['date_dt'].min().date() if not kpi_raw['date_dt'].dropna().empty else pd.to_datetime('today').date()
        max_date = kpi_raw['date_dt'].max().date() if not kpi_raw['date_dt'].dropna().empty else pd.to_datetime('today').date()
        
        col1, col2 = st.sidebar.columns(2)
        with col1:
            start_date = st.date_input("Start Date", value=min_date, min_value=min_date, max_value=max_date)
        with col2:
            end_date = st.date_input("End Date", value=max_date, min_value=start_date, max_value=max_date)
            
        if start_date > end_date:
            st.sidebar.error("End date cannot be before start date.")
            k_f = pd.DataFrame(columns=kpi_raw.columns)
            d_f = pd.DataFrame(columns=dsat_raw.columns)
        else:
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
            
            k_f = kpi_raw[(kpi_raw['date_dt'] >= start_dt) & (kpi_raw['date_dt'] <= end_dt)]
            if not dsat_raw.empty:
                d_f = dsat_raw[(dsat_raw['date_dt'] >= start_dt) & (dsat_raw['date_dt'] <= end_dt)]
            else:
                d_f = dsat_raw.copy()
else:
    k_f, d_f = kpi_raw.copy(), dsat_raw.copy()

if 'email' not in d_f.columns:
    if 'name' in d_f.columns:
        d_f = d_f.merge(team_db.dropna(subset=['name', 'email'])[['name', 'email']], on='name', how='left')
    if 'email' not in d_f.columns: 
        d_f['email'] = ""

if 'email' not in k_f.columns:
    k_f['email'] = ""

# --- 6. HIERARCHY DRILL-DOWN ---
access = str(user.get('level', 'IC')).strip()
scoped_emails = []

# New logic for hiding the advisor column in tables
show_advisor_col = True

if access in ["Admin", "Manager"]:
    mode = st.sidebar.selectbox("View Mode", ["Entire Team", "Specific Advisor"])
    
    all_team_emails = team_db['email'].dropna().unique().tolist()
    
    if mode == "Entire Team": 
        scoped_emails = all_team_emails
        show_advisor_col = True
    else:
        adv_options = sorted(team_db[team_db['level'] == 'IC']['name'].dropna().astype(str).unique().tolist())
        if not adv_options: 
            adv_options = sorted(team_db['name'].dropna().astype(str).unique().tolist())
            
        adv_sel = st.sidebar.selectbox("Select Advisor", adv_options)
        found = team_db[team_db['name'] == adv_sel]['email'].tolist()
        scoped_emails = found if found else all_team_emails
        show_advisor_col = False
else:
    scoped_emails = [user.get('email')]
    show_advisor_col = False

f_kpi = k_f[k_f['email'].isin(scoped_emails)]
f_dsat = d_f[d_f['email'].isin(scoped_emails)]

# --- 7. MAIN UI ---
header_col1, header_col2 = st.columns([1, 8])
with header_col1: st.image(LOGO_URL, width=130)
with header_col2: 
    st.write("") # Vertical alignment
    st.title("GoHighLevel Performance Hub")

st.success(f"Welcome **{user.get('name', 'User')}**! | Access Level : **{access}**")

# --- GLOBALLY PRE-CALCULATE TRUE AGGREGATES ---
if not f_dsat.empty and 'is_csat' in f_dsat.columns:
    agent_csat_stats = f_dsat.groupby('name').agg(
        true_surveys=('is_csat', 'count'),
        true_satisfied=('is_csat', 'sum') 
    ).reset_index()
    agent_csat_stats['true_sat_rate'] = np.where(agent_csat_stats['true_surveys'] > 0, (agent_csat_stats['true_satisfied'] / agent_csat_stats['true_surveys']) * 100, 0)
else:
    agent_csat_stats = pd.DataFrame(columns=['name', 'true_surveys', 'true_satisfied', 'true_sat_rate'])

tabs_list = ["📊 Performance Overview", "⭐ Satisfaction Survey"]
if access != "IC":
    tabs_list.append("🏆 Leaderboards")
if access == "Admin":
    tabs_list.append("📋 Scorecard")
tabs_list.append("📄 Detailed Report")

ui_tabs = st.tabs(tabs_list)
tab_perf = ui_tabs[0]
tab_dsat = ui_tabs[1]

current_tab_idx = 2

if access != "IC":
    tab_lead = ui_tabs[current_tab_idx]
    current_tab_idx += 1
else:
    tab_lead = None

if access == "Admin":
    tab_scorecard = ui_tabs[current_tab_idx]
    current_tab_idx += 1
else:
    tab_scorecard = None

tab_report = ui_tabs[current_tab_idx]

with tab_perf:
    st.markdown(f"### <img src='{LOGO_URL}' class='tab-logo'> Performance Narrative", unsafe_allow_html=True)
    
    avg_ia_hrs_n = (f_kpi['ia_min'].mean() / 60) if (not f_kpi.empty and 'ia_min' in f_kpi.columns) else 0
    tot_surveys_n = len(f_dsat)
    tot_satisfied_n = len(f_dsat[f_dsat['is_csat'] == True]) if 'is_csat' in f_dsat.columns else 0
    avg_sat_n = (tot_satisfied_n / tot_surveys_n * 100) if tot_surveys_n > 0 else 0
    
    tot_abandons_n = int(f_kpi['callabandons'].fillna(0).sum()) if not f_kpi.empty and 'callabandons' in f_kpi.columns else 0
    tot_fresh_made_n = int(f_kpi['fresh_calls_made'].fillna(0).sum()) if not f_kpi.empty and 'fresh_calls_made' in f_kpi.columns else 0
    
    fresh_ratio_text = ""
    if tot_abandons_n > 0:
        if tot_fresh_made_n >= tot_abandons_n:
            fresh_ratio_text = f"Outstanding follow-up effort! The team made **{tot_fresh_made_n}** fresh calls against **{tot_abandons_n}** abandons, successfully meeting the ideal 1:1 follow-up requirement."
        else:
            fresh_ratio_text = f"Attention needed on call follow-ups: The team recorded **{tot_abandons_n}** call abandons but only made **{tot_fresh_made_n}** fresh calls. The ideal target is to make at least one fresh call for every abandoned call to maximize engagement."
    else:
        fresh_ratio_text = f"Great job minimizing missed opportunities with **0** call abandons recorded, while successfully executing **{tot_fresh_made_n}** outbound fresh calls."

    st.info(f"During this period, the selected group achieved a True Aggregate Satisfaction of **{avg_sat_n:.2f}%** and averaged **{avg_ia_hrs_n:.1f} hours** of IA time. {fresh_ratio_text}")
    
    st.markdown(f"### <img src='{LOGO_URL}' class='tab-logo'> Performance Summary", unsafe_allow_html=True)
    
    c1, c2, c3, c4 = st.columns(4)
    c5, c6, c7, c8 = st.columns(4)
    
    avg_sent = f_kpi['sent_rate'].dropna().mean() if not f_kpi.empty and 'sent_rate' in f_kpi.columns else 0
    
    tot_surveys = len(f_dsat)
    tot_satisfied = len(f_dsat[f_dsat['is_csat'] == True]) if 'is_csat' in f_dsat.columns else 0
    avg_sat = (tot_satisfied / tot_surveys * 100) if tot_surveys > 0 else 0
    
    tot_ob = int(f_kpi['ob'].fillna(0).sum()) if not f_kpi.empty else 0
    tot_qa = int(f_kpi['qa'].fillna(0).sum()) if not f_kpi.empty else 0
    
    ob_time_str = "-"
    if not f_kpi.empty and 'avgobcalltime' in f_kpi.columns:
        mean_td_ob = pd.to_timedelta(f_kpi['avgobcalltime'].astype(str), errors='coerce').mean()
        if pd.notna(mean_td_ob):
            ts_ob = mean_td_ob.total_seconds()
            ob_time_str = f"{int(ts_ob // 3600):02d}:{int((ts_ob % 3600) // 60):02d}:{int(ts_ob % 60):02d}"

    qa_time_str = "-"
    if not f_kpi.empty and 'avgqacalltime' in f_kpi.columns:
        mean_td_qa = pd.to_timedelta(f_kpi['avgqacalltime'].astype(str), errors='coerce').mean()
        if pd.notna(mean_td_qa):
            ts_qa = mean_td_qa.total_seconds()
            qa_time_str = f"{int(ts_qa // 3600):02d}:{int((ts_qa % 3600) // 60):02d}:{int(ts_qa % 60):02d}"

    avg_ia_hrs = (f_kpi['ia_min'].mean() / 60) if (not f_kpi.empty and 'ia_min' in f_kpi.columns) else 0
    ia_color = "#22C55E" if avg_ia_hrs > 6 else ("#F59E0B" if avg_ia_hrs >= 5 else "#EF4444")
    
    # ROW 1
    c1.markdown(create_metric_card("Avg Survey Sent", avg_sent, 85, True, "Target: >85%"), unsafe_allow_html=True)
    c2.markdown(create_metric_card("Avg Satisfied (True Aggregate)", avg_sat, 90, True, "Target: >90%"), unsafe_allow_html=True)
    c3.markdown(create_metric_card("Total Surveys", tot_surveys, None, False), unsafe_allow_html=True)
    c4.markdown(format_custom_card("Avg IA Hours", f"{avg_ia_hrs:.1f}h", ia_color, "Target: >6.0h"), unsafe_allow_html=True)

    # ROW 2
    c5.markdown(create_metric_card("Total OB Calls", tot_ob, None, False), unsafe_allow_html=True)
    c6.markdown(format_custom_card("Avg OB Call Time", ob_time_str, "#0052FF", "Activity Metric"), unsafe_allow_html=True)
    c7.markdown(create_metric_card("Total QA Calls", tot_qa, None, False), unsafe_allow_html=True)
    c8.markdown(format_custom_card("Avg QA Call Time", qa_time_str, "#0052FF", "Activity Metric"), unsafe_allow_html=True)

    st.markdown("---")
    st.markdown(f"### <img src='{LOGO_URL}' class='tab-logo'> Call Abandons & Fresh Calls", unsafe_allow_html=True)
    ca1, ca2, ca3 = st.columns(3)
    
    tot_abandons = int(f_kpi['callabandons'].fillna(0).sum()) if 'callabandons' in f_kpi.columns else 0
    tot_fresh_made = int(f_kpi['fresh_calls_made'].fillna(0).sum()) if 'fresh_calls_made' in f_kpi.columns else 0
    tot_fresh_conn = int(f_kpi['connected_fresh_calls'].fillna(0).sum()) if 'connected_fresh_calls' in f_kpi.columns else 0
    
    ca1.markdown(format_custom_card("Total Call Abandons", tot_abandons, "#EF4444", "Activity Metric"), unsafe_allow_html=True)
    ca2.markdown(format_custom_card("Fresh Calls Made", tot_fresh_made, "#0052FF", "Activity Metric"), unsafe_allow_html=True)
    ca3.markdown(format_custom_card("Connected Fresh Calls", tot_fresh_conn, "#22C55E", "Activity Metric"), unsafe_allow_html=True)

    st.markdown("---")
    st.markdown(f"### <img src='{LOGO_URL}' class='tab-logo'> Performance Trends", unsafe_allow_html=True)
    if not f_kpi.empty:
        trend = f_kpi.groupby('date_dt').agg(
            sent_rate=('sent_rate', lambda x: x.dropna().mean()),
            sat_rate=('sat_rate', lambda x: x.dropna().mean()),
            ob=('ob', 'sum'),
            qa=('qa', 'sum'),
            ia_min=('ia_min', 'sum'),
            call_min=('call_min', 'sum')
        ).reset_index().sort_values('date_dt')
        
        trend['shift_score'] = np.where(trend['ia_min'] > 0, (trend['call_min'] / trend['ia_min']) * 100, 0)
        
        t1, t2 = st.columns(2)
        with t1: st.plotly_chart(px.line(trend, x='date_dt', y='sent_rate', title="Survey Sent Trend (%)", markers=True), use_container_width=True)
        with t2: st.plotly_chart(px.line(trend, x='date_dt', y='sat_rate', title="Satisfied Survey Trend (%)", markers=True), use_container_width=True)
        
        t3, t4, t5 = st.columns(3)
        with t3: st.plotly_chart(px.line(trend, x='date_dt', y='shift_score', title="Shift Score Trend (%)", markers=True), use_container_width=True)
        with t4: st.plotly_chart(px.bar(trend, x='date_dt', y='ob', title="Total OB Calls"), use_container_width=True)
        with t5: st.plotly_chart(px.bar(trend, x='date_dt', y='qa', title="Total OH Calls"), use_container_width=True)

with tab_dsat:
    st.markdown(f"### <img src='{LOGO_URL}' class='tab-logo'> Satisfaction Survey Summary", unsafe_allow_html=True)
    
    total_surveys = len(f_dsat)
    satisfied_surveys = len(f_dsat[f_dsat['is_csat'] == True]) if 'is_csat' in f_dsat.columns else 0
    dsat_surveys = len(f_dsat[f_dsat['is_csat'] == False]) if 'is_csat' in f_dsat.columns else total_surveys
    csat_pct = (satisfied_surveys / total_surveys * 100) if total_surveys > 0 else 0
    
    dsat_df = f_dsat[f_dsat['is_csat'] == False] if 'is_csat' in f_dsat.columns else f_dsat
    
    if 'feedback' in dsat_df.columns:
        is_missing = dsat_df['feedback'].isna() | dsat_df['feedback'].astype(str).str.strip().str.lower().isin(['', 'nan', '-', 'none', 'null'])
        pending_count = is_missing.sum()
    else:
        pending_count = len(dsat_df)
        
    control_count = len(dsat_df[dsat_df['type'] == 'Controllable']) if 'type' in dsat_df.columns else 0
    uncontrol_count = len(dsat_df[dsat_df['type'] == 'Uncontrollable']) if 'type' in dsat_df.columns else 0
    
    s1, s2, s3, s4 = st.columns(4)
    s1.markdown(format_custom_card("Total Surveys", total_surveys, "#0052FF", "Overall Volume"), unsafe_allow_html=True)
    s2.markdown(format_custom_card("Total Satisfied", satisfied_surveys, "#22C55E", "CSAT Count"), unsafe_allow_html=True)
    s3.markdown(format_custom_card("Total DSATs", dsat_surveys, "#EF4444", "Negative Count"), unsafe_allow_html=True)
    
    csat_color = "#22C55E" if csat_pct > 90 else ("#F59E0B" if csat_pct >= 75 else "#EF4444")
    s4.markdown(format_custom_card("Satisfaction % (True Aggregate)", f"{csat_pct:.2f}%", csat_color, "Target: >90%"), unsafe_allow_html=True)

    s5, s6, s7 = st.columns(3)
    s5.markdown(format_custom_card("Feedback Pending", pending_count, "#F59E0B" if pending_count > 0 else "#22C55E", "Action Required"), unsafe_allow_html=True)
    s6.markdown(format_custom_card("Controllable", control_count, "#8B5CF6", "DSAT Type"), unsafe_allow_html=True)
    s7.markdown(format_custom_card("Uncontrollable", uncontrol_count, "#64748B", "DSAT Type"), unsafe_allow_html=True)

    if access == "Admin":
        st.markdown("#### 🔒 Admin Insights")
        a3, a4 = st.columns(2)
        kpi_csat_dsat_tab = f_kpi['sat_rate'].dropna().mean() if not f_kpi.empty and 'sat_rate' in f_kpi.columns else 0
        kpi_surveys_dsat_tab = int(f_kpi['surveys'].fillna(0).sum()) if not f_kpi.empty and 'surveys' in f_kpi.columns else 0
        
        a3.markdown(format_custom_card("Admin Insight: KPI Sheet CSAT", f"{kpi_csat_dsat_tab:.2f}%", "#8B5CF6", "Average of Averages (from KPI Sheet)"), unsafe_allow_html=True)
        a4.markdown(format_custom_card("Admin Insight: KPI Sheet Surveys", kpi_surveys_dsat_tab, "#8B5CF6", "Total Surveys (from KPI Sheet)"), unsafe_allow_html=True)
        
        display_admin_discrepancies(f_kpi, f_dsat)

    st.markdown("---")
    
    tab_pos, tab_neg = st.tabs(["👍 Positive Feedback (CSAT)", "👎 Negative Feedback (DSAT)"])
    
    with tab_pos:
        st.markdown("#### Positive Customer Feedback")
        pos_df = f_dsat[f_dsat['is_csat'] == True] if 'is_csat' in f_dsat.columns else pd.DataFrame()
        
        if not pos_df.empty:
            pos_table = pos_df.copy()
            
            headers = ["Date"]
            col_w = [1.2]
            
            if show_advisor_col:
                headers.append("Advisor Name")
                col_w.append(1.5)
                
            headers.extend(["Customer Email", "Call Type", "Customer Comment", "Chat Link"])
            col_w.extend([2, 1, 3, 1.2])
            
            header_cols = st.columns(col_w)
            for i, h in enumerate(headers): header_cols[i].write(f"**{h}**")
            st.divider()
            
            for idx, row in pos_table.reset_index().iterrows():
                r = st.columns(col_w)
                date_str = str(row['date_dt'])[:10] if pd.notna(row['date_dt']) else "-"
                
                c_idx = 0
                r[c_idx].write(date_str); c_idx += 1
                
                if show_advisor_col:
                    r[c_idx].write(row.get('name', '-')); c_idx += 1
                    
                r[c_idx].write(row.get('customeremail', '-')); c_idx += 1
                r[c_idx].write(row.get('call_type', '-')); c_idx += 1
                
                cmt = str(row.get('customercomments', '-'))
                r[c_idx].write("-" if cmt.lower() in ['null', 'nan', ''] else cmt); c_idx += 1
                
                link_val = row.get('link', '-')
                if link_val != "-":
                    r[c_idx].markdown(f"[🔗 View Chat]({link_val})")
                else:
                    r[c_idx].write("-")
        else:
            st.info("No positive feedback available for the selected period.")
            
    with tab_neg:
        st.markdown("#### DSAT Details & Action Plan")
        
        dsat_filter = "All"
        if access in ["Admin", "Manager"]:
            dsat_filter = st.radio(
                "Filter DSATs:", 
                ["All", "Pending", "Controllable", "Uncontrollable"], 
                horizontal=True
            )
            st.write("") # small spacer
        
        if not dsat_df.empty:
            neg_table = dsat_df.copy()
            
            if dsat_filter == "Pending":
                is_missing = neg_table['feedback'].isna() | neg_table['feedback'].astype(str).str.strip().str.lower().isin(['', 'nan', '-', 'none', 'null'])
                neg_table = neg_table[is_missing]
            elif dsat_filter == "Controllable":
                neg_table = neg_table[neg_table['type'].astype(str).str.strip().str.title() == 'Controllable']
            elif dsat_filter == "Uncontrollable":
                neg_table = neg_table[neg_table['type'].astype(str).str.strip().str.title() == 'Uncontrollable']
            
            if not neg_table.empty:
                headers = ["Date"]
                col_w = [1.2]
                
                if show_advisor_col:
                    headers.append("Advisor Name")
                    col_w.append(1.5)
                    
                headers.extend(["Customer Email", "Call Type", "Customer Comment", "Chat Link", "Type", "Feedback"])
                col_w.extend([2, 1, 3, 1.2, 1, 2])
                
                if access != "IC": 
                    headers.append("Action")
                    col_w.append(1)
                
                header_cols = st.columns(col_w)
                for i, h in enumerate(headers): header_cols[i].write(f"**{h}**")
                st.divider()
                
                for idx, row in neg_table.reset_index().iterrows():
                    r = st.columns(col_w)
                    date_str = str(row['date_dt'])[:10] if pd.notna(row['date_dt']) else "-"
                    
                    fb = str(row.get('feedback', '-'))
                    fb = "-" if fb.lower() in ['nan', 'null', ''] else fb
                    
                    tp = str(row.get('type', '-'))
                    tp = "-" if tp.lower() in ['nan', 'null', ''] else tp
                    
                    cmt = str(row.get('customercomments', '-'))
                    cmt = "-" if cmt.lower() in ['nan', 'null', ''] else cmt
                    
                    link_val = row.get('link', '-')
                    
                    c_idx = 0
                    r[c_idx].write(date_str); c_idx += 1
                    
                    if show_advisor_col:
                        r[c_idx].write(row.get('name', '-')); c_idx += 1
                        
                    r[c_idx].write(row.get('customeremail', '-')); c_idx += 1
                    r[c_idx].write(row.get('call_type', '-')); c_idx += 1
                    r[c_idx].write(cmt); c_idx += 1
                    
                    if link_val != "-":
                        r[c_idx].markdown(f"[🔗 View Chat]({link_val})")
                    else:
                        r[c_idx].write("-")
                    c_idx += 1
                    
                    r[c_idx].write(tp); c_idx += 1
                    r[c_idx].write(fb); c_idx += 1
                    
                    if access != "IC":
                        if r[c_idx].button("📝 Update", key=f"upd_{idx}"):
                            open_form_dialog(row)
            else:
                st.info(f"No {dsat_filter.lower()} DSATs recorded for the selected period.")
        else:
            st.info("No DSATs recorded for the selected period.")

if tab_lead:
    with tab_lead:
        st.markdown(f"### <img src='{LOGO_URL}' class='tab-logo'> Success Champions", unsafe_allow_html=True)
        if not f_kpi.empty:
            ldb = f_kpi.groupby('name').agg(
                sent_rate=('sent_rate', lambda x: x.dropna().mean()),
                qa=('qa', 'sum'),
                ob=('ob', 'sum')
            ).reset_index()
            
            ldb = ldb.merge(agent_csat_stats[['name', 'true_sat_rate']], on='name', how='left')
            ldb['sat_rate'] = ldb['true_sat_rate'].fillna(0)
            
            ldb['sent_rate'] = ldb['sent_rate'].apply(lambda x: round(x, 2) if pd.notna(x) else 0.00)
            ldb['sat_rate'] = ldb['sat_rate'].apply(lambda x: round(x, 2) if pd.notna(x) else 0.00)
            ldb['qa'] = ldb['qa'].fillna(0)
            ldb['ob'] = ldb['ob'].fillna(0)
            
            st.caption("Advisors maintaining an Avg Survey Sent ≥ 85.00% AND Avg Satisfied Survey ≥ 90.00%.")
            champs = ldb[(ldb['sent_rate'] >= 85) & (ldb['sat_rate'] >= 90)].sort_values(['sat_rate', 'sent_rate'], ascending=[False, False])
            
            champs_fmt = champs.copy()
            champs_fmt['sat_rate'] = champs_fmt['sat_rate'].apply(lambda x: f"{x:.2f}%")
            champs_fmt['sent_rate'] = champs_fmt['sent_rate'].apply(lambda x: f"{x:.2f}%")
            
            if not champs_fmt.empty:
                st.dataframe(champs_fmt[['name', 'sat_rate', 'sent_rate']].rename(columns={'name': 'Advisor Name', 'sat_rate': 'Satisfied %', 'sent_rate': 'Survey Sent %'}), hide_index=True, use_container_width=True)
            else:
                st.info("No Success Champions met the criteria in this period.")

            st.markdown("---")
            
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("#### 📈 Survey Sent %")
                df_sent = ldb.sort_values('sent_rate', ascending=False)[['name', 'sent_rate']]
                df_sent['sent_rate'] = df_sent['sent_rate'].apply(lambda x: f"{x:.2f}%")
                st.dataframe(df_sent.rename(columns={'name': 'Advisor Name', 'sent_rate': 'Survey Sent %'}), hide_index=True, use_container_width=True)
                
            with c2:
                st.markdown("#### ⭐ Satisfied Survey %")
                df_sat = ldb.sort_values('sat_rate', ascending=False)[['name', 'sat_rate']]
                df_sat['sat_rate'] = df_sat['sat_rate'].apply(lambda x: f"{x:.2f}%")
                st.dataframe(df_sat.rename(columns={'name': 'Advisor Name', 'sat_rate': 'Satisfied %'}), hide_index=True, use_container_width=True)
                
            st.markdown("---")
            
            c3, c4 = st.columns(2)
            with c3:
                st.markdown("#### 📞 Top QA Guru")
                st.dataframe(ldb.sort_values('qa', ascending=False)[['name', 'qa']].rename(columns={'name': 'Advisor Name', 'qa': 'Total QA Calls'}), hide_index=True, use_container_width=True)
            with c4:
                st.markdown("#### 🚀 OB Expert")
                st.dataframe(ldb.sort_values('ob', ascending=False)[['name', 'ob']].rename(columns={'name': 'Advisor Name', 'ob': 'Total OB Calls'}), hide_index=True, use_container_width=True)

if tab_scorecard:
    with tab_scorecard:
        st.markdown(f"### <img src='{LOGO_URL}' class='tab-logo'> Advisor Scorecard", unsafe_allow_html=True)
        st.caption("Aggregated KPI overview for all advisors in the current filtered period.")
        
        if not f_kpi.empty:
            sc_df = f_kpi.groupby('name').agg(
                sent_rate=('sent_rate', lambda x: x.dropna().mean()),
                shift_score=('shift_score', lambda x: x.dropna().mean()),
                ob=('ob', 'sum'),
                qa=('qa', 'sum')
            ).reset_index()
            
            sc_df = sc_df.merge(agent_csat_stats[['name', 'true_surveys', 'true_sat_rate']], on='name', how='left')
            sc_df['sat_rate'] = sc_df['true_sat_rate'].fillna(0)
            sc_df['surveys'] = sc_df['true_surveys'].fillna(0)
            
            sc_df['sent_rate'] = sc_df['sent_rate'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "-")
            sc_df['sat_rate'] = sc_df['sat_rate'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "-")
            sc_df['shift_score'] = sc_df['shift_score'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "-")
            sc_df['surveys'] = sc_df['surveys'].astype(int)
            
            sc_df = sc_df.rename(columns={
                'name': 'Advisor Name', 'surveys': 'Total Surveys', 'sent_rate': 'Avg Sent %',
                'sat_rate': 'Avg Sat %', 'shift_score': 'Avg Shift Score %', 'ob': 'Total OB', 'qa': 'Total QA'
            })
            
            sc_cols = ['Advisor Name', 'Total Surveys', 'Avg Sent %', 'Avg Sat %', 'Avg Shift Score %', 'Total OB', 'Total QA']
            st.dataframe(sc_df[sc_cols], hide_index=True, use_container_width=True)
        else:
            st.info("No data available to generate scorecards.")

with tab_report:
    st.markdown(f"### <img src='{LOGO_URL}' class='tab-logo'> Detailed KPI Report", unsafe_allow_html=True)
    st.caption("Comprehensive view of all available data points (Excluding internal identifiers).")
    
    if not f_kpi.empty:
        rep_df = pd.DataFrame()
        
        rep_df['Date'] = f_kpi['date_dt'].dt.strftime('%Y-%m-%d')
        rep_df['Agent Name'] = f_kpi['name']
        if 'shift' in f_kpi.columns: rep_df['Shift'] = f_kpi['shift']
        
        rep_df['IA'] = f_kpi['ia_raw'] if 'ia_raw' in f_kpi.columns else "-"
        rep_df['Advisor Call Time'] = f_kpi['call_raw'] if 'call_raw' in f_kpi.columns else "-"
        rep_df['Avg OB Call Time'] = f_kpi['avgobcalltime'].fillna("-") if 'avgobcalltime' in f_kpi.columns else "-"
        rep_df['Avg Q/A Call Time'] = f_kpi['avgqacalltime'].fillna("-") if 'avgqacalltime' in f_kpi.columns else "-"
        rep_df['Time Off'] = f_kpi['timeoff'].fillna("-") if 'timeoff' in f_kpi.columns else "-"
        
        rep_df['Call Abandons'] = f_kpi['callabandons'].fillna(0).astype(int) if 'callabandons' in f_kpi.columns else 0
        rep_df['MOB'] = f_kpi['mob'].fillna(0).astype(int) if 'mob' in f_kpi.columns else 0
        rep_df['OB Calls'] = f_kpi['ob'].fillna(0).astype(int)
        rep_df['Q/A Calls'] = f_kpi['qa'].fillna(0).astype(int)
        rep_df['Tickets Created'] = f_kpi['ticketscreated'].fillna(0).astype(int) if 'ticketscreated' in f_kpi.columns else 0
        
        rep_df['Total Survey'] = f_kpi['surveys'].fillna(0).astype(int)
        rep_df['Sent Rate %'] = f_kpi['sent_rate'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "-")
        rep_df['Satisfied Survey %'] = f_kpi['sat_rate'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "-")
        
        rep_df['Fresh Calls Made'] = f_kpi['fresh_calls_made'].fillna(0).astype(int) if 'fresh_calls_made' in f_kpi.columns else 0
        rep_df['Connected Fresh Calls'] = f_kpi['connected_fresh_calls'].fillna(0).astype(int) if 'connected_fresh_calls' in f_kpi.columns else 0
        rep_df['Un-Resolved'] = f_kpi['unresolved'].fillna(0).astype(int) if 'unresolved' in f_kpi.columns else 0
        
        avg_row = {col: "" for col in rep_df.columns}
        avg_row['Date'] = "AVG & TOTALS" 
        
        avg_ia = f_kpi['ia_min'].mean()
        avg_row['IA'] = f"{int(avg_ia // 60)}h {int(avg_ia % 60)}m" if pd.notna(avg_ia) else "-"
        
        avg_call = f_kpi['call_min'].mean()
        avg_row['Advisor Call Time'] = f"{int(avg_call // 60)}h {int(avg_call % 60)}m" if pd.notna(avg_call) else "-"
        
        avg_row['Call Abandons'] = int(f_kpi['callabandons'].fillna(0).sum()) if 'callabandons' in f_kpi.columns else 0
        avg_row['MOB'] = int(f_kpi['mob'].fillna(0).sum()) if 'mob' in f_kpi.columns else 0
        avg_row['OB Calls'] = int(f_kpi['ob'].fillna(0).sum())
        avg_row['Q/A Calls'] = int(f_kpi['qa'].fillna(0).sum())
        avg_row['Tickets Created'] = int(f_kpi['ticketscreated'].fillna(0).sum()) if 'ticketscreated' in f_kpi.columns else 0
        
        tot_surveys = len(f_dsat)
        avg_row['Total Survey'] = tot_surveys
        
        avg_sent_val = f_kpi['sent_rate'].dropna().mean()
        avg_row['Sent Rate %'] = f"{avg_sent_val:.2f}%" if pd.notna(avg_sent_val) else "-"
        
        tot_satisfied = len(f_dsat[f_dsat['is_csat'] == True]) if 'is_csat' in f_dsat.columns else 0
        true_sat_pct = (tot_satisfied / tot_surveys * 100) if tot_surveys > 0 else 0
        avg_row['Satisfied Survey %'] = f"{true_sat_pct:.2f}%" if tot_surveys > 0 else "-"
        
        avg_row['Fresh Calls Made'] = int(f_kpi['fresh_calls_made'].fillna(0).sum()) if 'fresh_calls_made' in f_kpi.columns else 0
        avg_row['Connected Fresh Calls'] = int(f_kpi['connected_fresh_calls'].fillna(0).sum()) if 'connected_fresh_calls' in f_kpi.columns else 0
        avg_row['Un-Resolved'] = int(f_kpi['unresolved'].fillna(0).sum()) if 'unresolved' in f_kpi.columns else 0
        
        if 'Avg OB Call Time' in rep_df.columns:
            mean_td = pd.to_timedelta(f_kpi['avgobcalltime'].astype(str), errors='coerce').mean()
            if pd.notna(mean_td):
                ts = mean_td.total_seconds()
                avg_row['Avg OB Call Time'] = f"{int(ts // 3600):02d}:{int((ts % 3600) // 60):02d}:{int(ts % 60):02d}"
            else: avg_row['Avg OB Call Time'] = "-"
            
        if 'Avg Q/A Call Time' in rep_df.columns:
            mean_td = pd.to_timedelta(f_kpi['avgqacalltime'].astype(str), errors='coerce').mean()
            if pd.notna(mean_td):
                ts = mean_td.total_seconds()
                avg_row['Avg Q/A Call Time'] = f"{int(ts // 3600):02d}:{int((ts % 3600) // 60):02d}:{int(ts % 60):02d}"
            else: avg_row['Avg Q/A Call Time'] = "-"
            
        rep_df = pd.concat([rep_df, pd.DataFrame([avg_row])], ignore_index=True)
        
        def highlight_last_row(row):
            if row.name == rep_df.index[-1]:
                return ['font-weight: bold; background-color: #E5EDFF; color: #0052FF; padding: 15px 10px; font-size: 14px; border-top: 2px solid #0052FF;'] * len(row)
            return [''] * len(row)

        styled_df = rep_df.style.apply(highlight_last_row, axis=1)

        st.dataframe(styled_df, hide_index=True, use_container_width=True)
        
        csv_data = rep_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download Detailed Report CSV",
            data=csv_data,
            file_name="detailed_performance_report.csv",
            mime="text/csv",
        )
    else:
        st.info("No data available for the selected filters.")

# --- 8. LOGOUT (WIPES URL SESSION) ---
st.sidebar.divider()
if st.sidebar.button("Logout"): 
    st.session_state.auth = None
    st.query_params.clear()
    st.rerun()
