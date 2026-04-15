# ==============================================================================
# 1. CORE LIBRARIES
# ==============================================================================
import streamlit as st 
import pandas as pd     
import plotly.express as px  
import plotly.graph_objects as go  
import plotly.io as pio  
import sqlite3          
from datetime import datetime  
from fpdf import FPDF   
import base64           
import re               
import tempfile         
import os               

# ==============================================================================
# 2. SYSTEM CONFIGURATION & MEMORY
# ==============================================================================
st.set_page_config(page_title="Production Analytics Master", layout="wide")
pio.templates.default = "plotly_white"

# Initialize Session State
if 'raw_data' not in st.session_state: st.session_state.raw_data = None
if 'trend_event_code' not in st.session_state: st.session_state.trend_event_code = "All"
if 'tk_main' not in st.session_state: st.session_state.tk_main = 0
if 'tk_fault' not in st.session_state: st.session_state.tk_fault = 100
if 'dialog_trigger' not in st.session_state: st.session_state.dialog_trigger = None

def init_database():
    connection = sqlite3.connect('maintenance_data.db', check_same_thread=False)
    cursor = connection.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS notes 
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, DeviceName TEXT, 
                      EventCode TEXT, TechnicianName TEXT, CorrectiveAction TEXT, ActionDate TEXT)''')
    connection.commit()
    return connection

db_conn = init_database()

# ==============================================================================
# 3. ANALYTICS ENGINE (Mathematical Logic)
# ==============================================================================

def color_delta_styling(val):
    try:
        if val > 0.001: return 'color: #ff4b4b; font-weight: bold'
        elif val < -0.001: return 'color: #28a745; font-weight: bold'
    except: pass
    return 'color: #31333F'

def format_seconds_to_clock(seconds):
    seconds = max(0, seconds)
    h, m_rem = divmod(int(seconds), 3600)
    m, s = divmod(m_rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def clean_duration_with_safety(series):
    """
    1st PRINCIPLES FIX: Safety-Capped Duration Cleaner.
    WHY: Prevents malformed Excel data (dates/odometers) from scaling charts to 'Billions'.
    LIMIT: 86,400 seconds (24 hours).
    """
    def parse_one(x):
        try:
            x_str = str(x).strip().lower()
            if ':' in x_str:
                p = x_str.split(':')
                val = int(p[0])*3600 + int(p[1])*60 + float(p[2]) if len(p)==3 else int(p[0])*60 + float(p[1])
            else: 
                val = float(re.sub(r'[^0-9.]', '', x_str))
                if 0 < val < 1.0: val = val * 86400.0 # Excel Fraction to Seconds
            # THE SAFETY CAP (V65 LOGIC): 
            # If a duration is > 24h, it's likely a date error. Treat as 0 to protect chart scale.
            return val if 0 <= val <= 86400.0 else 0.0
        except: return 0.0
    return series.apply(parse_one)

@st.cache_data
def process_uploaded_file(file):
    df = pd.read_excel(file)
    df.columns = [str(c).strip() for c in df.columns]
    f = lambda keys: next((c for c in df.columns if any(k in c.lower() for k in keys)), None)
    
    clean = pd.DataFrame()
    clean['DeviceName'] = df[f(['device'])].astype(str)
    clean['EventCode'] = df[f(['code'])].astype(str).str.replace('.0', '', regex=False)
    clean['EventType'] = df[f(['type'])].astype(str) if f(['type']) else "Fault"
    clean['EventDescription'] = df[f(['desc'])].astype(str) if f(['desc']) else ""
    clean['EventDate'] = pd.to_datetime(df[f(['date', 'time'])], errors='coerce')
    
    dur_col = next((c for c in df.columns if 'duration' in c.lower() and 'date' not in c.lower()), None)
    clean['Duration'] = clean_duration_with_safety(df[dur_col]) if dur_col else 0.0
    clean['SystemCounter'] = pd.to_numeric(df[f(['counter'])], errors='coerce').fillna(0)
    return clean.dropna(subset=['EventDate'])

def get_metrics_calculation(source_df, dates, types, device, group_cols, global_cycles=None):
    """
    Calculates aggregated Count and Duration.
    Strictly isolates 'Duration' from 'SystemCounter' to prevent 1B scale error.
    """
    if len(dates) != 2: return pd.DataFrame()
    mask = (source_df['EventDate'].dt.date >= dates[0]) & (source_df['EventDate'].dt.date <= dates[1])
    active = source_df.loc[mask]
    if device != "All": active = active[active['DeviceName'] == device]
    if active.empty: return pd.DataFrame(columns=group_cols + ['Final_Count', 'Final_Duration', 'Cycles', 'Fault%'])

    v_cyc = active[active['SystemCounter'] > 0]
    cycles_by_m = v_cyc.groupby('DeviceName')['SystemCounter'].agg(lambda x: int(max(0, x.max() - x.min()))).reset_index(name='Cycles')
    
    events = active[active['EventType'].isin(types)]
    if events.empty:
        res = cycles_by_m.copy()
        for col in group_cols: 
            if col not in res.columns: res[col] = "N/A"
        res['Final_Count'], res['Final_Duration'] = 0, 0.0
    else:
        # Aggregate into specifically named columns to prevent data leakage
        agg = events.groupby(group_cols).agg(Final_Count=('EventCode', 'count'), Final_Duration=('Duration', 'sum')).reset_index()

        if group_cols == ['DeviceName']:
            dm = events.groupby('DeviceName')['EventDescription'].first().reset_index()
            agg = pd.merge(agg, dm, on='DeviceName', how='left')

        if 'DeviceName' in group_cols: res = pd.merge(agg, cycles_by_m, on='DeviceName', how='left').fillna(0)
        else:
            res = agg.copy()
            res['Cycles'] = global_cycles if global_cycles is not None else cycles_by_m['Cycles'].sum()
            
    res['Fault%'] = (res['Final_Count'] / res['Cycles'].replace(0, 1) * 100).round(2)
    return res

def get_total_fleet_cycles(source_df, dates, device):
    if len(dates) != 2: return 0
    mask = (source_df['EventDate'].dt.date >= dates[0]) & (source_df['EventDate'].dt.date <= dates[1])
    sub = source_df.loc[mask]
    if device != "All": sub = sub[sub['DeviceName'] == device]
    v = sub[sub['SystemCounter'] > 0]
    return int(v.groupby('DeviceName')['SystemCounter'].agg(lambda x: max(0, x.max() - x.min())).sum()) if not v.empty else 0

# ==============================================================================
# 4. CHART ENGINE (Corrected Dynamic Ranking)
# ==============================================================================

def create_cluster_stack_chart(df_w1, df_w2, x_col, stack_col, title, top_n, show_others, metric, desc_map=None, is_gallery=False):
    """
    Creates side-by-side clustered bars.
    Ranking strictly follows the left-hand Metric toggle (Duration or Count).
    """
    fig = go.Figure()
    val_col = 'Final_Count' if metric == "Count" else 'Final_Duration'
    y_label = "Total Occurrences" if metric == "Count" else "Total Minutes"
    
    w1_palette = ['#082245','#08306B','#08519C','#2171B5','#4292C6','#6BAED6','#9ECAE1','#C6DBEF','#DEEBF7','#F7FBFF']
    w2_palette = ['#67000D','#A50F15','#CB181D','#EF3B2C','#FB6A4A','#FC9272','#FCBBA1','#FEE0D2','#FFF5F0','#FFFBFB']
    
    def process_ranking(df_in):
        if df_in.empty or top_n == "All": return df_in
        limit = int(top_n.split()[-1])
        df_rank = df_in.copy()
        # RANK BY THE ACTIVE METRIC (V65 Style)
        df_rank['rank'] = df_rank.groupby(x_col)[val_col].rank(method='first', ascending=False)
        if show_others:
            df_rank.loc[df_rank['rank'] > limit, stack_col] = "Others"
            return df_rank.groupby([x_col, stack_col])[val_col].sum().reset_index()
        else: return df_rank[df_rank['rank'] <= limit]

    c_w1, c_w2 = process_ranking(df_w1), process_ranking(df_w2)

    def add_bars(df, label, offset, colors):
        if df.empty: return
        cats = sorted([c for c in df[stack_col].unique() if c != "Others"])
        if show_others and "Others" in df[stack_col].unique(): cats.append("Others")
        
        for i, category in enumerate(cats):
            sub = df[df[stack_col] == category]
            y_data = sub[val_col] if metric == "Count" else sub[val_col] / 60
            color = "#D3D3D3" if category == "Others" else colors[i % len(colors)]
            
            v_str = sub[val_col].apply(format_seconds_to_clock) if metric == "Duration" else sub[val_col].astype(str)
            if stack_col == 'EventCode' and desc_map:
                d_text = desc_map.get(category, "N/A")
                if is_gallery and len(d_text) > 50: d_text = d_text[:50] + "..."
                tip = f"<b>{label}</b><br>Code: {category}<br>Desc: {d_text}<br>{metric}: {v_str.iloc[0]}<extra></extra>"
            elif stack_col == 'DeviceName':
                tip = f"<b>{label}</b><br>Machine: {category}<br>{metric}: {v_str.iloc[0]}<extra></extra>"
            else:
                tip = f"<b>{label}</b><br>{stack_col}: {category}<br>{metric}: {v_str.iloc[0]}<extra></extra>"

            fig.add_trace(go.Bar(
                name=f"{label}: {category}", x=sub[x_col], y=y_data,
                offsetgroup=offset, marker=dict(color=color, line=dict(color='white', width=0.5)),
                legendgroup=label, hovertemplate=tip
            ))

    add_bars(c_w1, "Window 1", 0, w1_palette)
    add_bars(c_w2, "Window 2", 1, w2_palette)
    fig.update_layout(title=title, barmode='stack', yaxis_title=y_label, bargap=0.15, bargroupgap=0.1)
    return fig

# ==============================================================================
# 5. UI DIALOGS & PDF TOOLS
# ==============================================================================

@st.dialog("Fleet Log Detail", width="large")
def show_popup_logs(dev, code, t_type):
    st.subheader(f"History for {dev if t_type == 'main' else 'Code ' + str(code)}")
    sql = "SELECT * FROM notes WHERE DeviceName = ?" if t_type == "main" else "SELECT * FROM notes WHERE EventCode = ?"
    logs = pd.read_sql_query(sql, db_conn, params=(dev if t_type == 'main' else str(code),))
    if not logs.empty:
        def pop_styler(df_in):
            s = pd.DataFrame('', index=df_in.index, columns=df_in.columns); prev = None
            for i, m in enumerate(df_in['DeviceName']):
                if prev is not None and m != prev: s.iloc[i, :] = 'background-color: #deeaf7; border-top: 4px solid #31333F; font-weight: bold;'
                prev = m
            return s
        logs['idx'] = logs['DeviceName'].apply(lambda x: 0 if x == dev else 1)
        st.dataframe(logs.sort_values(by=['idx', 'ActionDate'], ascending=[True, False]).drop(columns=['idx'])[['DeviceName', 'ActionDate', 'EventCode', 'TechnicianName', 'CorrectiveAction']].style.apply(pop_styler, axis=None), use_container_width=True, hide_index=True)
    else: st.info("No records.")
    if st.button("Close"):
        if t_type == "main": st.session_state.tk_main += 1
        else: st.session_state.tk_fault += 1
        st.rerun()

class Reporter(FPDF):
    def __init__(self, dates):
        super().__init__(); self.dates = dates
    def header(self):
        self.set_font("helvetica", "B", 16); self.cell(0, 10, "Production Analytics Professional Report", align="C", ln=1)
        self.set_font("helvetica", "I", 10); self.cell(0, 10, f"Analysis Windows: {self.dates}", align="C", ln=1); self.ln(5)
    def footer(self): self.set_y(-15); self.set_font("helvetica", "I", 8); self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

def pdf_plot(pdf, fig, title, w=180):
    pdf.set_font("helvetica", "B", 12); pdf.cell(0, 10, title, ln=1)
    fid, path = tempfile.mkstemp(suffix=".png")
    try:
        os.close(fid)
        pio.write_image(fig, path, format="png", width=1000, height=500, scale=2)
        pdf.image(path, x=15, w=w)
    finally:
        if os.path.exists(path): os.remove(path)
    pdf.ln(5)

def pdf_table(pdf, df, title):
    pdf.set_font("helvetica", "B", 12); pdf.cell(0, 10, title, ln=1)
    pdf.set_font("helvetica", "B", 7); cw = 190 / len(df.columns)
    for c in df.columns: pdf.cell(cw, 8, str(c), 1, 0, 'C')
    pdf.ln(); pdf.set_font("helvetica", "", 6)
    for r in df.itertuples(index=False):
        for v in r: pdf.cell(cw, 7, str(v)[:15], 1, 0, 'C')
        pdf.ln()
    pdf.ln(5)

# ==============================================================================
# 6. MAIN UI & FILTERS
# ==============================================================================

upl = st.sidebar.file_uploader("Upload Excel File", type=["xlsx"])

if upl:
    raw_df_master = process_uploaded_file(upl)
    
    st.sidebar.markdown("### 🚫 Precise Filtering")
    # REQUIREMENT: EXCLUSION FILTERS AT THE TOP
    ignore_codes = st.sidebar.multiselect("⚠️ Ignore EventCodes (Global):", sorted(raw_df_master['EventCode'].unique().tolist()))
    ignore_devices_mas = st.sidebar.multiselect("⚠️ Hide from Master Gallery:", sorted(raw_df_master['DeviceName'].unique().tolist()))

    # Global Purge logic
    if ignore_codes: m_df = raw_df_master[~raw_df_master['EventCode'].isin(ignore_codes)]
    else: m_df = raw_df_master

    desc_lookup = m_df.groupby('EventCode')['EventDescription'].first().to_dict()
    
    st.sidebar.divider()
    s_dev = st.sidebar.selectbox("Active Device View", ["All"] + sorted(m_df['DeviceName'].unique().tolist()))
    s_typ = st.sidebar.multiselect("Event Type Filter", m_df['EventType'].unique(), default=list(m_df['EventType'].unique()))
    s_met = st.sidebar.radio("View Segments By:", ["Count", "Duration"], index=0)
    s_top = st.sidebar.selectbox("Detail Level", ["Top 5", "Top 10", "All"], index=0)
    s_oth = st.sidebar.checkbox("Include 'Others' Group", True)
    s_mas = st.sidebar.checkbox("Generate Master Dashboard Tab", False)
    s_unt = st.sidebar.radio("Trend Unit (Downtime):", ["Minutes", "Seconds"], key="unit_radio")
    
    st.sidebar.divider()
    w1_d = st.sidebar.date_input("Time Window 1", [m_df['EventDate'].min().date(), m_df['EventDate'].max().date()], key="d1")
    comp_on = st.sidebar.checkbox("Enable Comparison (Window 2)")
    w2_d = st.sidebar.date_input("Time Window 2", [m_df['EventDate'].min().date(), m_df['EventDate'].max().date()], key="d2") if comp_on else None

    # Global Calculations
    met1_dev_only = get_metrics_calculation(m_df, w1_d, s_typ, s_dev, ['DeviceName'])
    w1_cyc_fleet = get_total_fleet_cycles(m_df, w1_d, s_dev)
    tw_str = f"W1: {w1_d[0]} to {w1_d[1]}" + (f" | W2: {w2_d[0]} to {w2_d[1]}" if comp_on else "")

    # Pre-generate Figures
    if not comp_on:
        f_pie = px.pie(met1_dev_only, names='DeviceName', values='Final_Count' if s_met=="Count" else 'Final_Duration', hole=0.4, title="Fleet Impact Analysis", hover_data=['EventDescription'])
        f_hyb = None
    else:
        m1_c = get_metrics_calculation(m_df, w1_d, s_typ, s_dev, ['DeviceName', 'EventCode'])
        m2_c = get_metrics_calculation(m_df, w2_d, s_typ, s_dev, ['DeviceName', 'EventCode'])
        f_hyb = create_cluster_stack_chart(m1_c, m2_c, 'DeviceName', 'EventCode', "Machine Comparison", s_top, s_oth, s_met, desc_map=desc_lookup)

    t_titles = ["🏠 Main Dashboard", "🔍 Fault Analysis", "🛠️ Tech Log", "📄 Export Reports"]
    if s_mas: t_titles.insert(1, "📊 Master Gallery")
    tabs = st.tabs(t_titles); t_lookup = {name: tabs[i] for i, name in enumerate(t_titles)}

    with t_lookup["🏠 Main Dashboard"]:
        if not comp_on:
            m = met1_dev_only if s_dev=="All" else met1_dev_only[met1_dev_only['DeviceName']==s_dev]
            c = st.columns(4)
            c[0].metric("Total Events", f"{int(m['Final_Count'].sum())}")
            c[1].metric("Total Downtime", format_seconds_to_clock(m['Final_Duration'].sum()))
            c[2].metric("Avg Duration", f"{(m['Final_Duration'].sum()/max(1,m['Final_Count'].sum())):.1f}s")
            c[3].metric("Active Machines", m[m['Final_Count']>0]['DeviceName'].nunique())
            l, r = st.columns([1, 1.2])
            with l: st.plotly_chart(f_pie, use_container_width=True)
            with r:
                met1_dev_only['Action'], met1_dev_only['Total Downtime'] = "🔍 View Logs", met1_dev_only['Final_Duration'].apply(format_seconds_to_clock)
                met1_dev_only['Final_Count'] = met1_dev_only['Final_Count'].astype(int)
                grid1 = st.dataframe(met1_dev_only[['Action', 'DeviceName', 'Final_Count', 'Total Downtime', 'Cycles', 'Fault%']].style.format({'Cycles': '{:,.0f}', 'Fault%': '{:.2f}%'}), use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key=f"g1_{st.session_state.tk_main}")
                if grid1 and grid1.get('selection') and grid1['selection'].get('rows'): st.session_state.dialog_trigger = {'m': met1_dev_only.iloc[grid1['selection']['rows'][0]]['DeviceName'], 'c': "All", 't': 'main'}
        else:
            met2_dev_only = get_metrics_calculation(m_df, w2_d, s_typ, s_dev, ['DeviceName'])
            st.markdown(f"**Window 1 Metrics ({w1_d[0]})**"); c1 = st.columns(4)
            m1 = met1_dev_only if s_dev=="All" else met1_dev_only[met1_dev_only['DeviceName']==s_dev]
            c1[0].metric("Total Events", f"{int(m1['Final_Count'].sum())}"); c1[1].metric("Total Downtime", format_seconds_to_clock(m1['Final_Duration'].sum())); c1[2].metric("Avg Duration", f"{(m1['Final_Duration'].sum()/max(1,m1['Final_Count'].sum())):.1f}s"); c1[3].metric("Active Devices", m1[m1['Final_Count']>0]['DeviceName'].nunique())
            st.markdown(f"**Window 2 Metrics ({w2_d[0]})**"); c2 = st.columns(4)
            m2 = met2_dev_only if s_dev=="All" else met2_dev_only[met2_dev_only['DeviceName']==s_dev]
            c2[0].metric("Total Events", f"{int(m2['Final_Count'].sum())}"); c2[1].metric("Total Downtime", format_seconds_to_clock(m2['Final_Duration'].sum())); c2[2].metric("Avg Duration", f"{(m2['Final_Duration'].sum()/max(1,m2['Final_Count'].sum())):.1f}s"); c2[3].metric("Active Devices", m2[m2['Final_Count']>0]['DeviceName'].nunique())
            st.plotly_chart(f_hyb, use_container_width=True)
            diff = pd.merge(met1_dev_only, met2_dev_only, on='DeviceName', how='outer', suffixes=('_W1', '_W2')).fillna(0)
            for c in ['Final_Count_W1', 'Final_Count_W2', 'Cycles_W1', 'Cycles_W2']: diff[c] = diff[c].astype(int)
            diff['Action'], diff['Delta Count'] = "🔍 View Logs", diff['Final_Count_W2'] - diff['Final_Count_W1']
            diff['Delta DT (Hrs)'] = ((diff['Final_Duration_W2'] - diff['Final_Duration_W1']) / 3600).round(2)
            diff['DT_W1'], diff['DT_W2'] = diff['Final_Duration_W1'].apply(format_seconds_to_clock), diff['Final_Duration_W2'].apply(format_seconds_to_clock)
            grid_c = st.dataframe(diff[['Action', 'DeviceName', 'Final_Count_W1', 'Final_Count_W2', 'Delta Count', 'DT_W1', 'DT_W2', 'Delta DT (Hrs)', 'Cycles_W1', 'Cycles_W2', 'Fault%_W1', 'Fault%_W2']].style.map(color_delta_styling, subset=['Delta Count', 'Delta DT (Hrs)']).format({'Delta DT (Hrs)': '{:.2f}', 'Fault%_W1': '{:.2f}%', 'Fault%_W2': '{:.2f}%'}), use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key=f"gc_{st.session_state.tk_main}")
            if grid_c and grid_c.get('selection') and grid_c['selection'].get('rows'): st.session_state.dialog_trigger = {'m': diff.iloc[grid_c['selection']['rows'][0]]['DeviceName'], 'c': "All", 't': 'main'}

    if s_mas:
        with t_lookup["📊 Master Gallery"]:
            st.subheader("Machine Fleet Visuals")
            all_fleet = [m for m in sorted(m_df['DeviceName'].unique().tolist()) if m not in ignore_devices_mas]
            m_figs_dict = {}
            for i in range(0, len(all_fleet), 4):
                cols = st.columns(4)
                for j in range(4):
                    if i + j < len(all_fleet):
                        nm = all_fleet[i+j]
                        mw1 = get_metrics_calculation(m_df, w1_d, s_typ, nm, ['DeviceName', 'EventCode'])
                        mw2 = get_metrics_calculation(m_df, w2_d, s_typ, nm, ['DeviceName', 'EventCode']) if comp_on else pd.DataFrame()
                        f = create_cluster_stack_chart(mw1, mw2, 'DeviceName', 'EventCode', nm, s_top, s_oth, s_met, desc_map=desc_lookup, is_gallery=True)
                        f.update_layout(height=350, showlegend=False); m_figs_dict[nm] = f
                        with cols[j]: st.plotly_chart(f, use_container_width=True)

    with t_lookup["🔍 Fault Analysis"]:
        st.subheader(f"Event Analysis: {s_dev}")
        avail_codes = ["All"] + sorted(m_df['EventCode'].unique().tolist())
        st.session_state.trend_event_code = st.selectbox("Select Code to Trend:", avail_codes, index=avail_codes.index(st.session_state.trend_event_code) if st.session_state.trend_event_code in avail_codes else 0)
        
        f1_full = get_metrics_calculation(m_df, w1_d, s_typ, s_dev, ['EventCode', 'EventDescription'], global_cycles=w1_cyc_fleet)
        if not comp_on:
            f10 = f1_full.sort_values('Final_Count', ascending=False).head(10) if not f1_full.empty else pd.DataFrame()
            fig_fb = px.bar(f10, x='EventCode', y='Final_Count' if s_met=="Count" else 'Final_Duration', title="Top 10 High-Impact Codes").update_layout(yaxis_title=s_met)
            st.plotly_chart(fig_fb, use_container_width=True)
            f10['Action'], f10['Downtime'] = "🔍 View Logs", f10['Final_Duration'].apply(format_seconds_to_clock)
            grid_f = st.dataframe(f10[['Action', 'EventCode', 'Final_Count', 'Downtime', 'EventDescription']], use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key=f"gf1_{st.session_state.tk_fault}")
            if grid_f and grid_f.get('selection') and grid_f['selection'].get('rows'): st.session_state.dialog_trigger = {'m': s_dev, 'c': f10.iloc[grid_f['selection']['rows'][0]]['EventCode'], 't': 'fault'}
        else:
            f1c = get_metrics_calculation(m_df, w1_d, s_typ, s_dev, ['EventCode', 'DeviceName'], global_cycles=w1_cyc_fleet)
            f2c = get_metrics_calculation(m_df, w2_d, s_typ, s_dev, ['EventCode', 'DeviceName'], global_cycles=get_total_fleet_cycles(m_df, w2_d, s_dev))
            u_codes = list(set((f1_full.sort_values('Final_Count', ascending=False).head(10)['EventCode'].tolist() if not f1_full.empty else []) + (get_metrics_calculation(m_df, w2_d, s_typ, s_dev, ['EventCode']).sort_values('Final_Count', ascending=False).head(10)['EventCode'].tolist() if comp_on else [])))
            f_fh = create_cluster_stack_chart(f1c[f1c['EventCode'].isin(u_codes)], f2c[f2c['EventCode'].isin(u_codes)], 'EventCode', 'DeviceName', "Fault Comparison", s_top, s_oth, s_met)
            st.plotly_chart(f_fh, use_container_width=True)
            f2_full = get_metrics_calculation(m_df, w2_d, s_typ, s_dev, ['EventCode', 'EventDescription'], global_cycles=get_total_fleet_cycles(m_df, w2_d, s_dev))
            f_diff = pd.merge(f1_full[f1_full['EventCode'].isin(u_codes)], f2_full[f2_full['EventCode'].isin(u_codes)], on=['EventCode', 'EventDescription'], how='outer', suffixes=('_W1', '_W2')).fillna(0)
            f_diff['Delta Count'], f_diff['Action'] = f_diff['Final_Count_W2'] - f_diff['Final_Count_W1'], "🔍 View Logs"
            f_diff['Delta DT (Hrs)'], f_diff['DT_W1'], f_diff['DT_W2'] = ((f_diff['Final_Duration_W2'] - f_diff['Final_Duration_W1']) / 3600).round(2), f_diff['Final_Duration_W1'].apply(format_seconds_to_clock), f_diff['Final_Duration_W2'].apply(format_seconds_to_clock)
            f_diff['EventCode'] = f_diff['EventCode'].astype(str).str.split('.').str[0]
            st.dataframe(f_diff[['Action', 'EventCode', 'Final_Count_W1', 'Final_Count_W2', 'Delta Count', 'DT_W1', 'DT_W2', 'Delta DT (Hrs)', 'EventDescription']].style.map(color_delta_styling, subset=['Delta Count', 'Delta DT (Hrs)']).format({'Delta DT (Hrs)': '{:.2f}'}), use_container_width=True, hide_index=True)

        st.divider(); st.subheader("Time Trends")
        f_o = st.selectbox("Interval:", ['Daily', 'Weekly', 'Monthly']); f_m = {'Daily':'D', 'Weekly':'W', 'Monthly':'ME'}
        def get_tr(dr, code, dev, typs, unit):
            if len(dr) < 2: return pd.DataFrame()
            sub = m_df.loc[(m_df['EventDate'].dt.date >= dr[0]) & (m_df['EventDate'].dt.date <= dr[1])]
            if dev != "All": sub = sub[sub['DeviceName'] == dev]
            cp = sub[sub['SystemCounter'] > 0].set_index('EventDate').resample(f_m[f_o])['SystemCounter'].agg(lambda x: int(max(0, x.max() - x.min()))).reset_index(name='CP')
            sub = sub[sub['EventType'].isin(typs)]
            if code != "All": sub = sub[sub['EventCode'] == code]
            tr = sub.set_index('EventDate').groupby([pd.Grouper(freq=f_m[f_o]), 'EventType']).agg({'EventCode':'count', 'Duration':'sum'}).reset_index()
            tr = pd.merge(tr, cp, on='EventDate', how='left').fillna(0)
            tr['P'] = tr['EventCode'] if s_met=="Count" else tr['Duration']/60
            tr['D'] = tr['Duration'] / (60 if unit=="Minutes" else 1)
            return tr
        t1 = get_tr(w1_d, st.session_state.trend_event_code, s_dev, s_typ, s_unt); tc = st.columns(4)
        tc[0].plotly_chart(px.line(t1, x='EventDate', y='P', color='EventType', markers=True, title=f"W1 {s_met} Trend").update_layout(yaxis_title=s_met), use_container_width=True)
        tc[2].plotly_chart(px.line(t1, x='EventDate', y='D', color='EventType', markers=True, title=f"W1 Downtime Trend"), use_container_width=True)
        if comp_on:
            t2 = get_tr(w2_d, st.session_state.trend_event_code, s_dev, s_typ, s_unt)
            tc[1].plotly_chart(px.line(t2, x='EventDate', y='P', color='EventType', markers=True, title=f"W2 {s_met} Trend").update_layout(yaxis_title=s_met), use_container_width=True)
            tc[3].plotly_chart(px.line(t2, x='EventDate', y='D', color='EventType', markers=True, title=f"W2 Downtime Trend"), use_container_width=True)

    with t_lookup["🛠️ Tech Log"]:
        st.subheader("Technician Portal")
        with st.expander("📝 Record New maintenance Event", expanded=False):
            with st.form("entry_form", clear_on_submit=True):
                c1, c2 = st.columns(2)
                f_m = c1.selectbox("Machine", sorted(raw_df_master['DeviceName'].unique()))
                f_c = c1.selectbox("Code", sorted(raw_df_master[raw_df_master['DeviceName']==f_m]['EventCode'].unique()))
                f_t, f_d = c2.text_input("Technician"), c2.date_input("Date", datetime.now())
                f_a = st.text_area("Corrective Action Taken")
                if st.form_submit_button("Save"):
                    db_conn.cursor().execute("INSERT INTO notes (DeviceName, EventCode, TechnicianName, CorrectiveAction, ActionDate) VALUES (?,?,?,?,?)", (f_m, f_c, f_t, f_a, str(f_d)))
                    db_conn.commit(); st.success("Log Saved.")
        st.subheader("History Log")
        full_h = pd.read_sql_query("SELECT * FROM notes", db_conn).sort_values(by=['DeviceName', 'ActionDate'], ascending=[True, False])
        if not full_h.empty:
            def group_logic(df_in):
                s = pd.DataFrame('', index=df_in.index, columns=df_in.columns); prev = None
                for i, m in enumerate(df_in['DeviceName']):
                    if prev is not None and m != prev: s.iloc[i, :] = 'background-color: #deeaf7; border-top: 4px solid #31333F; font-weight: bold;'
                    prev = m
                return s
            st.dataframe(full_h.style.apply(group_logic, axis=None), use_container_width=True, hide_index=True)

    with t_lookup["📄 Export Reports"]:
        x1, x2 = st.columns(2)
        with x1: i_sm, i_dc, i_dt, i_ma = st.checkbox("KPIs", True), st.checkbox("Dash Visual", True), st.checkbox("Dash Table", True), st.checkbox("Fleet Gallery", False)
        with x2: i_fc, i_ft, i_tl = st.checkbox("Fault Visual", True), st.checkbox("Fault Table", True), st.checkbox("Tech Logs", False)
        if st.button("🚀 Generate PDF"):
            with st.spinner("Compiling..."):
                pdf = Reporter(tw_str); pdf.add_page()
                if i_sm:
                    pdf.set_font("helvetica", "B", 12); pdf.cell(0, 10, "1. KPI Summary", ln=1); pdf.set_font("helvetica", "", 10)
                    pdf.cell(0, 8, f"W1 Events: {int(m1['Final_Count'].sum() if comp_on else m['Final_Count'].sum())} | Time: {format_seconds_to_clock(m1['Final_Duration'].sum() if comp_on else m['Final_Duration'].sum())}", ln=1)
                if i_dc: pdf_plot(pdf, f_pie if not comp_on else f_hyb, "2. Dashboard Visual")
                if i_dt: pdf_table(pdf, diff.drop(columns=['Action']) if comp_on else met1_dev_only.drop(columns=['Action']), "3. Dashboard Table")
                if i_fc: pdf_plot(pdf, fig_fb if not comp_on else f_fh, "4. Fault Visual")
                if i_ft: pdf_table(pdf, f_diff.drop(columns=['Action']) if comp_on else f10.drop(columns=['Action']), "5. Fault Table")
                if i_ma and s_mas:
                    pdf.add_page(); pdf.set_font("helvetica", "B", 14); pdf.cell(0, 10, "Machine Health Gallery (Filtered)", ln=1)
                    for k, v in m_figs_dict.items(): pdf_plot(pdf, v, f"Machine: {k}", 160)
                if i_tl:
                    pdf.add_page(); pdf_table(pdf, full_h, "6. Tech logs")
                st.markdown(f'<a href="data:application/pdf;base64,{base64.b64encode(pdf.output()).decode()}" download="Report.pdf" style="padding:12px; background-color:#28a745; color:white; border-radius:8px; text-decoration:none; font-weight:bold;">📥 Download Report</a>', unsafe_allow_html=True)

    if st.session_state.dialog_trigger:
        d = st.session_state.dialog_trigger; st.session_state.dialog_trigger = None
        show_popup_logs(d['m'], d['c'], d['t'])
else: st.info("👋 Welcome. Please upload a production Excel file to begin.")
