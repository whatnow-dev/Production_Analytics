# ==============================================================================
# 1. CORE LIBRARIES
# ==============================================================================
import streamlit as st 
import pandas as pd     
import plotly.express as px  
import plotly.graph_objects as go  
import plotly.io as pio  
import sqlite3          
from datetime import datetime, time, timedelta
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

# Session State initialization
if 'raw_data' not in st.session_state: st.session_state.raw_data = None
if 'trend_event_code' not in st.session_state: st.session_state.trend_event_code = "All"
if 'tk_main' not in st.session_state: st.session_state.tk_main = 0
if 'tk_fault' not in st.session_state: st.session_state.tk_fault = 100
if 'dialog_trigger' not in st.session_state: st.session_state.dialog_trigger = None

def init_database():
    conn = sqlite3.connect('maintenance_data.db', check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS notes 
                     (id INTEGER PRIMARY KEY AUTOINCREMENT, DeviceName TEXT, 
                      EventCode TEXT, TechnicianName TEXT, CorrectiveAction TEXT, ActionDate TEXT)''')
    conn.commit()
    return conn

db_conn = init_database()

# ==============================================================================
# 3. ANALYTICS ENGINE (Mathematical Precision)
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

def clean_duration_literal(series):
    """
    1st PRINCIPLES FIX: Precision duration cleaning.
    Prevents Date/Counter values from scaling charts to 'Billions'.
    """
    def parse_one(x):
        try:
            x_str = str(x).strip().lower()
            if ':' in x_str:
                p = x_str.split(':')
                val = int(p[0])*3600 + int(p[1])*60 + float(p[2]) if len(p)==3 else int(p[0])*60 + float(p[1])
            else: 
                val = float(re.sub(r'[^0-9.]', '', x_str))
                if 0 < val < 1.0: val = val * 86400.0 # Excel fraction
            # RE-INTRODUCED SAFETY CAP: Single events cannot exceed 24 hours.
            return val if 0 <= val <= 86400.0 else 0.0
        except: return 0.0
    return series.apply(parse_one)

@st.cache_data
def process_upload(file):
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
    clean['Duration'] = clean_duration_literal(df[dur_col]) if dur_col else 0.0
    clean['SystemCounter'] = pd.to_numeric(df[f(['counter'])], errors='coerce').fillna(0)
    return clean.dropna(subset=['EventDate'])

def get_metrics_calculation(source_df, start_dt, end_dt, types, device, group_cols, global_cycles=None, break_window=None):
    """
    PRECISE FILTERING: Filters by datetime and recurring daily break periods.
    """
    # 1. Datetime Range Filter
    active = source_df[(source_df['EventDate'] >= start_dt) & (source_df['EventDate'] <= end_dt)].copy()
    
    # 2. Daily Break Exclusion (Requirement 5)
    if break_window and break_window[0] != break_window[1]:
        # Drop events where the time of day falls within the break window
        active = active[~((active['EventDate'].dt.time >= break_window[0]) & 
                          (active['EventDate'].dt.time <= break_window[1]))]

    if device != "All": active = active[active['DeviceName'] == device]
    if active.empty: return pd.DataFrame(columns=group_cols + ['CHART_VAL_COUNT', 'CHART_VAL_DUR', 'Cycles', 'Fault%'])

    # Cycles
    v_cyc = active[active['SystemCounter'] > 0]
    cycles_by_m = v_cyc.groupby('DeviceName')['SystemCounter'].agg(lambda x: int(max(0, x.max() - x.min()))).reset_index(name='Cycles')
    
    # Event filter (Fault/Warning/etc)
    events = active[active['EventType'].isin(types)]
    
    if events.empty:
        res = cycles_by_m.copy()
        for col in group_cols: 
            if col not in res.columns: res[col] = "N/A"
        res['CHART_VAL_COUNT'], res['CHART_VAL_DUR'] = 0, 0.0
    else:
        # Strict column naming to prevent System Counter leakage
        agg = events.groupby(group_cols).agg(CHART_VAL_COUNT=('EventCode', 'count'), CHART_VAL_DUR=('Duration', 'sum')).reset_index()
        
        # Max theoretical duration cap based on filtered window
        window_secs = (end_dt - start_dt).total_seconds()
        agg['CHART_VAL_DUR'] = agg['CHART_VAL_DUR'].clip(upper=window_secs)

        if group_cols == ['DeviceName']:
            dm = events.groupby('DeviceName')['EventDescription'].first().reset_index()
            agg = pd.merge(agg, dm, on='DeviceName', how='left')

        if 'DeviceName' in group_cols:
            res = pd.merge(agg, cycles_by_m, on='DeviceName', how='left').fillna(0)
        else:
            res = agg.copy()
            res['Cycles'] = global_cycles if global_cycles is not None else cycles_by_m['Cycles'].sum()
            
    res['Fault%'] = (res['CHART_VAL_COUNT'] / res['Cycles'].replace(0, 1) * 100).round(2)
    return res

# ==============================================================================
# 4. CHART ENGINE (Legend & Top 5 Requirement)
# ==============================================================================

def create_cluster_stack_chart(df_w1, df_w2, x_col, stack_col, title, top_n, show_others, metric, desc_map=None, is_gallery=False):
    """
    NEW LEGEND LOGIC: W1: Code: Description (Truncated 50 chars).
    NEW BUNDLING LOGIC: Bundles beyond Top 5 into 'Others'.
    """
    fig = go.Figure()
    val_col = 'CHART_VAL_COUNT' if metric == "Count" else 'CHART_VAL_DUR'
    
    w1_palette = ['#082245','#08306B','#08519C','#2171B5','#4292C6'] # Exact Top 5 Blues
    w2_palette = ['#67000D','#A50F15','#CB181D','#EF3B2C','#FB6A4A'] # Exact Top 5 Reds
    
    def process_ranking(df_in):
        if df_in.empty: return df_in
        # REQUIREMENT: Force bundling everything after Top 5 into Others
        limit = 5 
        df_rank = df_in.copy()
        # Rank by current metric to ensure visual logic
        df_rank['rank'] = df_rank.groupby(x_col)[val_col].rank(method='first', ascending=False)
        if show_others:
            df_rank.loc[df_rank['rank'] > limit, stack_col] = "Others"
            return df_rank.groupby([x_col, stack_col])[val_col].sum().reset_index()
        else: return df_rank[df_rank['rank'] <= limit]

    c_w1, c_w2 = process_ranking(df_w1), process_ranking(df_w2)

    def add_bars(df, label_prefix, offset, colors):
        if df.empty: return
        cats = sorted([c for c in df[stack_col].unique() if c != "Others"])
        if show_others and "Others" in df[stack_col].unique(): cats.append("Others")
        
        for i, category in enumerate(cats):
            sub = df[df[stack_col] == category]
            y_data = sub[val_col] if metric == "Count" else sub[val_col] / 60
            color = "#D3D3D3" if category == "Others" else colors[i % len(colors)]
            
            # REQUIREMENT: New Legend Format (W1: Code: Desc)
            desc_text = desc_map.get(category, "") if desc_map else ""
            truncated_desc = (desc_text[:47] + "...") if len(desc_text) > 50 else desc_text
            
            leg_name = f"{label_prefix}: {category}"
            if category != "Others" and truncated_desc:
                leg_name = f"{label_prefix}: {category}: {truncated_desc}"
            
            v_str = sub[val_col].apply(format_seconds_to_clock) if metric == "Duration" else sub[val_col].astype(str)
            tip = f"<b>{label_prefix}</b><br>Code: {category}<br>Desc: {desc_text}<br>{metric}: {v_str.iloc[0]}<extra></extra>"

            fig.add_trace(go.Bar(
                name=leg_name, x=sub[x_col], y=y_data,
                offsetgroup=offset, marker=dict(color=color, line=dict(color='white', width=0.5)),
                legendgroup=label_prefix, hovertemplate=tip
            ))

    add_bars(c_w1, "W1", 0, w1_palette)
    add_bars(c_w2, "W2", 1, w2_palette)
    fig.update_layout(title=title, barmode='stack', yaxis_title=metric, bargap=0.15, bargroupgap=0.1)
    return fig

# ==============================================================================
# 5. UI DIALOGS & PDF ENGINE
# ==============================================================================

@st.dialog("History Log", width="large")
def show_popup_logs(dev, code, t_type):
    st.subheader(f"Log History: {dev if t_type == 'main' else 'Code ' + str(code)}")
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
    if st.button("Close and Refresh"):
        if t_type == "main": st.session_state.tk_main += 1
        else: st.session_state.tk_fault += 1
        st.rerun()

class Reporter(FPDF):
    def __init__(self, dates): super().__init__(); self.dates = dates
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
    raw_df_full = process_upload(upl)
    
    st.sidebar.markdown("### 🚫 Exclusion Filters")
    ignore_codes = st.sidebar.multiselect("⚠️ Ignore EventCodes (Global):", sorted(raw_df_full['EventCode'].unique().tolist()))
    ignore_devices_mas = st.sidebar.multiselect("⚠️ Hide Machines from Master Dash:", sorted(raw_df_full['DeviceName'].unique().tolist()))

    if ignore_codes: m_df = raw_df_full[~raw_df_full['EventCode'].isin(ignore_codes)]
    else: m_df = raw_df_full

    desc_lookup = m_df.groupby('EventCode')['EventDescription'].first().to_dict()
    
    st.sidebar.divider()
    s_dev = st.sidebar.selectbox("Active Device Filter", ["All"] + sorted(m_df['DeviceName'].unique().tolist()))
    s_typ = st.sidebar.multiselect("Event Type Filter", m_df['EventType'].unique(), default=list(m_df['EventType'].unique()))
    s_met = st.sidebar.radio("View Segment Metric:", ["Count", "Duration"], index=0)
    s_top = st.sidebar.selectbox("Detail Level (Legend limited to 5)", ["Top 5", "Top 10", "All"], index=0)
    s_oth = st.sidebar.checkbox("Include 'Others' Group", True)
    s_mas = st.sidebar.checkbox("Generate Master Dashboard Tab", False)
    s_unt = st.sidebar.radio("Trend Unit (Downtime):", ["Minutes", "Seconds"], key="unit_sidebar")
    
    # ==========================================================================
    # TIME PICKING & EXCLUSION (Requirements 3, 4, 5)
    # ==========================================================================
    st.sidebar.divider()
    st.sidebar.header("🕒 Precision Timing")
    
    # Daily Break Exclusion
    st.sidebar.markdown("**Recurring Daily Break Exclusion:**")
    brk_c1, brk_c2 = st.sidebar.columns(2)
    brk_start = brk_c1.time_input("Break Start", time(12, 0))
    brk_end = brk_c2.time_input("Break End", time(13, 0))

    st.sidebar.markdown("**Window 1 Selection:**")
    w1_dates = st.sidebar.date_input("W1 Date Range", [m_df['EventDate'].min().date(), m_df['EventDate'].max().date()])
    w1_c1, w1_c2 = st.sidebar.columns(2)
    w1_start_t = w1_c1.time_input("W1 Start Time", time(0, 0), key="w1st")
    w1_end_t = w1_c2.time_input("W1 End Time", time(23, 59), key="w1et")

    comp_on = st.sidebar.checkbox("Enable Window 2 Comparison")
    w2_start_dt, w2_end_dt = None, None
    if comp_on:
        st.sidebar.markdown("**Window 2 Selection:**")
        w2_dates = st.sidebar.date_input("W2 Date Range", [m_df['EventDate'].min().date(), m_df['EventDate'].max().date()])
        w2_c1, w2_c2 = st.sidebar.columns(2)
        w2_start_t = w2_c1.time_input("W2 Start Time", time(0, 0), key="w2st")
        w2_end_t = w2_c2.time_input("W2 End Time", time(23, 59), key="w2et")
        if len(w2_dates) == 2:
            w2_start_dt = datetime.combine(w2_dates[0], w2_start_t)
            w2_end_dt = datetime.combine(w2_dates[1], w2_end_t)

    # Combine W1 inputs
    if len(w1_dates) == 2:
        w1_start_dt = datetime.combine(w1_dates[0], w1_start_t)
        w1_end_dt = datetime.combine(w1_dates[1], w1_end_t)
        
        # Calculations using precise datetimes
        w1_cyc_fleet = int(m_df[(m_df['EventDate'] >= w1_start_dt) & (m_df['EventDate'] <= w1_end_dt) & (m_df['DeviceName'] == s_dev if s_dev != "All" else True) & (m_df['SystemCounter'] > 0)].groupby('DeviceName')['SystemCounter'].agg(lambda x: max(0, x.max() - x.min())).sum())
        met1_dev_only = get_metrics_calculation(m_df, w1_start_dt, w1_end_dt, s_typ, s_dev, ['DeviceName'], break_window=(brk_start, brk_end))
        
        # Pre-gen charts
        if not comp_on:
            f_pie = px.pie(met1_dev_only, names='DeviceName', values='CHART_VAL_COUNT' if s_met=="Count" else 'CHART_VAL_DUR', hole=0.4, title="Impact Distribution", hover_data=['EventDescription'])
            f_hyb = None
        else:
            m1_c = get_metrics_calculation(m_df, w1_start_dt, w1_end_dt, s_typ, s_dev, ['DeviceName', 'EventCode'], break_window=(brk_start, brk_end))
            m2_c = get_metrics_calculation(m_df, w2_start_dt, w2_end_dt, s_typ, s_dev, ['DeviceName', 'EventCode'], break_window=(brk_start, brk_end))
            f_hyb = create_cluster_stack_chart(m1_c, m2_c, 'DeviceName', 'EventCode', "Machine Performance Comparison", s_top, s_oth, s_met, desc_map=desc_lookup)

        # Tabs
        t_titles = ["🏠 Main Dashboard", "🔍 Fault Analysis", "🛠️ Tech Log", "📄 Export Reports"]
        if s_mas: t_titles.insert(1, "📊 Master Gallery")
        tabs = st.tabs(t_titles); t_lookup = {name: tabs[i] for i, name in enumerate(t_titles)}

        with t_lookup["🏠 Main Dashboard"]:
            if not comp_on:
                m = met1_dev_only if s_dev=="All" else met1_dev_only[met1_dev_only['DeviceName']==s_dev]
                c = st.columns(4)
                c[0].metric("Total Events", f"{int(m['CHART_VAL_COUNT'].sum())}")
                c[1].metric("Total Downtime", format_seconds_to_clock(m['CHART_VAL_DUR'].sum()))
                c[2].metric("Avg Duration", f"{(m['CHART_VAL_DUR'].sum()/max(1,m['CHART_VAL_COUNT'].sum())):.1f}s")
                c[3].metric("Active Machines", m[m['CHART_VAL_COUNT']>0]['DeviceName'].nunique())
                l, r = st.columns([1, 1.2])
                with l: st.plotly_chart(f_pie, use_container_width=True)
                with r:
                    met1_dev_only['Action'], met1_dev_only['Total Downtime'] = "🔍 View Logs", met1_dev_only['CHART_VAL_DUR'].apply(format_seconds_to_clock)
                    met1_dev_only['CHART_VAL_COUNT'] = met1_dev_only['CHART_VAL_COUNT'].astype(int)
                    grid1 = st.dataframe(met1_dev_only[['Action', 'DeviceName', 'CHART_VAL_COUNT', 'Total Downtime', 'Cycles', 'Fault%']].style.format({'Cycles': '{:,.0f}', 'Fault%': '{:.2f}%'}), use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key=f"g1_{st.session_state.tk_main}")
                    if grid1 and grid1.get('selection') and grid1['selection'].get('rows'): st.session_state.dialog_trigger = {'m': met1_dev_only.iloc[grid1['selection']['rows'][0]]['DeviceName'], 'c': "All", 't': 'main'}
            else:
                met2_dev_only = get_metrics_calculation(m_df, w2_start_dt, w2_end_dt, s_typ, s_dev, ['DeviceName'], break_window=(brk_start, brk_end))
                st.markdown(f"**Window 1 Metrics ({w1_start_dt})**"); c1 = st.columns(4)
                m1 = met1_dev_only if s_dev=="All" else met1_dev_only[met1_dev_only['DeviceName']==s_dev]
                c1[0].metric("Events", int(m1['CHART_VAL_COUNT'].sum())); c1[1].metric("Downtime", format_seconds_to_clock(m1['CHART_VAL_DUR'].sum())); c1[2].metric("Avg", f"{(m1['CHART_VAL_DUR'].sum()/max(1,m1['CHART_VAL_COUNT'].sum())):.1f}s"); c1[3].metric("Active", m1[m1['CHART_VAL_COUNT']>0]['DeviceName'].nunique())
                st.markdown(f"**Window 2 Metrics ({w2_start_dt})**"); c2 = st.columns(4)
                m2 = met2_dev_only if s_dev=="All" else met2_dev_only[met2_dev_only['DeviceName']==s_dev]
                c2[0].metric("Events", int(m2['CHART_VAL_COUNT'].sum())); c2[1].metric("Downtime", format_seconds_to_clock(m2['CHART_VAL_DUR'].sum())); c2[2].metric("Avg", f"{(m2['CHART_VAL_DUR'].sum()/max(1,m2['CHART_VAL_COUNT'].sum())):.1f}s"); c2[3].metric("Active", m2[m2['CHART_VAL_COUNT']>0]['DeviceName'].nunique())
                st.plotly_chart(f_hyb, use_container_width=True)
                diff = pd.merge(met1_dev_only, met2_dev_only, on='DeviceName', how='outer', suffixes=('_W1', '_W2')).fillna(0)
                for c in ['CHART_VAL_COUNT_W1', 'CHART_VAL_COUNT_W2', 'Cycles_W1', 'Cycles_W2']: diff[c] = diff[c].astype(int)
                diff['Action'], diff['Delta Count'] = "🔍 View Logs", diff['CHART_VAL_COUNT_W2'] - diff['CHART_VAL_COUNT_W1']
                diff['Delta DT (Hrs)'] = ((diff['CHART_VAL_DUR_W2'] - diff['CHART_VAL_DUR_W1']) / 3600).round(2)
                diff['DT_W1'], diff['DT_W2'] = diff['CHART_VAL_DUR_W1'].apply(format_seconds_to_clock), diff['CHART_VAL_DUR_W2'].apply(format_seconds_to_clock)
                grid_c = st.dataframe(diff[['Action', 'DeviceName', 'CHART_VAL_COUNT_W1', 'CHART_VAL_COUNT_W2', 'Delta Count', 'DT_W1', 'DT_W2', 'Delta DT (Hrs)', 'Cycles_W1', 'Cycles_W2', 'Fault%_W1', 'Fault%_W2']].style.map(color_delta_styling, subset=['Delta Count', 'Delta DT (Hrs)']).format({'Delta DT (Hrs)': '{:.2f}', 'Fault%_W1': '{:.2f}%', 'Fault%_W2': '{:.2f}%'}), use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key=f"gc_{st.session_state.tk_main}")
                if grid_c and grid_c.get('selection') and grid_c['selection'].get('rows'): st.session_state.dialog_trigger = {'m': diff.iloc[grid_c['selection']['rows'][0]]['DeviceName'], 'c': "All", 't': 'main'}

        if s_mas:
            with t_lookup["📊 Master Gallery"]:
                all_fleet_active = [m for m in sorted(m_df['DeviceName'].unique().tolist()) if m not in ignore_devices_mas]
                m_figs_dict_save = {}
                for i in range(0, len(all_fleet_active), 4):
                    cols = st.columns(4)
                    for j in range(4):
                        if i + j < len(all_fleet_active):
                            nm = all_fleet_active[i+j]
                            mw1 = get_metrics_calculation(m_df, w1_start_dt, w1_end_dt, s_typ, nm, ['DeviceName', 'EventCode'], break_window=(brk_start, brk_end))
                            mw2 = get_metrics_calculation(m_df, w2_start_dt, w2_end_dt, s_typ, nm, ['DeviceName', 'EventCode'], break_window=(brk_start, brk_end)) if comp_on else pd.DataFrame()
                            f = create_cluster_stack_chart(mw1, mw2, 'DeviceName', 'EventCode', nm, s_top, s_oth, s_met, desc_map=desc_lookup, is_gallery=True)
                            f.update_layout(height=350, showlegend=False); m_figs_dict_save[nm] = f
                            with cols[j]: st.plotly_chart(f, use_container_width=True)

        with t_lookup["🔍 Fault Analysis"]:
            st.subheader(f"Detailed Analysis: {s_dev}")
            avail_codes = ["All"] + sorted(m_df['EventCode'].unique().tolist())
            st.session_state.trend_event_code = st.selectbox("Select Code to Trend:", avail_codes, index=avail_codes.index(st.session_state.trend_event_code) if st.session_state.trend_event_code in avail_codes else 0)
            
            f1_full_data = get_metrics_calculation(m_df, w1_start_dt, w1_end_dt, s_typ, s_dev, ['EventCode', 'EventDescription'], break_window=(brk_start, brk_end))
            if not comp_on:
                f10 = f1_full_data.sort_values('CHART_VAL_COUNT', ascending=False).head(10) if not f1_full_data.empty else pd.DataFrame()
                fig_fb = px.bar(f10, x='EventCode', y='CHART_VAL_COUNT' if s_met=="Count" else 'CHART_VAL_DUR', title="Top 10 High-Impact Codes").update_layout(yaxis_title=s_met)
                st.plotly_chart(fig_fb, use_container_width=True)
                f10['Action'], f10['Downtime'] = "🔍 View Logs", f10['CHART_VAL_DUR'].apply(format_seconds_to_clock)
                grid_f = st.dataframe(f10[['Action', 'EventCode', 'CHART_VAL_COUNT', 'Downtime', 'EventDescription']], use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key=f"gf1_{st.session_state.tk_fault}")
                if grid_f and grid_f.get('selection') and grid_f['selection'].get('rows'): st.session_state.dialog_trigger = {'m': s_dev, 'c': f10.iloc[grid_f['selection']['rows'][0]]['EventCode'], 't': 'fault'}
            else:
                f1c = get_metrics_calculation(m_df, w1_start_dt, w1_end_dt, s_typ, s_dev, ['EventCode', 'DeviceName'], break_window=(brk_start, brk_end))
                f2c = get_metrics_calculation(m_df, w2_start_dt, w2_end_dt, s_typ, s_dev, ['EventCode', 'DeviceName'], break_window=(brk_start, brk_end))
                u_codes = list(set((f1_full_data.sort_values('CHART_VAL_COUNT', ascending=False).head(10)['EventCode'].tolist() if not f1_full_data.empty else []) + (get_metrics_calculation(m_df, w2_start_dt, w2_end_dt, s_typ, s_dev, ['EventCode'], break_window=(brk_start, brk_end)).sort_values('CHART_VAL_COUNT', ascending=False).head(10)['EventCode'].tolist() if comp_on else [])))
                f_fh = create_cluster_stack_chart(f1c[f1c['EventCode'].isin(u_codes)], f2c[f2c['EventCode'].isin(u_codes)], 'EventCode', 'DeviceName', "Fault Comparison Analysis", s_top, s_oth, s_met, desc_map=desc_lookup)
                st.plotly_chart(f_fh, use_container_width=True)
                f2_full_calc = get_metrics_calculation(m_df, w2_start_dt, w2_end_dt, s_typ, s_dev, ['EventCode', 'EventDescription'], break_window=(brk_start, brk_end))
                f_diff_tbl = pd.merge(f1_full_data[f1_full_data['EventCode'].isin(u_codes)], f2_full_calc[f2_full_calc['EventCode'].isin(u_codes)], on=['EventCode', 'EventDescription'], how='outer', suffixes=('_W1', '_W2')).fillna(0)
                f_diff_tbl['Delta Count'], f_diff_tbl['Action'] = f_diff_tbl['CHART_VAL_COUNT_W2'] - f_diff_tbl['CHART_VAL_COUNT_W1'], "🔍 View Logs"
                f_diff_tbl['Delta DT (Hrs)'], f_diff_tbl['DT_W1'], f_diff_tbl['DT_W2'] = ((f_diff_tbl['CHART_VAL_DUR_W2'] - f_diff_tbl['CHART_VAL_DUR_W1']) / 3600).round(2), f_diff_tbl['CHART_VAL_DUR_W1'].apply(format_seconds_to_clock), f_diff_tbl['CHART_VAL_DUR_W2'].apply(format_seconds_to_clock)
                st.dataframe(f_diff_tbl[['Action', 'EventCode', 'CHART_VAL_COUNT_W1', 'CHART_VAL_COUNT_W2', 'Delta Count', 'DT_W1', 'DT_W2', 'Delta DT (Hrs)', 'EventDescription']].style.map(color_delta_styling, subset=['Delta Count', 'Delta DT (Hrs)']).format({'Delta DT (Hrs)': '{:.2f}'}), use_container_width=True, hide_index=True)

        with t_lookup["🛠️ Tech Log"]:
            with st.expander("📝 Record New Maintenance Action", expanded=False):
                with st.form("entry_form_portal", clear_on_submit=True):
                    c1_p, c2_p = st.columns(2)
                    f_machine_p = c1_p.selectbox("Machine", sorted(m_df['DeviceName'].unique()))
                    f_code_p = c1_p.selectbox("Fault Code", sorted(m_df[m_df['DeviceName']==f_machine_p]['EventCode'].unique()))
                    f_tech_p, f_date_p = c2_p.text_input("Tech Name"), c2_p.date_input("Action Date", datetime.now())
                    f_action_p = st.text_area("Corrective Action details")
                    if st.form_submit_button("Save Log Entry"):
                        db_conn.cursor().execute("INSERT INTO notes (DeviceName, EventCode, TechnicianName, CorrectiveAction, ActionDate) VALUES (?,?,?,?,?)", (f_machine_p, f_code_p, f_tech_p, f_action_p, str(f_date_p)))
                        db_conn.commit(); st.success("Log stored.")
            full_hist = pd.read_sql_query("SELECT * FROM notes", db_conn).sort_values(by=['DeviceName', 'ActionDate'], ascending=[True, False])
            if not full_hist.empty:
                def group_logic_hist(df_in):
                    s = pd.DataFrame('', index=df_in.index, columns=df_in.columns); prev = None
                    for i, m in enumerate(df_in['DeviceName']):
                        if prev is not None and m != prev: s.iloc[i, :] = 'background-color: #deeaf7; border-top: 4px solid #31333F; font-weight: bold;'
                        prev = m
                    return s
                st.dataframe(full_hist.style.apply(group_logic_hist, axis=None), use_container_width=True, hide_index=True)

        with t_lookup["📄 Export Reports"]:
            if st.button("🚀 Download Professional PDF"):
                with st.spinner("Compiling Professional Export..."):
                    pdf_report = Reporter(f"W1: {w1_start_dt} to {w1_end_dt}"); pdf_report.add_page()
                    pdf_plot(pdf_report, f_pie if not comp_on else f_hyb, "Dashboard Visualization")
                    pdf_table(pdf_report, met1_dev_only.drop(columns=['Action']), "Performance Summary Table")
                    st.markdown(f'<a href="data:application/pdf;base64,{base64.b64encode(pdf_report.output()).decode()}" download="Production_Analytics.pdf" style="padding:12px; background-color:#28a745; color:white; border-radius:8px; text-decoration:none; font-weight:bold;">📥 Download Full Report</a>', unsafe_allow_html=True)

        if st.session_state.dialog_trigger:
            d_p = st.session_state.dialog_trigger; st.session_state.dialog_trigger = None
            show_popup_logs(d_p['m'], d_p['c'], d_p['t'])
    else:
        st.warning("Please select valid date ranges for both time windows.")
else: st.info("👋 Ready to analyze. Please upload production Excel data in the sidebar.")
