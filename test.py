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
import base64           
import re               
import io               # For in-memory file handling (Excel/HTML)

# ==============================================================================
# 2. SYSTEM CONFIGURATION & MEMORY
# ==============================================================================
st.set_page_config(page_title="Machine Production Analytics", layout="wide")
pio.templates.default = "plotly_white"

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
# 3. ANALYTICS ENGINE (Standardized Logic)
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
    """Literal duration cleaner. Caps at 24h to prevent date-to-number errors."""
    def parse_one(x):
        try:
            x_str = str(x).strip().lower()
            if ':' in x_str:
                p = x_str.split(':')
                val = int(p[0])*3600 + int(p[1])*60 + float(p[2]) if len(p)==3 else int(p[0])*60 + float(p[1])
            else: 
                val = float(re.sub(r'[^0-9.]', '', x_str))
                if 0 < val < 1.0: val = val * 86400.0
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
    active = source_df[(source_df['EventDate'] >= start_dt) & (source_df['EventDate'] <= end_dt)].copy()
    if break_window and break_window[0] != break_window[1]:
        active = active[~((active['EventDate'].dt.time >= break_window[0]) & 
                          (active['EventDate'].dt.time <= break_window[1]))]

    if device != "All": active = active[active['DeviceName'] == device]
    if active.empty: return pd.DataFrame(columns=group_cols + ['Count', 'Duration', 'Cycles', 'Fault%'])

    v_cyc = active[active['SystemCounter'] > 0]
    cycles_by_m = v_cyc.groupby('DeviceName')['SystemCounter'].agg(lambda x: int(max(0, x.max() - x.min()))).reset_index(name='Cycles')
    events = active[active['EventType'].isin(types)]
    
    if events.empty:
        res = cycles_by_m.copy()
        for col in group_cols: 
            if col not in res.columns: res[col] = "N/A"
        res['Count'], res['Duration'] = 0, 0.0
    else:
        agg = events.groupby(group_cols).agg(Count=('EventCode', 'count'), Duration=('Duration', 'sum')).reset_index()
        window_secs = (end_dt - start_dt).total_seconds()
        agg['Duration'] = agg['Duration'].clip(upper=window_secs)
        if group_cols == ['DeviceName']:
            dm = events.groupby('DeviceName')['EventDescription'].first().reset_index()
            agg = pd.merge(agg, dm, on='DeviceName', how='left')
        if 'DeviceName' in group_cols: res = pd.merge(agg, cycles_by_m, on='DeviceName', how='left').fillna(0)
        else:
            res = agg.copy(); res['Cycles'] = global_cycles if global_cycles is not None else cycles_by_m['Cycles'].sum()
            
    res['Fault%'] = (res['Count'] / res['Cycles'].replace(0, 1) * 100).round(2)
    return res

# ==============================================================================
# 4. CHART ENGINE (Machine Analysis terminology)
# ==============================================================================

def create_cluster_stack_chart(df_w1, df_w2, x_col, stack_col, title, top_n, show_others, metric, desc_map=None, is_gallery=False):
    fig = go.Figure()
    val_col = 'Count' if metric == "Count" else 'Duration'
    w1_palette = ['#082245','#08306B','#08519C','#2171B5','#4292C6']
    w2_palette = ['#67000D','#A50F15','#CB181D','#EF3B2C','#FB6A4A']
    
    def process_ranking(df_in):
        if df_in.empty: return df_in
        limit = 5 
        df_rank = df_in.copy()
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
            leg_name = f"{label_prefix}: {category}"
            v_str = sub[val_col].apply(format_seconds_to_clock) if metric == "Duration" else sub[val_col].astype(str)
            tip = f"<b>{label_prefix}</b><br>Code: {category}<br>{metric}: {v_str.iloc[0]}<extra></extra>"
            fig.add_trace(go.Bar(name=leg_name, x=sub[x_col], y=y_data, offsetgroup=offset, marker=dict(color=color, line=dict(color='white', width=0.5)), legendgroup=label_prefix, hovertemplate=tip))

    add_bars(c_w1, "W1", 0, w1_palette)
    add_bars(c_w2, "W2", 1, w2_palette)
    fig.update_layout(title=title, barmode='stack', yaxis_title=metric, bargap=0.15, bargroupgap=0.1)
    return fig

# ==============================================================================
# 5. UI DIALOGS
# ==============================================================================

@st.dialog("Machine history Record", width="large")
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
    if st.button("Close"):
        if t_type == "main": st.session_state.tk_main += 1
        else: st.session_state.tk_fault += 1
        st.rerun()

# ==============================================================================
# 6. MAIN UI & FILTERS
# ==============================================================================

upl = st.sidebar.file_uploader("Upload Machine Data (Excel)", type=["xlsx"])

if upl:
    raw_df_full = process_upload(upl)
    st.sidebar.markdown("### 🚫 Exclusion Filters")
    ignore_codes = st.sidebar.multiselect("⚠️ Ignore EventCodes (Global):", sorted(raw_df_full['EventCode'].unique().tolist()))
    ignore_devices_mas = st.sidebar.multiselect("⚠️ Hide Machines from gallery:", sorted(raw_df_full['DeviceName'].unique().tolist()))
    if ignore_codes: m_df = raw_df_full[~raw_df_full['EventCode'].isin(ignore_codes)]
    else: m_df = raw_df_full
    desc_lookup = m_df.groupby('EventCode')['EventDescription'].first().to_dict()
    
    st.sidebar.divider()
    st.sidebar.header("Navigation & Metrics")
    s_dev = st.sidebar.selectbox("Active Machine View", ["All"] + sorted(m_df['DeviceName'].unique().tolist()))
    s_typ = st.sidebar.multiselect("Event Type Filter", m_df['EventType'].unique(), default=list(m_df['EventType'].unique()))
    s_met = st.sidebar.radio("View Segments By:", ["Count", "Duration"], index=0)
    s_top = st.sidebar.selectbox("Detail Level", ["Top 5", "All"], index=0)
    s_oth = st.sidebar.checkbox("Include 'Others' Group", True)
    s_mas = st.sidebar.checkbox("Generate Machine gallery Tab", False)
    s_unt = st.sidebar.radio("Trend Unit (Downtime):", ["Minutes", "Seconds"], key="unit_sidebar")
    
    st.sidebar.divider(); st.sidebar.header("🕒 Precision Timing")
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

    if len(w1_dates) == 2:
        w1_start_dt = datetime.combine(w1_dates[0], w1_start_t)
        w1_end_dt = datetime.combine(w1_dates[1], w1_end_t)
        met1_dev_only = get_metrics_calculation(m_df, w1_start_dt, w1_end_dt, s_typ, s_dev, ['DeviceName'], break_window=(brk_start, brk_end))
        
        if not comp_on:
            f_pie = px.pie(met1_dev_only, names='DeviceName', values='Count' if s_met=="Count" else 'Duration', hole=0.4, title="Machine Impact Distribution", hover_data=['EventDescription'])
            f_hyb = None
        else:
            m1_c = get_metrics_calculation(m_df, w1_start_dt, w1_end_dt, s_typ, s_dev, ['DeviceName', 'EventCode'], break_window=(brk_start, brk_end))
            m2_c = get_metrics_calculation(m_df, w2_start_dt, w2_end_dt, s_typ, s_dev, ['DeviceName', 'EventCode'], break_window=(brk_start, brk_end)) if comp_on else pd.DataFrame()
            f_hyb = create_cluster_stack_chart(m1_c, m2_c, 'DeviceName', 'EventCode', "Machine Analysis (W1 vs W2)", s_top, s_oth, s_met, desc_map=desc_lookup)

        # Tab Navigation
        tabs = st.tabs(["🏠 Dashboard", "🔍 Fault Analysis", "🛠️ Tech Log", "📄 Multi-Format Export"] + (["📊 Machine gallery"] if s_mas else []))
        t_map = {name: tabs[i] for i, name in enumerate(["🏠 Dashboard", "🔍 Fault Analysis", "🛠️ Tech Log", "📄 Multi-Format Export"] + (["📊 Machine gallery"] if s_mas else []))}

        with t_map["🏠 Dashboard"]:
            if not comp_on:
                m = met1_dev_only if s_dev=="All" else met1_dev_only[met1_dev_only['DeviceName']==s_dev]
                c = st.columns(4)
                c[0].metric("Total Events", f"{int(m['Count'].sum())}"); c[1].metric("Total Downtime", format_seconds_to_clock(m['Duration'].sum())); c[2].metric("Avg Duration", f"{(m['Duration'].sum()/max(1,m['Count'].sum())):.1f}s"); c[3].metric("Active Machines", m[m['Count']>0]['DeviceName'].nunique())
                l, r = st.columns([1, 1.2])
                with l: st.plotly_chart(f_pie, use_container_width=True)
                with r:
                    met1_dev_only['Action'], met1_dev_only['Total Downtime'] = "🔍 View Logs", met1_dev_only['Duration'].apply(format_seconds_to_clock)
                    met1_dev_only['Count'] = met1_dev_only['Count'].astype(int)
                    grid1 = st.dataframe(met1_dev_only[['Action', 'DeviceName', 'Count', 'Total Downtime', 'Cycles', 'Fault%']].style.format({'Cycles': '{:,.0f}', 'Fault%': '{:.2f}%'}), use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key=f"g1_{st.session_state.tk_main}")
                    if grid1 and grid1.get('selection') and grid1['selection'].get('rows'): st.session_state.dialog_trigger = {'m': met1_dev_only.iloc[grid1['selection']['rows'][0]]['DeviceName'], 'c': "All", 't': 'main'}
            else:
                met2_dev_only = get_metrics_calculation(m_df, w2_start_dt, w2_end_dt, s_typ, s_dev, ['DeviceName'], break_window=(brk_start, brk_end))
                st.markdown(f"**Window 1 Metrics ({w1_start_dt})**"); c1 = st.columns(4)
                m1 = met1_dev_only if s_dev=="All" else met1_dev_only[met1_dev_only['DeviceName']==s_dev]
                c1[0].metric("Events", int(m1['Count'].sum())); c1[1].metric("Downtime", format_seconds_to_clock(m1['Duration'].sum())); c1[2].metric("Avg", f"{(m1['Duration'].sum()/max(1,m1['Count'].sum())):.1f}s"); c1[3].metric("Active", m1[m1['Count']>0]['DeviceName'].nunique())
                st.markdown(f"**Window 2 Metrics ({w2_start_dt})**"); c2 = st.columns(4)
                m2 = met2_dev_only if s_dev=="All" else met2_dev_only[met2_dev_only['DeviceName']==s_dev]
                c2[0].metric("Events", int(m2['Count'].sum())); c2[1].metric("Downtime", format_seconds_to_clock(m2['Duration'].sum())); c2[2].metric("Avg", f"{(m2['Duration'].sum()/max(1,m2['Count'].sum())):.1f}s"); c2[3].metric("Active", m2[m2['Count']>0]['DeviceName'].nunique())
                st.plotly_chart(f_hyb, use_container_width=True)
                diff = pd.merge(met1_dev_only, met2_dev_only, on='DeviceName', how='outer', suffixes=('_w1', '_w2')).fillna(0)
                for c in ['Count_w1', 'Count_w2', 'Cycles_w1', 'Cycles_w2']: diff[c] = diff[c].astype(int)
                diff['Action'], diff['Delta Count'] = "🔍 View Logs", diff['Count_w2'] - diff['Count_w1']
                diff['Delta DT (Hrs)'] = ((diff['Duration_w2'] - diff['Duration_w1']) / 3600).round(2)
                diff['Duration_w1'], diff['Duration_w2'] = diff['Duration_w1'].apply(format_seconds_to_clock), diff['Duration_w2'].apply(format_seconds_to_clock)
                grid_c = st.dataframe(diff[['Action', 'DeviceName', 'Count_w1', 'Count_w2', 'Delta Count', 'Duration_w1', 'Duration_w2', 'Delta DT (Hrs)', 'Cycles_w1', 'Cycles_w2', 'Fault%_w1', 'Fault%_w2']].style.map(color_delta_styling, subset=['Delta Count', 'Delta DT (Hrs)']).format({'Delta DT (Hrs)': '{:.2f}', 'Fault%_w1': '{:.2f}%', 'Fault%_w2': '{:.2f}%'}), use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key=f"gc_{st.session_state.tk_main}")
                if grid_c and grid_c.get('selection') and grid_c['selection'].get('rows'): st.session_state.dialog_trigger = {'m': diff.iloc[grid_c['selection']['rows'][0]]['DeviceName'], 'c': "All", 't': 'main'}

        with t_map["🔍 Fault Analysis"]:
            f1_full_data = get_metrics_calculation(m_df, w1_start_dt, w1_end_dt, s_typ, s_dev, ['EventCode', 'EventDescription'], break_window=(brk_start, brk_end))
            if not comp_on:
                f10 = f1_full_data.sort_values('Count', ascending=False).head(10) if not f1_full_data.empty else pd.DataFrame()
                st.plotly_chart(px.bar(f10, x='EventCode', y='Count' if s_met=="Count" else 'Duration', title="Top 10 High-Impact Codes").update_layout(yaxis_title=s_met), use_container_width=True)
                f10['Action'], f10['Downtime'] = "🔍 View Logs", f10['Duration'].apply(format_seconds_to_clock)
                grid_f = st.dataframe(f10[['Action', 'EventCode', 'Count', 'Downtime', 'EventDescription']], use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key=f"gf1_{st.session_state.tk_fault}")
                if grid_f and grid_f.get('selection') and grid_f['selection'].get('rows'): st.session_state.dialog_trigger = {'m': s_dev, 'c': f10.iloc[grid_f['selection']['rows'][0]]['EventCode'], 't': 'fault'}
            else:
                f1c = get_metrics_calculation(m_df, w1_start_dt, w1_end_dt, s_typ, s_dev, ['EventCode', 'DeviceName'], break_window=(brk_start, brk_end))
                f2c = get_metrics_calculation(m_df, w2_start_dt, w2_end_dt, s_typ, s_dev, ['EventCode', 'DeviceName'], break_window=(brk_start, brk_end))
                u_codes = list(set((f1_full_data.sort_values('Count', ascending=False).head(10)['EventCode'].tolist() if not f1_full_data.empty else []) + (get_metrics_calculation(m_df, w2_start_dt, w2_end_dt, s_typ, s_dev, ['EventCode'], break_window=(brk_start, brk_end)).sort_values('Count', ascending=False).head(10)['EventCode'].tolist() if comp_on else [])))
                f_fh = create_cluster_stack_chart(f1c[f1c['EventCode'].isin(u_codes)], f2c[f2c['EventCode'].isin(u_codes)], 'EventCode', 'DeviceName', "Event Comparison Analysis", s_top, s_oth, s_met, desc_map=desc_lookup)
                st.plotly_chart(f_fh, use_container_width=True)
                f2_full_calc = get_metrics_calculation(m_df, w2_start_dt, w2_end_dt, s_typ, s_dev, ['EventCode', 'EventDescription'], break_window=(brk_start, brk_end))
                f_diff_tbl = pd.merge(f1_full_data[f1_full_data['EventCode'].isin(u_codes)], f2_full_calc[f2_full_calc['EventCode'].isin(u_codes)], on=['EventCode', 'EventDescription'], how='outer', suffixes=('_w1', '_w2')).fillna(0)
                f_diff_tbl['Delta Count'], f_diff_tbl['Action'] = f_diff_tbl['Count_w2'] - f_diff_tbl['Count_w1'], "🔍 View Logs"
                f_diff_tbl['Delta DT (Hrs)'], f_diff_tbl['Duration_w1'], f_diff_tbl['Duration_w2'] = ((f_diff_tbl['Duration_w2'] - f_diff_tbl['Duration_w1']) / 3600).round(2), f_diff_tbl['Duration_w1'].apply(format_seconds_to_clock), f_diff_tbl['Duration_w2'].apply(format_seconds_to_clock)
                grid_fc = st.dataframe(f_diff_tbl[['Action', 'EventCode', 'Count_w1', 'Count_w2', 'Delta Count', 'Duration_w1', 'Duration_w2', 'Delta DT (Hrs)', 'EventDescription']].style.map(color_delta_styling, subset=['Delta Count', 'Delta DT (Hrs)']).format({'Delta DT (Hrs)': '{:.2f}'}), use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key=f"gfc_{st.session_state.tk_fault}")
                if grid_fc and grid_fc.get('selection') and grid_fc['selection'].get('rows'): st.session_state.dialog_trigger = {'m': s_dev, 'c': f_diff_tbl.iloc[grid_fc['selection']['rows'][0]]['EventCode'], 't': 'fault'}

            st.divider(); st.subheader("Time-Series Trend Analysis")
            st.session_state.trend_event_code = st.selectbox("Code to Trend:", ["All"] + sorted(m_df['EventCode'].unique().tolist()), index=0)
            f_o = st.selectbox("Interval:", ['Daily', 'Weekly', 'Monthly']); f_m = {'Daily':'D', 'Weekly':'W', 'Monthly':'ME'}
            def get_tr(dr_s, dr_e, code, dev, typs, unit, b_win):
                sub = m_df[(m_df['EventDate'] >= dr_s) & (m_df['EventDate'] <= dr_e)].copy()
                if b_win and b_win[0] != b_win[1]: sub = sub[~((sub['EventDate'].dt.time >= b_win[0]) & (sub['EventDate'].dt.time <= b_win[1]))]
                if dev != "All": sub = sub[sub['DeviceName'] == dev]
                sub = sub[sub['EventType'].isin(typs)]
                if code != "All": sub = sub[sub['EventCode'] == code]
                tr = sub.set_index('EventDate').groupby([pd.Grouper(freq=f_m[f_o]), 'EventType']).agg({'EventCode':'count', 'Duration':'sum'}).reset_index()
                tr['P'] = tr['EventCode'] if s_met=="Count" else tr['Duration']/60
                tr['D'] = tr['Duration'] / (60 if unit=="Minutes" else 1)
                return tr
            t1 = get_tr(w1_start_dt, w1_end_dt, st.session_state.trend_event_code, s_dev, s_typ, s_unt, (brk_start, brk_end)); tc = st.columns(4)
            tc[0].plotly_chart(px.line(t1, x='EventDate', y='P', color='EventType', markers=True, title="W1 Trend"), use_container_width=True)
            tc[2].plotly_chart(px.line(t1, x='EventDate', y='D', color='EventType', markers=True, title="W1 Downtime"), use_container_width=True)
            if comp_on and w2_start_dt:
                t2 = get_tr(w2_start_dt, w2_end_dt, st.session_state.trend_event_code, s_dev, s_typ, s_unt, (brk_start, brk_end))
                tc[1].plotly_chart(px.line(t2, x='EventDate', y='P', color='EventType', markers=True, title="W2 Trend"), use_container_width=True)
                tc[3].plotly_chart(px.line(t2, x='EventDate', y='D', color='EventType', markers=True, title="W2 Downtime"), use_container_width=True)

        with t_map["🛠️ Tech Log"]:
            with st.expander("📝 Record New Maintenance Action", expanded=False):
                with st.form("entry_form", clear_on_submit=True):
                    c1_p, c2_p = st.columns(2)
                    f_m_p = c1_p.selectbox("Machine", sorted(m_df['DeviceName'].unique()))
                    f_c_p = c1_p.selectbox("Fault Code", sorted(m_df[m_df['DeviceName']==f_m_p]['EventCode'].unique()))
                    f_t_p, f_d_p, f_a_p = c2_p.text_input("Tech Name"), c2_p.date_input("Date", datetime.now()), st.text_area("Resolution")
                    if st.form_submit_button("Save"):
                        db_conn.cursor().execute("INSERT INTO notes (DeviceName, EventCode, TechnicianName, CorrectiveAction, ActionDate) VALUES (?,?,?,?,?)", (f_m_p, f_c_p, f_t_p, f_a_p, str(f_d_p)))
                        db_conn.commit(); st.success("Stored.")
            full_hist = pd.read_sql_query("SELECT * FROM notes", db_conn).sort_values(by=['DeviceName', 'ActionDate'], ascending=[True, False])
            if not full_hist.empty:
                def group_logic(df_in):
                    s = pd.DataFrame('', index=df_in.index, columns=df_in.columns); prev = None
                    for i, m in enumerate(df_in['DeviceName']):
                        if prev is not None and m != prev: s.iloc[i, :] = 'background-color: #deeaf7; border-top: 4px solid #31333F; font-weight: bold;'
                        prev = m
                    return s
                st.dataframe(full_hist.style.apply(group_logic, axis=None), use_container_width=True, hide_index=True)

        # ==============================================================================
        # REFACTORED: MACHINE ANALYSIS EXPORT BUILDER
        # ==============================================================================
        with t_map["📄 Multi-Format Export"]:
            st.subheader("Machine Analysis Report Builder")
            e1, e2 = st.columns(2)
            
            with e1:
                st.markdown("### 🌐 Interactive HTML Report")
                html_items = st.multiselect("Pick HTML Content:", 
                                         ["Date Window Headers", "KPI Comparison Summary", "Machine Analysis Chart", "Machine Performance Table", "Technician History Log"],
                                         default=["Date Window Headers", "KPI Comparison Summary", "Machine Analysis Chart"])
                if st.button("🚀 Generate HTML Machine Report"):
                    html_buffer = io.StringIO()
                    html_buffer.write("<html><head><style>body{font-family:sans-serif; padding:40px;} .card{border:1px solid #ddd; padding:20px; margin-bottom:20px; border-radius:8px;}</style></head><body>")
                    html_buffer.write("<h1>Production Line Machine Analysis</h1><hr>")
                    
                    if "Date Window Headers" in html_items:
                        html_buffer.write(f"<div class='card'><h3>Analysis Periods</h3><p><b>Window 1:</b> {w1_start_dt} to {w1_end_dt}</p>")
                        if comp_on: html_buffer.write(f"<p><b>Window 2:</b> {w2_start_dt} to {w2_end_dt}</p>")
                        html_buffer.write("</div>")
                        
                    if "KPI Comparison Summary" in html_items:
                        html_buffer.write("<div class='card'><h3>KPI Summary</h3>")
                        html_buffer.write(f"<p>W1 Events: {int(met1_dev_only['Count'].sum())} | W1 Downtime: {format_seconds_to_clock(met1_dev_only['Duration'].sum())}</p>")
                        if comp_on: html_buffer.write(f"<p>W2 Events: {int(met2_dev_only['Count'].sum())} | W2 Downtime: {format_seconds_to_clock(met2_dev_only['Duration'].sum())}</p>")
                        html_buffer.write("</div>")
                        
                    if "Machine Analysis Chart" in html_items:
                        chart_to_use = f_hyb if comp_on else f_pie
                        if chart_to_use:
                            html_buffer.write(f"<div class='card'><h3>Production Analysis Visual</h3>{chart_to_use.to_html(full_html=False, include_plotlyjs='cdn')}</div>")
                            
                    if "Machine Performance Table" in html_items:
                        table_to_use = diff if comp_on else met1_dev_only
                        html_buffer.write(f"<div class='card'><h3>Performance Data</h3>{table_to_use.drop(columns=['Action']).to_html(index=False)}</div>")
                    
                    if "Technician History Log" in html_items:
                        html_buffer.write(f"<div class='card'><h3>Maintenance Logs</h3>{full_hist.to_html(index=False)}</div>")
                    
                    html_buffer.write("</body></html>")
                    st.download_button("📥 Download HTML Machine Report", data=html_buffer.getvalue(), file_name="Machine_Analysis_Report.html", mime="text/html")

            with e2:
                st.markdown("### 📊 Multi-Sheet Excel Executive Summary")
                if st.button("🚀 Generate Excel Machine Summary"):
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        # Metadata Header Sheet (Requirement: Time Windows at top)
                        meta_df = pd.DataFrame({
                            'Analysis Period': ['Window 1 Start', 'Window 1 End', 'Window 2 Start', 'Window 2 End'],
                            'Timestamp': [str(w1_start_dt), str(w1_end_dt), str(w2_start_dt) if comp_on else "N/A", str(w2_end_dt) if comp_on else "N/A"]
                        })
                        meta_df.to_excel(writer, sheet_name='Executive Summary', index=False)
                        
                        # Sheet 1: Executive Machine Table (W1 vs W2)
                        sheet1_data = diff.drop(columns=['Action']) if comp_on else met1_dev_only.drop(columns=['Action'])
                        sheet1_data.to_excel(writer, sheet_name='Executive Summary', startrow=6, index=False)
                        
                        # Sheet 2: Detailed Fault Rankings
                        f1_full_data.to_excel(writer, sheet_name='Fault Analysis', index=False)
                        
                        # Sheet 3: Tech Logs
                        full_hist.to_excel(writer, sheet_name='Technician Logs', index=False)
                        
                    st.download_button("📥 Download Excel Machine Summary", data=output.getvalue(), file_name="Machine_Executive_Summary.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        if s_mas:
            with t_map["📊 Machine gallery"]:
                active_fleet = [m for m in sorted(m_df['DeviceName'].unique().tolist()) if m not in ignore_devices_mas]
                for i in range(0, len(active_fleet), 4):
                    cols = st.columns(4)
                    for j in range(4):
                        if i+j < len(active_fleet):
                            nm = active_fleet[i+j]
                            mw1 = get_metrics_calculation(m_df, w1_start_dt, w1_end_dt, s_typ, nm, ['DeviceName', 'EventCode'], break_window=(brk_start, brk_end))
                            mw2 = get_metrics_calculation(m_df, w2_start_dt, w2_end_dt, s_typ, nm, ['DeviceName', 'EventCode'], break_window=(brk_start, brk_end)) if comp_on else pd.DataFrame()
                            f = create_cluster_stack_chart(mw1, mw2, 'DeviceName', 'EventCode', nm, s_top, s_oth, s_met, desc_map=desc_lookup, is_gallery=True)
                            f.update_layout(height=350, showlegend=False)
                            with cols[j]: st.plotly_chart(f, use_container_width=True)

        if st.session_state.dialog_trigger:
            d_p = st.session_state.dialog_trigger; st.session_state.dialog_trigger = None
            show_popup_logs(d_p['m'], d_p['c'], d_p['t'])
    else:
        st.warning("Please select valid date ranges for both time windows.")
else: st.info("👋 Welcome. Please upload machine production data in the sidebar.")
