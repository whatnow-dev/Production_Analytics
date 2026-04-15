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
import io
import tempfile
import os

# --- 1. SYSTEM INITIALIZATION ---
st.set_page_config(page_title="Production Analytics Master", layout="wide")
pio.templates.default = "plotly_white"

if 'raw_data' not in st.session_state: st.session_state.raw_data = None
if 'trend_event_code' not in st.session_state: st.session_state.trend_event_code = "All"
if 'tk_main' not in st.session_state: st.session_state.tk_main = 0
if 'tk_fault' not in st.session_state: st.session_state.tk_fault = 100
if 'dialog_trigger' not in st.session_state: st.session_state.dialog_trigger = None

def init_db():
    conn = sqlite3.connect('maintenance_data.db', check_same_thread=False)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS notes 
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, DeviceName TEXT, 
                  EventCode TEXT, TechnicianName TEXT, CorrectiveAction TEXT, ActionDate TEXT)''')
    conn.commit()
    return conn

conn = init_db()

# --- 2. ANALYTICS HELPERS ---
def color_delta(val):
    try:
        if val > 0.001: return 'color: #ff4b4b; font-weight: bold' 
        elif val < -0.001: return 'color: #28a745; font-weight: bold'
    except: pass
    return 'color: #31333F'

def format_dhm(seconds):
    seconds = max(0, seconds)
    d, h_rem = divmod(int(seconds), 86400)
    h, m_rem = divmod(h_rem, 3600)
    m, _ = divmod(m_rem, 60)
    return f"{d}d {h}h {m}m"

def clean_duration(series):
    def parse(x):
        try:
            x = str(x).strip().lower()
            if ':' in x:
                p = x.split(':')
                v = int(p[0])*3600 + int(p[1])*60 + float(p[2]) if len(p)==3 else int(p[0])*60 + float(p[1])
            else:
                v = float(re.sub(r'[^0-9.]', '', x))
            return v if 0 <= v <= 86400 else 0.0 
        except: return 0.0
    return series.apply(parse)

@st.cache_data
def process_upload(file):
    df = pd.read_excel(file)
    df.columns = [str(c).strip() for c in df.columns]
    f = lambda kw: next((c for c in df.columns if any(k in c.lower() for k in kw)), None)
    clean = pd.DataFrame()
    clean['DeviceName'] = df[f(['device'])].astype(str)
    clean['EventCode'] = df[f(['code'])].astype(str).str.replace('.0', '', regex=False)
    clean['EventType'] = df[f(['type'])].astype(str) if f(['type']) else "Fault"
    clean['EventDescription'] = df[f(['desc'])].astype(str) if f(['desc']) else ""
    clean['EventDate'] = pd.to_datetime(df[f(['date', 'time'])], errors='coerce')
    dur_col = next((c for c in df.columns if 'duration' in c.lower() and 'date' not in c.lower()), None)
    clean['Duration'] = clean_duration(df[dur_col]) if dur_col else 0.0
    clean['SystemCounter'] = pd.to_numeric(df[f(['counter'])], errors='coerce').fillna(0)
    return clean.dropna(subset=['EventDate'])

def get_metrics_master(df_source, date_range, sel_types, sel_device, group_cols, global_cycles_val=None):
    if len(date_range) != 2: return pd.DataFrame()
    d_mask = (df_source['EventDate'].dt.date >= date_range[0]) & (df_source['EventDate'].dt.date <= date_range[1])
    window_df = df_source.loc[d_mask]
    if sel_device != "All": window_df = window_df[window_df['DeviceName'] == sel_device]
    if window_df.empty: return pd.DataFrame(columns=group_cols + ['EventCount', 'DurationTotal', 'Cycles', 'Fault%'])
    
    v_cyc = window_df[window_df['SystemCounter'] > 0]
    cycles_by_machine = v_cyc.groupby('DeviceName')['SystemCounter'].agg(lambda x: int(max(0, x.max() - x.min()))).reset_index(name='Cycles')
    
    event_df = window_df[window_df['EventType'].isin(sel_types)]
    if event_df.empty:
        res = cycles_by_machine.copy()
        for col in group_cols: 
            if col not in res.columns: res[col] = "N/A"
        res['EventCount'] = 0; res['DurationTotal'] = 0.0
    else:
        agg = event_df.groupby(group_cols).agg(EventCount=('EventCode', 'count'), DurationTotal=('Duration', 'sum')).reset_index()
        if 'DeviceName' in group_cols and len(group_cols) == 1:
            res = pd.merge(agg, cycles_by_machine, on='DeviceName', how='outer').fillna(0)
        elif 'DeviceName' in group_cols and len(group_cols) > 1:
            res = pd.merge(agg, cycles_by_machine, on='DeviceName', how='left').fillna(0)
        else:
            res = agg.copy(); res['Cycles'] = global_cycles_val if global_cycles_val is not None else cycles_by_machine['Cycles'].sum()
    
    res['Fault%'] = (res['EventCount'] / res['Cycles'].replace(0, 1) * 100).round(2)
    return res

def get_total_context_cycles(df_source, date_range, sel_device):
    if len(date_range) != 2: return 0
    d_mask = (df_source['EventDate'].dt.date >= date_range[0]) & (df_source['EventDate'].dt.date <= date_range[1])
    window_df = df_source.loc[d_mask]
    if sel_device != "All": window_df = window_df[window_df['DeviceName'] == sel_device]
    v_cyc = window_df[window_df['SystemCounter'] > 0]
    return int(v_cyc.groupby('DeviceName')['SystemCounter'].agg(lambda x: max(0, x.max() - x.min())).sum()) if not v_cyc.empty else 0

# --- THE HYBRID CHART ENGINE (WITH OTHERS TOGGLE) ---
def create_cluster_stack(df_w1, df_w2, x_col, stack_col, title, top_n, show_others):
    fig = go.Figure()
    w1_shades = ['#082245','#08306B','#08519C','#2171B5','#4292C6','#6BAED6','#9ECAE1','#C6DBEF','#DEEBF7','#F7FBFF']
    w2_shades = ['#67000D','#A50F15','#CB181D','#EF3B2C','#FB6A4A','#FC9272','#FCBBA1','#FEE0D2','#FFF5F0','#FFFBFB']
    
    def process_top_n(df):
        if df.empty or top_n == "All": return df
        n_val = int(top_n.split()[-1])
        df = df.copy()
        df['rank'] = df.groupby(x_col)['EventCount'].rank(method='first', ascending=False)
        if show_others:
            df.loc[df['rank'] > n_val, stack_col] = "Others"
            return df.groupby([x_col, stack_col])['EventCount'].sum().reset_index()
        else:
            return df[df['rank'] <= n_val]

    clean_w1 = process_top_n(df_w1)
    clean_w2 = process_top_n(df_w2)

    def add_traces(df, name, offset, shades):
        if df.empty: return
        cats = sorted([c for c in df[stack_col].unique() if c != "Others"])
        if show_others and "Others" in df[stack_col].unique(): cats.append("Others")
        
        for i, cat in enumerate(cats):
            sub = df[df[stack_col] == cat]
            m_color = "#D3D3D3" if cat == "Others" else shades[i % len(shades)]
            fig.add_trace(go.Bar(
                name=f"{name}: {cat}", x=sub[x_col], y=sub['EventCount'],
                offsetgroup=offset, marker=dict(color=m_color, line=dict(color='white', width=0.5)),
                legendgroup=name, legendgrouptitle_text=name,
                hovertemplate=f"<b>{name}</b><br>{stack_col}: {cat}<br>Count: %{{y}}<extra></extra>"
            ))

    add_traces(clean_w1, "Window 1", 0, w1_shades)
    add_traces(clean_w2, "Window 2", 1, w2_shades)
    fig.update_layout(title=title, barmode='stack', yaxis_title="Count of Event Code", bargap=0.15, bargroupgap=0.1)
    return fig

# --- 3. DIALOGS ---
@st.dialog("Technician History Log", width="large")
def show_logs(target_device, code, tab_type):
    code_str = str(code)
    if tab_type == "main":
        st.subheader(f"History for Device: {target_device}")
        query, params = "SELECT * FROM notes WHERE DeviceName = ?", (target_device,)
    else:
        st.subheader(f"Fleet-Wide History for Event: {code_str}")
        if target_device not in ["All", "Machine"]: st.info(f"Showing logs for **{target_device}** first.")
        query, params = "SELECT * FROM notes WHERE EventCode = ?", (code_str,)

    db_notes = pd.read_sql_query(query, conn, params=params)
    if not db_notes.empty:
        db_notes['priority'] = db_notes['DeviceName'].apply(lambda x: 0 if x == target_device else 1)
        logs_display = db_notes.sort_values(by=['priority', 'ActionDate'], ascending=[True, False]).drop(columns=['priority'])
        def log_style(df_in):
            styles = pd.DataFrame('', index=df_in.index, columns=df_in.columns); prev = None
            for i, m in enumerate(df_in['DeviceName']):
                if prev is not None and m != prev: styles.iloc[i, :] = 'background-color: #deeaf7; border-top: 4px solid #31333F; font-weight: bold;'
                prev = m
            return styles
        st.dataframe(logs_display[['DeviceName', 'ActionDate', 'EventCode', 'TechnicianName', 'CorrectiveAction']].style.apply(log_style, axis=None), use_container_width=True, hide_index=True)
    else: st.warning("No entries found.")
    if st.button("Close and Clear Selection"):
        if tab_type == "main": st.session_state.tk_main += 1
        else: st.session_state.tk_fault += 1
        st.rerun()

# --- 4. DASHBOARD UI ---
uploaded_file = st.sidebar.file_uploader("Upload Events Excel", type=["xlsx"])
if uploaded_file: st.session_state.raw_data = process_upload(uploaded_file)

if st.session_state.raw_data is not None:
    df = st.session_state.raw_data
    st.sidebar.header("⚙️ Global Controls")
    sel_device = st.sidebar.selectbox("Device Name Filter", ["All"] + sorted(df['DeviceName'].unique().tolist()))
    sel_types = st.sidebar.multiselect("Event Type Filter", df['EventType'].unique(), default=list(df['EventType'].unique()))
    
    top_n_filter = st.sidebar.selectbox("Cluster Detail Level", ["Top 5", "Top 10", "All"], index=0)
    # REQUIREMENT: TOGGLE FOR OTHERS
    show_others_toggle = st.sidebar.checkbox("Include 'Others' Category in Charts", value=True)
    
    w1_dates = st.sidebar.date_input("Time Window 1", [df['EventDate'].min().date(), df['EventDate'].max().date()], key="w1")
    enable_comp = st.sidebar.checkbox("Enable Comparison (Window 2)")
    w2_dates = st.sidebar.date_input("Time Window 2", [df['EventDate'].min().date(), df['EventDate'].max().date()], key="w2") if enable_comp else None

    w1_total_cycles = get_total_context_cycles(df, w1_dates, sel_device)
    w2_total_cycles = get_total_context_cycles(df, w2_dates, sel_device) if enable_comp else 0
    tw_str = f"W1: {w1_dates[0]} to {w1_dates[1]}"
    if enable_comp and len(w2_dates) == 2: tw_str += f" | W2: {w2_dates[0]} to {w2_dates[1]}"

    t1, t2, t3, t4 = st.tabs(["🏠 Main Dashboard", "🔍 Fault Analysis", "🛠️ Tech Log", "📄 Export Reports"])

    with t1:
        st.subheader("Production Line Performance Summary")
        met1_tab = get_metrics_master(df, w1_dates, sel_types, sel_device, ['DeviceName'])
        m_calc = met1_tab if sel_device == "All" else met1_tab[met1_tab['DeviceName'] == sel_device]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Events", f"{int(m_calc['EventCount'].sum())}")
        c2.metric("Total Downtime", format_dhm(m_calc['DurationTotal'].sum()))
        c3.metric("Avg Duration", f"{(m_calc['DurationTotal'].sum() / max(1, m_calc['EventCount'].sum())):.1f} sec")
        c4.metric("Active Devices", f"{m_calc[m_calc['EventCount'] > 0]['DeviceName'].nunique()}")

        if not enable_comp:
            cl, cr = st.columns([1, 1.2])
            with cl: st.plotly_chart(px.pie(met1_tab, names='DeviceName', values='EventCount', hole=0.4, title="Event Distribution"), use_container_width=True)
            with cr:
                st.markdown("### Device Performance Details")
                met1_tab['Action'] = "🔍 View Logs"; met1_tab['Total Downtime'] = met1_tab['DurationTotal'].apply(format_dhm)
                st.dataframe(met1_tab[['Action', 'DeviceName', 'EventCount', 'Total Downtime', 'Cycles', 'Fault%']].style.format({'Cycles': '{:,.0f}', 'Fault%': '{:.2f}%'}), use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key=f"t1_{st.session_state.tk_main}")
        else:
            m1_chart = get_metrics_master(df, w1_dates, sel_types, sel_device, ['DeviceName', 'EventCode'])
            m2_chart = get_metrics_master(df, w2_dates, sel_types, sel_device, ['DeviceName', 'EventCode'])
            st.plotly_chart(create_cluster_stack(m1_chart, m2_chart, 'DeviceName', 'EventCode', "Machine Comparison", top_n_filter, show_others_toggle), use_container_width=True)
            
            st.markdown("### Row 2: Performance Delta Analysis")
            met2_tab = get_metrics_master(df, w2_dates, sel_types, sel_device, ['DeviceName'])
            comp_df = pd.merge(met1_tab, met2_tab, on='DeviceName', how='outer', suffixes=('_W1', '_W2')).fillna(0)
            for col in ['EventCount_W1', 'EventCount_W2', 'Cycles_W1', 'Cycles_W2']: comp_df[col] = comp_df[col].astype(int)
            comp_df['Delta Count'] = comp_df['EventCount_W2'] - comp_df['EventCount_W1']
            comp_df['Delta DT (Hrs)'] = ((comp_df['DurationTotal_W2'] - comp_df['DurationTotal_W1']) / 3600).round(2)
            comp_df['DT_W1'], comp_df['DT_W2'], comp_df['Action'] = comp_df['DurationTotal_W1'].apply(format_dhm), comp_df['DurationTotal_W2'].apply(format_dhm), "🔍 View Logs"
            main_cols = ['Action', 'DeviceName', 'EventCount_W1', 'EventCount_W2', 'Delta Count', 'DT_W1', 'DT_W2', 'Delta DT (Hrs)', 'Cycles_W1', 'Cycles_W2', 'Fault%_W1', 'Fault%_W2']
            t1_sel_c = st.dataframe(comp_df[main_cols].style.map(color_delta, subset=['Delta Count', 'Delta DT (Hrs)']).format({'Delta DT (Hrs)': '{:.2f}', 'Fault%_W1': '{:.2f}%', 'Fault%_W2': '{:.2f}%'}), use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key=f"t1_c_{st.session_state.tk_main}")
            if t1_sel_c and t1_sel_c.get('selection') and t1_sel_c['selection'].get('rows'):
                row_c = comp_df.iloc[t1_sel_c['selection']['rows'][0]]; st.session_state.dialog_trigger = {'device': row_c['DeviceName'], 'codes': "All", 'tab': 'main'}

    with t2:
        st.subheader(f"Fault Analysis: {sel_device}")
        f1_full = get_metrics_master(df, w1_dates, sel_types, sel_device, ['EventCode', 'EventDescription'], global_cycles_val=w1_total_cycles)
        if not enable_comp:
            f1 = f1_full.sort_values('EventCount', ascending=False).head(10) if not f1_full.empty else pd.DataFrame()
            cl, cr = st.columns([1, 1.2])
            with cl: st.plotly_chart(px.bar(f1, x='EventCode', y='EventCount', title="Top 10 Faults").update_layout(yaxis_title="Count of Event Code"), use_container_width=True)
            with cr:
                st.markdown("### Top 10 Fault Details")
                if not f1.empty:
                    f1['Action'], f1['Downtime'] = "🔍 View Logs", f1['DurationTotal'].apply(format_dhm)
                    t2_sel_s = st.dataframe(f1[['Action', 'EventCode', 'EventCount', 'Downtime', 'Cycles', 'Fault%', 'EventDescription']].style.format({'Cycles': '{:,.0f}', 'Fault%': '{:.2f}%'}), use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key=f"t2_{st.session_state.tk_fault}")
                    if t2_sel_s and t2_sel_s.get('selection') and t2_sel_s['selection'].get('rows'):
                        row_s = f1.iloc[t2_sel_s['selection']['rows'][0]]; st.session_state.dialog_trigger = {'device': sel_device, 'codes': row_s['EventCode'], 'tab': 'fault'}
        else:
            f1_chart = get_metrics_master(df, w1_dates, sel_types, sel_device, ['EventCode', 'DeviceName'], global_cycles_val=w1_total_cycles)
            f2_chart = get_metrics_master(df, w2_dates, sel_types, sel_device, ['EventCode', 'DeviceName'], global_cycles_val=w2_total_cycles)
            u_codes = list(set(f1_full.sort_values('EventCount', ascending=False).head(10)['EventCode'].tolist() if not f1_full.empty else []) | set(get_metrics_master(df, w2_dates, sel_types, sel_device, ['EventCode']).sort_values('EventCount', ascending=False).head(10)['EventCode'].tolist() if enable_comp else []))
            st.plotly_chart(create_cluster_stack(f1_chart[f1_chart['EventCode'].isin(u_codes)], f2_chart[f2_chart['EventCode'].isin(u_codes)], 'EventCode', 'DeviceName', "Fault Comparison", top_n_filter, show_others_toggle), use_container_width=True)
            
            st.markdown("### Row 2: Fault Delta Analysis")
            f1_tab = get_metrics_master(df, w1_dates, sel_types, sel_device, ['EventCode', 'EventDescription'], global_cycles_val=w1_total_cycles)
            f2_tab = get_metrics_master(df, w2_dates, sel_types, sel_device, ['EventCode', 'EventDescription'], global_cycles_val=w2_total_cycles)
            f_comp = pd.merge(f1_tab[f1_tab['EventCode'].isin(u_codes)], f2_tab[f2_tab['EventCode'].isin(u_codes)], on=['EventCode', 'EventDescription'], how='outer', suffixes=('_W1', '_W2')).fillna(0)
            f_comp['Cycles_W1'], f_comp['Cycles_W2'] = w1_total_cycles, w2_total_cycles
            for col in ['EventCount_W1', 'EventCount_W2', 'Cycles_W1', 'Cycles_W2']: f_comp[col] = f_comp[col].astype(int)
            f_comp['Delta Count'] = f_comp['EventCount_W2'] - f_comp['EventCount_W1']
            f_comp['Delta DT (Hrs)'] = ((f_comp['DurationTotal_W2'] - f_comp['DurationTotal_W1']) / 3600).round(2)
            f_comp['DT_W1'], f_comp['DT_W2'], f_comp['Action'] = f_comp['DurationTotal_W1'].apply(format_dhm), f_comp['DurationTotal_W2'].apply(format_dhm), "🔍 View Logs"
            # REQUIREMENT: REMOVE REQUESTED COLUMNS
            f_cols = ['Action', 'EventCode', 'EventCount_W1', 'EventCount_W2', 'Delta Count', 'DT_W1', 'DT_W2', 'Delta DT (Hrs)', 'EventDescription']
            t2_sel_c = st.dataframe(f_comp[f_cols].style.map(color_delta, subset=['Delta Count', 'Delta DT (Hrs)']).format({'Delta DT (Hrs)': '{:.2f}'}), use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", key=f"t2_c_{st.session_state.tk_fault}")
            if t2_sel_c and t2_sel_c.get('selection') and t2_sel_c['selection'].get('rows'):
                row_f = f_comp.iloc[t2_sel_c['selection']['rows'][0]]; st.session_state.dialog_trigger = {'device': sel_device, 'codes': row_f['EventCode'], 'tab': 'fault'}

        st.divider()
        st.subheader("Trend Analysis")
        freq_map = {'Daily': 'D', 'Weekly': 'W', 'Monthly': 'ME'}; drill = freq_map[st.selectbox("Trend Frequency:", list(freq_map.keys()))]
        ui_l, ui_r = st.columns(2)
        with ui_l:
            s1, s2 = st.columns([2, 1])
            with s1: opts = ["All"] + sorted(u_codes if enable_comp else f1_full['EventCode'].unique().tolist()); st.session_state.trend_event_code = st.selectbox("Trend Filter", opts, index=opts.index(st.session_state.trend_event_code) if st.session_state.trend_event_code in opts else 0)
            with s2: tm = st.radio("Trend Metric:", ["Counts", "Fault%"])
        with ui_r: du = st.radio("Trend Unit:", ["Minutes", "Seconds"])
        def get_trend_data(dr, code, s_dev, types_filter):
            if len(dr) < 2: return pd.DataFrame()
            mask = (df['EventDate'].dt.date >= dr[0]) & (df['EventDate'].dt.date <= dr[1])
            t_sub = df.loc[mask]
            if s_dev != "All": t_sub = t_sub[t_sub['DeviceName'] == s_dev]
            p_cyc = t_sub[t_sub['SystemCounter'] > 0].set_index('EventDate').resample(drill)['SystemCounter'].agg(lambda x: int(max(0, x.max() - x.min()))).reset_index(name='CP')
            t_sub = t_sub[t_sub['EventType'].isin(types_filter)]
            if code != "All": t_sub = t_sub[t_sub['EventCode'] == code]
            tr = t_sub.set_index('EventDate').groupby([pd.Grouper(freq=drill), 'EventType']).agg({'EventCode':'count', 'Duration':'sum'}).reset_index()
            tr = pd.merge(tr, p_cyc, on='EventDate', how='left').fillna(0)
            tr['Plot'] = (tr['EventCode'] / tr['CP'].replace(0, 1) * 100).round(2) if tm == "Fault%" else tr['EventCode']
            tr['D_Plot'] = tr['Duration'] / (60 if du == "Minutes" else 1)
            return tr
        tr1 = get_trend_data(w1_dates, st.session_state.trend_event_code, sel_device, sel_types)
        tc1, tc2, tc3, tc4 = st.columns(4)
        with tc1: st.plotly_chart(px.line(tr1, x='EventDate', y='Plot', color='EventType', title=f"W1 {tm}", markers=True).update_layout(yaxis_title="Count of Event Code"), use_container_width=True)
        with tc3: st.plotly_chart(px.line(tr1, x='EventDate', y='D_Plot', color='EventType', title=f"W1 DT ({du})", markers=True), use_container_width=True)
        if enable_comp:
            tr2 = get_trend_data(w2_dates, st.session_state.trend_event_code, sel_device, sel_types)
            with tc2: st.plotly_chart(px.line(tr2, x='EventDate', y='Plot', color='EventType', title=f"W2 {tm}", markers=True).update_layout(yaxis_title="Count of Event Code"), use_container_width=True)
            with tc4: st.plotly_chart(px.line(tr2, x='EventDate', y='D_Plot', color='EventType', title=f"W2 DT ({du})", markers=True), use_container_width=True)

    with t3:
        st.subheader("Technician Log Management")
        with st.form("tech_entry", clear_on_submit=True):
            f_dev = st.selectbox("Machine", sorted(df['DeviceName'].unique())); f_code = st.selectbox("Code", sorted(df[df['DeviceName'] == f_dev]['EventCode'].unique())); f_tech, f_act, f_dt = st.text_input("Tech"), st.text_area("Action"), st.date_input("Date", datetime.now())
            if st.form_submit_button("Save"): conn.cursor().execute("INSERT INTO notes (DeviceName, EventCode, TechnicianName, CorrectiveAction, ActionDate) VALUES (?,?,?,?,?)", (f_dev, f_code, f_tech, f_act, str(f_dt))); conn.commit(); st.success("Log Saved.")
        logs_raw = pd.read_sql_query("SELECT * FROM notes", conn)
        if not logs_raw.empty:
            def machine_sep(df_in):
                styles = pd.DataFrame('', index=df_in.index, columns=df_in.columns); prev = None
                for i, machine in enumerate(df_in['DeviceName']):
                    if prev is not None and machine != prev: styles.iloc[i, :] = 'background-color: #deeaf7; border-top: 5px solid #31333F; font-weight: bold;'
                    prev = machine
                return styles
            st.dataframe(logs_raw.sort_values(by=['DeviceName', 'EventCode']).style.apply(machine_sep, axis=None), use_container_width=True, hide_index=True)

    with t4:
        st.subheader("PDF Export")
        if st.button("🚀 Download Full Colored PDF Report"):
            with st.spinner("Processing PDF..."):
                pdf = FPDF(); ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
                def add_chart(pdf_obj, fig_obj, width=175):
                    fig_obj.update_layout(template="plotly_white", margin=dict(l=20, r=20, t=40, b=20))
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
                        pio.write_image(fig_obj, tmp.name, format="png", width=1200, height=600, scale=2)
                        pdf_obj.image(tmp.name, x=15, w=width)
                    if os.path.exists(tmp.name): os.remove(tmp.name)
                pdf.add_page(); pdf.set_font("Arial", 'B', 20); pdf.cell(190, 15, "Production Analytics Report", 0, 1, 'C')
                pdf.set_font("Arial", 'B', 11); pdf.cell(190, 8, f"Time Windows: {tw_str}", 0, 1, 'C'); pdf.ln(10)
                pdf.set_font("Arial", 'B', 12); pdf.cell(190, 10, "1. Summary Data", 0, 1, 'L')
                pdf_t = comp_df[main_cols] if enable_comp else met1_tab; cols_p = pdf_t.columns[1:11]
                pdf.set_font("Arial", 'B', 7); [pdf.cell(19, 8, str(c)[:11], 1, 0, 'C') for c in cols_p]; pdf.ln()
                pdf.set_font("Arial", '', 6)
                for _, r_pdf in pdf_t.iterrows():
                    for c in cols_p: pdf.cell(19, 7, str(r_pdf[c]).encode('latin-1', 'ignore').decode('latin-1')[:15], 1, 0, 'C')
                    pdf.ln()
                p_out = pdf.output(dest='S').encode('latin-1'); b64 = base64.b64encode(p_out).decode(); fname = f"Report_{ts}.pdf"
                st.markdown(f'<a href="data:application/pdf;base64,{b64}" download="{fname}" style="padding:12px; background-color:#28a745; color:white; border-radius:8px; text-decoration:none; font-weight:bold;">📥 Download PDF Report</a>', unsafe_allow_html=True)

    if st.session_state.dialog_trigger:
        pdia = st.session_state.dialog_trigger; st.session_state.dialog_trigger = None
        show_logs(pdia['device'], pdia['codes'], pdia['tab'])
else: st.info("Upload Excel File to begin analysis.")