# join_opsifin.py — Opsifin v6.0 · Hotel Report Dashboard
# Glassmorphism Enterprise Edition
# Rifyal Tumber · MTT · 2025

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import io, requests, re, hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(
    page_title="Hotel Intelligence · MTT",
    page_icon="🏨",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Constants ─────────────────────────────────────────────────────────────────
GDRIVE_IDS = {
    "hotel_chain":       "1r8dp_Chp-8QWKk_qXDMEehGj_c54U7ka",
    "hotel_city":        "1RQkiBAJJYbdkZngtrVlicYQEBKj3kPYL",
    "hotel_name":        "1paYSVhvvunLCZMKm4EF8TawvyRSHDC19",
    "hotel_supplier":    "11BG3oFaNQNEHXxy7jpWXZ0-Z9P6CBpRx",
    "supplier_category": "1zBudcR8Ia1nK0k4daMAOkOrIgvRO3GQD",
}
GDRIVE_LABELS = {
    "hotel_chain":"Hotel Chain","hotel_city":"Hotel City","hotel_name":"Hotel Name",
    "hotel_supplier":"Supplier","supplier_category":"Supplier Category",
}
DROP = [
    "Branch","Customer Type","Customer Name","PNR","Base Fare","Airlines","Class","Route",
    "Departure Time","Arrival Time","NTA","Airline Code","Flight No","Hotel Address",
    "Hotel Group Chain","Description","Due Date","Group Chain","Source Reference","Sales Net",
    *[f"Remark {i}" for i in range(1,13)],
    "Supplier Code","Ticket No","Fare Tax","IWJR","Add Charge","Insurance","PSC",
    "Other Charge","Incentive","Agent Comm","Customer Code","Travel Services","VAT","Stamp Fee",
    "MDR","Extra Disc","Rounding","Base Sell","Currency","Sales Handler","Remark","Source Rescode"
]

# Argon-style color palettes
GLASS_PALETTE = ["#5E72E4","#2DCE89","#FB6340","#F5365C","#11CDEF","#8965E0","#FFD600","#172B4D"]
INDIGO_SCALE  = ["#e8ebff","#c5cdfb","#8392f5","#5E72E4","#4658d6","#3343c2"]
TEAL_SCALE    = ["#d4f5ec","#a8ebda","#2DCE89","#24b574","#1a9d62","#10784a"]

# ── Data Functions ────────────────────────────────────────────────────────────
def fetch_gdrive_mapping(file_id):
    try:
        r = requests.get(f"https://drive.google.com/uc?export=download&id={file_id}", timeout=20)
        r.raise_for_status()
        df_map = pd.read_excel(io.BytesIO(r.content))
        if df_map.shape[1] >= 2:
            return dict(zip(df_map.iloc[:,0], df_map.iloc[:,1])), len(df_map)
        return {}, 0
    except Exception as e:
        return None, str(e)

def fetch_all_mappings_parallel():
    nm, ss = {}, {}
    with ThreadPoolExecutor(max_workers=len(GDRIVE_IDS)) as ex:
        futs = {ex.submit(fetch_gdrive_mapping, fid): k for k, fid in GDRIVE_IDS.items()}
        for f in as_completed(futs):
            k = futs[f]; mapping, result = f.result()
            if mapping is None:
                ss[k]="err"; st.toast(f"✗ {GDRIVE_LABELS[k]}", icon="❌")
            else:
                nm[k]=mapping; ss[k]="ok"
                st.toast(f"✓ {GDRIVE_LABELS[k]} · {result:,} baris", icon="✅")
    return nm, ss

def compute_upload_hash(files):
    h = hashlib.md5()
    for f in sorted(files, key=lambda x: x.name):
        h.update(f.name.encode()); h.update(str(f.size).encode())
    return h.hexdigest()

def build_df_raw(files, norm_maps):
    df = pd.concat([pd.read_excel(f) for f in files], ignore_index=True)
    df.columns = [str(c).strip() for c in df.columns]
    for col in ["Check In","Check Out","Issued Date","Inv Date"]:
        if col in df.columns: df[col] = pd.to_datetime(df[col], errors="coerce")
    if "Issued Date" in df.columns:
        df["Issued_Month"] = df["Issued Date"].dt.strftime("%B")
        df["Issued_Year"]  = df["Issued Date"].dt.year
    if norm_maps.get("hotel_city") and "Hotel City" in df.columns:
        df["Hotel_City"] = df["Hotel City"].map(norm_maps["hotel_city"]).fillna(df["Hotel City"])
    if norm_maps.get("hotel_name") and "Hotel Name" in df.columns:
        df["Hotel_Name"] = df["Hotel Name"].map(norm_maps["hotel_name"]).fillna(df["Hotel Name"])
    if norm_maps.get("hotel_chain") and "Hotel Chain" in df.columns:
        df["Hotel_Chain"] = df["Hotel Chain"].map(norm_maps["hotel_chain"]).fillna(df["Hotel Chain"])
    if norm_maps.get("hotel_supplier") and "Supplier Name" in df.columns:
        df["Supplier_Name"] = df["Supplier Name"].map(norm_maps["hotel_supplier"]).fillna("Direct to Hotel")
    if norm_maps.get("supplier_category") and "Supplier Name" in df.columns:
        df["Supplier_Category"] = df["Supplier Name"].map(norm_maps["supplier_category"]).fillna("Uncategorized")
    else:
        df["Supplier_Category"] = "Uncategorized"
    # ── Normalize supplier category labels ──────────────────────────
    # Step 1: remap any existing Supplier_Category values (case-insensitive)
    def _norm_sc(val):
        if pd.isnull(val): return "Uncategorized"
        v = str(val).strip()
        vu = v.upper()
        if vu in {"DIRECT TO HOTEL","DIRECT HOTEL"}: return "DIRECT HOTEL"
        if vu in {"PTM CORP RATE","CORPORATE RATE"}:  return "CORPORATE RATE"
        if v == "Uncategorized": return "Uncategorized"
        return v
    df["Supplier_Category"] = df["Supplier_Category"].apply(_norm_sc)

    # Step 2: override category based on Supplier_Name when category is still generic
    #   "PTM CORP RATE"  → CORPORATE RATE
    #   "Direct to Hotel"→ DIRECT HOTEL
    if "Supplier_Name" in df.columns:
        def _sc_from_name(row):
            cat  = row["Supplier_Category"]
            name = str(row["Supplier_Name"]).strip().upper()
            if name == "PTM CORP RATE":
                return "CORPORATE RATE"
            if name in {"DIRECT TO HOTEL","DIRECT HOTEL"}:
                return "DIRECT HOTEL"
            # If still Uncategorized, try to derive from raw Supplier Name
            if cat == "Uncategorized" and "Supplier Name" in df.columns:
                raw = str(row.get("Supplier Name","")).strip().upper()
                if raw == "PTM CORP RATE":  return "CORPORATE RATE"
                if raw in {"DIRECT TO HOTEL","DIRECT HOTEL"}: return "DIRECT HOTEL"
            return cat
        # Only apply row-wise if Supplier_Name exists
        _has_raw = "Supplier Name" in df.columns
        if _has_raw:
            df["Supplier_Category"] = df.apply(_sc_from_name, axis=1)
        else:
            _name_mask_ptm = df["Supplier_Name"].str.strip().str.upper() == "PTM CORP RATE"
            _name_mask_dth = df["Supplier_Name"].str.strip().str.upper().isin({"DIRECT TO HOTEL","DIRECT HOTEL"})
            df.loc[_name_mask_ptm, "Supplier_Category"] = "CORPORATE RATE"
            df.loc[_name_mask_dth, "Supplier_Category"] = "DIRECT HOTEL"
    for raw, clean in [("Hotel City","Hotel_City"),("Hotel Name","Hotel_Name"),("Hotel Chain","Hotel_Chain")]:
        if raw in df.columns and clean not in df.columns: df[clean] = df[raw]
    if "Supplier Name" in df.columns and "Supplier_Name" not in df.columns:
        df["Supplier_Name"] = df["Supplier Name"].fillna("Direct to Hotel")
    df = df.drop(columns=[c for c in DROP if c in df.columns], errors="ignore")
    if "Room" in df.columns and "Night" in df.columns:
        df["Total Room Night"] = df["Room"].fillna(0).astype(float) * df["Night"].fillna(0).astype(float)
        cols = list(df.columns); cols.remove("Total Room Night")
        cols.insert(cols.index("Night")+1,"Total Room Night"); df = df[cols]
    def nc(s): return re.sub(r"[^a-z0-9]","",str(s).lower())
    cm = {nc(c): c for c in df.columns}
    def fc(kws):
        for kw in kws:
            for nk, orig in cm.items():
                if kw in nk: return orig
        return None
    first_col  = fc(["firstname","first","givenname","namadepan"])
    last_col   = fc(["lastname","last","surname","namabelakang"])
    single_col = fc(["fullname","full_name","name","nama"]) if not first_col and not last_col else None
    if single_col and single_col in df.columns:
        nf = df[single_col].fillna("").astype(str).str.strip()
    else:
        sf = df[first_col].fillna("").astype(str).str.strip() if first_col and first_col in df.columns else pd.Series([""]*len(df))
        sl = df[last_col].fillna("").astype(str).str.strip()  if last_col  and last_col  in df.columns else pd.Series([""]*len(df))
        nf = (sf+" "+sl).str.strip()
    df["Full Name"] = nf.str.upper()
    cols = [c for c in df.columns if c != "Full Name"]
    ia = last_col or first_col or single_col
    if ia and ia in cols: cols.insert(cols.index(ia)+1,"Full Name")
    else: cols.append("Full Name")
    df = df[cols]

    # ── Normalized Invoice To ──────────────────────────────────────────────────
    # Detect the "Invoice To" column (case-insensitive)
    _inv_to_raw = next(
        (c for c in df.columns if any(k in c.lower() for k in
         ["invoice to","invoiceto","bill to","billto","invoice_to"])), None
    )
    CBT_PERTAMINA_ALIASES = {
        "CBT PERTAMINA(HOTEL CM)",
        "CBT PERTAMINA (HOTEL)",
        "PERTAMINA ENERGY TERMINAL (CBT)",
    }
    if _inv_to_raw:
        def _norm_inv_to(val):
            if pd.isnull(val) or str(val).strip() == "": return "Unknown"
            clean = str(val).strip().upper()
            if clean in CBT_PERTAMINA_ALIASES:
                return "CBT PERTAMINA"
            return str(val).strip()
        df["Normalized_Inv_To"] = df[_inv_to_raw].apply(_norm_inv_to)
        # Insert right after the source column
        cols2 = [c for c in df.columns if c != "Normalized_Inv_To"]
        idx   = cols2.index(_inv_to_raw) + 1
        cols2.insert(idx, "Normalized_Inv_To")
        df = df[cols2]

    return df

def fmt(val):
    try:
        if val is None: return "N/A"
        if isinstance(val,(float,np.floating)) and np.isnan(val): return "N/A"
        if isinstance(val,(int,np.integer)): return f"{int(val):,}"
        vf = float(val)
        return f"{int(vf):,}" if vf.is_integer() else f"{vf:,.2f}"
    except: return str(val)

def get_prev_period_metrics(df_raw, df_view):
    """Compute metrics for the period immediately before df_view's date range."""
    try:
        if "Issued Date" not in df_raw.columns or df_view.empty: return {}
        curr_min = df_view["Issued Date"].dropna().min()
        curr_max = df_view["Issued Date"].dropna().max()
        if pd.isnull(curr_min) or pd.isnull(curr_max): return {}
        delta    = curr_max - curr_min
        prev_max = curr_min - pd.Timedelta(days=1)
        prev_min = prev_max - delta
        prev_df  = df_raw[(df_raw["Issued Date"] >= prev_min) & (df_raw["Issued Date"] <= prev_max)]
        if prev_df.empty: return {}
        m = {}
        m["ui"] = prev_df["Invoice No"].nunique()            if "Invoice No"       in prev_df.columns else None
        m["rn"] = int(np.ceil(prev_df["Total Room Night"].sum())) if "Total Room Night" in prev_df.columns else None
        m["sa"] = prev_df["Sales AR"].fillna(0).astype(float).sum() if "Sales AR"   in prev_df.columns else None
        m["up"] = prev_df["Full Name"].dropna().nunique()    if "Full Name"        in prev_df.columns else None
        if "Profit" in prev_df.columns and "Sales AR" in prev_df.columns:
            _p=prev_df["Profit"].fillna(0).astype(float); _s=prev_df["Sales AR"].fillna(0).astype(float); _mm=_s!=0
            m["pm"] = (_p[_mm]/_s[_mm]*100).mean() if _mm.any() else 0
        return m
    except: return {}

def trend_badge(curr_val, prev_val, fmt_suffix="", reverse=False):
    """Return HTML trend badge: green up if improved, red down if declined."""
    try:
        if prev_val is None or prev_val == 0: return '<span class="gkpi-trend neu">── No Ref</span>'
        curr_f = float(str(curr_val).replace(",","").replace("%","")) if isinstance(curr_val,str) else float(curr_val)
        prev_f = float(prev_val)
        if prev_f == 0: return '<span class="gkpi-trend neu">── No Ref</span>'
        pct = (curr_f - prev_f) / abs(prev_f) * 100
        is_up = pct > 0
        if reverse: is_up = not is_up
        cls  = "up" if is_up else "down"
        icon = "▲" if pct > 0 else "▼"
        return f'<span class="gkpi-trend {cls}">{icon} {abs(pct):.1f}% vs period sebelumnya</span>'
    except: return '<span class="gkpi-trend neu">── N/A</span>'

def theme(fig):
    fig.update_layout(
        font_family="Open Sans",
        font_color="#525F7F",
        font_size=12,
        plot_bgcolor="rgba(255,255,255,0)",
        paper_bgcolor="rgba(255,255,255,0)",
        margin=dict(l=12,r=12,t=40,b=12),
        title_font=dict(size=13,color="#32325D",family="Open Sans"),
        legend=dict(
            font=dict(size=11,family="Open Sans"),
            bgcolor="rgba(255,255,255,.8)",
            bordercolor="rgba(0,0,0,.06)",
            borderwidth=1,
        ),
        hoverlabel=dict(
            bgcolor="#ffffff",
            bordercolor="rgba(0,0,0,.1)",
            font_size=12,
            font_family="Open Sans",
            font_color="#32325D",
        ),
    )
    fig.update_xaxes(
        showgrid=True, gridcolor="rgba(0,0,0,.06)", zeroline=False,
        tickfont=dict(size=11,color="#8898AA"),
        linecolor="rgba(0,0,0,.08)",
    )
    fig.update_yaxes(
        showgrid=True, gridcolor="rgba(0,0,0,.06)", zeroline=False,
        tickfont=dict(size=11,color="#8898AA"),
        linecolor="rgba(0,0,0,.08)",
    )
    return fig

def gsec(title, icon=""):
    lbl = f'<span class="gsec-icon">{icon}</span>&thinsp;{title}' if icon else title
    st.markdown(f'<div class="gsec">{lbl}</div>', unsafe_allow_html=True)

# ═════════════════════════════════════════════════════════════════════════════
# ARGON DASHBOARD CSS — v8.0 · Light Enterprise Edition
# ═════════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Open+Sans:ital,wght@0,300;0,400;0,600;0,700;0,800;1,400&family=Nunito:wght@400;600;700;800;900&display=swap');

/* ══ TOKENS ══ */
:root {
  --bg:      #F4F5F7;
  --card:    #FFFFFF;
  --t1:  #32325D;
  --t2:  #525F7F;
  --t3:  #8898AA;
  --t4:  #ADB5BD;
  --primary:  #5E72E4;
  --primary2: #4658d6;
  --success:  #2DCE89;
  --danger:   #F5365C;
  --warning:  #FB6340;
  --info:     #11CDEF;
  --purple:   #8965E0;
  --dark:     #172B4D;
  --border:   #E9ECEF;
  --border2:  #DEE2E6;
  --shadow:    0 0 2rem 0 rgba(136,152,170,.15);
  --shadow-sm: 0 1px 3px rgba(50,50,93,.1), 0 1px 0 rgba(0,0,0,.02);
  --shadow-lg: 0 15px 35px rgba(50,50,93,.1), 0 5px 15px rgba(0,0,0,.07);
  --r:  .375rem;
  --r2: .5rem;
  --r3: .75rem;
  --r4: 1rem;
  --font: 'Open Sans', sans-serif;
  --font-head: 'Nunito', sans-serif;
}

/* ══ RESET ══ */
*, *::before, *::after { box-sizing:border-box; }
html, body, [class*="css"] {
  font-family: var(--font) !important;
  font-size: 13px !important;
  color: var(--t2) !important;
  background-color: var(--bg) !important;
  -webkit-font-smoothing: antialiased;
}

/* ══ APP BACKGROUND ══ */
.stApp, body, [data-testid="stAppViewContainer"] {
  background-color: var(--bg) !important;
  background-image: none !important;
}
.stApp::before, .stApp::after { display:none !important; }
[data-testid="stAppViewContainer"] > section > div { background:transparent !important; }
.block-container { padding:0!important; max-width:100%!important; background:transparent!important; }
.main .block-container { padding:24px 32px 80px!important; }

/* ══ HIDE CHROME ══ */
[data-testid="collapsedControl"],
[data-testid="stSidebarCollapseButton"],
button[data-testid="baseButton-header"],
#MainMenu, footer, header { display:none!important; }

/* ══ SCROLLBAR ══ */
::-webkit-scrollbar { width:5px; height:5px; }
::-webkit-scrollbar-track { background:#f1f3f4; }
::-webkit-scrollbar-thumb { background:#CDD0D5; border-radius:10px; }
::-webkit-scrollbar-thumb:hover { background:#B0B7C0; }

/* ══ HEADER ══ */
.ghdr {
  background: linear-gradient(87deg,#5E72E4 0%,#825EE4 100%);
  padding:0 32px; height:64px;
  display:flex; align-items:center; justify-content:space-between;
  position:sticky; top:0; z-index:500;
  box-shadow:0 4px 6px rgba(50,50,93,.11),0 1px 3px rgba(0,0,0,.08);
}
.ghdr-brand { display:flex; align-items:center; gap:14px; }
.ghdr-logo {
  width:38px; height:38px;
  background:rgba(255,255,255,.2); border-radius:var(--r2);
  display:grid; place-items:center; flex-shrink:0;
  border:1px solid rgba(255,255,255,.25);
}
.ghdr-name { font-family:var(--font-head); font-size:1rem; font-weight:800; color:#fff; letter-spacing:-.3px; }
.ghdr-name span { opacity:.8; font-weight:400; }
.ghdr-sub { font-size:.6rem; color:rgba(255,255,255,.65); margin-top:2px; letter-spacing:.8px; text-transform:uppercase; }
.ghdr-right { display:flex; align-items:center; gap:10px; }
.ghdr-live {
  display:flex; align-items:center; gap:7px;
  font-size:.62rem; font-weight:700; color:#fff; letter-spacing:.8px; text-transform:uppercase;
  padding:5px 14px; border-radius:20px;
  background:rgba(255,255,255,.2); border:1px solid rgba(255,255,255,.25);
}
.ghdr-dot { width:6px; height:6px; border-radius:50%; background:#2DCE89; animation:livebeat 1.8s ease-in-out infinite; }
@keyframes livebeat {
  0%,100%{ box-shadow:0 0 0 0 rgba(45,206,137,.5); }
  50%    { box-shadow:0 0 0 6px rgba(45,206,137,0); }
}
.ghdr-pill { font-size:.62rem; font-weight:700; color:#fff; padding:5px 14px; border-radius:20px; background:rgba(255,255,255,.15); border:1px solid rgba(255,255,255,.2); }

/* ══ TICKER ══ */
.gticker {
  background:linear-gradient(87deg,#4658d6,#6e48d6);
  padding:6px 0; overflow:hidden; position:relative;
}
.gticker::before,.gticker::after { content:''; position:absolute; top:0; width:100px; height:100%; z-index:2; }
.gticker::before { left:0;  background:linear-gradient(90deg,rgba(70,88,214,1),transparent); }
.gticker::after  { right:0; background:linear-gradient(270deg,rgba(70,88,214,1),transparent); }
.gticker-track {
  display:inline-block; white-space:nowrap;
  animation:tickslide 65s linear infinite;
  font-size:.6rem; letter-spacing:1px; text-transform:uppercase; font-family:var(--font);
}
.gticker-track:hover { animation-play-state:paused; }
.t-item { color:rgba(255,255,255,.45); }
.t-item.hi { color:rgba(255,255,255,.85); font-weight:600; }
.tsep { margin:0 28px; color:rgba(255,255,255,.25); }
@keyframes tickslide { from{transform:translateX(0)} to{transform:translateX(-50%)} }

/* ══ SIDEBAR ══ */
[data-testid="stSidebar"] {
  background:#fff !important;
  border-right:1px solid var(--border) !important;
  box-shadow:var(--shadow) !important;
  min-width:260px!important; max-width:260px!important;
}
[data-testid="stSidebar"]::before { display:none !important; }
[data-testid="stSidebar"] > div:first-child { padding:0!important; }
[data-testid="stSidebar"] * { font-family:var(--font)!important; }

.sb-top {
  padding:22px 20px 18px; border-bottom:1px solid var(--border);
  display:flex; align-items:center; gap:12px;
  background:linear-gradient(87deg,#5E72E4,#825EE4);
}
.sb-logo {
  width:36px; height:36px; background:rgba(255,255,255,.2);
  border-radius:var(--r2); display:grid; place-items:center; flex-shrink:0;
  border:1px solid rgba(255,255,255,.25);
}
.sb-appname { font-family:var(--font-head); font-size:.88rem; font-weight:800; color:#fff; }
.sb-appname span { opacity:.8; font-weight:400; }
.sb-ver { font-size:.57rem; color:rgba(255,255,255,.6); margin-top:3px; letter-spacing:.5px; }

.sb-section { padding:18px 20px 7px; font-size:.58rem; font-weight:700; color:var(--t3)!important; text-transform:uppercase; letter-spacing:2px; }
.sb-divider { height:1px; background:var(--border); margin:4px 20px; }

.sync-row { display:flex; align-items:center; justify-content:space-between; padding:7px 20px; transition:all .15s; border-radius:var(--r); margin:2px 8px; }
.sync-row:hover { background:var(--bg); }
.sync-label { font-size:.72rem; color:var(--t2); font-weight:600; }
.stag { font-size:.58rem; font-weight:700; padding:3px 10px; border-radius:20px; letter-spacing:.3px; }
.stag-ok   { background:#ECFDF3; color:#027A48; border:1px solid #ABEFC6; }
.stag-err  { background:#FFF1F3; color:#C01048; border:1px solid #FECDD6; }
.stag-wait { background:#F2F4F7; color:var(--t3); border:1px solid var(--border); }

/* Sidebar upload */
[data-testid="stSidebar"] [data-testid="stFileUploader"] {
  background:#F8F9FE!important; border:1.5px dashed #BFC8E2!important;
  border-radius:var(--r2)!important; transition:all .2s!important;
}
[data-testid="stSidebar"] [data-testid="stFileUploader"]:hover {
  border-color:var(--primary)!important; background:#EEF1FB!important;
  box-shadow:0 0 0 3px rgba(94,114,228,.1)!important;
}
[data-testid="stSidebar"] [data-testid="stFileUploaderDropzoneInstructions"] p { font-size:.67rem!important; color:var(--t3)!important; }
[data-testid="stSidebar"] [data-testid="stFileUploaderDropzoneInstructions"] span { color:var(--primary)!important; font-weight:700!important; }
[data-testid="stSidebar"] .stFileUploader { padding:0 14px!important; margin-bottom:6px!important; }
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] [data-testid="stWidgetLabel"] p { font-size:.68rem!important; font-weight:600!important; color:var(--t2)!important; margin-bottom:5px!important; }
[data-testid="stSidebar"] [data-testid="stMultiSelect"] > div > div,
[data-testid="stSidebar"] [data-testid="stDateInput"] input {
  background:#fff!important; border:1px solid var(--border2)!important;
  border-radius:var(--r)!important; color:var(--t1)!important;
  font-size:.7rem!important; backdrop-filter:none!important;
}
[data-testid="stSidebar"] [data-testid="stMultiSelect"] span[data-baseweb="tag"] {
  background:#EEF1FB!important; color:var(--primary)!important;
  border:1px solid #BFC8E2!important; border-radius:4px!important; font-size:.62rem!important;
}
[data-testid="stSidebar"] .stMultiSelect,
[data-testid="stSidebar"] .stDateInput { padding:0 14px!important; margin-bottom:8px!important; }

/* ══ BUTTONS ══ */
[data-testid="stButton"] > button {
  background:linear-gradient(87deg,#5E72E4,#825EE4) !important;
  color:#fff !important; border:none !important;
  border-radius:var(--r2) !important; font-size:.72rem !important;
  font-weight:700 !important; padding:10px 22px !important; font-family:var(--font) !important;
  box-shadow:0 4px 6px rgba(50,50,93,.11),0 1px 3px rgba(0,0,0,.08) !important;
  transition:all .2s !important;
}
[data-testid="stButton"] > button:hover {
  box-shadow:0 7px 14px rgba(50,50,93,.1),0 3px 6px rgba(0,0,0,.08) !important;
  transform:translateY(-1px) !important; filter:brightness(1.06) !important;
}
[data-testid="stDownloadButton"] > button {
  background:#fff !important; color:var(--primary) !important;
  border:1px solid var(--border2) !important; border-radius:var(--r2) !important;
  font-size:.7rem !important; font-weight:600 !important; padding:10px 22px !important;
  box-shadow:var(--shadow-sm) !important; transition:all .2s !important; font-family:var(--font) !important;
}
[data-testid="stDownloadButton"] > button:hover {
  background:#F6F9FC !important; box-shadow:var(--shadow-lg) !important; transform:translateY(-1px) !important;
}

/* ══ TABS ══ */
[data-testid="stTabs"] [data-baseweb="tab-list"] {
  background:#fff !important; border:1px solid var(--border) !important;
  border-radius:var(--r3) !important; gap:2px !important; padding:6px !important;
  margin-bottom:28px; box-shadow:var(--shadow-sm) !important;
}
[data-testid="stTabs"] [data-baseweb="tab"] {
  font-size:.73rem !important; font-weight:600 !important; color:var(--t3) !important;
  padding:9px 20px !important; border-bottom:none !important;
  transition:all .2s !important; font-family:var(--font) !important;
  border-radius:var(--r2) !important;
}
[data-testid="stTabs"] [data-baseweb="tab"]:hover { color:var(--t1) !important; background:#F6F9FC !important; }
[data-testid="stTabs"] [aria-selected="true"] {
  color:#fff !important; font-weight:700 !important;
  background:linear-gradient(87deg,#5E72E4,#825EE4) !important;
  box-shadow:0 4px 6px rgba(94,114,228,.3) !important;
}

/* ══ SECTION HEADER ══ */
.gsec {
  display:flex; align-items:center; gap:10px;
  font-family:var(--font-head); font-size:.72rem; font-weight:800; color:var(--t1);
  text-transform:uppercase; letter-spacing:1px; margin:8px 0 18px;
}
.gsec::after { content:''; flex:1; height:1px; background:var(--border); }
.gsec-icon { font-size:.8rem; }

/* ══ KPI SCORECARD — Minimalist Edition ══ */

/* ── Grid wrappers ── */
.sc-grid-3 { display:grid; grid-template-columns:repeat(3,1fr); gap:14px; margin-bottom:14px; }
.sc-strip   { display:grid; gap:0; margin-bottom:14px;
              background:#fff; border:1px solid var(--border);
              border-radius:var(--r3); overflow:hidden;
              box-shadow:0 1px 3px rgba(50,50,93,.06); }
.sc-strip-5 { grid-template-columns:repeat(5,1fr); }
.sc-strip-4 { grid-template-columns:repeat(4,1fr); }

@media(max-width:1200px){
  .sc-strip-5{grid-template-columns:repeat(3,1fr);}
}
@media(max-width:1100px){
  .sc-grid-3{grid-template-columns:repeat(2,1fr);}
  .sc-strip-4{grid-template-columns:repeat(2,1fr);}
  .sc-strip-5{grid-template-columns:repeat(2,1fr);}
}
@media(max-width:650px){
  .sc-grid-3{grid-template-columns:1fr;}
  .sc-strip-4,.sc-strip-5{grid-template-columns:1fr;}
}

/* ── Hero card (Row 1) ── */
.sc-hero {
  background:#fff;
  border:1px solid var(--border);
  border-radius:var(--r3);
  padding:20px 20px 16px;
  position:relative; overflow:hidden;
  transition:box-shadow .2s, transform .2s;
  cursor:default;
}
.sc-hero:hover { transform:translateY(-2px); box-shadow:0 8px 24px rgba(50,50,93,.1); }
/* sc-hero left accent is applied via inline style border-left */
.sc-hero-label {
  font-size:.6rem; font-weight:700; letter-spacing:1.2px;
  text-transform:uppercase; color:var(--t3);
  margin-bottom:8px; padding-left:10px;
}
.sc-hero-value {
  font-size:2rem; font-weight:800;
  font-family:var(--font-head);
  line-height:1.05; letter-spacing:-.5px;
  color:var(--t1); padding-left:10px;
  margin-bottom:4px;
}
.sc-hero-sub {
  font-size:.62rem; color:var(--t3);
  padding-left:10px; margin-bottom:10px;
}
.sc-badge {
  display:inline-flex; align-items:center; gap:4px;
  font-size:.6rem; font-weight:700;
  padding:3px 9px; border-radius:20px;
  margin-left:10px;
}
.sc-badge.up   { background:#ECFDF3; color:#059669; }
.sc-badge.down { background:#FFF1F3; color:#E11D48; }
.sc-badge.neu  { background:#F1F5F9; color:#94A3B8; }

/* ── Strip cell (Rows 2 & 3) ── */
.sc-cell {
  padding:14px 18px;
  border-right:1px solid var(--border);
  transition:background .15s;
  cursor:default;
  position:relative;
}
.sc-cell:last-child { border-right:none; }
.sc-cell:hover { background:#F8FAFC; }
.sc-cell-dot {
  width:7px; height:7px; border-radius:50%;
  background:#5E72E4;
  display:inline-block; margin-bottom:6px;
}
.sc-cell-label {
  font-size:.58rem; font-weight:700; letter-spacing:.9px;
  text-transform:uppercase; color:var(--t3);
  margin-bottom:4px; display:block;
}
.sc-cell-value {
  font-size:1.3rem; font-weight:800;
  font-family:var(--font-head); line-height:1;
  color:var(--t1); letter-spacing:-.3px;
  display:block; margin-bottom:3px;
}
.sc-cell-hint {
  font-size:.57rem; color:var(--t4);
  display:block; line-height:1.5;
}
.sc-badge-sm {
  display:inline-flex; align-items:center; gap:3px;
  font-size:.56rem; font-weight:700;
  padding:2px 7px; border-radius:20px; margin-top:4px;
}
.sc-badge-sm.up   { background:#ECFDF3; color:#059669; }
.sc-badge-sm.down { background:#FFF1F3; color:#E11D48; }
.sc-badge-sm.neu  { background:#F1F5F9; color:#94A3B8; }

/* ── Strip header label ── */
.sc-strip-hdr {
  grid-column:1/-1;
  padding:8px 18px 6px;
  font-size:.56rem; font-weight:700; letter-spacing:1.4px;
  text-transform:uppercase; color:var(--t4);
  border-bottom:1px solid var(--border);
  background:#FAFBFC;
}

/* keep old names as no-op so nothing breaks */
.kpi-row,.kpi-row-top,.kpi-row-bot,.kpi-row-mid,
.gkpi,.gkpi-sm,.gkpi-indigo,.gkpi-teal,.gkpi-emerald,
.gkpi-rose,.gkpi-amber,.gkpi-sky,.gkpi-violet { }
.gkpi-trend { display:none; }

/* ══ GLASS CARDS → white ══ */
.gcard {
  background:#fff; border:1px solid var(--border); border-radius:var(--r3);
  padding:24px 24px 20px; transition:box-shadow .2s; position:relative; overflow:hidden;
  margin-bottom:18px; box-shadow:var(--shadow-sm);
}
.gcard::before { display:none; }
.gcard:hover { box-shadow:var(--shadow-lg); }

/* ══ NORM BAR ══ */
.norm-bar {
  display:flex; align-items:center; gap:8px; flex-wrap:wrap;
  margin-bottom:24px; padding:11px 18px;
  background:#fff; border:1px solid var(--border);
  border-radius:var(--r2); box-shadow:var(--shadow-sm);
}
.norm-cap { font-size:.57rem; font-weight:700; color:var(--t3); text-transform:uppercase; letter-spacing:2px; margin-right:8px; }
.npill { font-size:.62rem; font-weight:600; padding:4px 13px; border-radius:20px; background:#F2F4F7; border:1px solid var(--border); color:var(--t3); transition:all .2s; }
.npill.on { background:#ECFDF3; border-color:#ABEFC6; color:#027A48; }

/* ══ TABLE ══ */
[data-testid="stDataFrame"] {
  border:1px solid var(--border)!important; border-radius:var(--r2)!important;
  overflow:hidden!important; background:#fff!important; box-shadow:var(--shadow-sm)!important;
}
[data-testid="stDataFrame"] th {
  background:#F6F9FC!important; font-size:.67rem!important; font-weight:700!important;
  color:var(--t3)!important; letter-spacing:.5px!important; text-transform:uppercase!important;
  border-bottom:1px solid var(--border)!important;
}
[data-testid="stDataFrame"] td { font-size:.72rem!important; color:var(--t2)!important; border-color:var(--border)!important; }
[data-testid="stDataFrame"] tr:hover td { background:#F6F9FC!important; }

/* ══ ALERT ══ */
[data-testid="stAlert"] {
  background:#FFF3CD!important; border:1px solid #FFE69C!important;
  border-left:4px solid #FB6340!important; border-radius:var(--r2)!important;
  font-size:.72rem!important; color:#664D03!important;
}

/* ══ MISC ══ */
hr { border-color:var(--border)!important; margin:24px 0!important; }
.stSpinner > div { border-top-color:var(--primary)!important; }
div[data-testid="stCaption"] p { color:var(--t3)!important; font-size:.67rem!important; }

/* ══ ANIMATIONS ══ */
@keyframes fadeSlideUp { from{opacity:0;transform:translateY(16px)} to{opacity:1;transform:translateY(0)} }
@keyframes fadeIn { from{opacity:0} to{opacity:1} }
@keyframes scaleIn { from{opacity:0;transform:scale(.97)} to{opacity:1;transform:scale(1)} }
.kpi-row  { animation:fadeSlideUp .45s ease .05s both; }
.gcard    { animation:fadeSlideUp .45s ease .1s both; }
.norm-bar { animation:fadeIn .35s ease both; }
.gsec     { animation:fadeIn .3s ease both; }
[data-testid="stTabs"] { animation:scaleIn .4s ease .06s both; }

[data-testid="stDivider"] { border-color:var(--border)!important; }

</style>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="ghdr">
  <div class="ghdr-brand">
    <div class="ghdr-logo">
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="rgba(255,255,255,.97)" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
        <path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/>
        <polyline points="9 22 9 12 15 12 15 22"/>
      </svg>
    </div>
    <div>
      <div class="ghdr-name">Hotel <span>Intelligence</span></div>
      <div class="ghdr-sub">MTT &nbsp;&bull;&nbsp; Opsifin Platform &nbsp;&bull;&nbsp; Travel Analytics</div>
    </div>
  </div>
  <div class="ghdr-right">
    <span class="ghdr-pill">v7.0</span>
    <div class="ghdr-live"><span class="ghdr-dot"></span>Live</div>
  </div>
</div>
<div class="gticker">
  <div class="gticker-track">
    <span class="t-item hi">Hotel Intelligence Platform</span><span class="tsep">&diams;</span>
    <span class="t-item">Invoice &amp; Supplier Analytics</span><span class="tsep">&diams;</span>
    <span class="t-item hi">Performance Agent Dashboard</span><span class="tsep">&diams;</span>
    <span class="t-item">Supplier Category Intelligence</span><span class="tsep">&diams;</span>
    <span class="t-item hi">MTT Travel Analytics</span><span class="tsep">&diams;</span>
    <span class="t-item">Google Drive Sync &nbsp;&middot;&nbsp; v7.0 &nbsp;&middot;&nbsp; 2025</span><span class="tsep">&diams;</span>
    <span class="t-item hi">Hotel Intelligence Platform</span><span class="tsep">&diams;</span>
    <span class="t-item">Invoice &amp; Supplier Analytics</span><span class="tsep">&diams;</span>
    <span class="t-item hi">Performance Agent Dashboard</span><span class="tsep">&diams;</span>
    <span class="t-item">Supplier Category Intelligence</span><span class="tsep">&diams;</span>
    <span class="t-item hi">MTT Travel Analytics</span><span class="tsep">&diams;</span>
    <span class="t-item">Google Drive Sync &nbsp;&middot;&nbsp; v7.0 &nbsp;&middot;&nbsp; 2025</span><span class="tsep">&diams;</span>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Build df_raw BEFORE sidebar ───────────────────────────────────────────────
_up = st.session_state.get("main_upload") or []
if _up:
    _h = compute_upload_hash(_up)
    _nm = st.session_state.get("norm_maps", {})
    if st.session_state.get("upload_hash") != _h or "df_raw" not in st.session_state:
        with st.spinner("Memproses & menormalisasi data..."):
            st.session_state["df_raw"]      = build_df_raw(_up, _nm)
            st.session_state["upload_hash"] = _h
else:
    for k in ["df_raw","upload_hash"]: st.session_state.pop(k, None)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div class="sb-top">
      <div class="sb-logo">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="rgba(255,255,255,.97)" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
          <path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/>
          <polyline points="9 22 9 12 15 12 15 22"/>
        </svg>
      </div>
      <div>
        <div class="sb-appname">Hotel <span>Report</span></div>
        <div class="sb-ver">Opsifin &middot; MTT &middot; v7.0</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # Upload
    st.markdown('<div class="sb-section">Data Utama</div>', unsafe_allow_html=True)
    st.file_uploader("Upload Custom Report (.xlsx)", type=["xlsx"],
                     accept_multiple_files=True, key="main_upload",
                     label_visibility="collapsed")

    # Norm status
    st.markdown('<div class="sb-divider"></div><div class="sb-section">Normalisasi · Google Drive</div>', unsafe_allow_html=True)
    _ss = st.session_state.get("sync_state", {})
    for k, lbl in GDRIVE_LABELS.items():
        s  = _ss.get(k,"wait")
        tc = {"ok":"stag stag-ok","err":"stag stag-err","wait":"stag stag-wait"}[s]
        tt = {"ok":"Synced","err":"Error","wait":"Pending"}[s]
        st.markdown(
            f'<div class="sync-row">'
            f'<span class="sync-label">{lbl}</span>'
            f'<span class="{tc}">{tt}</span>'
            f'</div>', unsafe_allow_html=True)

    st.markdown('<div style="height:12px"></div>', unsafe_allow_html=True)
    do_sync = st.button("🔄  Sync Data", use_container_width=True, key="btn_sync")

    if do_sync:
        nm2, ns2 = fetch_all_mappings_parallel()
        st.session_state["sync_state"] = ns2
        st.session_state["norm_maps"]  = nm2
        for k in ["df_raw","upload_hash"]: st.session_state.pop(k,None)
        all_ok = all(v == "ok" for v in ns2.values())
        if all_ok:
            st.toast("✅ Semua data normalisasi berhasil disinkronkan!", icon="✅")
        else:
            failed = [GDRIVE_LABELS[k] for k,v in ns2.items() if v != "ok"]
            st.toast(f"⚠️ Sync selesai — gagal: {', '.join(failed)}", icon="⚠️")
        st.rerun()

    # Filter
    st.markdown('<div class="sb-divider"></div><div class="sb-section">Filter Data</div>', unsafe_allow_html=True)
    if "df_raw" in st.session_state:
        _r = st.session_state["df_raw"]
        if "Issued_Year" in _r.columns:
            yr = sorted(_r["Issued_Year"].dropna().unique().tolist())
            st.multiselect("Tahun", yr, key="f_years")
        if "Inv Date" in _r.columns:
            _imin = _r["Inv Date"].min().date(); _imax = _r["Inv Date"].max().date()
            _id_raw = st.session_state.get("f_inv")
            # Guard against single-date tuple (user mid-selection) or wrong length
            if _id_raw and hasattr(_id_raw,"__len__") and len(_id_raw)==2:
                try:
                    _id = [max(_id_raw[0],_imin), min(_id_raw[1],_imax)]
                except Exception:
                    _id = [_imin, _imax]
            else:
                _id = [_imin, _imax]
            st.date_input("Periode Inv Date", value=_id, key="f_inv", min_value=_imin, max_value=_imax)
        if "Check In" in _r.columns and "Check Out" in _r.columns:
            _cmin = _r["Check In"].min().date(); _cmax = _r["Check Out"].max().date()
            _cd_raw = st.session_state.get("f_ci")
            # Guard against single-date tuple (user mid-selection) or wrong length
            if _cd_raw and hasattr(_cd_raw,"__len__") and len(_cd_raw)==2:
                try:
                    _cd = [max(_cd_raw[0],_cmin), min(_cd_raw[1],_cmax)]
                except Exception:
                    _cd = [_cmin, _cmax]
            else:
                _cd = [_cmin, _cmax]
            st.date_input("Check In – Check Out", value=_cd, key="f_ci", min_value=_cmin, max_value=_cmax)
    else:
        st.caption("Upload file untuk mengaktifkan filter.")

    st.markdown("""
    <div style="padding:20px 20px 18px; border-top:1px solid #E9ECEF; margin-top:16px;">
      <div style="font-size:.59rem; color:#ADB5BD; font-family:'Open Sans',sans-serif; line-height:2.2; letter-spacing:.3px;">
        Hotel Intelligence &nbsp;<span style="color:#5E72E4;font-weight:700;">v8.0</span> &middot; 2025<br>
        Rifyal Tumber &middot; MTT
      </div>
    </div>
    """, unsafe_allow_html=True)

# ── Main ──────────────────────────────────────────────────────────────────────
uploaded_files = st.session_state.get("main_upload") or []

if uploaded_files and "df_raw" in st.session_state:
    df_raw = st.session_state["df_raw"]

    # Apply filters
    df_view = df_raw.copy()
    sel_y = st.session_state.get("f_years",[])
    if sel_y and "Issued_Year" in df_view.columns:
        df_view = df_view[df_view["Issued_Year"].isin(sel_y)]
    sel_i = st.session_state.get("f_inv",[])
    if "Inv Date" in df_view.columns and isinstance(sel_i,(list,tuple)) and len(sel_i)==2:
        df_view = df_view[(df_view["Inv Date"]>=pd.to_datetime(sel_i[0]))&
                          (df_view["Inv Date"]<=pd.to_datetime(sel_i[1]))]
    sel_c = st.session_state.get("f_ci",[])
    if ("Check In" in df_view.columns and "Check Out" in df_view.columns and
            isinstance(sel_c,(list,tuple)) and len(sel_c)==2):
        df_view = df_view[(df_view["Check In"]>=pd.to_datetime(sel_c[0]))&
                          (df_view["Check Out"]<=pd.to_datetime(sel_c[1]))]

    # Norm bar
    ss2 = st.session_state.get("sync_state",{})
    pm  = {"Hotel Chain":ss2.get("hotel_chain")=="ok","Hotel City":ss2.get("hotel_city")=="ok",
           "Hotel Name":ss2.get("hotel_name")=="ok","Supplier":ss2.get("hotel_supplier")=="ok",
           "Supplier Cat":ss2.get("supplier_category")=="ok"}
    ph  = " ".join(f'<span class="npill {"on" if v else ""}">{k}</span>' for k,v in pm.items())
    st.markdown(f'<div class="norm-bar"><span class="norm-cap">Norm</span>{ph}</div>', unsafe_allow_html=True)

    # Tabs
    tab1,tab2,tab3,tab4,tab5,tab6,tab7 = st.tabs([
        "Summary","Tren Invoice","Supplier",
        "Product Type","Agent","PTM Corp","Kategori",
    ])

    # ═══════════════════════════════════════════════════════════════
    # TAB 1 — Summary
    # ═══════════════════════════════════════════════════════════════
    with tab1:
        tr = len(df_view); tc = len(df_view.columns)
        ui = df_view["Invoice No"].nunique()                    if "Invoice No"       in df_view.columns else "N/A"
        rn = int(np.ceil(df_view["Total Room Night"].sum()))    if "Total Room Night" in df_view.columns else "N/A"
        sa = df_view["Sales AR"].fillna(0).astype(float).sum() if "Sales AR"         in df_view.columns else "N/A"
        up = df_view["Full Name"].dropna().nunique()            if "Full Name"        in df_view.columns else "N/A"
        if "Profit" in df_view.columns and "Sales AR" in df_view.columns:
            _p=df_view["Profit"].fillna(0).astype(float); _s=df_view["Sales AR"].fillna(0).astype(float); _m=_s!=0
            pm_val = (_p[_m]/_s[_m]*100).mean() if _m.any() else 0
            pval = f"{pm_val:.2f}%"; pcls="c-emerald"
        else:
            pm_val=None; pval="N/A"; pcls=""

        # ── New metrics ─────────────────────────────────────────────
        # Avg Aging Invoice: mean days (Check In - Inv Date) per row
        aging_val = "N/A"; aging_hint = "hari rata-rata"
        if "Check In" in df_view.columns and "Inv Date" in df_view.columns:
            _ag = df_view.dropna(subset=["Check In","Inv Date"]).copy()
            _ag["_aging"] = (_ag["Check In"] - _ag["Inv Date"]).dt.days
            _ag_pos = _ag[_ag["_aging"] >= 0]   # keep only valid/positive aging
            if not _ag_pos.empty:
                aging_raw = _ag_pos["_aging"].mean()
                aging_val = f"{aging_raw:.1f}"
                aging_hint = "hari rata-rata (Check In − Inv Date)"

        tot_supplier = df_view["Supplier_Name"].dropna().nunique()                        if "Supplier_Name" in df_view.columns else "N/A"
        tot_hotel    = df_view["Hotel_Name"].dropna().nunique()                        if "Hotel_Name"    in df_view.columns else "N/A"
        tot_city     = df_view["Hotel_City"].dropna().nunique()                        if "Hotel_City"    in df_view.columns else "N/A"
        _pic_col     = next((c for c in df_view.columns
                             if "agent" in c.lower() or "handler" in c.lower()), None)
        tot_pic      = df_view[_pic_col].dropna().nunique() if _pic_col else "N/A"

        # ── Previous-period comparison ──────────────────────────────
        prev = get_prev_period_metrics(df_raw, df_view)
        tb_ui = trend_badge(ui,   prev.get("ui"))
        tb_sa = trend_badge(sa,   prev.get("sa"))
        tb_pm = trend_badge(pm_val, prev.get("pm")) if pm_val is not None else '<span class="gkpi-trend neu">── N/A</span>'
        tb_rn = trend_badge(rn,   prev.get("rn"))
        tb_up = trend_badge(up,   prev.get("up"))

        # ── inline badge helper (uses new sc-badge classes) ──────────
        def _sbadge(curr, prev_val):
            try:
                if prev_val is None or prev_val == 0:
                    return '<span class="sc-badge neu">— —</span>'
                c = float(str(curr).replace(",","").replace("%","")) if isinstance(curr,str) else float(curr)
                p = float(prev_val)
                if p == 0: return '<span class="sc-badge neu">— —</span>'
                pct = (c - p) / abs(p) * 100
                cls = "up" if pct > 0 else "down"
                arr = "▲" if pct > 0 else "▼"
                return f'<span class="sc-badge {cls}">{arr} {abs(pct):.1f}%</span>'
            except:
                return '<span class="sc-badge neu">— —</span>'

        def _smbadge(curr, prev_val):
            try:
                if prev_val is None or prev_val == 0:
                    return '<span class="sc-badge-sm neu">— —</span>'
                c = float(str(curr).replace(",","").replace("%","")) if isinstance(curr,str) else float(curr)
                p = float(prev_val)
                if p == 0: return '<span class="sc-badge-sm neu">— —</span>'
                pct = (c - p) / abs(p) * 100
                cls = "up" if pct > 0 else "down"
                arr = "▲" if pct > 0 else "▼"
                return f'<span class="sc-badge-sm {cls}">{arr} {abs(pct):.1f}%</span>'
            except:
                return '<span class="sc-badge-sm neu">— —</span>'

        b_ui = _sbadge(ui,     prev.get("ui"))
        b_sa = _sbadge(sa,     prev.get("sa"))
        b_pm = _sbadge(pm_val, prev.get("pm")) if pm_val is not None else '<span class="sc-badge neu">— —</span>'
        b_rn = _smbadge(rn,    prev.get("rn"))
        b_up = _smbadge(up,    prev.get("up"))

        sa_fmt  = ("IDR " + fmt(sa))  if sa != "N/A" else "N/A"

        # ── ROW 1: 3 Hero Cards ─────────────────────────────────────
        st.markdown(
            f'<div class="sc-grid-3">' +
            f'<div class="sc-hero" style="border-left:3px solid #5E72E4;">' +
              f'<div class="sc-hero-label">Invoice Unik</div>' +
              f'<div class="sc-hero-value">{fmt(ui)}</div>' +
              f'<div class="sc-hero-sub">Total transaksi invoice unik</div>' +
              f'{b_ui}</div>' +
            f'<div class="sc-hero" style="border-left:3px solid #2DCE89;">' +
              f'<div class="sc-hero-label">Sales AR</div>' +
              f'<div class="sc-hero-value">{fmt(sa)}</div>' +
              f'<div class="sc-hero-sub">Total nilai penjualan (IDR)</div>' +
              f'{b_sa}</div>' +
            f'<div class="sc-hero" style="border-left:3px solid #8965E0;">' +
              f'<div class="sc-hero-label">Avg Profit Margin</div>' +
              f'<div class="sc-hero-value">{pval}</div>' +
              f'<div class="sc-hero-sub">Rata-rata margin keuntungan</div>' +
              f'{b_pm}</div>' +
            f'</div>',
            unsafe_allow_html=True)

        # ── ROW 2: Volume Strip (4 cells) ────────────────────────────
        st.markdown(
            f'<div class="sc-strip sc-strip-4">' +
            f'<div class="sc-cell">' +
              f'<span class="sc-cell-dot" style="background:#5E72E4;"></span>' +
              f'<span class="sc-cell-label">Room Night</span>' +
              f'<span class="sc-cell-value">{fmt(rn)}</span>' +
              f'<span class="sc-cell-hint">Total malam kamar</span>' +
              f'{b_rn}</div>' +
            f'<div class="sc-cell">' +
              f'<span class="sc-cell-dot" style="background:#2DCE89;"></span>' +
              f'<span class="sc-cell-label">Pax Unik</span>' +
              f'<span class="sc-cell-value">{fmt(up)}</span>' +
              f'<span class="sc-cell-hint">Nama tamu unik</span>' +
              f'{b_up}</div>' +
            f'<div class="sc-cell">' +
              f'<span class="sc-cell-dot" style="background:#FB6340;"></span>' +
              f'<span class="sc-cell-label">Avg Aging Invoice</span>' +
              f'<span class="sc-cell-value">{aging_val} <span style="font-size:.72rem;font-weight:500;color:#ADB5BD;">hari</span></span>' +
              f'<span class="sc-cell-hint">Check In &minus; Inv Date</span>' +
              f'</div>' +
            f'<div class="sc-cell">' +
              f'<span class="sc-cell-dot" style="background:#8965E0;"></span>' +
              f'<span class="sc-cell-label">Total Baris</span>' +
              f'<span class="sc-cell-value">{fmt(tr)}</span>' +
              f'<span class="sc-cell-hint">Baris data aktif</span>' +
              f'</div>' +
            f'</div>',
            unsafe_allow_html=True)

        # ── ROW 3: Master Data Strip (5 cells) ───────────────────────
        st.markdown(
            f'<div class="sc-strip sc-strip-5">' +
            f'<div class="sc-cell">' +
              f'<span class="sc-cell-dot" style="background:#11CDEF;"></span>' +
              f'<span class="sc-cell-label">Total Supplier</span>' +
              f'<span class="sc-cell-value">{fmt(tot_supplier)}</span>' +
              f'<span class="sc-cell-hint">Supplier unik</span>' +
              f'</div>' +
            f'<div class="sc-cell">' +
              f'<span class="sc-cell-dot" style="background:#5E72E4;"></span>' +
              f'<span class="sc-cell-label">Total Hotel</span>' +
              f'<span class="sc-cell-value">{fmt(tot_hotel)}</span>' +
              f'<span class="sc-cell-hint">Hotel unik</span>' +
              f'</div>' +
            f'<div class="sc-cell">' +
              f'<span class="sc-cell-dot" style="background:#2DCE89;"></span>' +
              f'<span class="sc-cell-label">Total City</span>' +
              f'<span class="sc-cell-value">{fmt(tot_city)}</span>' +
              f'<span class="sc-cell-hint">Kota hotel unik</span>' +
              f'</div>' +
            f'<div class="sc-cell">' +
              f'<span class="sc-cell-dot" style="background:#F5365C;"></span>' +
              f'<span class="sc-cell-label">Total PIC</span>' +
              f'<span class="sc-cell-value">{fmt(tot_pic)}</span>' +
              f'<span class="sc-cell-hint">Agent / Handler unik</span>' +
              f'</div>' +
            f'<div class="sc-cell">' +
              f'<span class="sc-cell-dot" style="background:#ADB5BD;"></span>' +
              f'<span class="sc-cell-label">Kolom Aktif</span>' +
              f'<span class="sc-cell-value">{fmt(tc)}</span>' +
              f'<span class="sc-cell-hint">Field tersedia</span>' +
              f'</div>' +
            f'</div>',
            unsafe_allow_html=True)

        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

        # ── Tren Invoice bulanan (Invoice Unik & Room Night) ─────────
        if "Issued Date" in df_view.columns and "Invoice No" in df_view.columns:
            _dt = (df_view.dropna(subset=["Issued Date","Invoice No"])
                          .copy())
            _dt["_mon_label"] = _dt["Issued Date"].dt.strftime("%b %Y")
            _dt["_mon_num"]   = _dt["Issued Date"].dt.to_period("M").apply(lambda p: p.ordinal)

            _ti = (_dt.groupby(["_mon_label","_mon_num"], as_index=False)["Invoice No"]
                      .nunique()
                      .rename(columns={"Invoice No":"Invoice Unik"})
                      .sort_values("_mon_num"))
            _ti["Invoice Unik"] = pd.to_numeric(_ti["Invoice Unik"], errors="coerce").fillna(0).astype(int)

            has_rn = "Total Room Night" in df_view.columns
            if has_rn:
                _tr_s = (_dt.groupby(["_mon_label","_mon_num"], as_index=False)["Total Room Night"]
                            .sum()
                            .sort_values("_mon_num"))
                _tr_s["Total Room Night"] = pd.to_numeric(_tr_s["Total Room Night"], errors="coerce").fillna(0)

            st.markdown('<div style="height:4px"></div>', unsafe_allow_html=True)
            ct1, ct2 = st.columns(2)

            # ── Trend Col 1 : Invoice Unik ──────────────────────────
            with ct1:
                gsec("Tren Invoice Bulanan", "📈")
                _fig_ti = go.Figure()
                # Only label points that are >= 5% of the max value to avoid clutter/undefined
                _ti_max = _ti["Invoice Unik"].max()
                _ti_thresh = max(1, _ti_max * 0.05)
                _ti_labels = _ti["Invoice Unik"].apply(
                    lambda v: f"{int(v):,}" if v >= _ti_thresh else "")
                _fig_ti.add_trace(go.Scatter(
                    x=_ti["_mon_label"], y=_ti["Invoice Unik"],
                    mode="lines+markers+text",
                    name="Invoice Unik",
                    text=_ti_labels,
                    textposition="top center",
                    textfont=dict(size=10, color="#5E72E4", family="Open Sans"),
                    line=dict(color="#5E72E4", width=2.5, shape="spline"),
                    marker=dict(size=8, color="#818CF8",
                                line=dict(width=2, color="rgba(94,114,228,.3)")),
                    fill="tozeroy",
                    fillcolor="rgba(94,114,228,.08)",
                    hovertemplate="<b>%{x}</b><br>Invoice Unik: <b>%{y:,.0f}</b><extra></extra>",
                    cliponaxis=False,
                ))
                _max_ti = _ti.loc[_ti["Invoice Unik"].idxmax()]
                _fig_ti.add_annotation(
                    x=_max_ti["_mon_label"], y=_max_ti["Invoice Unik"],
                    text=f"▲ Peak: {int(_max_ti['Invoice Unik']):,}",
                    showarrow=True, arrowhead=2, arrowcolor="#5E72E4",
                    arrowsize=.8, ax=0, ay=-30,
                    font=dict(size=10, color="#5E72E4", family="Open Sans"),
                    bgcolor="rgba(238,242,255,.9)", bordercolor="#5E72E4",
                    borderwidth=1, borderpad=4)
                _fig_ti.update_layout(
                    hovermode="x unified", height=300,
                    xaxis=dict(tickangle=-30, showline=False),
                    yaxis_title="Invoice Unik", xaxis_title="",
                    showlegend=False,
                    margin=dict(l=8, r=8, t=40, b=8))
                st.plotly_chart(theme(_fig_ti), use_container_width=True)

            # ── Trend Col 2 : Total Room Night ─────────────────────
            with ct2:
                gsec("Tren Room Night Bulanan", "🌙")
                if has_rn:
                    _fig_rn = go.Figure()
                    _rn_max = _tr_s["Total Room Night"].max()
                    _rn_thresh = max(1, _rn_max * 0.05)
                    _rn_labels = _tr_s["Total Room Night"].apply(
                        lambda v: f"{int(v):,}" if v >= _rn_thresh else "")
                    _fig_rn.add_trace(go.Scatter(
                        x=_tr_s["_mon_label"], y=_tr_s["Total Room Night"],
                        mode="lines+markers+text",
                        name="Room Night",
                        text=_rn_labels,
                        textposition="top center",
                        textfont=dict(size=10, color="#2DCE89", family="Open Sans"),
                        line=dict(color="#2DCE89", width=2.5, shape="spline"),
                        marker=dict(size=8, color="#34D399",
                                    line=dict(width=2, color="rgba(45,206,137,.3)")),
                        fill="tozeroy",
                        fillcolor="rgba(45,206,137,.08)",
                        hovertemplate="<b>%{x}</b><br>Room Night: <b>%{y:,.0f}</b><extra></extra>",
                        cliponaxis=False,
                    ))
                    _max_rn = _tr_s.loc[_tr_s["Total Room Night"].idxmax()]
                    _fig_rn.add_annotation(
                        x=_max_rn["_mon_label"], y=_max_rn["Total Room Night"],
                        text=f"▲ Peak: {int(_max_rn['Total Room Night']):,}",
                        showarrow=True, arrowhead=2, arrowcolor="#2DCE89",
                        arrowsize=.8, ax=0, ay=-30,
                        font=dict(size=10, color="#2DCE89", family="Open Sans"),
                        bgcolor="rgba(230,251,244,.9)", bordercolor="#2DCE89",
                        borderwidth=1, borderpad=4)
                    _fig_rn.update_layout(
                        hovermode="x unified", height=300,
                        xaxis=dict(tickangle=-30, showline=False),
                        yaxis_title="Room Night", xaxis_title="",
                        showlegend=False,
                        margin=dict(l=8, r=8, t=40, b=8))
                    st.plotly_chart(theme(_fig_rn), use_container_width=True)
                else:
                    st.info("Kolom Total Room Night tidak tersedia.")

        st.markdown('<div style="height:12px"></div>', unsafe_allow_html=True)

        # ── Tren Profit & Tren Kota Unik Bulanan ─────────────────────
        ct3, ct4 = st.columns(2)

        # ── Trend Col 3 : Profit per Bulan ─────────────────────────
        with ct3:
            gsec("Tren Profit Bulanan", "💹")
            _has_profit = "Profit" in df_view.columns and "Issued Date" in df_view.columns
            if _has_profit and "Issued Date" in df_view.columns:
                _dt_pr = (df_view.dropna(subset=["Issued Date"])
                                 .assign(Profit=lambda d: pd.to_numeric(d["Profit"], errors="coerce").fillna(0))
                                 .copy())
                _dt_pr["_mon_label"] = _dt_pr["Issued Date"].dt.strftime("%b %Y")
                _dt_pr["_mon_num"]   = _dt_pr["Issued Date"].dt.to_period("M").apply(lambda p: p.ordinal)
                _pr_s = (_dt_pr.groupby(["_mon_label","_mon_num"], as_index=False)["Profit"]
                               .sum()
                               .sort_values("_mon_num"))
                _pr_s["Profit"] = pd.to_numeric(_pr_s["Profit"], errors="coerce").fillna(0)

                def _compact(v):
                    a = abs(v)
                    if a >= 1_000_000_000: return f"{v/1_000_000_000:.1f}B"
                    if a >= 1_000_000:     return f"{v/1_000_000:.1f}M"
                    if a >= 1_000:         return f"{v/1_000:.1f}K"
                    return f"{int(v):,}"

                _pr_max    = _pr_s["Profit"].abs().max()
                _pr_thresh = max(1, _pr_max * 0.05)
                _pr_labels = _pr_s["Profit"].apply(
                    lambda v: _compact(v) if abs(v) >= _pr_thresh else "")

                _fig_pr = go.Figure()
                _fig_pr.add_trace(go.Scatter(
                    x=_pr_s["_mon_label"], y=_pr_s["Profit"],
                    mode="lines+markers+text",
                    name="Profit",
                    text=_pr_labels,
                    textposition="top center",
                    textfont=dict(size=10, color="#FB6340", family="Open Sans"),
                    line=dict(color="#FB6340", width=2.5, shape="spline"),
                    marker=dict(size=8, color="#FD8D72",
                                line=dict(width=2, color="rgba(251,99,64,.3)")),
                    fill="tozeroy",
                    fillcolor="rgba(251,99,64,.07)",
                    hovertemplate="<b>%{x}</b><br>Profit: <b>%{y:,.0f}</b><extra></extra>",
                    cliponaxis=False,
                ))
                if not _pr_s.empty and _pr_s["Profit"].max() > 0:
                    _max_pr = _pr_s.loc[_pr_s["Profit"].idxmax()]
                    _fig_pr.add_annotation(
                        x=_max_pr["_mon_label"], y=_max_pr["Profit"],
                        text=f"▲ Peak: {int(_max_pr['Profit']):,}",
                        showarrow=True, arrowhead=2, arrowcolor="#FB6340",
                        arrowsize=.8, ax=0, ay=-30,
                        font=dict(size=10, color="#FB6340", family="Open Sans"),
                        bgcolor="rgba(255,244,238,.95)", bordercolor="#FB6340",
                        borderwidth=1, borderpad=4)
                _fig_pr.update_layout(
                    hovermode="x unified", height=300,
                    xaxis=dict(tickangle=-30, showline=False),
                    yaxis_title="Profit (IDR)", xaxis_title="",
                    showlegend=False,
                    margin=dict(l=8, r=8, t=40, b=8))
                st.plotly_chart(theme(_fig_pr), use_container_width=True)
            else:
                st.info("Kolom Profit tidak tersedia dalam data.")

        # ── Trend Col 4 : Kota Unik per Bulan ──────────────────────
        with ct4:
            gsec("Tren Kota Unik Bulanan", "🗺️")
            _has_city = "Hotel_City" in df_view.columns and "Issued Date" in df_view.columns
            if _has_city:
                _dt_cy = (df_view.dropna(subset=["Issued Date","Hotel_City"])
                                 .assign(Hotel_City=lambda d: d["Hotel_City"].astype(str).str.strip())
                                 .pipe(lambda d: d[~d["Hotel_City"].isin(["","nan","None","NaN"])])
                                 .copy())
                _dt_cy["_mon_label"] = _dt_cy["Issued Date"].dt.strftime("%b %Y")
                _dt_cy["_mon_num"]   = _dt_cy["Issued Date"].dt.to_period("M").apply(lambda p: p.ordinal)
                _cy_s = (_dt_cy.groupby(["_mon_label","_mon_num"], as_index=False)["Hotel_City"]
                               .nunique()
                               .rename(columns={"Hotel_City":"Kota Unik"})
                               .sort_values("_mon_num"))
                _cy_s["Kota Unik"] = pd.to_numeric(_cy_s["Kota Unik"], errors="coerce").fillna(0).astype(int)

                _cy_max    = _cy_s["Kota Unik"].max()
                _cy_thresh = max(1, _cy_max * 0.05)
                _cy_labels = _cy_s["Kota Unik"].apply(
                    lambda v: f"{int(v):,}" if v >= _cy_thresh else "")

                _fig_cy = go.Figure()
                _fig_cy.add_trace(go.Scatter(
                    x=_cy_s["_mon_label"], y=_cy_s["Kota Unik"],
                    mode="lines+markers+text",
                    name="Kota Unik",
                    text=_cy_labels,
                    textposition="top center",
                    textfont=dict(size=10, color="#8965E0", family="Open Sans"),
                    line=dict(color="#8965E0", width=2.5, shape="spline"),
                    marker=dict(size=8, color="#A78BFA",
                                line=dict(width=2, color="rgba(137,101,224,.3)")),
                    fill="tozeroy",
                    fillcolor="rgba(137,101,224,.07)",
                    hovertemplate="<b>%{x}</b><br>Kota Unik: <b>%{y:,.0f}</b><extra></extra>",
                    cliponaxis=False,
                ))
                if not _cy_s.empty:
                    _max_cy = _cy_s.loc[_cy_s["Kota Unik"].idxmax()]
                    _fig_cy.add_annotation(
                        x=_max_cy["_mon_label"], y=_max_cy["Kota Unik"],
                        text=f"▲ Peak: {int(_max_cy['Kota Unik']):,}",
                        showarrow=True, arrowhead=2, arrowcolor="#8965E0",
                        arrowsize=.8, ax=0, ay=-30,
                        font=dict(size=10, color="#8965E0", family="Open Sans"),
                        bgcolor="rgba(243,238,255,.95)", bordercolor="#8965E0",
                        borderwidth=1, borderpad=4)
                _fig_cy.update_layout(
                    hovermode="x unified", height=300,
                    xaxis=dict(tickangle=-30, showline=False),
                    yaxis_title="Jumlah Kota Unik", xaxis_title="",
                    showlegend=False,
                    margin=dict(l=8, r=8, t=40, b=8))
                st.plotly_chart(theme(_fig_cy), use_container_width=True)
            else:
                st.info("Kolom Hotel_City tidak tersedia dalam data.")

        st.markdown('<div style="height:12px"></div>', unsafe_allow_html=True)

        # ── Charts: Top 10 Invoice To + Domestic vs International ───
        st.markdown('<div style="height:4px"></div>', unsafe_allow_html=True)
        ch1, ch2 = st.columns(2)

        # ── Col 1 : Top 10 Invoice To ─────────────────────────────
        with ch1:
            gsec("Top 10 Invoice To", "🏢")
            # Prefer normalized column; fallback to raw detection
            if "Normalized_Inv_To" in df_view.columns:
                inv_to_col = "Normalized_Inv_To"
            else:
                inv_to_col = next((c for c in df_view.columns
                                   if any(k in c.lower() for k in
                                          ["invoice to","invoiceto","bill to","billto","sold to","client"])), None)
            if inv_to_col and "Invoice No" in df_view.columns:
                _df_inv = (df_view[[inv_to_col, "Invoice No"]]
                           .dropna(subset=[inv_to_col])
                           .assign(**{inv_to_col: lambda d: d[inv_to_col].astype(str).str.strip()})
                           .pipe(lambda d: d[~d[inv_to_col].isin(["","nan","None","NaN"])]))
                _total_inv = _df_inv["Invoice No"].nunique()
                top10_inv = (_df_inv.groupby(inv_to_col, dropna=True)["Invoice No"]
                                    .nunique().reset_index()
                                    .rename(columns={"Invoice No":"Invoice Unik"})
                                    .sort_values("Invoice Unik", ascending=False).head(10))
                top10_inv["Pct"] = (top10_inv["Invoice Unik"] / _total_inv * 100).round(1)
                top10_inv["Label"] = top10_inv.apply(
                    lambda r: f'{int(r["Invoice Unik"]):,}  ({r["Pct"]:.1f}%)', axis=1)
                fig_top = px.bar(top10_inv, x="Invoice Unik", y=inv_to_col,
                                 orientation="h", text="Label",
                                 color="Invoice Unik",
                                 color_continuous_scale=INDIGO_SCALE,
                                 custom_data=["Pct"])
                fig_top.update_traces(
                    textposition="outside",
                    textfont=dict(size=10, color="#525F7F", family="Open Sans"),
                    marker_line_width=0, marker_cornerradius=4,
                    cliponaxis=False,
                    hovertemplate=(
                        "<b>%{y}</b><br>"
                        "Invoice Unik : <b>%{x:,.0f}</b><br>"
                        "Porsi : <b>%{customdata[0]:.1f}%</b>"
                        "<extra></extra>"
                    ))
                fig_top.update_layout(
                    yaxis=dict(categoryorder="total ascending", automargin=True),
                    coloraxis_showscale=False, height=420,
                    xaxis_title="Invoice Unik", yaxis_title="",
                    margin=dict(l=8, r=120, t=30, b=8))
                st.plotly_chart(theme(fig_top), use_container_width=True)
                col_lbl = "Normalized Invoice To" if inv_to_col == "Normalized_Inv_To" else inv_to_col
                st.caption(f"*Berdasarkan kolom: {col_lbl} · % dihitung dari total {_total_inv:,} invoice unik")
            else:
                st.info("Kolom 'Invoice To' tidak ditemukan dalam data.\n"
                        "Pastikan nama kolom mengandung kata: invoice to, bill to, atau client.")

        # ── Col 2 : Domestic vs International ────────────────────
        with ch2:
            gsec("Domestic vs International", "🌏")
            dom_col = next((c for c in df_view.columns
                            if any(k in c.lower() for k in
                                   ["domestic","international","destination","dom/int","domint",
                                    "dom int","tipe","type hotel","lokasi"])), None)
            if dom_col and "Invoice No" in df_view.columns:
                _df_dom_raw = (df_view[[dom_col,"Invoice No"]]
                               .dropna(subset=[dom_col])
                               .assign(**{dom_col: lambda d: d[dom_col].astype(str).str.strip()})
                               .pipe(lambda d: d[~d[dom_col].isin(["","nan","None","NaN"])]))
                dom_grp = (_df_dom_raw.groupby(dom_col, dropna=True)["Invoice No"]
                                  .nunique().reset_index()
                                  .rename(columns={"Invoice No":"Invoice Unik"}))
                if len(dom_grp) > 4:
                    top_dom = dom_grp.nlargest(3, "Invoice Unik")
                    oth_dom = pd.DataFrame([{dom_col:"Others",
                                             "Invoice Unik": dom_grp.iloc[3:]["Invoice Unik"].sum()}])
                    dom_grp = pd.concat([top_dom, oth_dom], ignore_index=True)
                fig_dom = px.pie(dom_grp, names=dom_col, values="Invoice Unik",
                                 hole=0.52,
                                 color_discrete_sequence=["#5E72E4","#2DCE89","#F5365C","#FB6340"])
                fig_dom.update_traces(
                    textinfo="percent+label",
                    textfont=dict(size=12),
                    pull=[0.06]+[0]*(len(dom_grp)-1),
                    marker=dict(line=dict(color="rgba(255,255,255,.6)", width=2)),
                    hovertemplate="<b>%{label}</b><br>Invoice Unik: %{value:,.0f}<br>%{percent}<extra></extra>",
                )
                fig_dom.update_layout(height=420,
                    legend=dict(orientation="h", yanchor="bottom", y=-0.15, xanchor="center", x=0.5))
                st.plotly_chart(theme(fig_dom), use_container_width=True)
            elif "Product Type" in df_view.columns and "Invoice No" in df_view.columns:
                df_dom = df_view.copy()
                df_dom["Dom_Int"] = df_dom["Product Type"].astype(str).apply(
                    lambda x: "International" if any(k in x.upper() for k in ["INTER","LUAR","ABROAD","OVERSEA"])
                              else "Domestic")
                dom_grp2 = (df_dom.groupby("Dom_Int")["Invoice No"]
                                  .nunique().reset_index()
                                  .rename(columns={"Invoice No":"Invoice Unik"}))
                fig_dom2 = px.pie(dom_grp2, names="Dom_Int", values="Invoice Unik",
                                  hole=0.52,
                                  color_discrete_sequence=["#5E72E4","#2DCE89"])
                fig_dom2.update_traces(
                    textinfo="percent+label", textfont=dict(size=12),
                    pull=[0.06, 0],
                    marker=dict(line=dict(color="rgba(255,255,255,.6)", width=2)),
                    hovertemplate="<b>%{label}</b><br>Invoice Unik: %{value:,.0f}<br>%{percent}<extra></extra>",
                )
                fig_dom2.update_layout(height=420,
                    legend=dict(orientation="h", yanchor="bottom", y=-0.15, xanchor="center", x=0.5))
                st.plotly_chart(theme(fig_dom2), use_container_width=True)
                st.caption("*Diklasifikasikan dari kolom Product Type")
            else:
                st.info("Kolom Domestic/International tidak ditemukan.\n"
                        "Tambahkan kolom: Destination, Dom/Int, atau Domestic/International.")

        st.markdown('<div style="height:12px"></div>', unsafe_allow_html=True)

        gsec("Preview Data", "&#9776;")
        rpp=50; tp=max(1,(tr//rpp)+int(tr%rpp>0))
        if "pg" not in st.session_state: st.session_state.pg=0
        if st.session_state.pg>=tp: st.session_state.pg=0

        pc,pm2,pn = st.columns([1,5,1])
        with pc:
            if st.button("Prev", key="btn_prev") and st.session_state.pg>0:
                st.session_state.pg-=1; st.rerun()
        with pn:
            if st.button("Next", key="btn_next") and st.session_state.pg<tp-1:
                st.session_state.pg+=1; st.rerun()
        with pm2:
            st.markdown(
                f'<p style="text-align:center;font-size:.68rem;color:#64748B;padding:9px 0;margin:0;">'
                f'Hal&nbsp;{st.session_state.pg+1}&nbsp;/&nbsp;{tp} &nbsp;&middot;&nbsp; {tr:,} baris</p>',
                unsafe_allow_html=True)
        s,e = st.session_state.pg*rpp,(st.session_state.pg+1)*rpp
        st.dataframe(df_view.iloc[s:e], use_container_width=True)
        st.markdown('<div style="height:10px"></div>', unsafe_allow_html=True)
        dc,ec = st.columns(2)
        with dc:
            st.download_button("⬇ Download CSV", df_view.to_csv(index=False).encode("utf-8"),
                               "hotel_report.csv","text/csv",use_container_width=True)
        with ec:
            ob=io.BytesIO()
            with pd.ExcelWriter(ob,engine="xlsxwriter") as w: df_view.to_excel(w,index=False,sheet_name="Report")
            st.download_button("⬇ Download Excel", ob.getvalue(),"hotel_report.xlsx",
                               "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               use_container_width=True)

    # ═══════════════════════════════════════════════════════════════
    # TAB 2 — Tren Invoice
    # ═══════════════════════════════════════════════════════════════
    with tab2:
        if "Issued Date" in df_view.columns and "Invoice No" in df_view.columns:
            dt  = df_view.dropna(subset=["Issued Date","Invoice No"]).copy()
            dt  = dt[dt["Invoice No"].astype(str).str.strip().isin(["","nan","None","NaN"]) == False]
            dt["Mon"]  = dt["Issued Date"].dt.strftime("%b")
            dt["MonN"] = dt["Issued Date"].dt.month
            ti  = dt.groupby(["Mon","MonN"])["Invoice No"].nunique().reset_index()
            ti.columns = ["Bulan","MonN","Invoice"]; ti = ti.sort_values("MonN")
            # tr2 pakai kolom "Mon" agar konsisten dengan ti sebelum rename
            tr2 = dt.groupby("Mon")["Total Room Night"].sum().reset_index() if "Total Room Night" in dt.columns else None

            ca,cb = st.columns([3,2])
            with ca:
                gsec("Tren Invoice Bulanan", "📈")
                fig = go.Figure()
                # area fill — pakai fillcolor biasa, bukan fillgradient (tidak didukung Plotly)
                fig.add_trace(go.Scatter(
                    x=ti["Bulan"], y=ti["Invoice"],
                    mode="lines+markers", name="Invoice",
                    line=dict(color="#5E72E4",width=2.5,shape="spline"),
                    marker=dict(size=9,color="#818CF8",
                                line=dict(width=2.5,color="rgba(99,102,241,.25)"),
                                symbol="circle"),
                    fill="tozeroy",
                    fillcolor="rgba(94,114,228,.1)",
                    hovertemplate="<b>%{x}</b><br>Invoice: <b>%{y:,.0f}</b><extra></extra>",
                ))
                fig.update_layout(xaxis_title="", yaxis_title="Invoice Unik",
                                  hovermode="x unified", height=320,
                                  xaxis=dict(showline=False))
                st.plotly_chart(theme(fig), use_container_width=True)

            with cb:
                gsec("Ringkasan Bulanan", "📋")
                if tr2 is not None:
                    # tr2 kolom: ["Mon","Total Room Night"] — rename dulu agar bisa merge dengan ti["Bulan"]
                    tr2 = tr2.rename(columns={"Mon":"Bulan","Total Room Night":"Room Night"})
                    merged = ti[["Bulan","MonN","Invoice"]].merge(tr2, on="Bulan", how="left").drop("MonN",axis=1)
                    merged.columns = ["Bulan","Invoice Unik","Room Night"]
                else:
                    merged = ti[["Bulan","Invoice"]].rename(columns={"Invoice":"Invoice Unik"})
                st.dataframe(
                    merged.style
                          .format({c:"{:,.0f}" for c in merged.columns if merged[c].dtype!="O"})
                          .background_gradient(subset=["Invoice Unik"],cmap="Purples"),
                    use_container_width=True, height=320)

            st.markdown('<div style="height:4px"></div>', unsafe_allow_html=True)
            gsec("Volume Invoice per Bulan", "📊")
            ti["Invoice"] = pd.to_numeric(ti["Invoice"], errors="coerce").fillna(0)
            fig2 = px.bar(ti, x="Bulan", y="Invoice", text="Invoice",
                          color="Invoice",
                          color_continuous_scale=["rgba(99,102,241,.3)","#818CF8","#6366F1"])
            fig2.update_traces(
                texttemplate="%{y:,.0f}", textposition="outside",
                textfont=dict(size=11, color="#8898AA"),
                marker_line_width=0, marker_cornerradius=4,
                cliponaxis=False)
            fig2.update_layout(coloraxis_showscale=False, height=290,
                               xaxis_title="", yaxis_title="")
            st.plotly_chart(theme(fig2), use_container_width=True)

        else:
            st.warning("Kolom Issued Date atau Invoice No tidak ditemukan.")

    # ═══════════════════════════════════════════════════════════════
    # TAB 3 — Supplier
    # ═══════════════════════════════════════════════════════════════
    with tab3:
        if "Supplier_Name" in df_view.columns and "Total Room Night" in df_view.columns:
            _df_s3 = (df_view[["Supplier_Name","Total Room Night"]]
                      .dropna(subset=["Supplier_Name"])
                      .assign(Supplier_Name=lambda d: d["Supplier_Name"].astype(str).str.strip())
                      .pipe(lambda d: d[~d["Supplier_Name"].isin(["","nan","None","NaN"])]))
            ss3 = (_df_s3.groupby("Supplier_Name", dropna=True)["Total Room Night"]
                         .sum().reset_index().sort_values("Total Room Night",ascending=False))
            d3 = pd.concat([ss3.head(5),
                            pd.DataFrame([{"Supplier_Name":"Others",
                                           "Total Room Night":ss3.iloc[5:]["Total Room Night"].sum()}])
                            if len(ss3)>5 else pd.DataFrame()], ignore_index=True)
            ca,cb = st.columns([3,2])
            with ca:
                gsec("Distribusi Supplier", "🏢")
                fig3 = px.pie(d3,names="Supplier_Name",values="Total Room Night",
                              hole=0.52, color_discrete_sequence=GLASS_PALETTE)
                fig3.update_traces(
                    textinfo="percent+label",
                    textfont=dict(size=12,family="Space Grotesk"),
                    pull=[0.05]+[0]*(len(d3)-1),
                    marker=dict(line=dict(color="rgba(6,8,24,.8)",width=2)),
                    hovertemplate="<b>%{label}</b><br>Room Night: %{value:,.0f}<br>%{percent}<extra></extra>",
                )
                fig3.update_layout(height=360,
                    legend=dict(orientation="v",yanchor="middle",y=.5,xanchor="left",x=1.02))
                st.plotly_chart(theme(fig3), use_container_width=True)
            with cb:
                gsec("Top Supplier", "📊")
                fig3b = px.bar(ss3.head(8), x="Total Room Night", y="Supplier_Name",
                               orientation="h", text="Total Room Night",
                               color="Total Room Night",
                               color_continuous_scale=TEAL_SCALE)
                fig3b.update_traces(
                    texttemplate="%{x:,.0f}", textposition="outside",
                    textfont=dict(size=10, color="#8898AA"),
                    marker_line_width=0, marker_cornerradius=4,
                    cliponaxis=False)
                fig3b.update_layout(yaxis=dict(categoryorder="total ascending"),
                                    coloraxis_showscale=False, height=360,
                                    xaxis_title="", yaxis_title="")
                st.plotly_chart(theme(fig3b), use_container_width=True)

            st.dataframe(
                d3.sort_values("Total Room Night",ascending=False).reset_index(drop=True)
                  .style.format({"Total Room Night":"{:,.0f}"})
                  .background_gradient(subset=["Total Room Night"],cmap="Blues"),
                use_container_width=True)
        else:
            st.warning("Kolom Supplier_Name atau Total Room Night tidak tersedia.")

    # ═══════════════════════════════════════════════════════════════
    # TAB 4 — Product Type
    # ═══════════════════════════════════════════════════════════════
    with tab4:
        if "Product Type" in df_view.columns and "Total Room Night" in df_view.columns:
            _df_p4 = (df_view[["Product Type","Total Room Night"]]
                      .dropna(subset=["Product Type"])
                      .assign(**{"Product Type": lambda d: d["Product Type"].astype(str).str.strip()})
                      .pipe(lambda d: d[~d["Product Type"].isin(["","nan","None","NaN"])]))
            ps4 = (_df_p4.groupby("Product Type", dropna=True)["Total Room Night"]
                          .sum().reset_index().sort_values("Total Room Night",ascending=False))
            d4 = pd.concat([ps4.head(6),
                            pd.DataFrame([{"Product Type":"Others",
                                           "Total Room Night":ps4.iloc[6:]["Total Room Night"].sum()}])
                            if len(ps4)>6 else pd.DataFrame()], ignore_index=True)
            ca,cb = st.columns([3,2])
            with ca:
                gsec("Distribusi Product Type", "📦")
                fig4 = px.pie(d4,names="Product Type",values="Total Room Night",
                              hole=0.52, color_discrete_sequence=GLASS_PALETTE)
                fig4.update_traces(
                    textinfo="percent+label",
                    textfont=dict(size=12,family="Space Grotesk"),
                    pull=[0.05]+[0]*(len(d4)-1),
                    marker=dict(line=dict(color="rgba(6,8,24,.8)",width=2)),
                    hovertemplate="<b>%{label}</b><br>Room Night: %{value:,.0f}<br>%{percent}<extra></extra>",
                )
                fig4.update_layout(height=360)
                st.plotly_chart(theme(fig4), use_container_width=True)
            with cb:
                gsec("Tabel Product Type", "📋")
                st.dataframe(
                    d4.sort_values("Total Room Night",ascending=False).reset_index(drop=True)
                      .style.format({"Total Room Night":"{:,.0f}"})
                      .background_gradient(subset=["Total Room Night"],cmap="Blues"),
                    use_container_width=True, height=360)
        else:
            st.warning("Kolom Product Type atau Total Room Night tidak tersedia.")

    # ═══════════════════════════════════════════════════════════════
    # TAB 5 — Agent
    # ═══════════════════════════════════════════════════════════════
    with tab5:
        ac = next((c for c in df_view.columns if "agent" in c.lower() or "handler" in c.lower()), None)
        if ac and "Invoice No" in df_view.columns and "Total Room Night" in df_view.columns:
            agent_map = {
                "client-cre-mic-opc":"API-DTM","client-cre-ptrmtt-cp":"API-DTM",
                "farras":"Farras","firda":"Firda","rida.manora":"Rida","meijika":"Meiji",
                "veronica":"Vero","selvy":"Selvy","ade.puspita":"Ade","cbt.admin":"CBT-Admin",
                "shaiful.baldy":"Baldy","muhammad.geraldi":"Gerald","achmad.rifandi":"Fandi",
                "sulistia":"CBT-Tia","aliryodan":"CBT-Ali","rifyal.tumber":"Rifyal",
            }
            dfa = df_view.copy()
            dfa[ac] = dfa[ac].astype(str).str.strip().str.lower().replace(agent_map)
            # Drop rows where agent is blank/nan/none after str conversion
            _null_ac = {"nan","none","","nat","<na>","n/a","null"}
            dfa = dfa[~dfa[ac].str.lower().isin(_null_ac)]
            dfa = (dfa.groupby(ac, dropna=True).agg({"Invoice No":pd.Series.nunique,"Total Room Night":"sum"})
                      .reset_index().rename(columns={"Invoice No":"Invoice Unik"})
                      .sort_values("Invoice Unik",ascending=False))

            gsec("Performance Agent", "👤")
            ca,cb = st.columns(2)
            with ca:
                fi = px.bar(dfa.head(10),x="Invoice Unik",y=ac,orientation="h",
                            text="Invoice Unik",color="Invoice Unik",
                            color_continuous_scale=INDIGO_SCALE)
                fi.update_traces(
                    texttemplate="%{x:,.0f}", textposition="outside",
                    textfont=dict(size=11, color="#8898AA"),
                    marker_line_width=0, marker_cornerradius=4,
                    cliponaxis=False)
                fi.update_layout(yaxis=dict(categoryorder="total ascending"),
                                 coloraxis_showscale=False, height=380,
                                 xaxis_title="Invoice Unik", yaxis_title="")
                st.plotly_chart(theme(fi), use_container_width=True)
            with cb:
                fr = px.bar(dfa.head(10),x="Total Room Night",y=ac,orientation="h",
                            text="Total Room Night",color="Total Room Night",
                            color_continuous_scale=TEAL_SCALE)
                fr.update_traces(
                    texttemplate="%{x:,.0f}", textposition="outside",
                    textfont=dict(size=11, color="#8898AA"),
                    marker_line_width=0, marker_cornerradius=4,
                    cliponaxis=False)
                fr.update_layout(yaxis=dict(categoryorder="total ascending"),
                                 coloraxis_showscale=False, height=380,
                                 xaxis_title="Room Night", yaxis_title="")
                st.plotly_chart(theme(fr), use_container_width=True)

            excl = ["API-DTM","CBT-TIA","CBT-ALI","CBT-ADMIN"]
            dfp  = dfa[~dfa[ac].str.upper().isin(excl)]
            if not dfp.empty:
                st.divider()
                ca2,cb2 = st.columns([3,2])
                with ca2:
                    gsec("Distribusi PIC Room Night", "🎯")
                    fp = px.pie(dfp,names=ac,values="Total Room Night",
                                hole=0.52, color_discrete_sequence=GLASS_PALETTE)
                    fp.update_traces(
                        textinfo="percent+label",
                        textfont=dict(size=12,family="Space Grotesk"),
                        pull=[0.05]+[0]*(len(dfp)-1),
                        marker=dict(line=dict(color="rgba(6,8,24,.8)",width=2)),
                        hovertemplate="<b>%{label}</b><br>Room Night: %{value:,.0f}<extra></extra>",
                    )
                    fp.update_layout(height=360)
                    st.plotly_chart(theme(fp), use_container_width=True)
                with cb2:
                    gsec("Tabel PIC", "📋")
                    st.dataframe(
                        dfp.sort_values("Total Room Night",ascending=False).reset_index(drop=True)
                           .style.format({"Invoice Unik":"{:,.0f}","Total Room Night":"{:,.0f}"})
                           .background_gradient(subset=["Total Room Night"],cmap="Blues"),
                        use_container_width=True, height=360)
        else:
            st.warning("Kolom Agent, Invoice No, atau Total Room Night tidak ditemukan.")

    # ═══════════════════════════════════════════════════════════════
    # TAB 6 — PTM Corp Rate
    # ═══════════════════════════════════════════════════════════════
    with tab6:
        if all(c in df_view.columns for c in ["Supplier_Name","Hotel_Name","Total Room Night"]):
            dfptm = df_view[df_view["Supplier_Name"].astype(str).str.upper()=="PTM CORP RATE"]
            if dfptm.empty:
                st.warning("Tidak ditemukan data Supplier = 'PTM CORP RATE'.")
            else:
                _df_ptm = (dfptm[["Hotel_Name","Total Room Night"]]
                           .dropna(subset=["Hotel_Name"])
                           .assign(Hotel_Name=lambda d: d["Hotel_Name"].astype(str).str.strip())
                           .pipe(lambda d: d[~d["Hotel_Name"].isin(["","nan","None","NaN","nan"])]))
                dfh = (_df_ptm.groupby("Hotel_Name", dropna=True, as_index=False)
                              .agg({"Total Room Night":"sum"})
                              .sort_values("Total Room Night",ascending=False))
                ca,cb = st.columns([3,2])
                with ca:
                    gsec("Top Hotel PTM Corp Rate", "🏨")
                    fh = px.bar(dfh.head(15),x="Total Room Night",y="Hotel_Name",
                                orientation="h", text="Total Room Night",
                                color="Total Room Night",
                                color_continuous_scale=["rgba(252,211,77,.2)","rgba(252,211,77,.6)","#FCD34D"])
                    fh.update_traces(
                        texttemplate="%{x:,.0f}", textposition="outside",
                        textfont=dict(size=11, color="#8898AA"),
                        marker_line_width=0, marker_cornerradius=4,
                        cliponaxis=False)
                    fh.update_layout(yaxis=dict(categoryorder="total ascending", automargin=True),
                                     coloraxis_showscale=False, height=460,
                                     xaxis_title="", yaxis_title="",
                                     margin=dict(l=8, r=80, t=30, b=8))
                    st.plotly_chart(theme(fh), use_container_width=True)
                with cb:
                    gsec("Tabel Hotel PTM", "📋")
                    st.dataframe(
                        dfh.head(20).reset_index(drop=True)
                           .style.format({"Total Room Night":"{:,.0f}"})
                           .background_gradient(subset=["Total Room Night"],cmap="YlOrBr"),
                        use_container_width=True, height=400)
                    ob3=io.BytesIO()
                    with pd.ExcelWriter(ob3,engine="xlsxwriter") as w:
                        dfh.to_excel(w,index=False,sheet_name="Hotel_PTM")
                    st.download_button("⬇ Download Hotel PTM",ob3.getvalue(),
                                       "hotel_ptm_corp_rate.xlsx",
                                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                       use_container_width=True)
        else:
            st.warning("Kolom Supplier_Name, Hotel_Name, atau Total Room Night tidak ditemukan.")

    # ═══════════════════════════════════════════════════════════════
    # TAB 7 — Supplier Category
    # ═══════════════════════════════════════════════════════════════
    with tab7:
        if "Supplier_Category" in df_view.columns and "Total Room Night" in df_view.columns:
            _df_s7 = (df_view[["Supplier_Category","Total Room Night"]]
                      .dropna(subset=["Supplier_Category"])
                      .assign(Supplier_Category=lambda d: d["Supplier_Category"].astype(str).str.strip())
                      .pipe(lambda d: d[~d["Supplier_Category"].isin(["","nan","None","NaN"])]))
            cs7 = (_df_s7.groupby("Supplier_Category", dropna=True)["Total Room Night"]
                          .sum().reset_index().sort_values("Total Room Night",ascending=False))
            d7 = pd.concat([cs7.head(5),
                            pd.DataFrame([{"Supplier_Category":"Others",
                                           "Total Room Night":cs7.iloc[5:]["Total Room Night"].sum()}])
                            if len(cs7)>5 else pd.DataFrame()], ignore_index=True)
            ca,cb = st.columns([3,2])
            with ca:
                gsec("Distribusi Kategori Supplier", "🎯")
                fc7 = px.pie(d7,names="Supplier_Category",values="Total Room Night",
                             hole=0.52, color_discrete_sequence=GLASS_PALETTE)
                fc7.update_traces(
                    textinfo="percent+label",
                    textfont=dict(size=12,family="Space Grotesk"),
                    pull=[0.05]+[0]*(len(d7)-1),
                    marker=dict(line=dict(color="rgba(6,8,24,.8)",width=2)),
                    hovertemplate="<b>%{label}</b><br>Room Night: %{value:,.0f}<extra></extra>",
                )
                fc7.update_layout(height=380)
                st.plotly_chart(theme(fc7), use_container_width=True)
            with cb:
                gsec("Tabel Kategori", "📋")
                st.dataframe(
                    d7.sort_values("Total Room Night",ascending=False).reset_index(drop=True)
                      .style.format({"Total Room Night":"{:,.0f}"})
                      .background_gradient(subset=["Total Room Night"],cmap="Blues"),
                    use_container_width=True, height=380)
        else:
            st.warning("Kolom Supplier_Category atau Total Room Night tidak tersedia.")

# ── Empty State ───────────────────────────────────────────────────────────────
else:
    for k in ["df_raw","upload_hash"]: st.session_state.pop(k,None)
    st.markdown("""
    <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;
                padding:100px 40px;text-align:center;max-width:540px;margin:48px auto 0;">
      <div style="position:relative;width:80px;height:80px;margin-bottom:32px;">
        <div style="width:80px;height:80px;
                    background:linear-gradient(135deg,rgba(99,102,241,.15),rgba(139,92,246,.1));
                    backdrop-filter:blur(20px);
                    border:1px solid rgba(99,102,241,.22);
                    border-radius:22px;display:grid;place-items:center;
                    box-shadow:0 0 40px rgba(99,102,241,.15), inset 0 1px 0 rgba(255,255,255,.1);">
          <svg width="30" height="30" viewBox="0 0 24 24" fill="none" stroke="rgba(129,140,248,.9)" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round">
            <path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"/>
            <polyline points="13 2 13 9 20 9"/>
          </svg>
        </div>
        <div style="position:absolute;inset:-12px;border-radius:30px;
                    background:radial-gradient(circle,rgba(99,102,241,.15),transparent 70%);
                    animation:emptyGlow 2.5s ease-in-out infinite alternate;"></div>
      </div>
      <style>@keyframes emptyGlow{0%{opacity:.5;transform:scale(1);}100%{opacity:1;transform:scale(1.1);}}</style>
      <div style="font-family:'Syne',sans-serif;font-size:1.2rem;font-weight:800;color:#E2E8F0;margin-bottom:12px;letter-spacing:-.5px;">
        Belum ada data
      </div>
      <p style="font-size:.75rem;color:#3A4A6A;line-height:2;margin:0 auto 36px;max-width:380px;font-family:'Space Grotesk',sans-serif;">
        Upload file Excel Custom Report di sidebar, lalu klik
        <span style="color:#818CF8;font-weight:700;background:rgba(99,102,241,.12);padding:1px 8px;border-radius:6px;border:1px solid rgba(99,102,241,.25);">Sync Data</span>
        untuk memuat normalisasi dari Google Drive.
      </p>
      <div style="display:flex;gap:10px;flex-wrap:wrap;justify-content:center;">
        <span style="font-size:.64rem;font-weight:600;padding:7px 18px;border-radius:20px;
                     background:rgba(99,102,241,.08);color:#818CF8;border:1px solid rgba(99,102,241,.2);
                     backdrop-filter:blur(10px);font-family:'JetBrains Mono',monospace;letter-spacing:.3px;">Custom Report .xlsx</span>
        <span style="font-size:.64rem;font-weight:600;padding:7px 18px;border-radius:20px;
                     background:rgba(52,211,153,.08);color:#34D399;border:1px solid rgba(52,211,153,.2);
                     backdrop-filter:blur(10px);font-family:'JetBrains Mono',monospace;letter-spacing:.3px;">Google Drive Sync</span>
        <span style="font-size:.64rem;font-weight:600;padding:7px 18px;border-radius:20px;
                     background:rgba(251,191,36,.06);color:#FCD34D;border:1px solid rgba(251,191,36,.18);
                     backdrop-filter:blur(10px);font-family:'JetBrains Mono',monospace;letter-spacing:.3px;">AI Pivot Analysis</span>
      </div>
    </div>
    """, unsafe_allow_html=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="margin-top:60px; border-top:1px solid #E9ECEF; background:#fff; box-shadow:0 -1px 0 rgba(0,0,0,.04);">

  <!-- Disclaimer Bar -->
  <div style="background:#FFF8F0; border-bottom:1px solid #FFE4C4; padding:10px 32px; display:flex; align-items:flex-start; gap:10px;">
    <span style="font-size:.85rem; margin-top:1px;">⚠️</span>
    <p style="margin:0; font-size:.62rem; color:#7C4A00; font-family:'Open Sans',sans-serif; line-height:1.9;">
      <strong style="color:#5C3300;">DISCLAIMER &nbsp;|&nbsp;</strong>
      Data yang ditampilkan dalam dashboard ini bersumber dari file Custom Report yang diunggah oleh pengguna
      dan referensi normalisasi dari Google Drive MTT. Seluruh informasi bersifat <em>internal dan rahasia</em> —
      dilarang disebarluaskan tanpa izin tertulis dari manajemen.
      Akurasi data bergantung pada kualitas sumber yang diunggah. MTT tidak bertanggung jawab atas keputusan bisnis
      yang diambil semata-mata berdasarkan output sistem ini tanpa verifikasi lebih lanjut.
      Platform ini dikembangkan untuk keperluan analitik operasional internal.
    </p>
  </div>

  <!-- Copyright Bar -->
  <div style="padding:14px 32px; display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:8px;">
    <span style="font-size:.62rem; color:#8898AA; font-family:'Open Sans',sans-serif;">
      &copy; 2025 <strong style="color:#5E72E4;">Hotel Intelligence</strong> &middot; MTT &middot; All rights reserved</span>
    <span style="font-size:.62rem; color:#8898AA; font-family:'Open Sans',sans-serif;">
      Powered by Streamlit &nbsp;&middot;&nbsp; v8.0 &nbsp;&middot;&nbsp; Argon Enterprise Edition</span>
    <span style="font-size:.62rem; color:#8898AA; font-family:'Open Sans',sans-serif;">
      Built by <strong style="color:#5E72E4;">Rifyal Tumber</strong> &middot; MTT &middot; 2025</span>
  </div>

</div>
""", unsafe_allow_html=True)