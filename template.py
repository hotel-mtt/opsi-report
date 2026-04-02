# join_opsifin.py — Opsifin v9.2 · Hotel Report Dashboard
# Concentric Rings Edition · Rifyal Tumber · MTT · 2025

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import io, requests, re, hashlib
import streamlit.components.v1 as components
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

GLASS_PALETTE = ["#0D9488","#134E4A","#2DD4BF","#5EEAD4","#99F6E4","#CCFBF1","#0F766E","#042F2E"]
INDIGO_SCALE  = ["#CCFBF1","#99F6E4","#2DD4BF","#0D9488","#0F766E","#134E4A"]
TEAL_SCALE    = ["#CCFBF1","#99F6E4","#2DD4BF","#0D9488","#0F766E","#134E4A"]

KNOWN_PICS = ["Farras","Ade","Meiji","Vero","Firda","Selvy","Rida","Rifyal","Gerald","Baldy","Fandi","API-DTM"]

# ── Avatar photo loader ───────────────────────────────────────────────────────
import base64, os as _os
def _load_avatar_b64(pic_name: str) -> str:
    _base   = _os.path.dirname(_os.path.abspath(__file__))
    _assets = _os.path.join(_base, "assets")
    for ext in [".jpg", ".jpeg", ".png", ".webp"]:
        _p = _os.path.join(_assets, pic_name + ext)
        if _os.path.isfile(_p):
            with open(_p, "rb") as _f:
                _data = base64.b64encode(_f.read()).decode()
            _mime = "image/jpeg" if ext in [".jpg",".jpeg"] else ("image/png" if ext==".png" else "image/webp")
            return f"data:{_mime};base64,{_data}"
    return ""

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

def _is_valid_file(f):
    """Cek apakah file upload masih valid (bukan DeletedFile)."""
    try:
        _ = f.name
        _ = f.size
        return True
    except Exception:
        return False

def compute_upload_hash(files):
    valid = [f for f in files if _is_valid_file(f)]
    if not valid:
        return ""
    h = hashlib.md5()
    for f in sorted(valid, key=lambda x: x.name):
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
        sc_map = norm_maps["supplier_category"]
        df["Supplier_Category"] = df["Supplier Name"].map(sc_map)
        if "Supplier_Name" in df.columns:
            _mask_nan = df["Supplier_Category"].isna()
            df.loc[_mask_nan, "Supplier_Category"] = df.loc[_mask_nan, "Supplier_Name"].map(sc_map)
        _sc_map_upper = {str(k).strip().upper(): v for k, v in sc_map.items()}
        _mask_nan2 = df["Supplier_Category"].isna()
        if _mask_nan2.any():
            df.loc[_mask_nan2, "Supplier_Category"] = (
                df.loc[_mask_nan2, "Supplier Name"]
                  .str.strip().str.upper()
                  .map(_sc_map_upper)
            )
        if "Supplier_Name" in df.columns:
            _mask_nan3 = df["Supplier_Category"].isna()
            if _mask_nan3.any():
                df.loc[_mask_nan3, "Supplier_Category"] = (
                    df.loc[_mask_nan3, "Supplier_Name"]
                      .str.strip().str.upper()
                      .map(_sc_map_upper)
                )
        df["Supplier_Category"] = df["Supplier_Category"].fillna("Uncategorized")
    else:
        df["Supplier_Category"] = "Uncategorized"

    _KNOWN_WHOLESALERS = {
        "MG BEDBANK","MG BED BANK","MGBEDBANK","KLIKNBOOK","KLIK N BOOK","KLOOK",
        "HOTELBEDS","HOTEL BEDS","WEBBEDS","WEB BEDS","TOURICO","GTA","JUMBO TOURS",
        "WORLDHOTELS","RESTEL","BONOTEL","RECONLINE",
    }
    _KNOWN_CORPORATE = {"PTM CORP RATE","CORPORATE RATE"}
    _KNOWN_DIRECT    = {"DIRECT TO HOTEL","DIRECT HOTEL","DIRECT"}
    _KNOWN_OTA       = {
        "TRAVELOKA","TRAVELOKA BUSINESS","TIKET.COM","BOOKING.COM","BOOKING COM",
        "AGODA","AGODA CORPORATE","EXPEDIA","HOTELS.COM",
    }

    def _norm_sc(val):
        if pd.isnull(val): return "Uncategorized"
        v = str(val).strip(); vu = v.upper()
        if vu in _KNOWN_DIRECT or vu in {"DIRECT TO HOTEL","DIRECT HOTEL"}: return "DIRECT HOTEL"
        if vu in _KNOWN_CORPORATE or vu in {"PTM CORP RATE","CORPORATE RATE"}: return "CORPORATE RATE"
        if vu == "WHOLESALER": return "WHOLESALER"
        if vu == "OTA": return "OTA"
        if v == "Uncategorized": return "Uncategorized"
        return v
    df["Supplier_Category"] = df["Supplier_Category"].apply(_norm_sc)

    if "Supplier_Name" in df.columns:
        def _sc_from_name(row):
            cat  = row["Supplier_Category"]
            name = str(row["Supplier_Name"]).strip().upper()
            raw  = str(row.get("Supplier Name","")).strip().upper() if "Supplier Name" in row.index else ""
            for n in [name, raw]:
                if n in _KNOWN_DIRECT:      return "DIRECT HOTEL"
                if n in _KNOWN_CORPORATE:   return "CORPORATE RATE"
                if n in _KNOWN_WHOLESALERS: return "WHOLESALER"
                if n in _KNOWN_OTA:         return "OTA"
            if cat == "Uncategorized":
                for n in [name, raw]:
                    if "BEDBANK" in n or "WHOLESAL" in n: return "WHOLESALER"
                    if "CORP" in n and "RATE" in n:       return "CORPORATE RATE"
                    if "DIRECT" in n:                      return "DIRECT HOTEL"
                    if n in {"TRAVELOKA","AGODA","EXPEDIA","BOOKING.COM","TIKET.COM"}: return "OTA"
            return cat
        _has_raw = "Supplier Name" in df.columns
        if _has_raw:
            df["Supplier_Category"] = df.apply(_sc_from_name, axis=1)
        else:
            _sn = df["Supplier_Name"].str.strip().str.upper()
            df.loc[_sn.isin(_KNOWN_DIRECT),     "Supplier_Category"] = "DIRECT HOTEL"
            df.loc[_sn.isin(_KNOWN_CORPORATE),  "Supplier_Category"] = "CORPORATE RATE"
            df.loc[_sn.isin(_KNOWN_WHOLESALERS),"Supplier_Category"] = "WHOLESALER"
            df.loc[_sn.isin(_KNOWN_OTA),        "Supplier_Category"] = "OTA"
            _unc = df["Supplier_Category"] == "Uncategorized"
            df.loc[_unc & (_sn.str.contains("BEDBANK|WHOLESAL", regex=True)), "Supplier_Category"] = "WHOLESALER"
            df.loc[_unc & (_sn.str.contains("DIRECT", regex=True)),           "Supplier_Category"] = "DIRECT HOTEL"

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

    _inv_to_raw = next(
        (c for c in df.columns if any(k in c.lower() for k in
         ["invoice to","invoiceto","bill to","billto","invoice_to"])), None
    )
    CBT_PERTAMINA_ALIASES = {
        "CBT PERTAMINA(HOTEL CM)","CBT PERTAMINA (HOTEL)","PERTAMINA ENERGY TERMINAL (CBT)",
    }
    if _inv_to_raw:
        def _norm_inv_to(val):
            if pd.isnull(val) or str(val).strip() == "": return "Unknown"
            clean = str(val).strip().upper()
            if clean in CBT_PERTAMINA_ALIASES: return "CBT PERTAMINA"
            return str(val).strip()
        df["Normalized_Inv_To"] = df[_inv_to_raw].apply(_norm_inv_to)
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

def compact_num(v):
    try:
        v = float(v)
        a = abs(v)
        if a >= 1_000_000_000: return f"{v/1_000_000_000:.1f}B"
        if a >= 1_000_000:     return f"{v/1_000_000:.1f}M"
        if a >= 1_000:         return f"{v/1_000:.1f}K"
        return f"{int(v):,}"
    except: return str(v)

def get_prev_period_metrics(df_raw, df_view):
    try:
        if "Issued Date" not in df_raw.columns or df_view.empty: return {}
        curr_min = df_view["Issued Date"].dropna().min()
        curr_max = df_view["Issued Date"].dropna().max()
        if pd.isnull(curr_min) or pd.isnull(curr_max): return {}
        delta    = curr_max - curr_min
        prev_max = curr_min - pd.Timedelta(days=1)
        prev_min = prev_max - delta
        raw_min  = df_raw["Issued Date"].dropna().min()
        raw_max  = df_raw["Issued Date"].dropna().max()
        overlap_start = max(prev_min, raw_min)
        overlap_end   = min(prev_max, raw_max)
        overlap_days  = (overlap_end - overlap_start).days + 1
        period_days   = max((prev_max - prev_min).days + 1, 1)
        coverage_pct  = overlap_days / period_days
        if coverage_pct < 0.80: return {}
        prev_df = df_raw[
            (df_raw["Issued Date"] >= prev_min) &
            (df_raw["Issued Date"] <= prev_max)
        ]
        if prev_df.empty or len(prev_df) < 5: return {}
        m = {}
        m["rows"]     = len(prev_df)
        m["prev_min"] = prev_min.strftime("%d %b %Y")
        m["prev_max"] = prev_max.strftime("%d %b %Y")
        m["ui"]  = int(prev_df["Invoice No"].nunique())                     if "Invoice No"       in prev_df.columns else None
        m["rn"]  = int(np.ceil(prev_df["Total Room Night"].sum()))          if "Total Room Night" in prev_df.columns else None
        m["sa"]  = float(prev_df["Sales AR"].fillna(0).astype(float).sum()) if "Sales AR"         in prev_df.columns else None
        m["up"]  = int(prev_df["Full Name"].dropna().nunique())             if "Full Name"        in prev_df.columns else None
        if "Profit" in prev_df.columns and "Sales AR" in prev_df.columns:
            _p  = prev_df["Profit"].fillna(0).astype(float)
            _s  = prev_df["Sales AR"].fillna(0).astype(float)
            _mm = _s != 0
            m["pm"] = float((_p[_mm] / _s[_mm] * 100).mean()) if _mm.any() else 0.0
        return m
    except:
        return {}

def trend_badge(curr_val, prev_val, fmt_suffix="", reverse=False):
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
        font_family="Open Sans", font_color="#525F7F", font_size=12,
        plot_bgcolor="rgba(255,255,255,0)", paper_bgcolor="rgba(255,255,255,0)",
        margin=dict(l=12,r=12,t=40,b=12),
        title_font=dict(size=13,color="#32325D",family="Open Sans"),
        legend=dict(font=dict(size=11,family="Open Sans"),bgcolor="rgba(255,255,255,.8)",
                    bordercolor="rgba(0,0,0,.06)",borderwidth=1),
        hoverlabel=dict(bgcolor="#ffffff",bordercolor="rgba(0,0,0,.1)",
                        font_size=12,font_family="Open Sans",font_color="#32325D"),
    )
    fig.update_xaxes(showgrid=True,gridcolor="rgba(0,0,0,.06)",zeroline=False,
                     tickfont=dict(size=11,color="#8898AA"),linecolor="rgba(0,0,0,.08)")
    fig.update_yaxes(showgrid=True,gridcolor="rgba(0,0,0,.06)",zeroline=False,
                     tickfont=dict(size=11,color="#8898AA"),linecolor="rgba(0,0,0,.08)")
    return fig

def gsec(title, icon=""):
    lbl = f'<span class="gsec-icon">{icon}</span>&thinsp;{title}' if icon else title
    st.markdown(f'<div class="gsec">{lbl}</div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# Fungsi build_donut_html (clean minimal donut chart)
# ══════════════════════════════════════════════════════════════════════════════
def build_donut_html(segments, total_label, subtitle=""):
    import json
    segs_js = json.dumps(segments, ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="UTF-8">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Space+Grotesk:wght@600;700;800&display=swap" rel="stylesheet">
<style>
*{{box-sizing:border-box;margin:0;padding:0;}}
html{{-webkit-font-smoothing:antialiased;}}
body{{background:transparent;font-family:'DM Sans',sans-serif;}}
.wrap{{
  background:#fff;
  border:1px solid #E2E8F0;
  border-radius:16px;
  padding:18px 20px 14px;
  box-shadow:0 1px 4px rgba(0,0,0,.05),0 8px 24px -6px rgba(13,148,136,.08);
  width:100%;
}}
.hdr{{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:16px;gap:8px;}}
.hdr-left{{}}
.eyebrow{{font-size:.52rem;font-weight:700;letter-spacing:1.2px;text-transform:uppercase;color:#94A3B8;margin-bottom:3px;}}
.title{{font-family:'Space Grotesk',sans-serif;font-size:.9rem;font-weight:700;color:#0F172A;line-height:1.2;}}
.subtitle{{font-size:.55rem;color:#94A3B8;margin-top:2px;}}
.live{{
  display:flex;align-items:center;gap:5px;
  background:#F0FDF9;border:1px solid #CCFBF1;
  border-radius:8px;padding:4px 10px;
  font-size:.54rem;font-weight:700;color:#0D9488;white-space:nowrap;flex-shrink:0;
}}
.live-dot{{width:6px;height:6px;border-radius:50%;background:#0D9488;animation:pulse 2s infinite;}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
.body{{display:flex;align-items:center;gap:20px;flex-wrap:wrap;}}
.chart-wrap{{position:relative;flex-shrink:0;}}
svg.donut{{display:block;}}
.center-txt{{
  position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);
  text-align:center;pointer-events:none;
}}
.center-num{{
  font-family:'Space Grotesk',sans-serif;font-size:1.2rem;font-weight:800;
  color:#0F172A;letter-spacing:-.5px;line-height:1;
}}
.center-lbl{{font-size:.5rem;font-weight:600;letter-spacing:.8px;text-transform:uppercase;color:#94A3B8;margin-top:2px;}}
.legend{{flex:1;min-width:140px;display:flex;flex-direction:column;gap:2px;}}
.leg-row{{
  display:flex;align-items:center;gap:10px;
  padding:7px 10px;border-radius:10px;
  transition:background .12s;cursor:default;
}}
.leg-row:hover{{background:#F0FDF9;}}
.leg-color{{
  width:10px;height:10px;border-radius:3px;flex-shrink:0;
}}
.leg-body{{flex:1;min-width:0;}}
.leg-name{{
  font-size:.66rem;font-weight:700;color:#0F172A;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
  margin-bottom:3px;
}}
.leg-bar-wrap{{height:4px;background:#F1F5F9;border-radius:4px;overflow:hidden;margin-bottom:3px;}}
.leg-bar{{height:100%;border-radius:4px;width:0;transition:width 1s cubic-bezier(.4,0,.2,1);}}
.leg-meta{{display:flex;justify-content:space-between;align-items:center;}}
.leg-val{{font-family:'Space Grotesk',sans-serif;font-size:.6rem;font-weight:700;}}
.leg-pct{{font-size:.54rem;font-weight:600;padding:1px 6px;border-radius:10px;}}
.footer{{
  margin-top:12px;padding-top:10px;border-top:1px solid #F1F5F9;
  display:flex;justify-content:space-between;align-items:center;
  font-size:.51rem;color:#CBD5E1;
}}
.footer-note{{color:#94A3B8;font-size:.52rem;}}
</style>
</head>
<body>
<div class="wrap">
  <div class="hdr">
    <div class="hdr-left">
      <div class="eyebrow">Distribusi Invoice</div>
      <div class="title">&#127758; Domestic vs International</div>
      <div class="subtitle">{subtitle}</div>
    </div>
    <div class="live"><span class="live-dot"></span>Live</div>
  </div>
  <div class="body">
    <div class="chart-wrap">
      <svg class="donut" width="180" height="180" viewBox="0 0 180 180" id="svg"></svg>
      <div class="center-txt">
        <div class="center-num" id="cnum">—</div>
        <div class="center-lbl">INVOICE</div>
      </div>
    </div>
    <div class="legend" id="lg"></div>
  </div>
  <div class="footer">
    <div class="footer-note">Invoice unik per kategori destinasi</div>
    <div id="ts"></div>
  </div>
</div>
<script>
const SEGS={segs_js};
const TOTAL="{total_label}";
const CX=90,CY=90,R=72,SW=28,GAP=2.4;
const TAU=Math.PI*2;
const svg=document.getElementById('svg');
document.getElementById('cnum').textContent=TOTAL;
document.getElementById('ts').textContent=
  new Date().toLocaleDateString('id-ID',{{day:'2-digit',month:'short',year:'numeric'}});

const total_pct=SEGS.reduce((a,s)=>a+s.pct,0)||100;
const gap_deg=GAP;
const available=360 - gap_deg*SEGS.length;

function polarToXY(cx,cy,r,deg){{
  const rad=(deg-90)*Math.PI/180;
  return [cx+r*Math.cos(rad), cy+r*Math.sin(rad)];
}}
function arcPath(cx,cy,r,sw,startDeg,endDeg){{
  const [x1,y1]=polarToXY(cx,cy,r,startDeg);
  const [x2,y2]=polarToXY(cx,cy,r,endDeg);
  const large=endDeg-startDeg>180?1:0;
  const ro=r+sw/2,ri=r-sw/2;
  const [ox1,oy1]=polarToXY(cx,cy,ro,startDeg+1);
  const [ox2,oy2]=polarToXY(cx,cy,ro,endDeg-1);
  const [ix1,iy1]=polarToXY(cx,cy,ri,endDeg-1);
  const [ix2,iy2]=polarToXY(cx,cy,ri,startDeg+1);
  return `M ${{ox1}} ${{oy1}} A ${{ro}} ${{ro}} 0 ${{large}} 1 ${{ox2}} ${{oy2}} `+
         `L ${{ix1}} ${{iy1}} A ${{ri}} ${{ri}} 0 ${{large}} 0 ${{ix2}} ${{iy2}} Z`;
}}

const trackEl=document.createElementNS('http://www.w3.org/2000/svg','circle');
trackEl.setAttribute('cx',CX);trackEl.setAttribute('cy',CY);trackEl.setAttribute('r',R);
trackEl.setAttribute('fill','none');trackEl.setAttribute('stroke','#F1F5F9');
trackEl.setAttribute('stroke-width',SW);
svg.appendChild(trackEl);

let startDeg=0;
const paths=[];
SEGS.forEach((sg,i)=>{{
  const sweep=sg.pct/total_pct*available;
  const endDeg=startDeg+sweep;
  const path=document.createElementNS('http://www.w3.org/2000/svg','path');
  path.setAttribute('fill',sg.color);
  path.setAttribute('d',arcPath(CX,CY,R,SW,startDeg,endDeg));
  path.style.opacity='0';
  path.style.transition=`opacity .3s ease ${{i*0.12}}s`;
  svg.appendChild(path);
  paths.push({{path,startDeg,endDeg,sg}});
  startDeg=endDeg+gap_deg;
}});

setTimeout(()=>paths.forEach(p=>p.path.style.opacity='1'),100);

const lg=document.getElementById('lg');
SEGS.forEach((sg,i)=>{{
  const row=document.createElement('div');
  row.className='leg-row';
  const _bg=sg.color+'18';
  row.innerHTML=`
    <div class="leg-color" style="background:${{sg.color}};"></div>
    <div class="leg-body">
      <div class="leg-name">${{sg.label}}</div>
      <div class="leg-bar-wrap"><div class="leg-bar" id="b${{i}}" style="background:${{sg.color}};"></div></div>
      <div class="leg-meta">
        <span class="leg-val" style="color:${{sg.color}};">${{Number(sg.value).toLocaleString('id-ID')}}</span>
        <span class="leg-pct" style="background:${{_bg}};color:${{sg.color}};">${{sg.pct}}%</span>
      </div>
    </div>`;
  lg.appendChild(row);
}});
setTimeout(()=>SEGS.forEach((_,i)=>
  document.getElementById('b'+i).style.width=SEGS[i].pct+'%'
),350);
</script>
</body>
</html>"""

# ═════════════════════════════════════════════════════════════════════════════
# CSS
# ═════════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=DM+Sans:ital,wght@0,400;0,500;0,600;0,700;0,800;1,400&family=Sora:wght@400;600;700;800&display=swap');

:root {
  --bg:      #F4F6F9;
  --card:    #FFFFFF;
  --t1:  #0F172A;
  --t2:  #334155;
  --t3:  #64748B;
  --t4:  #94A3B8;
  --primary:  #0D9488;
  --primary2: #0F766E;
  --primary3: #134E4A;
  --primary-light: #F0FDFA;
  --primary-mid:   #CCFBF1;
  --success:  #10B981;
  --danger:   #EF4444;
  --border:   #E2E8F0;
  --border2:  #CBD5E1;
  --shadow:    0 1px 3px 0 rgba(0,0,0,.07), 0 1px 2px -1px rgba(0,0,0,.07);
  --shadow-sm: 0 1px 2px 0 rgba(0,0,0,.05);
  --shadow-lg: 0 10px 25px -3px rgba(0,0,0,.08), 0 4px 6px -4px rgba(0,0,0,.05);
  --shadow-teal: 0 8px 24px -4px rgba(13,148,136,.18);
  --r:  .375rem;
  --r2: .5rem;
  --r3: .75rem;
  --r4: 1rem;
  --font: 'Inter', sans-serif;
  --font-head: 'DM Sans', sans-serif;
  --font-display: 'Sora', sans-serif;
}

*, *::before, *::after { box-sizing:border-box; }
html, body, [class*="css"] {
  font-family: var(--font) !important;
  font-size: 13px !important;
  color: var(--t2) !important;
  background-color: var(--bg) !important;
  -webkit-font-smoothing: antialiased;
}

.stApp, body, [data-testid="stAppViewContainer"] {
  background-color: var(--bg) !important;
  background-image: none !important;
}
.stApp::before, .stApp::after { display:none !important; }
[data-testid="stAppViewContainer"] > section > div { background:transparent !important; }
.block-container { padding:0!important; max-width:100%!important; background:transparent!important; overflow-x:hidden!important; }
.main .block-container { padding:24px 38px 80px!important; }
[data-testid="stMainBlockContainer"] { padding:24px 38px 80px!important; max-width:100%!important; }
[data-testid="block-container"] { padding:24px 38px 80px!important; }
div[class*="block-container"] { padding:24px 38px 80px!important; }
section[data-testid="stMain"] > div { padding:24px 38px 80px!important; overflow-x:hidden!important; }

[data-testid="collapsedControl"],
[data-testid="stSidebarCollapseButton"],
button[data-testid="baseButton-header"],
#MainMenu, footer, header { display:none!important; }

::-webkit-scrollbar { width:5px; height:5px; }
::-webkit-scrollbar-track { background:#f1f3f4; }
::-webkit-scrollbar-thumb { background:#CDD0D5; border-radius:10px; }
::-webkit-scrollbar-thumb:hover { background:#B0B7C0; }

/* ══ HEADER ══ */
.ghdr {
  background: #FFFFFF;
  padding:0 36px; height:60px;
  display:flex; align-items:center; justify-content:space-between;
  position:sticky; top:0; z-index:500;
  border-bottom:1px solid var(--border);
  box-shadow:0 1px 0 rgba(0,0,0,.04);
}
.ghdr-brand { display:flex; align-items:center; gap:12px; }
.ghdr-logo {
  width:34px; height:34px;
  background:var(--primary-light); border-radius:var(--r2);
  display:grid; place-items:center; flex-shrink:0;
  border:1px solid var(--primary-mid);
}
.ghdr-name { font-family:var(--font-head); font-size:.93rem; font-weight:700; color:var(--t1); letter-spacing:-.3px; }
.ghdr-name span { color:var(--primary); font-weight:500; }
.ghdr-sub { font-size:.58rem; color:var(--t4); margin-top:2px; letter-spacing:.5px; text-transform:uppercase; }
.ghdr-right { display:flex; align-items:center; gap:8px; }
.ghdr-live {
  display:flex; align-items:center; gap:6px;
  font-size:.6rem; font-weight:600; color:var(--primary); letter-spacing:.5px; text-transform:uppercase;
  padding:5px 12px; border-radius:20px;
  background:var(--primary-light); border:1px solid var(--primary-mid);
}
.ghdr-dot { width:6px; height:6px; border-radius:50%; background:var(--primary); animation:livebeat 2s ease-in-out infinite; }
@keyframes livebeat { 0%,100%{ opacity:1; } 50%{ opacity:.4; } }
.ghdr-pill { font-size:.6rem; font-weight:600; color:var(--t3); padding:5px 12px; border-radius:20px; background:var(--bg); border:1px solid var(--border); }

/* ══ TICKER ══ */
.gticker {
  background:var(--primary-light);
  border-bottom:1px solid var(--primary-mid);
  padding:6px 0; overflow:hidden; position:relative;
}
.gticker::before,.gticker::after { content:''; position:absolute; top:0; width:80px; height:100%; z-index:2; }
.gticker::before { left:0;  background:linear-gradient(90deg,var(--primary-light),transparent); }
.gticker::after  { right:0; background:linear-gradient(270deg,var(--primary-light),transparent); }
.gticker-track {
  display:inline-block; white-space:nowrap;
  animation:tickslide 65s linear infinite;
  font-size:.58rem; letter-spacing:.8px; text-transform:uppercase; font-family:var(--font);
}
.gticker-track:hover { animation-play-state:paused; }
.t-item { color:var(--t4); }
.t-item.hi { color:var(--primary); font-weight:600; }
.tsep { margin:0 24px; color:var(--primary-mid); }
@keyframes tickslide { from{transform:translateX(0)} to{transform:translateX(-50%)} }

/* ══ SIDEBAR ══ */
[data-testid="stSidebar"] {
  background:#FFFFFF !important;
  border-right:1px solid var(--border) !important;
  box-shadow:none !important;
  min-width:256px!important; max-width:256px!important;
}
[data-testid="stSidebar"]::before { display:none !important; }
[data-testid="stSidebar"] > div:first-child { padding:0!important; }
[data-testid="stSidebar"] * { font-family:var(--font)!important; }

.sb-top {
  padding:20px 18px 16px; border-bottom:1px solid var(--border);
  display:flex; align-items:center; gap:11px;
}
.sb-logo {
  width:32px; height:32px; background:var(--primary-light);
  border-radius:var(--r2); display:grid; place-items:center; flex-shrink:0;
  border:1px solid var(--primary-mid);
}
.sb-appname { font-family:var(--font-head); font-size:.85rem; font-weight:700; color:var(--t1); }
.sb-appname span { color:var(--primary); font-weight:500; }
.sb-ver { font-size:.56rem; color:var(--t4); margin-top:2px; letter-spacing:.3px; }

.sb-section { padding:16px 18px 6px; font-size:.58rem; font-weight:600; color:var(--t4)!important; text-transform:uppercase; letter-spacing:1.5px; }
.sb-divider { height:1px; background:var(--border); margin:4px 16px; }

.sync-row { display:flex; align-items:center; justify-content:space-between; padding:6px 18px; transition:all .15s; border-radius:var(--r); margin:1px 6px; }
.sync-row:hover { background:var(--bg); }
.sync-label { font-size:.7rem; color:var(--t2); font-weight:500; }
.stag { font-size:.56rem; font-weight:600; padding:2px 9px; border-radius:20px; letter-spacing:.2px; }
.stag-ok   { background:#F0FDF4; color:#16A34A; border:1px solid #BBF7D0; }
.stag-err  { background:#FFF1F2; color:#DC2626; border:1px solid #FECACA; }
.stag-wait { background:#F8FAFC; color:var(--t4); border:1px solid var(--border); }

/* ══ FIX: Uploader — sembunyikan label bawaan Streamlit sepenuhnya ══ */
[data-testid="stSidebar"] [data-testid="stFileUploader"] label,
[data-testid="stSidebar"] [data-testid="stFileUploader"] [data-testid="stWidgetLabel"],
[data-testid="stSidebar"] [data-testid="stFileUploader"] [data-testid="stWidgetLabel"] p,
[data-testid="stSidebar"] .stFileUploader label,
[data-testid="stSidebar"] .stFileUploader [data-testid="stWidgetLabel"],
[data-testid="stSidebar"] [data-testid="stFileUploaderDropzoneInstructions"] ~ div label {
  display: none !important;
  visibility: hidden !important;
  height: 0 !important;
  margin: 0 !important;
  padding: 0 !important;
  overflow: hidden !important;
}

[data-testid="stSidebar"] [data-testid="stFileUploader"] {
  background:#FAFFFE!important; border:1.5px dashed var(--primary-mid)!important;
  border-radius:var(--r)!important; transition:all .2s!important;
}
[data-testid="stSidebar"] [data-testid="stFileUploader"]:hover {
  border-color:var(--primary)!important; background:var(--primary-light)!important;
  box-shadow:0 0 0 3px rgba(13,148,136,.08)!important;
}
[data-testid="stSidebar"] [data-testid="stFileUploaderDropzoneInstructions"] p { font-size:.65rem!important; color:var(--t4)!important; }
[data-testid="stSidebar"] [data-testid="stFileUploaderDropzoneInstructions"] span { color:var(--primary)!important; font-weight:600!important; }
[data-testid="stSidebar"] .stFileUploader { padding:0 12px!important; margin-bottom:6px!important; }
[data-testid="stSidebar"] [data-testid="stMultiSelect"] > div > div,
[data-testid="stSidebar"] [data-testid="stDateInput"] input {
  background:#fff!important; border:1px solid var(--border)!important;
  border-radius:var(--r)!important; color:var(--t1)!important;
  font-size:.7rem!important; backdrop-filter:none!important;
}
[data-testid="stSidebar"] [data-testid="stMultiSelect"] span[data-baseweb="tag"] {
  background:var(--primary-light)!important; color:var(--primary)!important;
  border:1px solid var(--primary-mid)!important; border-radius:4px!important; font-size:.62rem!important;
}
[data-testid="stSidebar"] .stMultiSelect,
[data-testid="stSidebar"] .stDateInput { padding:0 12px!important; margin-bottom:8px!important; }

/* ══ Label untuk widget lain di sidebar (bukan uploader) ══ */
[data-testid="stSidebar"] [data-testid="stMultiSelect"] label,
[data-testid="stSidebar"] [data-testid="stMultiSelect"] [data-testid="stWidgetLabel"] p,
[data-testid="stSidebar"] [data-testid="stDateInput"] label,
[data-testid="stSidebar"] [data-testid="stDateInput"] [data-testid="stWidgetLabel"] p {
  font-size:.67rem!important; font-weight:500!important; color:var(--t2)!important; margin-bottom:4px!important;
}

/* ══ BUTTONS ══ */
[data-testid="stButton"] > button {
  background:var(--primary) !important;
  color:#fff !important; border:none !important;
  border-radius:var(--r) !important; font-size:.72rem !important;
  font-weight:600 !important; padding:9px 20px !important; font-family:var(--font) !important;
  box-shadow:0 1px 2px rgba(0,0,0,.08) !important;
  transition:all .15s !important;
}
[data-testid="stButton"] > button:hover {
  background:var(--primary2) !important;
  box-shadow:0 4px 12px rgba(13,148,136,.25) !important;
  transform:translateY(-1px) !important;
}
[data-testid="stDownloadButton"] > button {
  background:#fff !important; color:var(--t2) !important;
  border:1px solid var(--border) !important; border-radius:var(--r) !important;
  font-size:.7rem !important; font-weight:500 !important; padding:9px 20px !important;
  box-shadow:var(--shadow-sm) !important; transition:all .15s !important; font-family:var(--font) !important;
}
[data-testid="stDownloadButton"] > button:hover {
  background:var(--bg) !important; border-color:var(--border2) !important;
  box-shadow:var(--shadow) !important; transform:translateY(-1px) !important;
}

/* ══ TABS ══ */
[data-testid="stTabs"] [data-baseweb="tab-list"] {
  background:var(--bg) !important; border:1px solid var(--border) !important;
  border-radius:var(--r2) !important; gap:2px !important; padding:4px !important;
  margin-bottom:24px; box-shadow:none !important;
}
[data-testid="stTabs"] [data-baseweb="tab"] {
  font-size:.71rem !important; font-weight:500 !important; color:var(--t3) !important;
  padding:8px 18px !important; border-bottom:none !important;
  transition:all .15s !important; font-family:var(--font) !important;
  border-radius:var(--r) !important;
}
[data-testid="stTabs"] [data-baseweb="tab"]:hover { color:var(--t1) !important; background:#fff !important; }
[data-testid="stTabs"] [aria-selected="true"] {
  color:var(--t1) !important; font-weight:600 !important;
  background:#fff !important;
  box-shadow:0 1px 3px rgba(0,0,0,.08), 0 1px 2px rgba(0,0,0,.04) !important;
}

/* ══ SECTION HEADER ══ */
.gsec {
  display:flex; align-items:center; gap:10px;
  font-family:var(--font); font-size:.65rem; font-weight:600; color:var(--t3);
  text-transform:uppercase; letter-spacing:.8px; margin:8px 0 16px;
}
.gsec::after { content:''; flex:1; height:1px; background:var(--border); }
.gsec-icon { font-size:.75rem; }

/* ══ KPI / Scorecard ══ */
.sc-grid-3 { display:grid; grid-template-columns:repeat(3,1fr); gap:14px; margin-bottom:14px; }
.sc-grid-2 { display:grid; grid-template-columns:repeat(2,1fr); gap:14px; margin-bottom:14px; }
.sc-strip   { display:grid; gap:0; margin-bottom:14px;
              background:#fff; border:1px solid var(--border);
              border-radius:var(--r3); overflow:hidden;
              box-shadow:var(--shadow-sm); }
.sc-strip-5 { grid-template-columns:repeat(5,1fr); }
.sc-strip-4 { grid-template-columns:repeat(4,1fr); }
@media(max-width:1200px){ .sc-strip-5{grid-template-columns:repeat(3,1fr);} }
@media(max-width:1100px){
  .sc-grid-3{grid-template-columns:repeat(2,1fr);}
  .sc-grid-2{grid-template-columns:1fr;}
  .sc-strip-4{grid-template-columns:repeat(2,1fr);}
  .sc-strip-5{grid-template-columns:repeat(2,1fr);}
}
@media(max-width:650px){
  .sc-grid-3{grid-template-columns:1fr;}
  .sc-strip-4,.sc-strip-5{grid-template-columns:1fr;}
}

.sc-hero {
  background:#fff; border:1px solid var(--border); border-radius:var(--r3);
  padding:0; position:relative; overflow:hidden;
  transition:box-shadow .2s, transform .2s; cursor:default;
}
.sc-hero:hover { transform:translateY(-2px); box-shadow:var(--shadow-teal); }
.sc-hero-inner { padding:18px 20px 16px; position:relative; z-index:1; }
.sc-hero-accent {
  position:absolute; top:0; left:0; right:0; height:3px;
  background:linear-gradient(90deg, var(--primary), var(--primary3));
  border-radius:var(--r3) var(--r3) 0 0;
}
.sc-hero-bg {
  position:absolute; bottom:-20px; right:-10px; z-index:0;
  width:80px; height:80px; border-radius:50%;
  background:radial-gradient(circle, rgba(13,148,136,.08) 0%, transparent 70%);
  pointer-events:none;
}
.sc-hero-icon {
  width:36px; height:36px; border-radius:var(--r2); display:grid; place-items:center;
  flex-shrink:0; margin-bottom:12px; font-size:1.1rem; line-height:1;
}
.sc-hero-label {
  font-size:.56rem; font-weight:700; letter-spacing:1.2px; text-transform:uppercase;
  color:var(--t4); margin-bottom:6px;
}
.sc-hero-value {
  font-size:2rem; font-weight:800; font-family:var(--font-display);
  line-height:1; letter-spacing:-1px; color:var(--t1); margin-bottom:4px;
}
.sc-hero-value.sm-text { font-size:1.5rem; }
.sc-hero-sub { font-size:.59rem; color:var(--t4); margin-bottom:10px; line-height:1.5; }
.sc-badge { display:inline-flex; align-items:center; gap:4px; font-size:.58rem; font-weight:700; padding:3px 9px; border-radius:20px; }
.sc-badge.up   { background:#ECFDF5; color:#059669; border:1px solid #A7F3D0; }
.sc-badge.down { background:#FFF1F2; color:#DC2626; border:1px solid #FECACA; }
.sc-badge.neu  { background:#F8FAFC; color:var(--t4); border:1px solid var(--border); }

.sc-strip-hdr {
  grid-column:1/-1; padding:8px 16px 6px;
  font-size:.54rem; font-weight:700; letter-spacing:1.2px; text-transform:uppercase; color:var(--t4);
  border-bottom:1px solid var(--border); background:#FAFBFC;
  display:flex; align-items:center; gap:6px;
}
.sc-strip-hdr-dot { width:5px; height:5px; border-radius:50%; background:var(--primary); display:inline-block; }

.sc-cell {
  padding:14px 18px 12px; border-right:1px solid var(--border);
  transition:background .12s; cursor:default; position:relative;
}
.sc-cell:last-child { border-right:none; }
.sc-cell:hover { background:#F8FDFC; }
.sc-cell::before {
  content:''; position:absolute; top:0; left:0; width:2px; height:0;
  background:var(--primary); transition:height .2s ease; border-radius:0 2px 2px 0;
}
.sc-cell:hover::before { height:100%; }
.sc-cell-label {
  font-size:.55rem; font-weight:700; letter-spacing:.9px; text-transform:uppercase;
  color:var(--t4); margin-bottom:5px; display:block;
}
.sc-cell-value {
  font-size:1.25rem; font-weight:800; font-family:var(--font-display); line-height:1;
  color:var(--t1); letter-spacing:-.5px; display:block; margin-bottom:3px;
}
.sc-cell-hint { font-size:.55rem; color:var(--t4); display:block; line-height:1.5; margin-bottom:4px; }
.sc-badge-sm { display:inline-flex; align-items:center; gap:3px; font-size:.54rem; font-weight:700; padding:2px 7px; border-radius:20px; margin-top:2px; }
.sc-badge-sm.up   { background:#ECFDF5; color:#059669; border:1px solid #A7F3D0; }
.sc-badge-sm.down { background:#FFF1F2; color:#DC2626; border:1px solid #FECACA; }
.sc-badge-sm.neu  { background:#F8FAFC; color:var(--t4); border:1px solid var(--border); }

.gkpi-trend { display:none; }

/* ══ PIC2 SCORECARD ══ */
.pic2-grid {
  display: grid; grid-template-columns: repeat(auto-fill, minmax(270px, 1fr));
  gap: 18px; margin-bottom: 28px;
}
@media(max-width:1100px){ .pic2-grid{ grid-template-columns: repeat(2,1fr); } }
@media(max-width:580px){  .pic2-grid{ grid-template-columns: 1fr; } }
.pic2-card {
  background: #fff; border: 1px solid var(--border); border-radius: 16px;
  overflow: hidden; display: flex; flex-direction: column;
  position: relative; transition: box-shadow .22s ease, transform .22s ease;
  cursor: default; box-shadow: 0 1px 4px rgba(0,0,0,.06);
}
.pic2-card:hover {
  transform: translateY(-4px);
  box-shadow: 0 16px 40px -8px rgba(13,148,136,.20), 0 4px 12px -4px rgba(13,148,136,.10);
}
.pic2-card.other { border-color: #E2E8F0; }
.pic2-card.other:hover { box-shadow: 0 12px 32px -8px rgba(100,116,139,.16); transform: translateY(-3px); }
.p2-banner {
  background: linear-gradient(135deg, #0D9488 0%, #042F2E 100%);
  padding: 20px 18px 16px; display: flex; align-items: center; gap: 14px;
  position: relative; overflow: hidden;
}
.p2-banner::before {
  content: ''; position: absolute; top: -24px; right: -20px;
  width: 100px; height: 100px; border-radius: 50%; background: rgba(255,255,255,.06); pointer-events: none;
}
.p2-banner::after {
  content: ''; position: absolute; bottom: -14px; left: 40%;
  width: 70px; height: 70px; border-radius: 50%; background: rgba(255,255,255,.04); pointer-events: none;
}
.pic2-card.other .p2-banner { background: linear-gradient(135deg, #475569 0%, #1E293B 100%); }
.p2av {
  width: 54px; height: 54px; border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  font-family: var(--font-display); font-size: .92rem; font-weight: 800;
  color: #fff; letter-spacing: -.5px;
  background: rgba(255,255,255,.16); border: 2.5px solid rgba(255,255,255,.30);
  box-shadow: 0 4px 16px rgba(0,0,0,.22); flex-shrink: 0; position: relative; z-index: 1;
}
.p2av.p2av-photo { background: #E2E8F0; padding: 0; overflow: hidden; border: 2.5px solid rgba(255,255,255,.50); }
.p2av.p2av-photo img {
  width: 100%; height: 100%; object-fit: cover; object-position: center 8%;
  transform: scale(1.35); transform-origin: center 20%; border-radius: 50%; display: block;
}
.p2-banner-info { flex: 1; min-width: 0; position: relative; z-index: 1; }
.p2-name { font-family: var(--font-head); font-size: 1rem; font-weight: 800; color: #fff; letter-spacing: -.3px; line-height: 1.2; }
.p2-role { font-size: .56rem; font-weight: 500; color: rgba(255,255,255,.60); letter-spacing: .4px; margin-top: 2px; }
.p2-share {
  display: inline-flex; align-items: center; gap: 5px; margin-top: 8px;
  background: rgba(255,255,255,.12); border: 1px solid rgba(255,255,255,.20);
  border-radius: 20px; padding: 3px 9px;
}
.p2-share-dot { width: 5px; height: 5px; border-radius: 50%; background: #2DD4BF; flex-shrink: 0; }
.p2-share-txt { font-size: .55rem; font-weight: 600; color: rgba(255,255,255,.90); letter-spacing: .3px; white-space: nowrap; }
.p2-body { padding: 14px 16px 6px; display: flex; flex-direction: column; gap: 0; flex: 1; }
.p2-section-lbl {
  font-size: .52rem; font-weight: 700; text-transform: uppercase; letter-spacing: 1px; color: var(--t4);
  margin: 10px 0 6px; display: flex; align-items: center; gap: 6px;
}
.p2-section-lbl::after { content: ''; flex: 1; height: 1px; background: #F1F5F9; }
.p2-body .p2-section-lbl:first-child { margin-top: 0; }
.p2-mgroup { display: grid; grid-template-columns: 1fr 1fr; gap: 1px; background: #F1F5F9; border-radius: 10px; overflow: hidden; border: 1px solid #F1F5F9; }
.p2-mrow { background: #fff; padding: 10px 12px 8px; display: flex; flex-direction: column; gap: 2px; transition: background .12s; }
.p2-mrow:hover { background: #F8FDFC; }
.p2-mrow.accent { background: #F0FDFA; }
.p2-mrow.accent:hover { background: #E6FBF8; }
.p2m-top { display: flex; align-items: center; gap: 4px; margin-bottom: 2px; }
.p2m-icon { font-size: .7rem; line-height: 1; }
.p2m-label { font-size: .5rem; font-weight: 700; text-transform: uppercase; letter-spacing: .9px; color: var(--t4); }
.p2m-val { font-family: var(--font-display); font-size: 1.15rem; font-weight: 800; color: var(--t1); letter-spacing: -.5px; line-height: 1; }
.pic2-card.other .p2m-val { color: var(--t2); }
.p2m-hint { font-size: .49rem; color: var(--t4); margin-top: 1px; line-height: 1.4; }
.p2-bar { height: 3px; background: #EEF2FF; border-radius: 10px; overflow: hidden; margin-top: 5px; }
.p2-bar-fill { height: 100%; border-radius: 10px; background: linear-gradient(90deg, #0D9488, #2DD4BF); transition: width .6s cubic-bezier(.4,0,.2,1); }
.p2-bar-fill.muted { background: linear-gradient(90deg, #0F766E, #0D9488); }
.pic2-card.other .p2-bar-fill, .pic2-card.other .p2-bar-fill.muted { background: linear-gradient(90deg, #94A3B8, #CBD5E1); }
.p2-margin-strip {
  display: grid; grid-template-columns: 1fr 1fr; gap: 1px;
  background: #E0F2FE; border-radius: 10px; border: 1px solid #BAE6FD; overflow: hidden; margin-top: 10px;
}
.pic2-card.other .p2-margin-strip { background: #F1F5F9; border-color: var(--border); }
.p2-margin-left, .p2-margin-right { background: #F0F9FF; padding: 9px 12px; }
.pic2-card.other .p2-margin-left, .pic2-card.other .p2-margin-right { background: #F8FAFC; }
.p2-margin-right { text-align: right; }
.p2-margin-lbl { font-size: .5rem; font-weight: 700; text-transform: uppercase; letter-spacing: .8px; color: #0369A1; }
.pic2-card.other .p2-margin-lbl { color: var(--t3); }
.p2-margin-val { font-family: var(--font-display); font-size: 1.05rem; font-weight: 800; letter-spacing: -.4px; line-height: 1.1; margin-top: 2px; color: var(--t1); }
.p2-footer { padding: 10px 16px 14px; border-top: 1px solid #F1F5F9; background: #FAFBFC; }
.p2-footer-lbl { font-size: .5rem; font-weight: 700; text-transform: uppercase; letter-spacing: 1px; color: var(--t4); margin-bottom: 6px; }
.p2-sup-row { display: flex; align-items: center; justify-content: space-between; gap: 8px; background: var(--primary-light); border: 1px solid var(--primary-mid); border-radius: 8px; padding: 7px 11px; }
.pic2-card.other .p2-sup-row { background: #F1F5F9; border-color: var(--border); }
.p2-sup-name { font-size: .62rem; font-weight: 600; color: var(--primary2); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1; }
.pic2-card.other .p2-sup-name { color: var(--t3); }
.p2-sup-rn { font-size: .57rem; font-weight: 700; color: var(--primary); background: #fff; padding: 2px 8px; border-radius: 12px; border: 1px solid var(--primary-mid); flex-shrink: 0; }
.pic2-card.other .p2-sup-rn { color: var(--t4); border-color: var(--border); }
.p2-no-sup { font-size: .58rem; color: var(--t4); font-style: italic; }

/* ══ CARDS & TABLE ══ */
.gcard {
  background:#fff; border:1px solid var(--border); border-radius:var(--r2);
  padding:20px 20px 16px; transition:box-shadow .15s; position:relative; overflow:hidden;
  margin-bottom:16px; box-shadow:var(--shadow-sm);
}
.gcard:hover { box-shadow:var(--shadow); }
.norm-bar {
  display:flex; align-items:center; gap:6px; flex-wrap:wrap;
  margin-bottom:20px; padding:9px 14px; background:#fff;
  border:1px solid var(--border); border-radius:var(--r); box-shadow:var(--shadow-sm);
}
.norm-cap { font-size:.56rem; font-weight:600; color:var(--t4); text-transform:uppercase; letter-spacing:1.5px; margin-right:6px; }
.npill { font-size:.6rem; font-weight:500; padding:3px 11px; border-radius:20px; background:var(--bg); border:1px solid var(--border); color:var(--t4); transition:all .15s; }
.npill.on { background:var(--primary-light); border-color:var(--primary-mid); color:var(--primary2); }
[data-testid="stDataFrame"] {
  border:1px solid var(--border)!important; border-radius:var(--r)!important;
  overflow:hidden!important; background:#fff!important; box-shadow:var(--shadow-sm)!important;
}
[data-testid="stDataFrame"] th {
  background:#FAFAFA!important; font-size:.64rem!important; font-weight:600!important;
  color:var(--t4)!important; letter-spacing:.3px!important; text-transform:uppercase!important;
  border-bottom:1px solid var(--border)!important;
}
[data-testid="stDataFrame"] td { font-size:.71rem!important; color:var(--t2)!important; border-color:var(--border)!important; }
[data-testid="stDataFrame"] tr:hover td { background:#F8FAFC!important; }
[data-testid="stAlert"] {
  background:var(--primary-light)!important; border:1px solid var(--primary-mid)!important;
  border-left:3px solid var(--primary)!important; border-radius:var(--r)!important;
  font-size:.71rem!important; color:var(--primary2)!important;
}
hr { border-color:var(--border)!important; margin:24px 0!important; }
.stSpinner > div { border-top-color:var(--primary)!important; }
div[data-testid="stCaption"] p { color:var(--t3)!important; font-size:.67rem!important; }
@keyframes fadeSlideUp { from{opacity:0;transform:translateY(16px)} to{opacity:1;transform:translateY(0)} }
@keyframes fadeIn { from{opacity:0} to{opacity:1} }
@keyframes scaleIn { from{opacity:0;transform:scale(.97)} to{opacity:1;transform:scale(1)} }
.sc-hero,.sc-strip { animation:fadeSlideUp .4s ease both; }
.gcard    { animation:fadeSlideUp .45s ease .1s both; }
.norm-bar { animation:fadeIn .35s ease both; }
.gsec     { animation:fadeIn .3s ease both; }
[data-testid="stTabs"] { animation:scaleIn .4s ease .06s both; }
[data-testid="stDivider"] { border-color:var(--border)!important; }
</style>
""", unsafe_allow_html=True)

# ── Margin fix JS ─────────────────────────────────────────────────────────────
st.markdown("""
<script>
(function applyMargin() {
  const MARGIN = "38px";
  const selectors = [
    '[data-testid="stMainBlockContainer"]','[data-testid="block-container"]',
    '.main .block-container','.block-container',
  ];
  function fix() {
    selectors.forEach(sel => {
      document.querySelectorAll(sel).forEach(el => {
        el.style.setProperty('padding-left',  MARGIN, 'important');
        el.style.setProperty('padding-right', MARGIN, 'important');
        el.style.setProperty('max-width', '100%', 'important');
      });
    });
  }
  fix();
  new MutationObserver(fix).observe(document.body, {childList:true, subtree:true});
})();
</script>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="ghdr">
  <div class="ghdr-brand">
    <div class="ghdr-logo">
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#0D9488" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/>
        <polyline points="9 22 9 12 15 12 15 22"/>
      </svg>
    </div>
    <div>
      <div class="ghdr-name">Hotel <span>Intelligence</span></div>
      <div class="ghdr-sub">MTT &nbsp;·&nbsp; Opsifin Platform &nbsp;·&nbsp; Travel Analytics</div>
    </div>
  </div>
  <div class="ghdr-right">
    <span class="ghdr-pill">v9.2</span>
    <div class="ghdr-live"><span class="ghdr-dot"></span>Live</div>
  </div>
</div>
<div class="gticker">
  <div class="gticker-track">
    <span class="t-item hi">Hotel Intelligence Platform</span><span class="tsep">·</span>
    <span class="t-item">Invoice &amp; Supplier Analytics</span><span class="tsep">·</span>
    <span class="t-item hi">Performance Agent Dashboard</span><span class="tsep">·</span>
    <span class="t-item">Supplier Category Intelligence</span><span class="tsep">·</span>
    <span class="t-item hi">MTT Travel Analytics</span><span class="tsep">·</span>
    <span class="t-item">Google Drive Sync &nbsp;·&nbsp; v9.2 &nbsp;·&nbsp; 2025</span><span class="tsep">·</span>
    <span class="t-item hi">Hotel Intelligence Platform</span><span class="tsep">·</span>
    <span class="t-item">Invoice &amp; Supplier Analytics</span><span class="tsep">·</span>
    <span class="t-item hi">Performance Agent Dashboard</span><span class="tsep">·</span>
    <span class="t-item">Supplier Category Intelligence</span><span class="tsep">·</span>
    <span class="t-item hi">MTT Travel Analytics</span><span class="tsep">·</span>
    <span class="t-item">Google Drive Sync &nbsp;·&nbsp; v9.2 &nbsp;·&nbsp; 2025</span><span class="tsep">·</span>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Sidebar (dengan file_uploader — HANYA di sini, satu kali) ─────────────────
# BUG FIX: Sidebar didefinisikan SEBELUM membaca session state upload,
# agar widget ter-render lebih dulu dan key "main_upload" sudah terdaftar.
with st.sidebar:
    st.markdown("""
    <div class="sb-top">
      <div class="sb-logo">
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="#0D9488" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/>
          <polyline points="9 22 9 12 15 12 15 22"/>
        </svg>
      </div>
      <div>
        <div class="sb-appname">Hotel <span>Report</span></div>
        <div class="sb-ver">Opsifin · MTT · v9.2</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="sb-section">Data Utama</div>', unsafe_allow_html=True)

    # ── BUG FIX: Gunakan label kosong "" dan label_visibility="collapsed"
    # agar tidak ada teks label yang muncul ganda.
    # Tambahan: key unik dan stabil agar tidak re-render dobel.
    st.file_uploader(
        "Upload",                      # label pendek — disembunyikan via CSS
        type=["xlsx"],
        accept_multiple_files=True,
        key="main_upload",
        label_visibility="collapsed",  # sembunyikan label bawaan Streamlit
        help="Upload file Excel Custom Report (.xlsx) · maks 200MB per file"
    )

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

    st.markdown('<div class="sb-divider"></div><div class="sb-section">Filter Data</div>', unsafe_allow_html=True)
    if "df_raw" in st.session_state:
        _r = st.session_state["df_raw"]
        if "Issued_Year" in _r.columns:
            yr = sorted(_r["Issued_Year"].dropna().unique().tolist())
            st.multiselect("Tahun", yr, key="f_years")
        if "Inv Date" in _r.columns:
            _imin = _r["Inv Date"].min().date(); _imax = _r["Inv Date"].max().date()
            _id_raw = st.session_state.get("f_inv")
            if _id_raw and hasattr(_id_raw,"__len__") and len(_id_raw)==2:
                try: _id = [max(_id_raw[0],_imin), min(_id_raw[1],_imax)]
                except: _id = [_imin, _imax]
            else: _id = [_imin, _imax]
            st.date_input("Periode Inv Date", value=_id, key="f_inv", min_value=_imin, max_value=_imax)
        if "Check In" in _r.columns and "Check Out" in _r.columns:
            _cmin = _r["Check In"].min().date(); _cmax = _r["Check Out"].max().date()
            _cd_raw = st.session_state.get("f_ci")
            if _cd_raw and hasattr(_cd_raw,"__len__") and len(_cd_raw)==2:
                try: _cd = [max(_cd_raw[0],_cmin), min(_cd_raw[1],_cmax)]
                except: _cd = [_cmin, _cmax]
            else: _cd = [_cmin, _cmax]
            st.date_input("Check In – Check Out", value=_cd, key="f_ci", min_value=_cmin, max_value=_cmax)
    else:
        st.caption("Upload file untuk mengaktifkan filter.")

    st.markdown("""
    <div style="padding:16px 18px 14px; border-top:1px solid #E2E8F0; margin-top:16px;">
      <div style="font-size:.57rem; color:#94A3B8; font-family:'Inter',sans-serif; line-height:2; letter-spacing:.2px;">
        Hotel Intelligence &nbsp;<span style="color:#0D9488;font-weight:600;">v9.2</span> · 2025<br>
        Rifyal Tumber · MTT
      </div>
    </div>
    """, unsafe_allow_html=True)

# ── Build df_raw SETELAH sidebar (widget sudah terdaftar) ─────────────────────
# BUG FIX: Pindahkan pemrosesan upload ke SETELAH blok sidebar,
# sehingga "main_upload" sudah ada di session_state saat dibaca.
_up_raw = st.session_state.get("main_upload") or []
_up     = [f for f in _up_raw if _is_valid_file(f)]

if _up:
    _h  = compute_upload_hash(_up)
    _nm = st.session_state.get("norm_maps", {})
    if _h and (st.session_state.get("upload_hash") != _h or "df_raw" not in st.session_state):
        with st.spinner("Memproses & menormalisasi data..."):
            st.session_state["df_raw"]      = build_df_raw(_up, _nm)
            st.session_state["upload_hash"] = _h
else:
    if not _up_raw:
        for k in ["df_raw","upload_hash"]: st.session_state.pop(k, None)

# ── Main ──────────────────────────────────────────────────────────────────────
uploaded_files = [f for f in (st.session_state.get("main_upload") or []) if _is_valid_file(f)]

if uploaded_files and "df_raw" in st.session_state:
    df_raw = st.session_state["df_raw"]

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

    ss2 = st.session_state.get("sync_state",{})
    pm  = {"Hotel Chain":ss2.get("hotel_chain")=="ok","Hotel City":ss2.get("hotel_city")=="ok",
           "Hotel Name":ss2.get("hotel_name")=="ok","Supplier":ss2.get("hotel_supplier")=="ok",
           "Supplier Cat":ss2.get("supplier_category")=="ok"}
    ph  = " ".join(f'<span class="npill {"on" if v else ""}">{k}</span>' for k,v in pm.items())
    st.markdown(f'<div class="norm-bar"><span class="norm-cap">Norm</span>{ph}</div>', unsafe_allow_html=True)

    tab1,tab2,tab3,tab4,tab5,tab6,tab7 = st.tabs([
        "Summary","Tren Invoice","Supplier",
        "Product Type","Agent","PTM Corp","Kategori",
    ])

    # ═══════════════════════════════════════════════════════════════
    # TAB 1 — Summary
    # ═══════════════════════════════════════════════════════════════
    with tab1:
        # ── Compute KPIs ─────────────────────────────────────────────
        tr  = len(df_view)
        tc  = len(df_view.columns)
        ui  = df_view["Invoice No"].nunique()                    if "Invoice No"       in df_view.columns else None
        rn  = int(np.ceil(df_view["Total Room Night"].sum()))    if "Total Room Night" in df_view.columns else None
        sa  = df_view["Sales AR"].fillna(0).astype(float).sum() if "Sales AR"         in df_view.columns else None
        up  = df_view["Full Name"].dropna().nunique()            if "Full Name"        in df_view.columns else None

        pm_val = None
        if "Profit" in df_view.columns and "Sales AR" in df_view.columns:
            _p = df_view["Profit"].fillna(0).astype(float)
            _s = df_view["Sales AR"].fillna(0).astype(float)
            _m = _s != 0
            pm_val = float((_p[_m] / _s[_m] * 100).mean()) if _m.any() else 0.0

        aging_val = None
        if "Check In" in df_view.columns and "Inv Date" in df_view.columns:
            _ag = df_view.dropna(subset=["Check In","Inv Date"]).copy()
            _ag["_aging"] = (_ag["Check In"] - _ag["Inv Date"]).dt.days
            _ag_pos = _ag[_ag["_aging"] >= 0]
            if not _ag_pos.empty:
                aging_val = float(_ag_pos["_aging"].mean())

        tot_supplier = df_view["Supplier_Name"].dropna().nunique() if "Supplier_Name" in df_view.columns else None
        tot_hotel    = df_view["Hotel_Name"].dropna().nunique()    if "Hotel_Name"    in df_view.columns else None
        tot_city     = df_view["Hotel_City"].dropna().nunique()    if "Hotel_City"    in df_view.columns else None
        _pic_col     = next((c for c in df_view.columns if "agent" in c.lower() or "handler" in c.lower()), None)
        tot_pic      = df_view[_pic_col].dropna().nunique() if _pic_col else None

        prev = get_prev_period_metrics(df_raw, df_view)

        # ── Badge helpers ─────────────────────────────────────────────
        def _badge(curr, prev_val, reverse=False, size="normal"):
            _neu_sm  = ('<span style="display:inline-flex;align-items:center;gap:3px;'
                        'font-size:.53rem;font-weight:600;padding:2px 7px;border-radius:20px;margin-top:2px;'
                        'background:#F8FAFC;color:#94A3B8;border:1px solid #E2E8F0;">'
                        '── No ref</span>')
            _neu_lg  = ('<span style="display:inline-flex;align-items:center;gap:4px;'
                        'font-size:.58rem;font-weight:600;padding:3px 9px;border-radius:20px;'
                        'background:#F8FAFC;color:#94A3B8;border:1px solid #E2E8F0;">'
                        '── Belum ada data periode sebelumnya</span>')
            try:
                if curr is None: return _neu_sm if size == "sm" else _neu_lg
                c = float(str(curr).replace(",","").replace("%","")) if isinstance(curr,str) else float(curr)
                if prev_val is None or prev_val == 0:
                    return _neu_sm if size == "sm" else _neu_lg
                p   = float(prev_val)
                if p == 0: return _neu_sm if size == "sm" else _neu_lg
                pct   = (c - p) / abs(p) * 100
                is_up = pct > 0
                if reverse: is_up = not is_up
                arr     = "▲" if pct > 0 else "▼"
                abs_pct = f"{abs(pct):.1f}%"
                _bg  = "#ECFDF5" if is_up else "#FFF1F2"
                _col = "#059669" if is_up else "#DC2626"
                _bdr = "#A7F3D0" if is_up else "#FECACA"
                if size == "sm":
                    return (f'<span style="display:inline-flex;align-items:center;gap:3px;'
                            f'font-size:.53rem;font-weight:700;padding:2px 7px;border-radius:20px;margin-top:2px;'
                            f'background:{_bg};color:{_col};border:1px solid {_bdr};">'
                            f'{arr} {abs_pct}</span>')
                return (f'<span style="display:inline-flex;align-items:center;gap:4px;'
                        f'font-size:.58rem;font-weight:700;padding:3px 9px;border-radius:20px;'
                        f'background:{_bg};color:{_col};border:1px solid {_bdr};">'
                        f'{arr} {abs_pct} vs periode sebelumnya</span>')
            except:
                return _neu_sm if size == "sm" else _neu_lg

        def _counter(value, suffix="", prefix=""):
            if value is None: return "N/A"
            if isinstance(value, float) and value >= 1e9:
                v_disp = f"{value/1e9:.1f}"; suf = "B" + suffix
            elif isinstance(value, float) and value >= 1e6:
                v_disp = f"{value/1e6:.1f}"; suf = "M" + suffix
            elif (isinstance(value,(int,float))) and float(value) >= 1000:
                v_disp = f"{float(value)/1000:.1f}"; suf = "K" + suffix
            elif isinstance(value, float):
                v_disp = f"{value:.1f}"; suf = suffix
            else:
                v_disp = str(int(value)); suf = suffix
            return (f'<span data-counter="{v_disp}" data-suffix="{suf}" data-prefix="{prefix}" '
                    f'style="font-family:\'Sora\',var(--font-display),sans-serif;">{prefix}{v_disp}{suf}</span>')

        # ── Comparison banner ─────────────────────────────────────────
        _has_prev = bool(prev)
        if _has_prev:
            _prev_min_lbl = prev.get("prev_min","")
            _prev_max_lbl = prev.get("prev_max","")
            _prev_rows    = prev.get("rows",0)
            _date_range   = f" &nbsp;·&nbsp; {_prev_min_lbl} – {_prev_max_lbl}" if _prev_min_lbl else ""
            _prev_html = (
                '<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;'
                'padding:8px 14px;background:#fff;border:1px solid #E2E8F0;'
                'border-radius:10px;margin-bottom:20px;box-shadow:0 1px 2px rgba(0,0,0,.04);">'
                '<span style="width:7px;height:7px;border-radius:50%;background:#0D9488;flex-shrink:0;display:inline-block;"></span>'
                f'<span style="font-size:.58rem;color:#0F766E;font-weight:600;">'
                f'✓ Perbandingan aktif{_date_range}&nbsp;·&nbsp;{_prev_rows:,} baris'
                '</span></div>'
            )
        else:
            _prev_html = (
                '<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;'
                'padding:8px 14px;background:#F8FAFC;border:1px solid #E2E8F0;'
                'border-radius:10px;margin-bottom:20px;">'
                '<span style="font-size:.58rem;color:#94A3B8;font-weight:600;">'
                'ⓘ Atur filter periode untuk mengaktifkan perbandingan vs periode sebelumnya'
                '</span></div>'
            )
        st.markdown(_prev_html, unsafe_allow_html=True)

        # ── Hero KPI cards ────────────────────────────────────────────
        b_ui = _badge(ui,     prev.get("ui"))
        b_sa = _badge(sa,     prev.get("sa"))
        b_pm = _badge(pm_val, prev.get("pm")) if pm_val is not None else ""

        def _hero(icon, label, value_html, sub, badge_html, accent="linear-gradient(90deg,#0D9488,#134E4A)"):
            return (
                f'<div style="background:#fff;border:1px solid #E2E8F0;border-radius:14px;'
                f'padding:0;position:relative;overflow:hidden;transition:box-shadow .2s,transform .2s;cursor:default;"'
                f' onmouseover="this.style.transform=\'translateY(-3px)\';this.style.boxShadow=\'0 12px 32px -6px rgba(13,148,136,.16),0 2px 8px rgba(0,0,0,.05)\'"'
                f' onmouseout="this.style.transform=\'\';this.style.boxShadow=\'\'">'
                f'<div style="height:3px;background:{accent};border-radius:14px 14px 0 0;"></div>'
                f'<div style="position:absolute;bottom:-20px;right:-16px;width:88px;height:88px;border-radius:50%;'
                f'background:radial-gradient(circle,rgba(13,148,136,.07) 0%,transparent 70%);pointer-events:none;z-index:0;"></div>'
                f'<div style="padding:18px 20px 16px;position:relative;z-index:1;">'
                f'<div style="width:38px;height:38px;border-radius:10px;display:grid;place-items:center;'
                f'margin-bottom:12px;font-size:1.1rem;background:#F0FDFA;border:1px solid #CCFBF1;">{icon}</div>'
                f'<div style="font-size:.55rem;font-weight:700;letter-spacing:1.2px;text-transform:uppercase;'
                f'color:#94A3B8;margin-bottom:6px;font-family:\'Inter\',sans-serif;">{label}</div>'
                f'<div style="font-size:2rem;font-weight:800;line-height:1;letter-spacing:-1.5px;'
                f'color:#0F172A;margin-bottom:4px;">{value_html}</div>'
                f'<div style="font-size:.58rem;color:#94A3B8;margin-bottom:10px;line-height:1.5;">{sub}</div>'
                f'{badge_html}'
                f'</div></div>'
            )

        sa_display = sa if sa is not None else 0

        st.markdown(
            f'<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:14px;">'
            + _hero("📄", "Invoice Unik",    _counter(int(ui) if ui else 0), "Total transaksi invoice unik", b_ui)
            + _hero("💰", "Sales AR",         _counter(sa_display), "Total nilai penjualan (IDR)", b_sa,
                    "linear-gradient(90deg,#134E4A,#0D9488)")
            + _hero("📈", "Avg Profit Margin",
                    (f'<span style="font-family:\'Sora\',sans-serif;">{pm_val:.1f}%</span>'
                     if pm_val is not None else "N/A"),
                    "Rata-rata margin keuntungan", b_pm,
                    "linear-gradient(90deg,#0D9488,#2DD4BF)")
            + '</div>',
            unsafe_allow_html=True)

        # ── Volume strip ──────────────────────────────────────────────
        b_rn = _badge(rn, prev.get("rn"), size="sm")
        b_up = _badge(up, prev.get("up"), size="sm")

        def _cell(label, value_html, hint, badge=""):
            return (
                f'<div style="padding:14px 18px 12px;border-right:1px solid #E2E8F0;'
                f'transition:background .12s;cursor:default;position:relative;overflow:hidden;"'
                f' onmouseover="this.style.background=\'#F8FDFC\';this.querySelector(\'.cell-bar\').style.height=\'100%\'"'
                f' onmouseout="this.style.background=\'\';this.querySelector(\'.cell-bar\').style.height=\'0%\'">'
                f'<div class="cell-bar" style="position:absolute;top:0;left:0;width:2px;height:0;'
                f'background:#0D9488;transition:height .2s;border-radius:0 2px 2px 0;"></div>'
                f'<span style="font-size:.53rem;font-weight:700;letter-spacing:.9px;text-transform:uppercase;'
                f'color:#94A3B8;margin-bottom:5px;display:block;font-family:\'Inter\',sans-serif;">{label}</span>'
                f'<span style="font-family:\'Sora\',sans-serif;font-size:1.3rem;font-weight:800;color:#0F172A;'
                f'letter-spacing:-.5px;display:block;margin-bottom:3px;line-height:1;">{value_html}</span>'
                f'<span style="font-size:.54rem;color:#94A3B8;display:block;line-height:1.5;margin-bottom:4px;">{hint}</span>'
                f'{badge}</div>'
            )

        rn_html    = _counter(int(rn)   if rn else 0) if rn is not None else "N/A"
        up_html    = _counter(int(up)   if up else 0) if up is not None else "N/A"
        aging_html = (f'<span style="font-family:\'Sora\',sans-serif;">{aging_val:.1f} hari</span>'
                     ) if aging_val is not None else "N/A"
        tr_html    = _counter(float(tr))

        st.markdown(
            '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:0;background:#fff;'
            'border:1px solid #E2E8F0;border-radius:12px;overflow:hidden;'
            'box-shadow:0 1px 3px rgba(0,0,0,.05);margin-bottom:14px;">'
            '<div style="grid-column:1/-1;padding:8px 16px 6px;font-size:.53rem;font-weight:700;'
            'letter-spacing:1.1px;text-transform:uppercase;color:#94A3B8;'
            'border-bottom:1px solid #E2E8F0;background:#FAFBFC;display:flex;align-items:center;gap:6px;">'
            '<span style="width:5px;height:5px;border-radius:50%;background:#0D9488;display:inline-block;"></span>'
            'Volume &amp; Trafik</div>'
            + _cell("Room Night",       rn_html,    "Total malam kamar",     b_rn)
            + _cell("Pax Unik",         up_html,    "Nama tamu unik",        b_up)
            + _cell("Avg Aging Invoice", aging_html, "Check In − Inv Date")
            + _cell("Total Baris",      tr_html,    "Baris data aktif")
            + '</div>',
            unsafe_allow_html=True)

        # ── Master data strip ─────────────────────────────────────────
        def _cell_last(label, value_html, hint):
            return (
                f'<div style="padding:14px 18px 12px;border-right:1px solid #E2E8F0;'
                f'transition:background .12s;cursor:default;"'
                f' onmouseover="this.style.background=\'#F8FDFC\'"'
                f' onmouseout="this.style.background=\'\'">'
                f'<span style="font-size:.53rem;font-weight:700;letter-spacing:.9px;text-transform:uppercase;'
                f'color:#94A3B8;margin-bottom:5px;display:block;">{label}</span>'
                f'<span style="font-family:\'Sora\',sans-serif;font-size:1.3rem;font-weight:800;color:#0F172A;'
                f'letter-spacing:-.5px;display:block;margin-bottom:3px;line-height:1;">{value_html}</span>'
                f'<span style="font-size:.54rem;color:#94A3B8;display:block;">{hint}</span>'
                f'</div>'
            )

        st.markdown(
            '<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:0;background:#fff;'
            'border:1px solid #E2E8F0;border-radius:12px;overflow:hidden;'
            'box-shadow:0 1px 3px rgba(0,0,0,.05);margin-bottom:20px;">'
            '<div style="grid-column:1/-1;padding:8px 16px 6px;font-size:.53rem;font-weight:700;'
            'letter-spacing:1.1px;text-transform:uppercase;color:#94A3B8;'
            'border-bottom:1px solid #E2E8F0;background:#FAFBFC;display:flex;align-items:center;gap:6px;">'
            '<span style="width:5px;height:5px;border-radius:50%;background:#0D9488;display:inline-block;"></span>'
            'Master Data</div>'
            + _cell_last("Total Supplier", _counter(int(tot_supplier)) if tot_supplier else "N/A", "Supplier unik")
            + _cell_last("Total Hotel",    _counter(int(tot_hotel))    if tot_hotel    else "N/A", "Hotel unik")
            + _cell_last("Total City",     _counter(int(tot_city))     if tot_city     else "N/A", "Kota hotel unik")
            + _cell_last("Total PIC",      _counter(int(tot_pic))      if tot_pic      else "N/A", "Agent / Handler unik")
            + _cell_last("Kolom Aktif",    _counter(int(tc)),                                       "Field tersedia")
            + '</div>',
            unsafe_allow_html=True)

        # ── Tren charts row 1 ─────────────────────────────────────────
        if "Issued Date" in df_view.columns and "Invoice No" in df_view.columns:
            _dt = df_view.dropna(subset=["Issued Date","Invoice No"]).copy()
            _dt["_mon_label"] = _dt["Issued Date"].dt.strftime("%b %Y")
            _dt["_mon_num"]   = _dt["Issued Date"].dt.to_period("M").apply(lambda p: p.ordinal)
            _ti = (_dt.groupby(["_mon_label","_mon_num"], as_index=False)["Invoice No"]
                      .nunique().rename(columns={"Invoice No":"Invoice Unik"})
                      .sort_values("_mon_num"))
            _ti["Invoice Unik"] = pd.to_numeric(_ti["Invoice Unik"], errors="coerce").fillna(0).astype(int)
            has_rn = "Total Room Night" in df_view.columns
            if has_rn:
                _tr_s = (_dt.groupby(["_mon_label","_mon_num"], as_index=False)["Total Room Night"]
                            .sum().sort_values("_mon_num"))
                _tr_s["Total Room Night"] = pd.to_numeric(_tr_s["Total Room Night"], errors="coerce").fillna(0)

            def _mini_stats(items):
                parts = []
                for lbl, val in items:
                    parts.append(
                        f'<div style="display:flex;flex-direction:column;gap:1px;">'
                        f'<span style="font-size:.5rem;font-weight:700;text-transform:uppercase;'
                        f'letter-spacing:.8px;color:#94A3B8;">{lbl}</span>'
                        f'<span style="font-family:\'Sora\',sans-serif;font-size:.88rem;font-weight:800;'
                        f'color:#0F172A;letter-spacing:-.3px;">{val}</span>'
                        f'</div>'
                    )
                return (
                    '<div style="display:flex;gap:16px;margin-bottom:12px;padding-bottom:12px;'
                    'border-bottom:1px solid #F1F5F9;">' + "".join(parts) + '</div>'
                )

            gsec("📈 Tren Bulanan")

            ct1, ct2 = st.columns(2)
            with ct1:
                _ti_total = int(_ti["Invoice Unik"].sum())
                _ti_peak  = _ti.loc[_ti["Invoice Unik"].idxmax()]
                _ti_avg   = int(_ti["Invoice Unik"].mean())
                st.markdown(
                    '<div style="background:#fff;border:1px solid #E2E8F0;border-radius:12px;'
                    'padding:16px 18px 4px;box-shadow:0 1px 3px rgba(0,0,0,.05);">'
                    '<div style="font-family:\'DM Sans\',sans-serif;font-size:.72rem;font-weight:700;color:#0F172A;margin-bottom:3px;">📄 Tren Invoice Bulanan</div>'
                    '<div style="font-size:.57rem;color:#94A3B8;margin-bottom:10px;">Invoice unik per bulan</div>'
                    + _mini_stats([
                        ("Total", compact_num(_ti_total)),
                        ("Peak",  f'{_ti_peak["_mon_label"]} · {compact_num(int(_ti_peak["Invoice Unik"]))}'),
                        ("Avg/bln", compact_num(_ti_avg)),
                    ]) + '</div>', unsafe_allow_html=True)
                _fig_ti = go.Figure()
                _ti_max    = _ti["Invoice Unik"].max()
                _ti_thresh = max(1, _ti_max * 0.05)
                _ti_labels = _ti["Invoice Unik"].apply(lambda v: f"{int(v):,}" if v >= _ti_thresh else "")
                _fig_ti.add_trace(go.Scatter(
                    x=_ti["_mon_label"], y=_ti["Invoice Unik"],
                    mode="lines+markers+text", name="Invoice Unik",
                    text=_ti_labels, textposition="top center",
                    textfont=dict(size=10, color="#0D9488", family="Open Sans"),
                    line=dict(color="#0D9488", width=2.5, shape="spline"),
                    marker=dict(size=8, color="#0D9488", line=dict(width=2, color="rgba(13,148,136,.3)")),
                    fill="tozeroy", fillcolor="rgba(13,148,136,.08)",
                    hovertemplate="<b>%{x}</b><br>Invoice Unik: <b>%{y:,.0f}</b><extra></extra>",
                    cliponaxis=False,
                ))
                _max_ti = _ti.loc[_ti["Invoice Unik"].idxmax()]
                _fig_ti.add_annotation(
                    x=_max_ti["_mon_label"], y=_max_ti["Invoice Unik"],
                    text=f"▲ Peak: {int(_max_ti['Invoice Unik']):,}",
                    showarrow=True, arrowhead=2, arrowcolor="#0D9488", arrowsize=.8, ax=0, ay=-32,
                    font=dict(size=10, color="#0D9488", family="Open Sans"),
                    bgcolor="rgba(240,253,250,.9)", bordercolor="#0D9488", borderwidth=1, borderpad=4)
                _fig_ti.update_layout(hovermode="x unified", height=280,
                    xaxis=dict(tickangle=-30, showline=False), yaxis_title="", xaxis_title="",
                    showlegend=False, margin=dict(l=8, r=8, t=12, b=8))
                st.plotly_chart(theme(_fig_ti), use_container_width=True)

            with ct2:
                if has_rn:
                    _rn_total = int(_tr_s["Total Room Night"].sum())
                    _rn_peak  = _tr_s.loc[_tr_s["Total Room Night"].idxmax()]
                    _rn_avg   = int(_tr_s["Total Room Night"].mean())
                    st.markdown(
                        '<div style="background:#fff;border:1px solid #E2E8F0;border-radius:12px;'
                        'padding:16px 18px 4px;box-shadow:0 1px 3px rgba(0,0,0,.05);">'
                        '<div style="font-family:\'DM Sans\',sans-serif;font-size:.72rem;font-weight:700;color:#0F172A;margin-bottom:3px;">🌙 Tren Room Night Bulanan</div>'
                        '<div style="font-size:.57rem;color:#94A3B8;margin-bottom:10px;">Total room night per bulan</div>'
                        + _mini_stats([
                            ("Total", compact_num(_rn_total)),
                            ("Peak",  f'{_rn_peak["_mon_label"]} · {compact_num(int(_rn_peak["Total Room Night"]))}'),
                            ("Avg/bln", compact_num(_rn_avg)),
                        ]) + '</div>', unsafe_allow_html=True)
                    _fig_rn = go.Figure()
                    _rn_max    = _tr_s["Total Room Night"].max()
                    _rn_thresh = max(1, _rn_max * 0.05)
                    _rn_labels = _tr_s["Total Room Night"].apply(lambda v: f"{int(v):,}" if v >= _rn_thresh else "")
                    _fig_rn.add_trace(go.Scatter(
                        x=_tr_s["_mon_label"], y=_tr_s["Total Room Night"],
                        mode="lines+markers+text", name="Room Night",
                        text=_rn_labels, textposition="top center",
                        textfont=dict(size=10, color="#0D9488", family="Open Sans"),
                        line=dict(color="#0D9488", width=2.5, shape="spline"),
                        marker=dict(size=8, color="#2DD4BF", line=dict(width=2, color="rgba(13,148,136,.3)")),
                        fill="tozeroy", fillcolor="rgba(13,148,136,.08)",
                        hovertemplate="<b>%{x}</b><br>Room Night: <b>%{y:,.0f}</b><extra></extra>",
                        cliponaxis=False,
                    ))
                    _max_rn = _tr_s.loc[_tr_s["Total Room Night"].idxmax()]
                    _fig_rn.add_annotation(
                        x=_max_rn["_mon_label"], y=_max_rn["Total Room Night"],
                        text=f"▲ Peak: {int(_max_rn['Total Room Night']):,}",
                        showarrow=True, arrowhead=2, arrowcolor="#0D9488", arrowsize=.8, ax=0, ay=-32,
                        font=dict(size=10, color="#0D9488", family="Open Sans"),
                        bgcolor="rgba(240,253,250,.9)", bordercolor="#0D9488", borderwidth=1, borderpad=4)
                    _fig_rn.update_layout(hovermode="x unified", height=280,
                        xaxis=dict(tickangle=-30, showline=False), yaxis_title="", xaxis_title="",
                        showlegend=False, margin=dict(l=8, r=8, t=12, b=8))
                    st.plotly_chart(theme(_fig_rn), use_container_width=True)
                else:
                    st.info("Kolom Total Room Night tidak tersedia.")

            # ── Tren row 2: Profit + Kota ─────────────────────────────
            ct3, ct4 = st.columns(2)
            with ct3:
                _has_profit = "Profit" in df_view.columns and "Issued Date" in df_view.columns
                if _has_profit:
                    _dt_pr = (df_view.dropna(subset=["Issued Date"])
                                     .assign(Profit=lambda d: pd.to_numeric(d["Profit"], errors="coerce").fillna(0))
                                     .copy())
                    _dt_pr["_mon_label"] = _dt_pr["Issued Date"].dt.strftime("%b %Y")
                    _dt_pr["_mon_num"]   = _dt_pr["Issued Date"].dt.to_period("M").apply(lambda p: p.ordinal)
                    _pr_s = (_dt_pr.groupby(["_mon_label","_mon_num"], as_index=False)["Profit"]
                                   .sum().sort_values("_mon_num"))
                    _pr_s["Profit"] = pd.to_numeric(_pr_s["Profit"], errors="coerce").fillna(0)
                    _pr_total = float(_pr_s["Profit"].sum())
                    _pr_peak  = _pr_s.loc[_pr_s["Profit"].idxmax()] if not _pr_s.empty else None
                    st.markdown(
                        '<div style="background:#fff;border:1px solid #E2E8F0;border-radius:12px;'
                        'padding:16px 18px 4px;box-shadow:0 1px 3px rgba(0,0,0,.05);">'
                        '<div style="font-family:\'DM Sans\',sans-serif;font-size:.72rem;font-weight:700;color:#0F172A;margin-bottom:3px;">💹 Tren Profit Bulanan</div>'
                        '<div style="font-size:.57rem;color:#94A3B8;margin-bottom:10px;">Total profit per bulan · IDR</div>'
                        + _mini_stats([
                            ("Total Profit", compact_num(_pr_total)),
                            ("Margin avg",   f"{pm_val:.1f}%" if pm_val is not None else "—"),
                            ("Peak",         _pr_peak["_mon_label"] if _pr_peak is not None else "—"),
                        ]) + '</div>', unsafe_allow_html=True)
                    _pr_max    = _pr_s["Profit"].abs().max()
                    _pr_thresh = max(1, _pr_max * 0.05)
                    _pr_labels = _pr_s["Profit"].apply(lambda v: compact_num(v) if abs(v) >= _pr_thresh else "")
                    _fig_pr = go.Figure()
                    _fig_pr.add_trace(go.Scatter(
                        x=_pr_s["_mon_label"], y=_pr_s["Profit"],
                        mode="lines+markers+text", name="Profit",
                        text=_pr_labels, textposition="top center",
                        textfont=dict(size=10, color="#134E4A", family="Open Sans"),
                        line=dict(color="#134E4A", width=2.5, shape="spline"),
                        marker=dict(size=8, color="#2DD4BF", line=dict(width=2, color="rgba(19,78,74,.3)")),
                        fill="tozeroy", fillcolor="rgba(19,78,74,.07)",
                        hovertemplate="<b>%{x}</b><br>Profit: <b>%{y:,.0f}</b><extra></extra>",
                        cliponaxis=False,
                    ))
                    if not _pr_s.empty and _pr_s["Profit"].max() > 0:
                        _max_pr = _pr_s.loc[_pr_s["Profit"].idxmax()]
                        _fig_pr.add_annotation(
                            x=_max_pr["_mon_label"], y=_max_pr["Profit"],
                            text=f"▲ Peak: {compact_num(_max_pr['Profit'])}",
                            showarrow=True, arrowhead=2, arrowcolor="#134E4A", arrowsize=.8, ax=0, ay=-32,
                            font=dict(size=10, color="#134E4A", family="Open Sans"),
                            bgcolor="rgba(255,244,238,.95)", bordercolor="#134E4A", borderwidth=1, borderpad=4)
                    _fig_pr.update_layout(hovermode="x unified", height=280,
                        xaxis=dict(tickangle=-30, showline=False), yaxis_title="", xaxis_title="",
                        showlegend=False, margin=dict(l=8, r=8, t=12, b=8))
                    st.plotly_chart(theme(_fig_pr), use_container_width=True)
                else:
                    st.info("Kolom Profit tidak tersedia.")

            with ct4:
                _has_city = "Hotel_City" in df_view.columns and "Issued Date" in df_view.columns
                if _has_city:
                    _dt_cy = (df_view.dropna(subset=["Issued Date","Hotel_City"])
                                     .assign(Hotel_City=lambda d: d["Hotel_City"].astype(str).str.strip())
                                     .pipe(lambda d: d[~d["Hotel_City"].isin(["","nan","None","NaN"])])
                                     .copy())
                    _dt_cy["_mon_label"] = _dt_cy["Issued Date"].dt.strftime("%b %Y")
                    _dt_cy["_mon_num"]   = _dt_cy["Issued Date"].dt.to_period("M").apply(lambda p: p.ordinal)
                    _cy_s = (_dt_cy.groupby(["_mon_label","_mon_num"], as_index=False)["Hotel_City"]
                                   .nunique().rename(columns={"Hotel_City":"Kota Unik"})
                                   .sort_values("_mon_num"))
                    _cy_s["Kota Unik"] = pd.to_numeric(_cy_s["Kota Unik"], errors="coerce").fillna(0).astype(int)
                    _cy_peak = _cy_s.loc[_cy_s["Kota Unik"].idxmax()] if not _cy_s.empty else None
                    _cy_avg  = int(_cy_s["Kota Unik"].mean()) if not _cy_s.empty else 0
                    st.markdown(
                        '<div style="background:#fff;border:1px solid #E2E8F0;border-radius:12px;'
                        'padding:16px 18px 4px;box-shadow:0 1px 3px rgba(0,0,0,.05);">'
                        '<div style="font-family:\'DM Sans\',sans-serif;font-size:.72rem;font-weight:700;color:#0F172A;margin-bottom:3px;">🗺️ Tren Kota Unik Bulanan</div>'
                        '<div style="font-size:.57rem;color:#94A3B8;margin-bottom:10px;">Jumlah kota hotel unik per bulan</div>'
                        + _mini_stats([
                            ("Max Kota", str(int(_cy_s["Kota Unik"].max())) if not _cy_s.empty else "—"),
                            ("Peak",     f'{_cy_peak["_mon_label"]} · {int(_cy_peak["Kota Unik"])}' if _cy_peak is not None else "—"),
                            ("Avg/bln",  str(_cy_avg)),
                        ]) + '</div>', unsafe_allow_html=True)
                    _cy_max    = _cy_s["Kota Unik"].max()
                    _cy_thresh = max(1, _cy_max * 0.05)
                    _cy_labels = _cy_s["Kota Unik"].apply(lambda v: f"{int(v):,}" if v >= _cy_thresh else "")
                    _fig_cy = go.Figure()
                    _fig_cy.add_trace(go.Scatter(
                        x=_cy_s["_mon_label"], y=_cy_s["Kota Unik"],
                        mode="lines+markers+text", name="Kota Unik",
                        text=_cy_labels, textposition="top center",
                        textfont=dict(size=10, color="#134E4A", family="Open Sans"),
                        line=dict(color="#134E4A", width=2.5, shape="spline"),
                        marker=dict(size=8, color="#A78BFA", line=dict(width=2, color="rgba(19,78,74,.3)")),
                        fill="tozeroy", fillcolor="rgba(19,78,74,.07)",
                        hovertemplate="<b>%{x}</b><br>Kota Unik: <b>%{y:,.0f}</b><extra></extra>",
                        cliponaxis=False,
                    ))
                    if _cy_peak is not None:
                        _fig_cy.add_annotation(
                            x=_cy_peak["_mon_label"], y=_cy_peak["Kota Unik"],
                            text=f"▲ Peak: {int(_cy_peak['Kota Unik']):,}",
                            showarrow=True, arrowhead=2, arrowcolor="#134E4A", arrowsize=.8, ax=0, ay=-32,
                            font=dict(size=10, color="#134E4A", family="Open Sans"),
                            bgcolor="rgba(243,238,255,.95)", bordercolor="#134E4A", borderwidth=1, borderpad=4)
                    _fig_cy.update_layout(hovermode="x unified", height=280,
                        xaxis=dict(tickangle=-30, showline=False), yaxis_title="", xaxis_title="",
                        showlegend=False, margin=dict(l=8, r=8, t=12, b=8))
                    st.plotly_chart(theme(_fig_cy), use_container_width=True)
                else:
                    st.info("Kolom Hotel_City tidak tersedia.")

        # ── Bottom row: Top 10 + Concentric Rings ─────────────────────
        st.markdown('<div style="height:4px"></div>', unsafe_allow_html=True)
        gsec("🏢 Distribusi &amp; Analisis")

        ch1, ch2 = st.columns(2)

        with ch1:
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
                _max_inv_top = int(top10_inv["Invoice Unik"].max())

                RANK_COLORS = [
                    ("#0D9488","#F0FDFA","#CCFBF1"),
                    ("#0F766E","#F0FDFA","#99F6E4"),
                    ("#134E4A","#F0FDFA","#5EEAD4"),
                    ("#334155","#F8FAFC","#E2E8F0"),
                ]
                rows_html = ""
                for i, row in top10_inv.iterrows():
                    ri   = top10_inv.index.get_loc(i)
                    name = str(row[inv_to_col])
                    w    = (int(row["Invoice Unik"]) / _max_inv_top * 100)
                    pct  = row["Pct"]
                    val  = f'{int(row["Invoice Unik"]):,}'
                    ci   = min(ri, 3)
                    bar_color, bg_rank, bd_rank = RANK_COLORS[ci]
                    rank_badge = (
                        f'<div style="width:20px;height:20px;border-radius:6px;flex-shrink:0;'
                        f'display:flex;align-items:center;justify-content:center;'
                        f'font-size:.58rem;font-weight:800;font-family:\'Sora\',sans-serif;'
                        f'background:{bg_rank};color:{bar_color};border:1px solid {bd_rank};">'
                        f'{ri+1}</div>'
                    )
                    row_bg = "#fff" if ri % 2 == 0 else "#FAFBFC"
                    rows_html += (
                        f'<div style="display:grid;grid-template-columns:24px 1fr auto;'
                        f'align-items:center;gap:10px;padding:7px 16px;'
                        f'background:{row_bg};border-bottom:1px solid #F1F5F9;'
                        f'transition:background .12s;cursor:default;"'
                        f' onmouseover="this.style.background=\'#F0FDFA\'"'
                        f' onmouseout="this.style.background=\'{row_bg}\'">'
                        f'{rank_badge}'
                        f'<div style="min-width:0;">'
                        f'<div style="font-size:.68rem;font-weight:600;color:#0F172A;line-height:1.3;margin-bottom:4px;">{name}</div>'
                        f'<div style="display:flex;align-items:center;gap:6px;">'
                        f'<div style="flex:1;height:5px;background:#F1F5F9;border-radius:5px;overflow:hidden;">'
                        f'<div style="width:{w:.1f}%;height:100%;border-radius:5px;'
                        f'background:linear-gradient(90deg,{bar_color},{"#2DD4BF" if ci<3 else "#94A3B8"});'
                        f'transition:width .9s cubic-bezier(.4,0,.2,1);"></div></div>'
                        f'<span style="font-size:.54rem;color:#94A3B8;white-space:nowrap;flex-shrink:0;">{pct:.1f}%</span>'
                        f'</div></div>'
                        f'<div style="text-align:right;flex-shrink:0;">'
                        f'<span style="font-family:\'Sora\',sans-serif;font-size:.78rem;font-weight:800;color:#0F172A;">{val}</span>'
                        f'<div style="font-size:.5rem;color:#94A3B8;">invoice</div>'
                        f'</div>'
                        f'</div>'
                    )

                col_lbl = "Normalized Invoice To" if inv_to_col == "Normalized_Inv_To" else inv_to_col
                st.markdown(
                    '<div style="background:#fff;border:1px solid #E2E8F0;border-radius:12px;'
                    'overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.06);">'
                    '<div style="padding:12px 16px 10px;border-bottom:1px solid #E2E8F0;'
                    'display:flex;align-items:center;justify-content:space-between;'
                    'background:linear-gradient(90deg,#F0FDFA,#fff);">'
                    '<span style="font-family:\'DM Sans\',sans-serif;font-size:.75rem;'
                    'font-weight:700;color:#0F172A;display:flex;align-items:center;gap:7px;">'
                    '🏢 Top 10 Invoice To</span>'
                    f'<span style="font-size:.55rem;color:#94A3B8;font-style:italic;">'
                    f'dari {_total_inv:,} invoice unik</span>'
                    '</div>'
                    + rows_html + '</div>',
                    unsafe_allow_html=True)
                st.caption(f"*Kolom: {col_lbl}")
            else:
                st.info("Kolom 'Invoice To' tidak ditemukan dalam data.")

        with ch2:
            dom_col = next(
                (c for c in df_view.columns
                 if any(k in c.lower() for k in [
                     "domestic","international","destination",
                     "dom/int","domint","dom int",
                     "tipe","type hotel","lokasi"
                 ])),
                None
            )

            RING_PALETTE = [
                ("#0D9488", "rgba(13,148,136,.09)"),
                ("#0F766E", "rgba(15,118,110,.09)"),
                ("#14B8A6", "rgba(20,184,166,.09)"),
                ("#5EEAD4", "rgba(94,234,212,.09)"),
                ("#99F6E4", "rgba(153,246,228,.09)"),
            ]

            def _fmt_label(n):
                if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
                if n >= 1_000:     return f"{n/1_000:.1f}K"
                return str(n)

            def _build_segs(grp_df, name_col):
                total = int(grp_df["Invoice Unik"].sum())
                segs  = []
                for idx, (_, row) in enumerate(grp_df.iterrows()):
                    col, track = RING_PALETTE[min(idx, len(RING_PALETTE) - 1)]
                    pct = round(row["Invoice Unik"] / total * 100, 1) if total > 0 else 0
                    segs.append({
                        "label"      : str(row[name_col]),
                        "sub"        : f"{int(row['Invoice Unik']):,} invoice",
                        "value"      : int(row["Invoice Unik"]),
                        "pct"        : pct,
                        "color"      : col,
                        "trackColor" : track,
                    })
                return segs, total

            def _render_rings(segs, total, caption_text):
                subtitle = f"Berdasarkan invoice unik · {_fmt_label(total)} total"
                components.html(
                    build_donut_html(segs, _fmt_label(total), subtitle),
                    height=370,
                    scrolling=False
                )
                st.caption(caption_text)

            if dom_col and "Invoice No" in df_view.columns:
                _df_dom_raw = (
                    df_view[[dom_col, "Invoice No"]]
                    .dropna(subset=[dom_col])
                    .assign(**{dom_col: lambda d: d[dom_col].astype(str).str.strip()})
                    .pipe(lambda d: d[~d[dom_col].isin(["", "nan", "None", "NaN"])])
                )
                dom_grp = (
                    _df_dom_raw
                    .groupby(dom_col, dropna=True)["Invoice No"]
                    .nunique()
                    .reset_index()
                    .rename(columns={"Invoice No": "Invoice Unik"})
                )
                if len(dom_grp) > 4:
                    top4 = dom_grp.nlargest(4, "Invoice Unik").reset_index(drop=True)
                    oth  = dom_grp.nlargest(len(dom_grp), "Invoice Unik").iloc[4:]["Invoice Unik"].sum()
                    dom_grp = pd.concat(
                        [top4, pd.DataFrame([{dom_col: "Others", "Invoice Unik": oth}])],
                        ignore_index=True
                    )
                else:
                    dom_grp = dom_grp.reset_index(drop=True)

                segs, total = _build_segs(dom_grp, dom_col)
                col_lbl = "Normalized Invoice To" if dom_col == "Normalized_Inv_To" else dom_col
                _render_rings(segs, total, f"*Kolom: {col_lbl}")

            elif "Product Type" in df_view.columns and "Invoice No" in df_view.columns:
                df_dom = df_view.copy()
                df_dom["Dom_Int"] = df_dom["Product Type"].astype(str).apply(
                    lambda x: "International"
                    if any(k in x.upper() for k in ["INTER", "LUAR", "ABROAD", "OVERSEA"])
                    else "Domestic"
                )
                dom_grp2 = (
                    df_dom.groupby("Dom_Int")["Invoice No"]
                    .nunique()
                    .reset_index()
                    .rename(columns={"Invoice No": "Invoice Unik"})
                    .reset_index(drop=True)
                )
                segs, total = _build_segs(dom_grp2, "Dom_Int")
                _render_rings(segs, total, "*Diklasifikasikan dari kolom Product Type")

            else:
                st.info("Kolom Domestic/International tidak ditemukan dalam data.")

        # ── Preview data ──────────────────────────────────────────────
        st.markdown('<div style="height:12px"></div>', unsafe_allow_html=True)
        gsec("&#9776; Preview Data")
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
                f'<p style="text-align:center;font-size:.68rem;color:#475569;padding:9px 0;margin:0;">'
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
            tr2 = dt.groupby("Mon")["Total Room Night"].sum().reset_index() if "Total Room Night" in dt.columns else None

            ca,cb = st.columns([3,2])
            with ca:
                gsec("Tren Invoice Bulanan", "📈")
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=ti["Bulan"], y=ti["Invoice"],
                    mode="lines+markers", name="Invoice",
                    line=dict(color="#0D9488",width=2.5,shape="spline"),
                    marker=dict(size=9,color="#0D9488",line=dict(width=2.5,color="rgba(99,102,241,.25)"),symbol="circle"),
                    fill="tozeroy", fillcolor="rgba(13,148,136,.1)",
                    hovertemplate="<b>%{x}</b><br>Invoice: <b>%{y:,.0f}</b><extra></extra>",
                ))
                fig.update_layout(xaxis_title="",yaxis_title="Invoice Unik",
                                  hovermode="x unified",height=320,xaxis=dict(showline=False))
                st.plotly_chart(theme(fig), use_container_width=True)
            with cb:
                gsec("Ringkasan Bulanan", "📋")
                if tr2 is not None:
                    tr2 = tr2.rename(columns={"Mon":"Bulan","Total Room Night":"Room Night"})
                    merged = ti[["Bulan","MonN","Invoice"]].merge(tr2, on="Bulan", how="left").drop("MonN",axis=1)
                    merged.columns = ["Bulan","Invoice Unik","Room Night"]
                else:
                    merged = ti[["Bulan","Invoice"]].rename(columns={"Invoice":"Invoice Unik"})
                st.dataframe(
                    merged.style.format({c:"{:,.0f}" for c in merged.columns if merged[c].dtype!="O"})
                          .background_gradient(subset=["Invoice Unik"],cmap="Purples"),
                    use_container_width=True, height=320)

            st.markdown('<div style="height:4px"></div>', unsafe_allow_html=True)
            gsec("Volume Invoice per Bulan", "📊")
            ti["Invoice"] = pd.to_numeric(ti["Invoice"], errors="coerce").fillna(0)
            fig2 = px.bar(ti, x="Bulan", y="Invoice", text="Invoice",
                          color="Invoice", color_continuous_scale=["rgba(99,102,241,.3)","#0D9488","#0D9488"])
            fig2.update_traces(
                texttemplate="%{y:,.0f}", textposition="outside",
                textfont=dict(size=11,color="#8898AA"),
                marker_line_width=0, marker_cornerradius=4, cliponaxis=False)
            fig2.update_layout(coloraxis_showscale=False, height=290, xaxis_title="", yaxis_title="")
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
            ss3 = (_df_s3.groupby("Supplier_Name",dropna=True)["Total Room Night"]
                         .sum().reset_index().sort_values("Total Room Night",ascending=False))
            d3 = pd.concat([ss3.head(5),
                            pd.DataFrame([{"Supplier_Name":"Others","Total Room Night":ss3.iloc[5:]["Total Room Night"].sum()}])
                            if len(ss3)>5 else pd.DataFrame()], ignore_index=True)
            ca,cb = st.columns([3,2])
            with ca:
                gsec("Distribusi Supplier", "🏢")
                fig3 = px.pie(d3,names="Supplier_Name",values="Total Room Night",
                              hole=0.52, color_discrete_sequence=GLASS_PALETTE)
                fig3.update_traces(
                    textinfo="percent+label", textfont=dict(size=12,family="Space Grotesk"),
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
                               color="Total Room Night", color_continuous_scale=TEAL_SCALE)
                fig3b.update_traces(
                    texttemplate="%{x:,.0f}", textposition="outside",
                    textfont=dict(size=10,color="#8898AA"),
                    marker_line_width=0, marker_cornerradius=4, cliponaxis=False)
                fig3b.update_layout(yaxis=dict(categoryorder="total ascending"),
                                    coloraxis_showscale=False, height=360, xaxis_title="", yaxis_title="")
                st.plotly_chart(theme(fig3b), use_container_width=True)
            st.dataframe(
                d3.sort_values("Total Room Night",ascending=False).reset_index(drop=True)
                  .style.format({"Total Room Night":"{:,.0f}"})
                  .background_gradient(subset=["Total Room Night"],cmap="YlGnBu"),
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
            ps4 = (_df_p4.groupby("Product Type",dropna=True)["Total Room Night"]
                          .sum().reset_index().sort_values("Total Room Night",ascending=False))
            d4 = pd.concat([ps4.head(6),
                            pd.DataFrame([{"Product Type":"Others","Total Room Night":ps4.iloc[6:]["Total Room Night"].sum()}])
                            if len(ps4)>6 else pd.DataFrame()], ignore_index=True)
            ca,cb = st.columns([3,2])
            with ca:
                gsec("Distribusi Product Type", "📦")
                fig4 = px.pie(d4,names="Product Type",values="Total Room Night",
                              hole=0.52, color_discrete_sequence=GLASS_PALETTE)
                fig4.update_traces(
                    textinfo="percent+label", textfont=dict(size=12,family="Space Grotesk"),
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
                      .background_gradient(subset=["Total Room Night"],cmap="YlGnBu"),
                    use_container_width=True, height=360)
        else:
            st.warning("Kolom Product Type atau Total Room Night tidak tersedia.")

    # ═══════════════════════════════════════════════════════════════
    # TAB 5 — Agent Scorecard
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
            _null_ac = {"nan","none","","nat","<na>","n/a","null"}
            dfa = dfa[~dfa[ac].str.lower().isin(_null_ac)]

            def _classify_pic(name):
                nu = str(name).strip().upper()
                for p in KNOWN_PICS:
                    if p.upper() == nu: return p
                return "Other"

            dfa["PIC_Group"] = dfa[ac].apply(_classify_pic)

            _n_months = 1
            if "Issued Date" in dfa.columns:
                _periods  = dfa["Issued Date"].dropna().dt.to_period("M").unique()
                _n_months = max(len(_periods), 1)

            _company_col = None
            if "Normalized_Inv_To" in dfa.columns:
                _company_col = "Normalized_Inv_To"
            else:
                _company_col = next((c for c in dfa.columns if any(k in c.lower() for k in
                    ["invoice to","invoiceto","bill to","billto","sold to","client"])), None)

            pic_order = KNOWN_PICS + ["Other"]
            pic_data  = {}

            for _pic in pic_order:
                _sub = dfa[dfa["PIC_Group"] == _pic]
                if _sub.empty: continue
                _inv_u   = int(_sub["Invoice No"].nunique())
                _rn      = float(_sub["Total Room Night"].sum())
                _sa      = float(_sub["Sales AR"].fillna(0).astype(float).sum()) if "Sales AR" in _sub.columns else None
                _pr      = float(_sub["Profit"].fillna(0).astype(float).sum())   if "Profit"  in _sub.columns else None
                _avg_inv = _inv_u / _n_months
                _avg_rn  = _rn    / _inv_u if _inv_u > 0 else 0
                _avg_sa  = _sa    / _inv_u if (_sa is not None and _inv_u > 0) else None
                _avg_pr  = _pr    / _inv_u if (_pr is not None and _inv_u > 0) else None
                _co_count= int(_sub[_company_col].dropna().nunique()) if _company_col else 0

                _avg_pm = None
                if "Profit" in _sub.columns and "Sales AR" in _sub.columns:
                    _pm_sub = _sub.copy()
                    _pm_sub["_sa_f"] = pd.to_numeric(_pm_sub["Sales AR"], errors="coerce").fillna(0)
                    _pm_sub["_pr_f"] = pd.to_numeric(_pm_sub["Profit"],   errors="coerce").fillna(0)
                    _pm_mask = _pm_sub["_sa_f"] != 0
                    if _pm_mask.any():
                        _avg_pm = float((_pm_sub.loc[_pm_mask,"_pr_f"] / _pm_sub.loc[_pm_mask,"_sa_f"] * 100).mean())

                _top_sup = "—"; _top_sup_rn = 0
                if "Supplier_Name" in _sub.columns:
                    _sup_rn = (_sub.groupby("Supplier_Name",dropna=True)["Total Room Night"]
                                   .sum().sort_values(ascending=False))
                    _sup_rn = _sup_rn[_sup_rn.index.astype(str).str.strip().str.upper().isin(
                        ["","NAN","NONE","DIRECT TO HOTEL"]) == False]
                    if not _sup_rn.empty:
                        _top_sup    = str(_sup_rn.index[0])
                        _top_sup_rn = int(_sup_rn.iloc[0])

                _total_inv_all = dfa["Invoice No"].nunique()
                _total_rn_all  = float(dfa["Total Room Night"].sum())
                _inv_pct = (_inv_u / _total_inv_all * 100) if _total_inv_all > 0 else 0
                _rn_pct  = (_rn    / _total_rn_all  * 100) if _total_rn_all  > 0 else 0

                pic_data[_pic] = {
                    "inv_u":_inv_u,"rn":_rn,"sa":_sa,"pr":_pr,
                    "avg_inv":_avg_inv,"avg_rn":_avg_rn,"avg_sa":_avg_sa,"avg_pr":_avg_pr,
                    "avg_pm":_avg_pm,"co_count":_co_count,
                    "top_sup":_top_sup,"top_sup_rn":_top_sup_rn,
                    "inv_pct":_inv_pct,"rn_pct":_rn_pct,
                }

            def _initials(name):
                parts = name.split()
                if len(parts) >= 2: return (parts[0][0]+parts[1][0]).upper()
                return name[:2].upper() if len(name) >= 2 else name.upper()

            def _build_card2(pic, d):
                is_other = (pic == "Other")
                ini      = "OTH" if is_other else _initials(pic)
                card_cls = "pic2-card other" if is_other else "pic2-card"
                inv_u=d["inv_u"]; rn=d["rn"]; sa=d["sa"]; pr=d["pr"]
                avg_inv=d["avg_inv"]; avg_rn=d["avg_rn"]
                avg_sa=d["avg_sa"]; avg_pr=d["avg_pr"]; avg_pm=d.get("avg_pm",None)
                co=d["co_count"]; inv_pct=d["inv_pct"]; rn_pct=d["rn_pct"]
                sa_str     = compact_num(sa)     if sa     is not None else "—"
                pr_str     = compact_num(pr)     if pr     is not None else "—"
                avg_sa_str = compact_num(avg_sa) if avg_sa is not None else "—"
                avg_pr_str = compact_num(avg_pr) if avg_pr is not None else "—"
                pm_pct_str,pm_raw = "—",None
                if sa is not None and pr is not None and sa > 0:
                    pm_raw = pr/sa*100; pm_pct_str = f"{pm_raw:.1f}%"
                avg_rn_str = f"{avg_rn:.1f}"
                _photo_uri = _load_avatar_b64(pic) if not is_other else ""
                if _photo_uri:
                    _avatar_html = '<div class="p2av p2av-photo"><img src="'+_photo_uri+'" alt="'+pic+'" /></div>'
                else:
                    _avatar_html = '<div class="p2av">'+ini+'</div>'
                share_html = (
                    '<div class="p2-share">'
                    +'<span class="p2-share-dot"></span>'
                    +f'<span class="p2-share-txt">{inv_pct:.1f}% inv&thinsp;·&thinsp;{rn_pct:.1f}% RN</span>'
                    +'</div>'
                )
                def _bar(pct, muted=False):
                    w = min(float(pct),100)
                    cls = "p2-bar-fill muted" if muted else "p2-bar-fill"
                    return '<div class="p2-bar"><div class="'+cls+'" style="width:'+f"{w:.1f}"+'%;"></div></div>'
                def _mrow(icon, label, val, hint="", bar_pct=None, accent=False, muted=False):
                    bar_h  = _bar(bar_pct,muted) if bar_pct is not None else ""
                    hint_h = ('<div class="p2m-hint">'+hint+'</div>') if hint else ""
                    acc_cls= " accent" if accent else ""
                    return (
                        '<div class="p2-mrow'+acc_cls+'">'
                        +'<div class="p2m-top">'
                        +'<span class="p2m-icon">'+icon+'</span>'
                        +'<span class="p2m-label">'+label+'</span>'
                        +'</div>'
                        +'<div class="p2m-val">'+val+'</div>'
                        +hint_h+bar_h+'</div>'
                    )
                def _mgroup(rows_html):
                    return '<div class="p2-mgroup">'+rows_html+'</div>'

                h = '<div class="'+card_cls+'">'
                h += '<div class="p2-banner">'+_avatar_html
                h += '<div class="p2-banner-info">'
                h += '<div class="p2-name">'+pic+'</div>'
                h += '<div class="p2-role">Hotel Bookers · MTT</div>'
                h += share_html+'</div></div>'
                h += '<div class="p2-body">'
                h += '<div class="p2-section-lbl">📋 Volume Transaksi</div>'
                h += _mgroup(
                    _mrow("🧾","Invoice Unik",f'{inv_u:,}',f'avg {avg_inv:.1f} / bulan',bar_pct=inv_pct)
                    +_mrow("🌙","Room Night",compact_num(rn),f'avg {avg_rn_str} RN / inv',bar_pct=rn_pct,muted=True)
                )
                h += '<div class="p2-section-lbl">💰 Finansial</div>'
                h += _mgroup(
                    _mrow("📦","Sales AR",sa_str,f'avg {avg_sa_str} / inv')
                    +_mrow("💹","Profit",pr_str,f'avg {avg_pr_str} / inv')
                )
                avg_pm_str   = f"{avg_pm:.1f}%" if avg_pm is not None else "—"
                avg_pm_color = "#059669" if (avg_pm is not None and avg_pm >= 0) else "#DC2626"
                pm_color     = "#059669" if (pm_raw is not None and pm_raw >= 0) else "#DC2626"
                h += (
                    '<div class="p2-margin-strip">'
                    +'<div class="p2-margin-left">'
                    +'<div class="p2-margin-lbl">Profit Margin</div>'
                    +'<div class="p2-margin-val" style="color:'+pm_color+';">'+pm_pct_str+'</div>'
                    +'<div style="margin-top:6px;padding-top:6px;border-top:1px dashed rgba(13,148,136,.2);">'
                    +'<div class="p2-margin-lbl" style="margin-bottom:2px;">Avg Profit Margin</div>'
                    +'<div style="font-family:\'Sora\',sans-serif;font-size:.78rem;font-weight:700;color:'+avg_pm_color+';">'+avg_pm_str+'</div>'
                    +'<div style="font-size:.48rem;color:#94A3B8;margin-top:1px;">rata-rata per transaksi</div>'
                    +'</div></div>'
                    +'<div class="p2-margin-right">'
                    +'<div class="p2-margin-lbl">Companies</div>'
                    +'<div class="p2-margin-val">'+f'{co:,}'+'</div>'
                    +'</div></div>'
                )
                h += '</div>'
                h += '<div class="p2-footer"><div class="p2-footer-lbl">🏨 Supplier Preference</div>'
                if d["top_sup"] != "—":
                    short_sup = d["top_sup"][:28]+"…" if len(d["top_sup"])>28 else d["top_sup"]
                    h += (
                        '<div class="p2-sup-row">'
                        +'<span class="p2-sup-name">'+short_sup+'</span>'
                        +'<span class="p2-sup-rn">'+f'{d["top_sup_rn"]:,} RN'+'</span>'
                        +'</div>'
                    )
                else:
                    h += '<span class="p2-no-sup">Tidak ada data supplier</span>'
                h += '</div></div>'
                return h

            gsec("Scorecard PIC Agent", "🏅")
            _known_sorted = sorted(
                [p for p in pic_order if p != "Other" and p in pic_data],
                key=lambda p: pic_data[p]["sa"] if pic_data[p]["sa"] is not None else 0, reverse=True
            )
            _render_order = _known_sorted + (["Other"] if "Other" in pic_data else [])
            cards_parts = ['<div class="pic2-grid">']
            for _pic in _render_order:
                cards_parts.append(_build_card2(_pic, pic_data[_pic]))
            cards_parts.append('</div>')
            st.markdown("".join(cards_parts), unsafe_allow_html=True)

            st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
            gsec("Tabel Ringkasan Scorecard PIC", "📋")

            _tbl_rows = []
            for _pic in _render_order:
                d = pic_data[_pic]
                _sa_v = d["sa"] if d["sa"] is not None else 0
                _pr_v = d["pr"] if d["pr"] is not None else 0
                _pm_v = (_pr_v/_sa_v*100) if _sa_v > 0 else None
                _tbl_rows.append({
                    "pic":_pic,"inv_u":d["inv_u"],"avg_inv":round(d["avg_inv"],1),
                    "rn":int(d["rn"]),"avg_rn":round(d["avg_rn"],1),
                    "sa":_sa_v,"avg_sa":d["avg_sa"] if d["avg_sa"] is not None else 0,
                    "pr":_pr_v,"avg_pr":d["avg_pr"] if d["avg_pr"] is not None else 0,
                    "pm":_pm_v,"co":d["co_count"],
                    "inv_pct":round(d["inv_pct"],1),"rn_pct":round(d["rn_pct"],1),
                    "top_sup":d["top_sup"],
                })

            _df_tbl_dl = pd.DataFrame([{
                "PIC":r["pic"],"Invoice Unik":r["inv_u"],"Avg Inv/Bulan":r["avg_inv"],
                "Room Night":r["rn"],"Avg RN/Inv":r["avg_rn"],"Sales AR":r["sa"],
                "Avg Sales/Inv":r["avg_sa"],"Profit":r["pr"],"Avg Profit/Inv":r["avg_pr"],
                "Profit Margin%":f"{r['pm']:.1f}%" if r["pm"] is not None else "—",
                "Companies":r["co"],"% Invoice":r["inv_pct"],"% RN":r["rn_pct"],"Supplier Utama":r["top_sup"],
            } for r in _tbl_rows])

            _max_sa  = max((r["sa"]    for r in _tbl_rows), default=1) or 1
            _max_pr  = max((r["pr"]    for r in _tbl_rows), default=1) or 1
            _max_rn  = max((r["rn"]    for r in _tbl_rows), default=1) or 1
            _max_inv = max((r["inv_u"] for r in _tbl_rows), default=1) or 1

            def _bar_spark(val, mx, color="#0D9488", bg="#F0FDFA"):
                w = min(val/mx*100,100) if mx > 0 else 0
                return (
                    f'<div style="background:{bg};border-radius:4px;overflow:hidden;height:4px;width:100%;margin-top:3px;">'
                    f'<div style="width:{w:.1f}%;height:100%;background:{color};border-radius:4px;"></div></div>'
                )
            def _pm_badge(pm):
                if pm is None: return '<span style="color:#94A3B8;font-size:.62rem;">—</span>'
                color  = "#059669" if pm >= 0 else "#DC2626"
                bg     = "#ECFDF5" if pm >= 0 else "#FFF1F2"
                border = "#A7F3D0" if pm >= 0 else "#FECACA"
                icon   = "▲" if pm >= 0 else "▼"
                return (f'<span style="display:inline-block;padding:2px 8px;border-radius:12px;'
                        f'font-size:.62rem;font-weight:700;color:{color};background:{bg};border:1px solid {border};">'
                        f'{icon} {abs(pm):.1f}%</span>')
            def _pct_pill(pct, kind="inv"):
                color = "#0D9488" if kind == "inv" else "#0F766E"
                return (f'<span style="display:inline-block;padding:1px 7px;border-radius:10px;'
                        f'font-size:.6rem;font-weight:700;color:{color};background:#F0FDFA;border:1px solid #CCFBF1;">'
                        f'{pct:.1f}%</span>')

            th_style = ('style="padding:9px 14px;font-size:.56rem;font-weight:700;text-transform:uppercase;'
                        'letter-spacing:.9px;color:#64748B;background:#FAFBFC;border-bottom:2px solid #E2E8F0;'
                        'white-space:nowrap;font-family:Inter,sans-serif;"')
            th_r = ('style="padding:9px 14px;font-size:.56rem;font-weight:700;text-transform:uppercase;'
                    'letter-spacing:.9px;color:#64748B;background:#FAFBFC;border-bottom:2px solid #E2E8F0;'
                    'white-space:nowrap;text-align:right;font-family:Inter,sans-serif;"')

            html_tbl = (
                '<div style="border:1px solid #E2E8F0;border-radius:14px;overflow:hidden;'
                'box-shadow:0 2px 12px rgba(0,0,0,.06);background:#fff;margin-bottom:16px;">'
                '<div style="overflow-x:auto;">'
                '<table style="width:100%;border-collapse:collapse;font-family:Inter,sans-serif;">'
                '<thead><tr>'
                f'<th {th_style}>#</th><th {th_style}>Agent</th>'
                f'<th {th_r}>Sales AR</th><th {th_r}>Profit</th><th {th_r}>Margin</th>'
                f'<th {th_r}>Invoice</th><th {th_r}>Room Night</th><th {th_r}>Avg RN/Inv</th>'
                f'<th {th_r}>Avg Sales/Inv</th><th {th_r}>Companies</th>'
                f'<th {th_r}>% Inv</th><th {th_r}>% RN</th><th {th_style}>Supplier Utama</th>'
                '</tr></thead><tbody>'
            )
            for rank, r in enumerate(_tbl_rows, 1):
                is_other = r["pic"] == "Other"
                row_bg   = "#FAFAFA" if rank % 2 == 0 else "#FFFFFF"
                if is_other: row_bg = "#F8FAFC"
                if rank <= 3 and not is_other:
                    rank_html = f'<span style="font-size:.85rem;">{["🥇","🥈","🥉"][rank-1]}</span>'
                else:
                    rank_html = (f'<span style="display:inline-flex;width:22px;height:22px;border-radius:50%;'
                                 f'background:#F1F5F9;font-size:.6rem;font-weight:700;color:#94A3B8;'
                                 f'align-items:center;justify-content:center;">{rank}</span>')
                ini   = "OTH" if is_other else _initials(r["pic"])
                av_bg = "#94A3B8" if is_other else "#0D9488"
                name_cell = (
                    f'<div style="display:flex;align-items:center;gap:9px;">'
                    f'<div style="width:30px;height:30px;border-radius:50%;'
                    f'background:linear-gradient(135deg,{av_bg},{av_bg}CC);color:#fff;'
                    f'font-size:.62rem;font-weight:800;display:flex;align-items:center;justify-content:center;'
                    f'flex-shrink:0;">{ini}</div>'
                    f'<span style="font-weight:700;font-size:.73rem;color:#0F172A;">{r["pic"]}</span>'
                    f'</div>'
                )
                td  = 'style="padding:10px 14px;border-bottom:1px solid #F1F5F9;vertical-align:middle;"'
                tdr = 'style="padding:10px 14px;border-bottom:1px solid #F1F5F9;vertical-align:middle;text-align:right;"'
                sa_cell = (f'<div style="font-size:.75rem;font-weight:700;color:#0F172A;font-family:\'Sora\',sans-serif;">{compact_num(r["sa"])}</div>'
                           +_bar_spark(r["sa"],_max_sa,"#0D9488","#F0FDFA"))
                pr_cell = (f'<div style="font-size:.75rem;font-weight:700;color:{"#059669" if r["pr"]>=0 else "#DC2626"};font-family:\'Sora\',sans-serif;">{compact_num(r["pr"])}</div>'
                           +_bar_spark(max(r["pr"],0),_max_pr,"#059669","#F0FDF4"))
                inv_cell= (f'<div style="font-size:.73rem;font-weight:700;color:#0F172A;">{r["inv_u"]:,}</div>'
                           f'<div style="font-size:.56rem;color:#94A3B8;">avg {r["avg_inv"]:.1f}/bln</div>'
                           +_bar_spark(r["inv_u"],_max_inv,"#0D9488","#F0FDFA"))
                rn_cell = (f'<div style="font-size:.73rem;font-weight:700;color:#0F172A;">{compact_num(r["rn"])}</div>'
                           +_bar_spark(r["rn"],_max_rn,"#0F766E","#F0FDFA"))
                sup_name= r["top_sup"][:22]+"…" if len(str(r["top_sup"]))>22 else str(r["top_sup"])
                sup_cell= (f'<span style="font-size:.63rem;color:#0F766E;font-weight:500;">{sup_name}</span>'
                           if r["top_sup"]!="—" else
                           f'<span style="font-size:.6rem;color:#94A3B8;font-style:italic;">—</span>')
                html_tbl += (
                    f'<tr style="background:{row_bg};" '
                    f'onmouseover="this.style.background=\'#F0FDFA\'" '
                    f'onmouseout="this.style.background=\'{row_bg}\'">'
                    f'<td {td} style="padding:10px 14px;border-bottom:1px solid #F1F5F9;vertical-align:middle;text-align:center;">{rank_html}</td>'
                    f'<td {td}>{name_cell}</td>'
                    f'<td {tdr}>{sa_cell}</td><td {tdr}>{pr_cell}</td>'
                    f'<td {tdr}>{_pm_badge(r["pm"])}</td>'
                    f'<td {tdr}>{inv_cell}</td><td {tdr}>{rn_cell}</td>'
                    f'<td {tdr} style="padding:10px 14px;border-bottom:1px solid #F1F5F9;vertical-align:middle;text-align:right;font-size:.73rem;color:#334155;">{r["avg_rn"]:.1f}</td>'
                    f'<td {tdr} style="padding:10px 14px;border-bottom:1px solid #F1F5F9;vertical-align:middle;text-align:right;font-size:.73rem;color:#334155;">{compact_num(r["avg_sa"])}</td>'
                    f'<td {tdr} style="padding:10px 14px;border-bottom:1px solid #F1F5F9;vertical-align:middle;text-align:right;font-size:.73rem;color:#334155;">{r["co"]:,}</td>'
                    f'<td {tdr}>{_pct_pill(r["inv_pct"],"inv")}</td>'
                    f'<td {tdr}>{_pct_pill(r["rn_pct"],"rn")}</td>'
                    f'<td {td}>{sup_cell}</td>'
                    f'</tr>'
                )
            html_tbl += '</tbody></table></div></div>'
            st.markdown(html_tbl, unsafe_allow_html=True)

            _ob_ag = io.BytesIO()
            with pd.ExcelWriter(_ob_ag, engine="xlsxwriter") as _w:
                _df_tbl_dl.to_excel(_w, index=False, sheet_name="Scorecard_PIC")
            st.download_button(
                "⬇ Download Tabel Scorecard PIC", _ob_ag.getvalue(),
                "scorecard_pic_agent.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True, key="dl_scorecard_pic")
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
                           .pipe(lambda d: d[~d["Hotel_Name"].isin(["","nan","None","NaN"])]))
                dfh = (_df_ptm.groupby("Hotel_Name",dropna=True,as_index=False)
                              .agg({"Total Room Night":"sum"})
                              .sort_values("Total Room Night",ascending=False))
                ca,cb = st.columns([3,2])
                with ca:
                    gsec("Top Hotel PTM Corp Rate", "🏨")
                    fh = px.bar(dfh.head(15),x="Total Room Night",y="Hotel_Name",
                                orientation="h",text="Total Room Night",
                                color="Total Room Night",
                                color_continuous_scale=["rgba(252,211,77,.2)","rgba(252,211,77,.6)","#2DD4BF"])
                    fh.update_traces(texttemplate="%{x:,.0f}",textposition="outside",
                        textfont=dict(size=11,color="#8898AA"),marker_line_width=0,marker_cornerradius=4,cliponaxis=False)
                    fh.update_layout(yaxis=dict(categoryorder="total ascending",automargin=True),
                                     coloraxis_showscale=False,height=460,xaxis_title="",yaxis_title="",
                                     margin=dict(l=8,r=80,t=30,b=8))
                    st.plotly_chart(theme(fh), use_container_width=True)
                with cb:
                    gsec("Tabel Hotel PTM", "📋")
                    st.dataframe(
                        dfh.head(20).reset_index(drop=True)
                           .style.format({"Total Room Night":"{:,.0f}"})
                           .background_gradient(subset=["Total Room Night"],cmap="YlGnBu"),
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
            cs7 = (_df_s7.groupby("Supplier_Category",dropna=True)["Total Room Night"]
                          .sum().reset_index().sort_values("Total Room Night",ascending=False))
            d7 = pd.concat([cs7.head(5),
                            pd.DataFrame([{"Supplier_Category":"Others","Total Room Night":cs7.iloc[5:]["Total Room Night"].sum()}])
                            if len(cs7)>5 else pd.DataFrame()], ignore_index=True)
            ca,cb = st.columns([3,2])
            with ca:
                gsec("Distribusi Kategori Supplier", "🎯")
                fc7 = px.pie(d7,names="Supplier_Category",values="Total Room Night",
                             hole=0.52, color_discrete_sequence=GLASS_PALETTE)
                fc7.update_traces(
                    textinfo="percent+label",textfont=dict(size=12,family="Space Grotesk"),
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
                      .background_gradient(subset=["Total Room Night"],cmap="YlGnBu"),
                    use_container_width=True, height=380)
        else:
            st.warning("Kolom Supplier_Category atau Total Room Night tidak tersedia.")

# ── Empty State ───────────────────────────────────────────────────────────────
else:
    for k in ["df_raw","upload_hash"]: st.session_state.pop(k,None)
    st.markdown("""
    <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;
                padding:80px 40px;text-align:center;max-width:480px;margin:60px auto 0;">
      <div style="width:64px;height:64px;margin-bottom:24px;
                  background:#F0FDFA;border:1px solid #CCFBF1;
                  border-radius:16px;display:grid;place-items:center;">
        <svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="#0D9488" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">
          <path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"/>
          <polyline points="13 2 13 9 20 9"/>
        </svg>
      </div>
      <div style="font-family:'DM Sans',sans-serif;font-size:1.05rem;font-weight:700;color:#0F172A;margin-bottom:10px;letter-spacing:-.3px;">
        Belum ada data
      </div>
      <p style="font-size:.72rem;color:#94A3B8;line-height:1.9;margin:0 auto 28px;max-width:340px;font-family:'Inter',sans-serif;">
        Upload file Excel Custom Report di sidebar, lalu klik
        <span style="color:#0D9488;font-weight:600;background:#F0FDFA;padding:1px 7px;border-radius:5px;border:1px solid #CCFBF1;">Sync Data</span>
        untuk memuat normalisasi dari Google Drive.
      </p>
      <div style="display:flex;gap:8px;flex-wrap:wrap;justify-content:center;">
        <span style="font-size:.62rem;font-weight:500;padding:6px 14px;border-radius:20px;background:#F0FDFA;color:#0D9488;border:1px solid #CCFBF1;font-family:'Inter',monospace;">Custom Report .xlsx</span>
        <span style="font-size:.62rem;font-weight:500;padding:6px 14px;border-radius:20px;background:#F8FAFC;color:#64748B;border:1px solid #E2E8F0;font-family:'Inter',monospace;">Google Drive Sync</span>
        <span style="font-size:.62rem;font-weight:500;padding:6px 14px;border-radius:20px;background:#F0FDFA;color:#0D9488;border:1px solid #CCFBF1;font-family:'Inter',monospace;">AI Pivot Analysis</span>
      </div>
    </div>
    """, unsafe_allow_html=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("""
<div style="margin-top:56px; border-top:1px solid #E2E8F0; background:#fff;">
  <div style="background:#F0FDFA; border-bottom:1px solid #CCFBF1; padding:10px 36px; display:flex; align-items:flex-start; gap:10px;">
    <span style="font-size:.8rem; margin-top:1px; flex-shrink:0;">⚠️</span>
    <p style="margin:0; font-size:.6rem; color:#0F766E; font-family:'Inter',sans-serif; line-height:1.8;">
      <strong style="color:#0D9488;">DISCLAIMER &nbsp;|&nbsp;</strong>
      Data yang ditampilkan bersumber dari file Custom Report yang diunggah dan referensi normalisasi dari Google Drive MTT.
      Seluruh informasi bersifat <em>internal dan rahasia</em> — dilarang disebarluaskan tanpa izin tertulis dari manajemen.
      Akurasi data bergantung pada kualitas sumber yang diunggah.
    </p>
  </div>
  <div style="padding:12px 36px; display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:8px;">
    <span style="font-size:.6rem; color:#94A3B8; font-family:'Inter',sans-serif;">
      &copy; 2025 <strong style="color:#0D9488;">Hotel Intelligence</strong> · MTT · All rights reserved</span>
    <span style="font-size:.6rem; color:#94A3B8; font-family:'Inter',sans-serif;">
      Powered by Streamlit · v9.2 · Concentric Rings Edition</span>
    <span style="font-size:.6rem; color:#94A3B8; font-family:'Inter',sans-serif;">
      Built by <strong style="color:#0D9488;">Rifyal Tumber</strong> · MTT · 2025</span>
  </div>
</div>
""", unsafe_allow_html=True)
