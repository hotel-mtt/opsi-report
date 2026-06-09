# join_opsifin.py — Opsifin v9.3 · Hotel Report Dashboard
# Optimized Edition · Rifyal Tumber · MTT · 2025

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import io, requests, re, hashlib, base64, os as _os
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

DROP_SET = frozenset([
    "Branch","Customer Type","Customer Name","PNR","Base Fare","Airlines","Class","Route",
    "Departure Time","Arrival Time","NTA","Airline Code","Flight No","Hotel Address",
    "Hotel Group Chain","Description","Due Date","Group Chain","Source Reference","Sales Net",
    *[f"Remark {i}" for i in range(1,13)],
    "Supplier Code","Ticket No","Fare Tax","IWJR","Add Charge","Insurance","PSC",
    "Other Charge","Incentive","Agent Comm","Customer Code","Travel Services","VAT","Stamp Fee",
    "MDR","Extra Disc","Rounding","Base Sell","Currency","Sales Handler","Remark","Source Rescode"
])

GLASS_PALETTE = ["#0D9488","#134E4A","#2DD4BF","#5EEAD4","#99F6E4","#CCFBF1","#0F766E","#042F2E"]
TEAL_SCALE    = ["#CCFBF1","#99F6E4","#2DD4BF","#0D9488","#0F766E","#134E4A"]
KNOWN_PICS    = ["Farras","Ade","Meiji","Vero","Firda","Selvy","Rida","Rifyal","Gerald","Baldy","Fandi","API-DTM"]

# Supplier category sets (frozenset O(1) lookup)
_KNOWN_WHOLESALERS_SET = frozenset({
    "MG BEDBANK","MG BED BANK","MGBEDBANK","KLIKNBOOK","KLIK N BOOK","KLOOK",
    "HOTELBEDS","HOTEL BEDS","WEBBEDS","WEB BEDS","TOURICO","GTA","JUMBO TOURS",
    "WORLDHOTELS","RESTEL","BONOTEL","RECONLINE",
})
_KNOWN_CORPORATE_SET = frozenset({"PTM CORP RATE","CORPORATE RATE"})
_KNOWN_DIRECT_SET    = frozenset({"DIRECT TO HOTEL","DIRECT HOTEL","DIRECT"})
_KNOWN_OTA_SET       = frozenset({
    "TRAVELOKA","TRAVELOKA BUSINESS","TIKET.COM","BOOKING.COM","BOOKING COM",
    "AGODA","AGODA CORPORATE","EXPEDIA","HOTELS.COM",
})

# ── Avatar loader ─────────────────────────────────────────────────────────────
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

# ── Helpers ───────────────────────────────────────────────────────────────────
def _is_valid_file(f):
    try: _ = f.name; _ = f.size; return True
    except: return False

def compute_upload_hash(files):
    valid = [f for f in files if _is_valid_file(f)]
    if not valid: return ""
    h = hashlib.md5()
    for f in sorted(valid, key=lambda x: x.name):
        h.update(f.name.encode()); h.update(str(f.size).encode())
    return h.hexdigest()

def make_view_hash(df: pd.DataFrame) -> str:
    try:
        return hashlib.md5(
            pd.util.hash_pandas_object(df, index=True).values.tobytes()
        ).hexdigest()
    except:
        return hashlib.md5(f"{df.shape}{list(df.columns)}".encode()).hexdigest()

def compact_num(v):
    try:
        v = float(v); a = abs(v)
        if a >= 1_000_000_000: return f"{v/1_000_000_000:.1f}B"
        if a >= 1_000_000:     return f"{v/1_000_000:.1f}M"
        if a >= 1_000:         return f"{v/1_000:.1f}K"
        return f"{int(v):,}"
    except: return str(v)

def theme(fig):
    fig.update_layout(
        font_family="Open Sans", font_color="#525F7F", font_size=12,
        plot_bgcolor="rgba(255,255,255,0)", paper_bgcolor="rgba(255,255,255,0)",
        margin=dict(l=12,r=12,t=40,b=12),
        title_font=dict(size=13,color="#32325D",family="Open Sans"),
        legend=dict(font=dict(size=11),bgcolor="rgba(255,255,255,.8)",bordercolor="rgba(0,0,0,.06)",borderwidth=1),
        hoverlabel=dict(bgcolor="#ffffff",bordercolor="rgba(0,0,0,.1)",font_size=12,font_color="#32325D"),
    )
    fig.update_xaxes(showgrid=True,gridcolor="rgba(0,0,0,.06)",zeroline=False,tickfont=dict(size=11,color="#8898AA"),linecolor="rgba(0,0,0,.08)")
    fig.update_yaxes(showgrid=True,gridcolor="rgba(0,0,0,.06)",zeroline=False,tickfont=dict(size=11,color="#8898AA"),linecolor="rgba(0,0,0,.08)")
    return fig

def gsec(title, icon=""):
    lbl = f'<span class="gsec-icon">{icon}</span>&thinsp;{title}' if icon else title
    st.markdown(f'<div class="gsec">{lbl}</div>', unsafe_allow_html=True)

# ── GDrive fetch (cached 1 jam) ───────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_gdrive_mapping(file_id: str):
    try:
        r = requests.get(f"https://drive.google.com/uc?export=download&id={file_id}", timeout=20)
        r.raise_for_status()
        df_map = pd.read_excel(io.BytesIO(r.content), engine="openpyxl")
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

# ── Excel reader (optimized) ──────────────────────────────────────────────────
def _read_excel_fast(file_obj) -> pd.DataFrame:
    df_header = pd.read_excel(file_obj, nrows=0, engine="openpyxl")
    file_obj.seek(0)
    available_cols = [c for c in df_header.columns if str(c).strip() not in DROP_SET]
    dtype_hints = {}
    for c in available_cols:
        cl = str(c).strip().lower()
        if any(k in cl for k in ["sales","profit","room","night","fare","charge"]):
            dtype_hints[c] = "float32"
    return pd.read_excel(file_obj, usecols=available_cols, dtype=dtype_hints, engine="openpyxl")

# ── Build df_raw (optimized, vectorized) ─────────────────────────────────────
def build_df_raw(files, norm_maps):
    dfs = []
    for f in files:
        try:
            dfs.append(_read_excel_fast(f))
        except Exception as e:
            st.toast(f"⚠️ Gagal baca {getattr(f,'name','file')}: {e}", icon="⚠️")
    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)
    df.columns = [str(c).strip() for c in df.columns]

    for col in ["Check In","Check Out","Issued Date","Inv Date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    if "Issued Date" in df.columns:
        df["Issued_Month"] = df["Issued Date"].dt.strftime("%B")
        df["Issued_Year"]  = df["Issued Date"].dt.year.astype("Int16")

    for map_key, src_col, dst_col in [
        ("hotel_city",    "Hotel City",    "Hotel_City"),
        ("hotel_name",    "Hotel Name",    "Hotel_Name"),
        ("hotel_chain",   "Hotel Chain",   "Hotel_Chain"),
        ("hotel_supplier","Supplier Name", "Supplier_Name"),
    ]:
        if norm_maps.get(map_key) and src_col in df.columns:
            df[dst_col] = df[src_col].map(norm_maps[map_key])
            if map_key == "hotel_supplier":
                df[dst_col] = df[dst_col].fillna(df[src_col].fillna("Direct to Hotel"))
            else:
                df[dst_col] = df[dst_col].fillna(df[src_col])

    # Supplier Category — vectorized np.select
    if "Supplier Name" in df.columns or "Supplier_Name" in df.columns:
        _raw  = df["Supplier Name"].astype(str).str.strip().str.upper() \
                if "Supplier Name" in df.columns \
                else pd.Series([""] * len(df), index=df.index)
        _norm = df["Supplier_Name"].astype(str).str.strip().str.upper() \
                if "Supplier_Name" in df.columns else _raw

        if norm_maps.get("supplier_category"):
            sc_map_upper = {str(k).strip().upper(): v for k, v in norm_maps["supplier_category"].items()}
            df["Supplier_Category"] = _raw.map(sc_map_upper)
            _nan = df["Supplier_Category"].isna()
            if _nan.any():
                df.loc[_nan, "Supplier_Category"] = _norm[_nan].map(sc_map_upper)
        else:
            df["Supplier_Category"] = np.nan

        _unc = df["Supplier_Category"].isna() | \
               (df["Supplier_Category"].astype(str).str.strip() == "Uncategorized")
        if _unc.any():
            conds = [
                (_raw.isin(_KNOWN_DIRECT_SET)      | _norm.isin(_KNOWN_DIRECT_SET))     & _unc,
                (_raw.isin(_KNOWN_CORPORATE_SET)   | _norm.isin(_KNOWN_CORPORATE_SET))  & _unc,
                (_raw.isin(_KNOWN_WHOLESALERS_SET) | _norm.isin(_KNOWN_WHOLESALERS_SET))& _unc,
                (_raw.isin(_KNOWN_OTA_SET)         | _norm.isin(_KNOWN_OTA_SET))         & _unc,
                (_raw.str.contains("BEDBANK|WHOLESAL", regex=True, na=False))             & _unc,
                (_raw.str.contains("DIRECT", regex=False, na=False))                      & _unc,
                (_raw.str.contains("CORP.*RATE|RATE.*CORP", regex=True, na=False))        & _unc,
            ]
            choices = ["DIRECT HOTEL","CORPORATE RATE","WHOLESALER","OTA",
                       "WHOLESALER","DIRECT HOTEL","CORPORATE RATE"]
            df.loc[_unc, "Supplier_Category"] = np.select(
                [c[_unc] for c in conds], choices, default="Uncategorized"
            )

        df["Supplier_Category"] = df["Supplier_Category"].fillna("Uncategorized")
        _sc_rename = {
            "DIRECT TO HOTEL":"DIRECT HOTEL","DIRECT HOTEL":"DIRECT HOTEL",
            "PTM CORP RATE":"CORPORATE RATE","CORPORATE RATE":"CORPORATE RATE",
            "WHOLESALER":"WHOLESALER","OTA":"OTA",
        }
        _sc_upper = df["Supplier_Category"].astype(str).str.strip().str.upper()
        df["Supplier_Category"] = _sc_upper.map(_sc_rename).fillna(
            df["Supplier_Category"].astype(str).str.strip()
        )
    else:
        df["Supplier_Category"] = "Uncategorized"

    for raw, clean in [("Hotel City","Hotel_City"),("Hotel Name","Hotel_Name"),("Hotel Chain","Hotel_Chain")]:
        if raw in df.columns and clean not in df.columns:
            df[clean] = df[raw]
    if "Supplier Name" in df.columns and "Supplier_Name" not in df.columns:
        df["Supplier_Name"] = df["Supplier Name"].fillna("Direct to Hotel")

    if "Room" in df.columns and "Night" in df.columns:
        df["Total Room Night"] = (
            pd.to_numeric(df["Room"],  errors="coerce").fillna(0) *
            pd.to_numeric(df["Night"], errors="coerce").fillna(0)
        )
        cols = list(df.columns); cols.remove("Total Room Night")
        if "Night" in cols: cols.insert(cols.index("Night")+1,"Total Room Night")
        else: cols.append("Total Room Night")
        df = df[cols]

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
        df["Full Name"] = df[single_col].fillna("").astype(str).str.strip().str.upper()
    else:
        sf = df[first_col].fillna("").astype(str).str.strip() if first_col and first_col in df.columns else pd.Series([""]*len(df), index=df.index)
        sl = df[last_col].fillna("").astype(str).str.strip()  if last_col  and last_col  in df.columns else pd.Series([""]*len(df), index=df.index)
        df["Full Name"] = (sf+" "+sl).str.strip().str.upper()

    _inv_col = next(
        (c for c in df.columns if any(k in c.lower() for k in
         ["invoice to","invoiceto","bill to","billto","invoice_to"])), None
    )
    if _inv_col:
        CBT_ALIASES = frozenset({
            "CBT PERTAMINA(HOTEL CM)","CBT PERTAMINA (HOTEL)",
            "PERTAMINA ENERGY TERMINAL (CBT)",
        })
        _inv_str = df[_inv_col].astype(str).str.strip()
        df["Normalized_Inv_To"] = np.where(
            _inv_str.str.upper().isin(CBT_ALIASES), "CBT PERTAMINA",
            np.where(_inv_str.isin(["","nan","None","NaN"]), "Unknown", _inv_str)
        )

    df.drop(columns=[c for c in DROP_SET if c in df.columns], errors="ignore", inplace=True)
    return df

# ── maybe_rebuild_df ──────────────────────────────────────────────────────────
def maybe_rebuild_df(uploaded_files, norm_maps):
    if not uploaded_files:
        for k in ["df_raw","upload_hash"]: st.session_state.pop(k,None)
        return False
    _fh = compute_upload_hash(uploaded_files)
    _nh = hashlib.md5(str(sorted((k,len(v)) for k,v in norm_maps.items())).encode()).hexdigest()
    _combined = _fh + _nh
    if st.session_state.get("upload_hash") == _combined and "df_raw" in st.session_state:
        return False
    with st.spinner("⏳ Memproses data..."):
        st.session_state["df_raw"]      = build_df_raw(uploaded_files, norm_maps)
        st.session_state["upload_hash"] = _combined
    return True

# ── Cached per-tab computations ───────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def _cached_invoice_trend(vh, _df):
    if "Issued Date" not in _df.columns or "Invoice No" not in _df.columns:
        return None, None
    dt = _df.dropna(subset=["Issued Date","Invoice No"]).copy()
    dt = dt[~dt["Invoice No"].astype(str).str.strip().isin(["","nan","None","NaN"])]
    dt["Mon"]  = dt["Issued Date"].dt.strftime("%b")
    dt["MonN"] = dt["Issued Date"].dt.month
    ti = dt.groupby(["Mon","MonN"])["Invoice No"].nunique().reset_index()
    ti.columns = ["Bulan","MonN","Invoice"]; ti = ti.sort_values("MonN")
    ti["Invoice"] = pd.to_numeric(ti["Invoice"], errors="coerce").fillna(0)
    tr2 = None
    if "Total Room Night" in dt.columns:
        tr2 = dt.groupby("Mon")["Total Room Night"].sum().reset_index()
        tr2 = tr2.rename(columns={"Mon":"Bulan","Total Room Night":"Room Night"})
    return ti, tr2

@st.cache_data(show_spinner=False)
def _cached_supplier(vh, _df):
    if "Supplier_Name" not in _df.columns or "Total Room Night" not in _df.columns:
        return None, None
    _s = (_df[["Supplier_Name","Total Room Night"]]
          .dropna(subset=["Supplier_Name"])
          .assign(Supplier_Name=lambda d: d["Supplier_Name"].astype(str).str.strip())
          .pipe(lambda d: d[~d["Supplier_Name"].isin(["","nan","None","NaN"])]))
    ss = _s.groupby("Supplier_Name",dropna=True)["Total Room Night"].sum().reset_index().sort_values("Total Room Night",ascending=False)
    d  = pd.concat([ss.head(5),
                    pd.DataFrame([{"Supplier_Name":"Others","Total Room Night":ss.iloc[5:]["Total Room Night"].sum()}])
                    if len(ss)>5 else pd.DataFrame()], ignore_index=True)
    return ss, d

@st.cache_data(show_spinner=False)
def _cached_product(vh, _df):
    if "Product Type" not in _df.columns or "Total Room Night" not in _df.columns:
        return None, None
    _p = (_df[["Product Type","Total Room Night"]]
          .dropna(subset=["Product Type"])
          .assign(**{"Product Type": lambda d: d["Product Type"].astype(str).str.strip()})
          .pipe(lambda d: d[~d["Product Type"].isin(["","nan","None","NaN"])]))
    ps = _p.groupby("Product Type",dropna=True)["Total Room Night"].sum().reset_index().sort_values("Total Room Night",ascending=False)
    d  = pd.concat([ps.head(6),
                    pd.DataFrame([{"Product Type":"Others","Total Room Night":ps.iloc[6:]["Total Room Night"].sum()}])
                    if len(ps)>6 else pd.DataFrame()], ignore_index=True)
    return ps, d

@st.cache_data(show_spinner=False)
def _cached_ptm(vh, _df):
    if not all(c in _df.columns for c in ["Supplier_Name","Hotel_Name","Total Room Night"]):
        return None
    dfptm = _df[_df["Supplier_Name"].astype(str).str.upper()=="PTM CORP RATE"]
    if dfptm.empty: return pd.DataFrame()
    _dp = (dfptm[["Hotel_Name","Total Room Night"]]
           .dropna(subset=["Hotel_Name"])
           .assign(Hotel_Name=lambda d: d["Hotel_Name"].astype(str).str.strip())
           .pipe(lambda d: d[~d["Hotel_Name"].isin(["","nan","None","NaN"])]))
    return (_dp.groupby("Hotel_Name",dropna=True,as_index=False)
               .agg({"Total Room Night":"sum"})
               .sort_values("Total Room Night",ascending=False))

@st.cache_data(show_spinner=False)
def _cached_cat(vh, _df):
    if "Supplier_Category" not in _df.columns or "Total Room Night" not in _df.columns:
        return None, None
    _c = (_df[["Supplier_Category","Total Room Night"]]
          .dropna(subset=["Supplier_Category"])
          .assign(Supplier_Category=lambda d: d["Supplier_Category"].astype(str).str.strip())
          .pipe(lambda d: d[~d["Supplier_Category"].isin(["","nan","None","NaN"])]))
    cs = _c.groupby("Supplier_Category",dropna=True)["Total Room Night"].sum().reset_index().sort_values("Total Room Night",ascending=False)
    d  = pd.concat([cs.head(5),
                    pd.DataFrame([{"Supplier_Category":"Others","Total Room Night":cs.iloc[5:]["Total Room Night"].sum()}])
                    if len(cs)>5 else pd.DataFrame()], ignore_index=True)
    return cs, d

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
        overlap_start = max(prev_min, raw_min); overlap_end = min(prev_max, raw_max)
        overlap_days  = (overlap_end - overlap_start).days + 1
        period_days   = max((prev_max - prev_min).days + 1, 1)
        if overlap_days / period_days < 0.80: return {}
        prev_df = df_raw[(df_raw["Issued Date"] >= prev_min) & (df_raw["Issued Date"] <= prev_max)]
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
            _p = prev_df["Profit"].fillna(0).astype(float)
            _s = prev_df["Sales AR"].fillna(0).astype(float)
            _mm = _s != 0
            m["pm"] = float((_p[_mm] / _s[_mm] * 100).mean()) if _mm.any() else 0.0
        return m
    except: return {}

# ── Donut HTML ────────────────────────────────────────────────────────────────
def build_donut_html(segments, total_label, subtitle=""):
    import json
    segs_js = json.dumps(segments, ensure_ascii=False)
    return f"""<!DOCTYPE html><html lang="id"><head><meta charset="UTF-8">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;600;700&family=Space+Grotesk:wght@600;700;800&display=swap" rel="stylesheet">
<style>*{{box-sizing:border-box;margin:0;padding:0;}}body{{background:transparent;font-family:'DM Sans',sans-serif;}}
.wrap{{background:#fff;border:1px solid #E2E8F0;border-radius:16px;padding:18px 20px 14px;box-shadow:0 1px 4px rgba(0,0,0,.05),0 8px 24px -6px rgba(13,148,136,.08);width:100%;}}
.hdr{{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:16px;gap:8px;}}
.eyebrow{{font-size:.52rem;font-weight:700;letter-spacing:1.2px;text-transform:uppercase;color:#94A3B8;margin-bottom:3px;}}
.title{{font-family:'Space Grotesk',sans-serif;font-size:.9rem;font-weight:700;color:#0F172A;}}
.subtitle{{font-size:.55rem;color:#94A3B8;margin-top:2px;}}
.live{{display:flex;align-items:center;gap:5px;background:#F0FDF9;border:1px solid #CCFBF1;border-radius:8px;padding:4px 10px;font-size:.54rem;font-weight:700;color:#0D9488;white-space:nowrap;}}
.live-dot{{width:6px;height:6px;border-radius:50%;background:#0D9488;animation:pulse 2s infinite;}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
.body{{display:flex;align-items:center;gap:20px;flex-wrap:wrap;}}
.chart-wrap{{position:relative;flex-shrink:0;}}
.center-txt{{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);text-align:center;}}
.center-num{{font-family:'Space Grotesk',sans-serif;font-size:1.2rem;font-weight:800;color:#0F172A;}}
.center-lbl{{font-size:.5rem;font-weight:600;letter-spacing:.8px;text-transform:uppercase;color:#94A3B8;}}
.legend{{flex:1;min-width:140px;display:flex;flex-direction:column;gap:2px;}}
.leg-row{{display:flex;align-items:center;gap:10px;padding:7px 10px;border-radius:10px;cursor:default;}}
.leg-row:hover{{background:#F0FDF9;}}
.leg-color{{width:10px;height:10px;border-radius:3px;flex-shrink:0;}}
.leg-body{{flex:1;min-width:0;}}
.leg-name{{font-size:.66rem;font-weight:700;color:#0F172A;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-bottom:3px;}}
.leg-bar-wrap{{height:4px;background:#F1F5F9;border-radius:4px;overflow:hidden;margin-bottom:3px;}}
.leg-bar{{height:100%;border-radius:4px;width:0;transition:width 1s cubic-bezier(.4,0,.2,1);}}
.leg-meta{{display:flex;justify-content:space-between;align-items:center;}}
.leg-val{{font-family:'Space Grotesk',sans-serif;font-size:.6rem;font-weight:700;}}
.leg-pct{{font-size:.54rem;font-weight:600;padding:1px 6px;border-radius:10px;}}
.footer{{margin-top:12px;padding-top:10px;border-top:1px solid #F1F5F9;display:flex;justify-content:space-between;font-size:.51rem;color:#CBD5E1;}}
</style></head><body>
<div class="wrap">
  <div class="hdr"><div><div class="eyebrow">Distribusi Invoice</div><div class="title">&#127758; Domestic vs International</div><div class="subtitle">{subtitle}</div></div>
  <div class="live"><span class="live-dot"></span>Live</div></div>
  <div class="body">
    <div class="chart-wrap"><svg width="180" height="180" viewBox="0 0 180 180" id="svg"></svg>
    <div class="center-txt"><div class="center-num" id="cnum">—</div><div class="center-lbl">INVOICE</div></div></div>
    <div class="legend" id="lg"></div>
  </div>
  <div class="footer"><div>Invoice unik per kategori destinasi</div><div id="ts"></div></div>
</div>
<script>
const SEGS={segs_js},TOTAL="{total_label}",CX=90,CY=90,R=72,SW=28,GAP=2.4;
const svg=document.getElementById('svg');
document.getElementById('cnum').textContent=TOTAL;
document.getElementById('ts').textContent=new Date().toLocaleDateString('id-ID',{{day:'2-digit',month:'short',year:'numeric'}});
const total_pct=SEGS.reduce((a,s)=>a+s.pct,0)||100,available=360-GAP*SEGS.length;
function P(cx,cy,r,deg){{const rad=(deg-90)*Math.PI/180;return[cx+r*Math.cos(rad),cy+r*Math.sin(rad)];}}
function arc(s,e){{const ro=R+SW/2,ri=R-SW/2,lg=e-s>180?1:0;
const[x1,y1]=P(CX,CY,ro,s+1),[x2,y2]=P(CX,CY,ro,e-1),[ix1,iy1]=P(CX,CY,ri,e-1),[ix2,iy2]=P(CX,CY,ri,s+1);
return`M ${{x1}} ${{y1}} A ${{ro}} ${{ro}} 0 ${{lg}} 1 ${{x2}} ${{y2}} L ${{ix1}} ${{iy1}} A ${{ri}} ${{ri}} 0 ${{lg}} 0 ${{ix2}} ${{iy2}} Z`;}}
const tr=document.createElementNS('http://www.w3.org/2000/svg','circle');
tr.setAttribute('cx',CX);tr.setAttribute('cy',CY);tr.setAttribute('r',R);tr.setAttribute('fill','none');tr.setAttribute('stroke','#F1F5F9');tr.setAttribute('stroke-width',SW);svg.appendChild(tr);
let sd=0;SEGS.forEach((sg,i)=>{{const sw=sg.pct/total_pct*available,ed=sd+sw;
const p=document.createElementNS('http://www.w3.org/2000/svg','path');p.setAttribute('fill',sg.color);p.setAttribute('d',arc(sd,ed));p.style.opacity='0';p.style.transition=`opacity .3s ease ${{i*.12}}s`;svg.appendChild(p);sd=ed+GAP;}});
setTimeout(()=>svg.querySelectorAll('path').forEach(p=>p.style.opacity='1'),100);
const lg=document.getElementById('lg');
SEGS.forEach((sg,i)=>{{const row=document.createElement('div');row.className='leg-row';
row.innerHTML=`<div class="leg-color" style="background:${{sg.color}};"></div><div class="leg-body"><div class="leg-name">${{sg.label}}</div><div class="leg-bar-wrap"><div class="leg-bar" id="b${{i}}" style="background:${{sg.color}};"></div></div><div class="leg-meta"><span class="leg-val" style="color:${{sg.color}};">${{Number(sg.value).toLocaleString('id-ID')}}</span><span class="leg-pct" style="background:${{sg.color}}18;color:${{sg.color}};">${{sg.pct}}%</span></div></div>`;
lg.appendChild(row);}});
setTimeout(()=>SEGS.forEach((_,i)=>document.getElementById('b'+i).style.width=SEGS[i].pct+'%'),350);
</script></body></html>"""

# ══════════════════════════════════════════════════════════════════════════════
# CSS
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=DM+Sans:wght@400;500;600;700;800&family=Sora:wght@400;600;700;800&display=swap');
:root{--bg:#F4F6F9;--card:#FFFFFF;--t1:#0F172A;--t2:#334155;--t3:#64748B;--t4:#94A3B8;--primary:#0D9488;--primary2:#0F766E;--primary3:#134E4A;--primary-light:#F0FDFA;--primary-mid:#CCFBF1;--border:#E2E8F0;--border2:#CBD5E1;--shadow-sm:0 1px 2px 0 rgba(0,0,0,.05);--shadow-teal:0 8px 24px -4px rgba(13,148,136,.18);--r:.375rem;--r2:.5rem;--r3:.75rem;--font:'Inter',sans-serif;--font-head:'DM Sans',sans-serif;--font-display:'Sora',sans-serif;}
*,*::before,*::after{box-sizing:border-box;}
html,body,[class*="css"]{font-family:var(--font)!important;font-size:13px!important;color:var(--t2)!important;background-color:var(--bg)!important;-webkit-font-smoothing:antialiased;}
.stApp,body{background-color:var(--bg)!important;background-image:none!important;}
.block-container{padding:0!important;max-width:100%!important;background:transparent!important;overflow-x:hidden!important;}
.main .block-container,[data-testid="stMainBlockContainer"]{padding:24px 38px 80px!important;max-width:100%!important;}
section[data-testid="stMain"]>div{padding:24px 38px 80px!important;overflow-x:hidden!important;}
[data-testid="collapsedControl"],[data-testid="stSidebarCollapseButton"],button[data-testid="baseButton-header"],#MainMenu,footer,header{display:none!important;}
::-webkit-scrollbar{width:5px;height:5px;}::-webkit-scrollbar-track{background:#f1f3f4;}::-webkit-scrollbar-thumb{background:#CDD0D5;border-radius:10px;}
.ghdr{background:#FFF;padding:0 36px;height:60px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:500;border-bottom:1px solid var(--border);}
.ghdr-brand{display:flex;align-items:center;gap:12px;}
.ghdr-logo{width:34px;height:34px;background:var(--primary-light);border-radius:var(--r2);display:grid;place-items:center;flex-shrink:0;border:1px solid var(--primary-mid);}
.ghdr-name{font-family:var(--font-head);font-size:.93rem;font-weight:700;color:var(--t1);}
.ghdr-name span{color:var(--primary);font-weight:500;}
.ghdr-sub{font-size:.58rem;color:var(--t4);margin-top:2px;letter-spacing:.5px;text-transform:uppercase;}
.ghdr-right{display:flex;align-items:center;gap:8px;}
.ghdr-live{display:flex;align-items:center;gap:6px;font-size:.6rem;font-weight:600;color:var(--primary);padding:5px 12px;border-radius:20px;background:var(--primary-light);border:1px solid var(--primary-mid);}
.ghdr-dot{width:6px;height:6px;border-radius:50%;background:var(--primary);animation:livebeat 2s ease-in-out infinite;}
@keyframes livebeat{0%,100%{opacity:1;}50%{opacity:.4;}}
.ghdr-pill{font-size:.6rem;font-weight:600;color:var(--t3);padding:5px 12px;border-radius:20px;background:var(--bg);border:1px solid var(--border);}
.gticker{background:var(--primary-light);border-bottom:1px solid var(--primary-mid);padding:6px 0;overflow:hidden;position:relative;}
.gticker::before,.gticker::after{content:'';position:absolute;top:0;width:80px;height:100%;z-index:2;}
.gticker::before{left:0;background:linear-gradient(90deg,var(--primary-light),transparent);}
.gticker::after{right:0;background:linear-gradient(270deg,var(--primary-light),transparent);}
.gticker-track{display:inline-block;white-space:nowrap;animation:tickslide 65s linear infinite;font-size:.58rem;letter-spacing:.8px;text-transform:uppercase;}
.gticker-track:hover{animation-play-state:paused;}
.t-item{color:var(--t4);}.t-item.hi{color:var(--primary);font-weight:600;}
.tsep{margin:0 24px;color:var(--primary-mid);}
@keyframes tickslide{from{transform:translateX(0)}to{transform:translateX(-50%)}}
[data-testid="stSidebar"]{background:#FFF!important;border-right:1px solid var(--border)!important;min-width:256px!important;max-width:256px!important;}
[data-testid="stSidebar"]>div:first-child{padding:0!important;}
.sb-top{padding:20px 18px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:11px;}
.sb-logo{width:32px;height:32px;background:var(--primary-light);border-radius:var(--r2);display:grid;place-items:center;flex-shrink:0;border:1px solid var(--primary-mid);}
.sb-appname{font-family:var(--font-head);font-size:.85rem;font-weight:700;color:var(--t1);}
.sb-appname span{color:var(--primary);font-weight:500;}
.sb-ver{font-size:.56rem;color:var(--t4);margin-top:2px;}
.sb-section{padding:16px 18px 6px;font-size:.58rem;font-weight:600;color:var(--t4)!important;text-transform:uppercase;letter-spacing:1.5px;}
.sb-divider{height:1px;background:var(--border);margin:4px 16px;}
.sync-row{display:flex;align-items:center;justify-content:space-between;padding:6px 18px;transition:all .15s;border-radius:var(--r);margin:1px 6px;}
.sync-row:hover{background:var(--bg);}
.sync-label{font-size:.7rem;color:var(--t2);font-weight:500;}
.stag{font-size:.56rem;font-weight:600;padding:2px 9px;border-radius:20px;}
.stag-ok{background:#F0FDF4;color:#16A34A;border:1px solid #BBF7D0;}
.stag-err{background:#FFF1F2;color:#DC2626;border:1px solid #FECACA;}
.stag-wait{background:#F8FAFC;color:var(--t4);border:1px solid var(--border);}
[data-testid="stSidebar"] [data-testid="stFileUploader"]{background:#FAFFFE!important;border:1.5px dashed var(--primary-mid)!important;border-radius:var(--r)!important;}
[data-testid="stButton"]>button{background:var(--primary)!important;color:#fff!important;border:none!important;border-radius:var(--r)!important;font-size:.72rem!important;font-weight:600!important;padding:9px 20px!important;transition:all .15s!important;}
[data-testid="stButton"]>button:hover{background:var(--primary2)!important;transform:translateY(-1px)!important;}
[data-testid="stDownloadButton"]>button{background:#fff!important;color:var(--t2)!important;border:1px solid var(--border)!important;border-radius:var(--r)!important;font-size:.7rem!important;font-weight:500!important;padding:9px 20px!important;transition:all .15s!important;}
[data-testid="stTabs"] [data-baseweb="tab-list"]{background:var(--bg)!important;border:1px solid var(--border)!important;border-radius:var(--r2)!important;gap:2px!important;padding:4px!important;margin-bottom:24px;}
[data-testid="stTabs"] [data-baseweb="tab"]{font-size:.71rem!important;font-weight:500!important;color:var(--t3)!important;padding:8px 18px!important;border-bottom:none!important;border-radius:var(--r)!important;}
[data-testid="stTabs"] [aria-selected="true"]{color:var(--t1)!important;font-weight:600!important;background:#fff!important;box-shadow:0 1px 3px rgba(0,0,0,.08)!important;}
.gsec{display:flex;align-items:center;gap:10px;font-size:.65rem;font-weight:600;color:var(--t3);text-transform:uppercase;letter-spacing:.8px;margin:8px 0 16px;}
.gsec::after{content:'';flex:1;height:1px;background:var(--border);}
.gsec-icon{font-size:.75rem;}
.norm-bar{display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin-bottom:20px;padding:9px 14px;background:#fff;border:1px solid var(--border);border-radius:var(--r);box-shadow:var(--shadow-sm);}
.norm-cap{font-size:.56rem;font-weight:600;color:var(--t4);text-transform:uppercase;letter-spacing:1.5px;margin-right:6px;}
.npill{font-size:.6rem;font-weight:500;padding:3px 11px;border-radius:20px;background:var(--bg);border:1px solid var(--border);color:var(--t4);}
.npill.on{background:var(--primary-light);border-color:var(--primary-mid);color:var(--primary2);}
[data-testid="stDataFrame"]{border:1px solid var(--border)!important;border-radius:var(--r)!important;overflow:hidden!important;background:#fff!important;}
[data-testid="stDataFrame"] th{background:#FAFAFA!important;font-size:.64rem!important;font-weight:600!important;color:var(--t4)!important;text-transform:uppercase!important;border-bottom:1px solid var(--border)!important;}
[data-testid="stDataFrame"] td{font-size:.71rem!important;color:var(--t2)!important;border-color:var(--border)!important;}
[data-testid="stAlert"]{background:var(--primary-light)!important;border:1px solid var(--primary-mid)!important;border-left:3px solid var(--primary)!important;border-radius:var(--r)!important;font-size:.71rem!important;}
.pic2-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(270px,1fr));gap:18px;margin-bottom:28px;}
.pic2-card{background:#fff;border:1px solid var(--border);border-radius:16px;overflow:hidden;display:flex;flex-direction:column;transition:box-shadow .22s,transform .22s;cursor:default;box-shadow:0 1px 4px rgba(0,0,0,.06);}
.pic2-card:hover{transform:translateY(-4px);box-shadow:0 16px 40px -8px rgba(13,148,136,.20);}
.p2-banner{background:linear-gradient(135deg,#0D9488 0%,#042F2E 100%);padding:20px 18px 16px;display:flex;align-items:center;gap:14px;position:relative;overflow:hidden;}
.pic2-card.other .p2-banner{background:linear-gradient(135deg,#475569 0%,#1E293B 100%);}
.p2av{width:54px;height:54px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-family:var(--font-display);font-size:.92rem;font-weight:800;color:#fff;background:rgba(255,255,255,.16);border:2.5px solid rgba(255,255,255,.30);flex-shrink:0;z-index:1;}
.p2av.p2av-photo{background:#E2E8F0;padding:0;overflow:hidden;border:2.5px solid rgba(255,255,255,.50);}
.p2av.p2av-photo img{width:100%;height:100%;object-fit:cover;object-position:center 8%;transform:scale(1.35);transform-origin:center 20%;border-radius:50%;display:block;}
.p2-banner-info{flex:1;min-width:0;z-index:1;}
.p2-name{font-family:var(--font-head);font-size:1rem;font-weight:800;color:#fff;line-height:1.2;}
.p2-role{font-size:.56rem;color:rgba(255,255,255,.60);margin-top:2px;}
.p2-share{display:inline-flex;align-items:center;gap:5px;margin-top:8px;background:rgba(255,255,255,.12);border:1px solid rgba(255,255,255,.20);border-radius:20px;padding:3px 9px;}
.p2-share-dot{width:5px;height:5px;border-radius:50%;background:#2DD4BF;}
.p2-share-txt{font-size:.55rem;font-weight:600;color:rgba(255,255,255,.90);white-space:nowrap;}
.p2-body{padding:14px 16px 6px;display:flex;flex-direction:column;gap:0;flex:1;}
.p2-section-lbl{font-size:.52rem;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:var(--t4);margin:10px 0 6px;display:flex;align-items:center;gap:6px;}
.p2-section-lbl::after{content:'';flex:1;height:1px;background:#F1F5F9;}
.p2-mgroup{display:grid;grid-template-columns:1fr 1fr;gap:1px;background:#F1F5F9;border-radius:10px;overflow:hidden;border:1px solid #F1F5F9;}
.p2-mrow{background:#fff;padding:10px 12px 8px;display:flex;flex-direction:column;gap:2px;}
.p2-mrow:hover{background:#F8FDFC;}
.p2m-top{display:flex;align-items:center;gap:4px;margin-bottom:2px;}
.p2m-icon{font-size:.7rem;}
.p2m-label{font-size:.5rem;font-weight:700;text-transform:uppercase;letter-spacing:.9px;color:var(--t4);}
.p2m-val{font-family:var(--font-display);font-size:1.15rem;font-weight:800;color:var(--t1);letter-spacing:-.5px;line-height:1;}
.p2m-hint{font-size:.49rem;color:var(--t4);margin-top:1px;line-height:1.4;}
.p2-bar{height:3px;background:#EEF2FF;border-radius:10px;overflow:hidden;margin-top:5px;}
.p2-bar-fill{height:100%;border-radius:10px;background:linear-gradient(90deg,#0D9488,#2DD4BF);transition:width .6s cubic-bezier(.4,0,.2,1);}
.p2-bar-fill.muted{background:linear-gradient(90deg,#0F766E,#0D9488);}
.pic2-card.other .p2-bar-fill{background:linear-gradient(90deg,#94A3B8,#CBD5E1);}
.p2-margin-strip{display:grid;grid-template-columns:1fr 1fr;gap:1px;background:#E0F2FE;border-radius:10px;border:1px solid #BAE6FD;overflow:hidden;margin-top:10px;}
.pic2-card.other .p2-margin-strip{background:#F1F5F9;border-color:var(--border);}
.p2-margin-left,.p2-margin-right{background:#F0F9FF;padding:9px 12px;}
.pic2-card.other .p2-margin-left,.pic2-card.other .p2-margin-right{background:#F8FAFC;}
.p2-margin-right{text-align:right;}
.p2-margin-lbl{font-size:.5rem;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#0369A1;}
.pic2-card.other .p2-margin-lbl{color:var(--t3);}
.p2-margin-val{font-family:var(--font-display);font-size:1.05rem;font-weight:800;line-height:1.1;margin-top:2px;color:var(--t1);}
.p2-footer{padding:10px 16px 14px;border-top:1px solid #F1F5F9;background:#FAFBFC;}
.p2-footer-lbl{font-size:.5rem;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:var(--t4);margin-bottom:6px;}
.p2-sup-row{display:flex;align-items:center;justify-content:space-between;gap:8px;background:var(--primary-light);border:1px solid var(--primary-mid);border-radius:8px;padding:7px 11px;}
.pic2-card.other .p2-sup-row{background:#F1F5F9;border-color:var(--border);}
.p2-sup-name{font-size:.62rem;font-weight:600;color:var(--primary2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1;}
.p2-sup-rn{font-size:.57rem;font-weight:700;color:var(--primary);background:#fff;padding:2px 8px;border-radius:12px;border:1px solid var(--primary-mid);flex-shrink:0;}
.p2-no-sup{font-size:.58rem;color:var(--t4);font-style:italic;}
@keyframes fadeSlideUp{from{opacity:0;transform:translateY(16px)}to{opacity:1;transform:translateY(0)}}
@keyframes fadeIn{from{opacity:0}to{opacity:1}}
.gsec{animation:fadeIn .3s ease both;}
</style>
""", unsafe_allow_html=True)

st.markdown("""<script>
(function(){const M="38px",sels=['[data-testid="stMainBlockContainer"]','.main .block-container','.block-container'];
function fix(){sels.forEach(s=>document.querySelectorAll(s).forEach(el=>{el.style.setProperty('padding-left',M,'important');el.style.setProperty('padding-right',M,'important');el.style.setProperty('max-width','100%','important');}));}
fix();new MutationObserver(fix).observe(document.body,{childList:true,subtree:true});})();
</script>""", unsafe_allow_html=True)

st.markdown("""
<div class="ghdr">
  <div class="ghdr-brand">
    <div class="ghdr-logo"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#0D9488" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/></svg></div>
    <div><div class="ghdr-name">Hotel <span>Intelligence</span></div><div class="ghdr-sub">MTT &nbsp;·&nbsp; Opsifin Platform &nbsp;·&nbsp; Travel Analytics</div></div>
  </div>
  <div class="ghdr-right"><span class="ghdr-pill">v9.3</span><div class="ghdr-live"><span class="ghdr-dot"></span>Live</div></div>
</div>
<div class="gticker"><div class="gticker-track">
<span class="t-item hi">Hotel Intelligence Platform</span><span class="tsep">·</span>
<span class="t-item">Invoice &amp; Supplier Analytics</span><span class="tsep">·</span>
<span class="t-item hi">Performance Agent Dashboard</span><span class="tsep">·</span>
<span class="t-item">Supplier Category Intelligence</span><span class="tsep">·</span>
<span class="t-item hi">MTT Travel Analytics · v9.3 · 2025</span><span class="tsep">·</span>
<span class="t-item hi">Hotel Intelligence Platform</span><span class="tsep">·</span>
<span class="t-item">Invoice &amp; Supplier Analytics</span><span class="tsep">·</span>
<span class="t-item hi">Performance Agent Dashboard</span><span class="tsep">·</span>
<span class="t-item">Supplier Category Intelligence</span><span class="tsep">·</span>
<span class="t-item hi">MTT Travel Analytics · v9.3 · 2025</span><span class="tsep">·</span>
</div></div>
""", unsafe_allow_html=True)

# ── Pre-sidebar: rebuild df_raw jika perlu ────────────────────────────────────
_up_raw = st.session_state.get("main_upload") or []
_up     = [f for f in _up_raw if _is_valid_file(f)]
_nm     = st.session_state.get("norm_maps", {})
if _up:
    maybe_rebuild_df(_up, _nm)
elif not _up_raw:
    for k in ["df_raw","upload_hash"]: st.session_state.pop(k,None)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div class="sb-top">
      <div class="sb-logo"><svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="#0D9488" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/></svg></div>
      <div><div class="sb-appname">Hotel <span>Report</span></div><div class="sb-ver">Opsifin · MTT · v9.3</div></div>
    </div>""", unsafe_allow_html=True)

    st.markdown('<div class="sb-section">Data Utama</div>', unsafe_allow_html=True)
    st.file_uploader("Upload Custom Report (.xlsx)", type=["xlsx"],
                     accept_multiple_files=True, key="main_upload",
                     label_visibility="collapsed")

    st.markdown('<div class="sb-divider"></div><div class="sb-section">Normalisasi · Google Drive</div>', unsafe_allow_html=True)
    _ss = st.session_state.get("sync_state", {})
    for k, lbl in GDRIVE_LABELS.items():
        s  = _ss.get(k,"wait")
        tc = {"ok":"stag stag-ok","err":"stag stag-err","wait":"stag stag-wait"}[s]
        tt = {"ok":"Synced","err":"Error","wait":"Pending"}[s]
        st.markdown(f'<div class="sync-row"><span class="sync-label">{lbl}</span><span class="{tc}">{tt}</span></div>', unsafe_allow_html=True)

    st.markdown('<div style="height:12px"></div>', unsafe_allow_html=True)
    do_sync = st.button("🔄  Sync Data", use_container_width=True, key="btn_sync")

    if do_sync:
        nm2, ns2 = fetch_all_mappings_parallel()
        st.session_state["sync_state"] = ns2
        st.session_state["norm_maps"]  = nm2
        for k in ["df_raw","upload_hash"]: st.session_state.pop(k,None)
        all_ok = all(v=="ok" for v in ns2.values())
        if all_ok: st.toast("✅ Semua data normalisasi berhasil!", icon="✅")
        else:
            failed = [GDRIVE_LABELS[k] for k,v in ns2.items() if v!="ok"]
            st.toast(f"⚠️ Gagal: {', '.join(failed)}", icon="⚠️")
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
    <div style="padding:16px 18px 14px;border-top:1px solid #E2E8F0;margin-top:16px;">
      <div style="font-size:.57rem;color:#94A3B8;line-height:2;">
        Hotel Intelligence <span style="color:#0D9488;font-weight:600;">v9.3</span> · 2025<br>
        Rifyal Tumber · MTT
      </div>
    </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
uploaded_files = [f for f in (st.session_state.get("main_upload") or []) if _is_valid_file(f)]

if uploaded_files and "df_raw" in st.session_state:
    df_raw = st.session_state["df_raw"]

    # ── Apply filters ──────────────────────────────────────────────────────────
    df_view = df_raw.copy()
    sel_y = st.session_state.get("f_years",[])
    if sel_y and "Issued_Year" in df_view.columns:
        df_view = df_view[df_view["Issued_Year"].isin(sel_y)]
    sel_i = st.session_state.get("f_inv",[])
    if "Inv Date" in df_view.columns and isinstance(sel_i,(list,tuple)) and len(sel_i)==2:
        df_view = df_view[(df_view["Inv Date"]>=pd.to_datetime(sel_i[0]))&(df_view["Inv Date"]<=pd.to_datetime(sel_i[1]))]
    sel_c = st.session_state.get("f_ci",[])
    if ("Check In" in df_view.columns and "Check Out" in df_view.columns and isinstance(sel_c,(list,tuple)) and len(sel_c)==2):
        df_view = df_view[(df_view["Check In"]>=pd.to_datetime(sel_c[0]))&(df_view["Check Out"]<=pd.to_datetime(sel_c[1]))]

    _vh = make_view_hash(df_view)

    # Normalisasi pill
    ss2 = st.session_state.get("sync_state",{})
    pm_norm = {"Hotel Chain":ss2.get("hotel_chain")=="ok","Hotel City":ss2.get("hotel_city")=="ok",
           "Hotel Name":ss2.get("hotel_name")=="ok","Supplier":ss2.get("hotel_supplier")=="ok",
           "Supplier Cat":ss2.get("supplier_category")=="ok"}
    ph = " ".join(f'<span class="npill {"on" if v else ""}">{k}</span>' for k,v in pm_norm.items())
    st.markdown(f'<div class="norm-bar"><span class="norm-cap">Norm</span>{ph}</div>', unsafe_allow_html=True)

    tab1,tab2,tab3,tab4,tab5,tab6,tab7 = st.tabs([
        "Summary","Tren Invoice","Supplier","Product Type","Agent","PTM Corp","Kategori",
    ])

    # ════════════════════════════════════════════════════════════════
    # TAB 1 — SUMMARY
    # ════════════════════════════════════════════════════════════════
    with tab1:
        tr  = len(df_view)
        tc  = len(df_view.columns)
        ui  = df_view["Invoice No"].nunique()                    if "Invoice No"       in df_view.columns else None
        rn  = int(np.ceil(df_view["Total Room Night"].sum()))    if "Total Room Night" in df_view.columns else None
        sa  = df_view["Sales AR"].fillna(0).astype(float).sum() if "Sales AR"         in df_view.columns else None
        up  = df_view["Full Name"].dropna().nunique()            if "Full Name"        in df_view.columns else None
        pm_val = None
        if "Profit" in df_view.columns and "Sales AR" in df_view.columns:
            _p = df_view["Profit"].fillna(0).astype(float); _s = df_view["Sales AR"].fillna(0).astype(float)
            _m = _s != 0
            pm_val = float((_p[_m]/_s[_m]*100).mean()) if _m.any() else 0.0
        aging_val = None
        if "Check In" in df_view.columns and "Inv Date" in df_view.columns:
            _ag = df_view.dropna(subset=["Check In","Inv Date"]).copy()
            _ag["_aging"] = (_ag["Check In"]-_ag["Inv Date"]).dt.days
            _ag_pos = _ag[_ag["_aging"]>=0]
            if not _ag_pos.empty: aging_val = float(_ag_pos["_aging"].mean())
        tot_supplier = df_view["Supplier_Name"].dropna().nunique() if "Supplier_Name" in df_view.columns else None
        tot_hotel    = df_view["Hotel_Name"].dropna().nunique()    if "Hotel_Name"    in df_view.columns else None
        tot_city     = df_view["Hotel_City"].dropna().nunique()    if "Hotel_City"    in df_view.columns else None
        _pic_col     = next((c for c in df_view.columns if "agent" in c.lower() or "handler" in c.lower()), None)
        tot_pic      = df_view[_pic_col].dropna().nunique() if _pic_col else None
        prev         = get_prev_period_metrics(df_raw, df_view)

        def _badge(curr, prev_val, size="normal"):
            _neu_sm  = '<span style="display:inline-flex;align-items:center;gap:3px;font-size:.53rem;font-weight:600;padding:2px 7px;border-radius:20px;background:#F8FAFC;color:#94A3B8;border:1px solid #E2E8F0;">── No ref</span>'
            _neu_lg  = '<span style="display:inline-flex;align-items:center;gap:4px;font-size:.58rem;font-weight:600;padding:3px 9px;border-radius:20px;background:#F8FAFC;color:#94A3B8;border:1px solid #E2E8F0;">── Belum ada data periode sebelumnya</span>'
            try:
                if curr is None: return _neu_sm if size=="sm" else _neu_lg
                c = float(str(curr).replace(",","").replace("%","")) if isinstance(curr,str) else float(curr)
                if prev_val is None or prev_val==0: return _neu_sm if size=="sm" else _neu_lg
                p=float(prev_val); pct=(c-p)/abs(p)*100 if p!=0 else 0
                is_up=pct>0; arr="▲" if pct>0 else "▼"; abs_pct=f"{abs(pct):.1f}%"
                _bg="#ECFDF5" if is_up else "#FFF1F2"; _col="#059669" if is_up else "#DC2626"; _bdr="#A7F3D0" if is_up else "#FECACA"
                if size=="sm":
                    return f'<span style="display:inline-flex;align-items:center;gap:3px;font-size:.53rem;font-weight:700;padding:2px 7px;border-radius:20px;background:{_bg};color:{_col};border:1px solid {_bdr};">{arr} {abs_pct}</span>'
                return f'<span style="display:inline-flex;align-items:center;gap:4px;font-size:.58rem;font-weight:700;padding:3px 9px;border-radius:20px;background:{_bg};color:{_col};border:1px solid {_bdr};">{arr} {abs_pct} vs periode sebelumnya</span>'
            except: return _neu_sm if size=="sm" else _neu_lg

        def _counter(value, suffix="", prefix=""):
            if value is None: return "N/A"
            v=float(value); a=abs(v)
            if a>=1e9: v_d=f"{v/1e9:.1f}"; suf="B"+suffix
            elif a>=1e6: v_d=f"{v/1e6:.1f}"; suf="M"+suffix
            elif a>=1000: v_d=f"{v/1000:.1f}"; suf="K"+suffix
            elif isinstance(value,float): v_d=f"{v:.1f}"; suf=suffix
            else: v_d=str(int(value)); suf=suffix
            return f'<span style="font-family:\'Sora\',var(--font-display),sans-serif;">{prefix}{v_d}{suf}</span>'

        _has_prev = bool(prev)
        if _has_prev:
            _prev_min_lbl=prev.get("prev_min",""); _prev_max_lbl=prev.get("prev_max",""); _prev_rows=prev.get("rows",0)
            _date_range=f" &nbsp;·&nbsp; {_prev_min_lbl} – {_prev_max_lbl}" if _prev_min_lbl else ""
            _prev_html=(f'<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;padding:8px 14px;background:#fff;border:1px solid #E2E8F0;border-radius:10px;margin-bottom:20px;">'
                        f'<span style="width:7px;height:7px;border-radius:50%;background:#0D9488;display:inline-block;"></span>'
                        f'<span style="font-size:.58rem;color:#0F766E;font-weight:600;">✓ Perbandingan aktif{_date_range}&nbsp;·&nbsp;{_prev_rows:,} baris</span></div>')
        else:
            _prev_html='<div style="padding:8px 14px;background:#F8FAFC;border:1px solid #E2E8F0;border-radius:10px;margin-bottom:20px;"><span style="font-size:.58rem;color:#94A3B8;font-weight:600;">ⓘ Atur filter periode untuk mengaktifkan perbandingan vs periode sebelumnya</span></div>'
        st.markdown(_prev_html, unsafe_allow_html=True)

        b_ui=_badge(ui,prev.get("ui")); b_sa=_badge(sa,prev.get("sa"))
        b_pm=_badge(pm_val,prev.get("pm")) if pm_val is not None else ""

        def _hero(icon,label,value_html,sub,badge_html,accent="linear-gradient(90deg,#0D9488,#134E4A)"):
            return (f'<div style="background:#fff;border:1px solid #E2E8F0;border-radius:14px;padding:0;position:relative;overflow:hidden;transition:box-shadow .2s,transform .2s;"'
                    f' onmouseover="this.style.transform=\'translateY(-3px)\';this.style.boxShadow=\'0 12px 32px -6px rgba(13,148,136,.16)\'"'
                    f' onmouseout="this.style.transform=\'\';this.style.boxShadow=\'\'">'
                    f'<div style="height:3px;background:{accent};border-radius:14px 14px 0 0;"></div>'
                    f'<div style="padding:18px 20px 16px;">'
                    f'<div style="width:38px;height:38px;border-radius:10px;display:grid;place-items:center;margin-bottom:12px;font-size:1.1rem;background:#F0FDFA;border:1px solid #CCFBF1;">{icon}</div>'
                    f'<div style="font-size:.55rem;font-weight:700;letter-spacing:1.2px;text-transform:uppercase;color:#94A3B8;margin-bottom:6px;">{label}</div>'
                    f'<div style="font-size:2rem;font-weight:800;line-height:1;letter-spacing:-1.5px;color:#0F172A;margin-bottom:4px;">{value_html}</div>'
                    f'<div style="font-size:.58rem;color:#94A3B8;margin-bottom:10px;">{sub}</div>'
                    f'{badge_html}</div></div>')

        sa_display = sa if sa is not None else 0
        st.markdown(
            '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:14px;">'
            +_hero("📄","Invoice Unik",_counter(int(ui) if ui else 0),"Total transaksi invoice unik",b_ui)
            +_hero("💰","Sales AR",_counter(sa_display),"Total nilai penjualan (IDR)",b_sa,"linear-gradient(90deg,#134E4A,#0D9488)")
            +_hero("📈","Avg Profit Margin",
                   (f'<span style="font-family:\'Sora\',sans-serif;">{pm_val:.1f}%</span>' if pm_val is not None else "N/A"),
                   "Rata-rata margin keuntungan",b_pm,"linear-gradient(90deg,#0D9488,#2DD4BF)")
            +'</div>', unsafe_allow_html=True)

        b_rn=_badge(rn,prev.get("rn"),size="sm"); b_up=_badge(up,prev.get("up"),size="sm")

        def _cell(label,value_html,hint,badge=""):
            return (f'<div style="padding:14px 18px 12px;border-right:1px solid #E2E8F0;transition:background .12s;cursor:default;"'
                    f' onmouseover="this.style.background=\'#F8FDFC\'" onmouseout="this.style.background=\'\'">'
                    f'<span style="font-size:.53rem;font-weight:700;letter-spacing:.9px;text-transform:uppercase;color:#94A3B8;margin-bottom:5px;display:block;">{label}</span>'
                    f'<span style="font-family:\'Sora\',sans-serif;font-size:1.3rem;font-weight:800;color:#0F172A;letter-spacing:-.5px;display:block;margin-bottom:3px;">{value_html}</span>'
                    f'<span style="font-size:.54rem;color:#94A3B8;display:block;margin-bottom:4px;">{hint}</span>'
                    f'{badge}</div>')

        rn_html    = _counter(int(rn)) if rn is not None else "N/A"
        up_html    = _counter(int(up)) if up is not None else "N/A"
        aging_html = f'<span style="font-family:\'Sora\',sans-serif;">{aging_val:.1f} hari</span>' if aging_val is not None else "N/A"
        tr_html    = _counter(float(tr))

        st.markdown(
            '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:0;background:#fff;border:1px solid #E2E8F0;border-radius:12px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.05);margin-bottom:14px;">'
            '<div style="grid-column:1/-1;padding:8px 16px 6px;font-size:.53rem;font-weight:700;letter-spacing:1.1px;text-transform:uppercase;color:#94A3B8;border-bottom:1px solid #E2E8F0;background:#FAFBFC;">'
            '<span style="width:5px;height:5px;border-radius:50%;background:#0D9488;display:inline-block;margin-right:6px;"></span>Volume &amp; Trafik</div>'
            +_cell("Room Night",rn_html,"Total malam kamar",b_rn)
            +_cell("Pax Unik",up_html,"Nama tamu unik",b_up)
            +_cell("Avg Aging Invoice",aging_html,"Check In − Inv Date")
            +_cell("Total Baris",tr_html,"Baris data aktif")
            +'</div>', unsafe_allow_html=True)

        st.markdown(
            '<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:0;background:#fff;border:1px solid #E2E8F0;border-radius:12px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.05);margin-bottom:20px;">'
            '<div style="grid-column:1/-1;padding:8px 16px 6px;font-size:.53rem;font-weight:700;letter-spacing:1.1px;text-transform:uppercase;color:#94A3B8;border-bottom:1px solid #E2E8F0;background:#FAFBFC;">'
            '<span style="width:5px;height:5px;border-radius:50%;background:#0D9488;display:inline-block;margin-right:6px;"></span>Master Data</div>'
            +_cell("Total Supplier",_counter(int(tot_supplier)) if tot_supplier else "N/A","Supplier unik")
            +_cell("Total Hotel",_counter(int(tot_hotel)) if tot_hotel else "N/A","Hotel unik")
            +_cell("Total City",_counter(int(tot_city)) if tot_city else "N/A","Kota hotel unik")
            +_cell("Total PIC",_counter(int(tot_pic)) if tot_pic else "N/A","Agent / Handler unik")
            +_cell("Kolom Aktif",_counter(int(tc)),"Field tersedia")
            +'</div>', unsafe_allow_html=True)

        # ── Tren bulanan ──────────────────────────────────────────────────────
        if "Issued Date" in df_view.columns and "Invoice No" in df_view.columns:
            _dt = df_view.dropna(subset=["Issued Date","Invoice No"]).copy()
            _dt["_mon_label"] = _dt["Issued Date"].dt.strftime("%b %Y")
            _dt["_mon_num"]   = _dt["Issued Date"].dt.to_period("M").apply(lambda p: p.ordinal)
            _ti = (_dt.groupby(["_mon_label","_mon_num"],as_index=False)["Invoice No"]
                      .nunique().rename(columns={"Invoice No":"Invoice Unik"}).sort_values("_mon_num"))
            _ti["Invoice Unik"] = pd.to_numeric(_ti["Invoice Unik"],errors="coerce").fillna(0).astype(int)
            has_rn = "Total Room Night" in df_view.columns
            if has_rn:
                _tr_s = (_dt.groupby(["_mon_label","_mon_num"],as_index=False)["Total Room Night"].sum().sort_values("_mon_num"))
                _tr_s["Total Room Night"] = pd.to_numeric(_tr_s["Total Room Night"],errors="coerce").fillna(0)

            def _mini_stats(items):
                parts=[f'<div style="display:flex;flex-direction:column;gap:1px;"><span style="font-size:.5rem;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#94A3B8;">{lbl}</span><span style="font-family:\'Sora\',sans-serif;font-size:.88rem;font-weight:800;color:#0F172A;">{val}</span></div>' for lbl,val in items]
                return '<div style="display:flex;gap:16px;margin-bottom:12px;padding-bottom:12px;border-bottom:1px solid #F1F5F9;">'+"".join(parts)+'</div>'

            gsec("📈 Tren Bulanan")
            ct1,ct2=st.columns(2)
            with ct1:
                _ti_total=int(_ti["Invoice Unik"].sum()); _ti_peak=_ti.loc[_ti["Invoice Unik"].idxmax()]; _ti_avg=int(_ti["Invoice Unik"].mean())
                st.markdown('<div style="background:#fff;border:1px solid #E2E8F0;border-radius:12px;padding:16px 18px 4px;box-shadow:0 1px 3px rgba(0,0,0,.05);">'
                            '<div style="font-size:.72rem;font-weight:700;color:#0F172A;margin-bottom:3px;">📄 Tren Invoice Bulanan</div>'
                            '<div style="font-size:.57rem;color:#94A3B8;margin-bottom:10px;">Invoice unik per bulan</div>'
                            +_mini_stats([("Total",compact_num(_ti_total)),("Peak",f'{_ti_peak["_mon_label"]} · {compact_num(int(_ti_peak["Invoice Unik"]))}'),("Avg/bln",compact_num(_ti_avg))])
                            +'</div>', unsafe_allow_html=True)
                _fig_ti=go.Figure()
                _ti_max=_ti["Invoice Unik"].max(); _ti_thresh=max(1,_ti_max*.05)
                _ti_labels=_ti["Invoice Unik"].apply(lambda v: f"{int(v):,}" if v>=_ti_thresh else "")
                _fig_ti.add_trace(go.Scatter(x=_ti["_mon_label"],y=_ti["Invoice Unik"],mode="lines+markers+text",text=_ti_labels,textposition="top center",textfont=dict(size=10,color="#0D9488"),line=dict(color="#0D9488",width=2.5,shape="spline"),marker=dict(size=8,color="#0D9488"),fill="tozeroy",fillcolor="rgba(13,148,136,.08)",hovertemplate="<b>%{x}</b><br>Invoice: <b>%{y:,.0f}</b><extra></extra>",cliponaxis=False))
                _max_ti=_ti.loc[_ti["Invoice Unik"].idxmax()]
                _fig_ti.add_annotation(x=_max_ti["_mon_label"],y=_max_ti["Invoice Unik"],text=f"▲ Peak: {int(_max_ti['Invoice Unik']):,}",showarrow=True,arrowhead=2,arrowcolor="#0D9488",ax=0,ay=-32,font=dict(size=10,color="#0D9488"),bgcolor="rgba(240,253,250,.9)",bordercolor="#0D9488",borderpad=4)
                _fig_ti.update_layout(hovermode="x unified",height=280,xaxis=dict(tickangle=-30),showlegend=False,margin=dict(l=8,r=8,t=12,b=8))
                st.plotly_chart(theme(_fig_ti),use_container_width=True)
            with ct2:
                if has_rn:
                    _rn_total=int(_tr_s["Total Room Night"].sum()); _rn_peak=_tr_s.loc[_tr_s["Total Room Night"].idxmax()]; _rn_avg=int(_tr_s["Total Room Night"].mean())
                    st.markdown('<div style="background:#fff;border:1px solid #E2E8F0;border-radius:12px;padding:16px 18px 4px;box-shadow:0 1px 3px rgba(0,0,0,.05);">'
                                '<div style="font-size:.72rem;font-weight:700;color:#0F172A;margin-bottom:3px;">🌙 Tren Room Night Bulanan</div>'
                                '<div style="font-size:.57rem;color:#94A3B8;margin-bottom:10px;">Total room night per bulan</div>'
                                +_mini_stats([("Total",compact_num(_rn_total)),("Peak",f'{_rn_peak["_mon_label"]} · {compact_num(int(_rn_peak["Total Room Night"]))}'),("Avg/bln",compact_num(_rn_avg))])
                                +'</div>', unsafe_allow_html=True)
                    _fig_rn=go.Figure()
                    _rn_max=_tr_s["Total Room Night"].max()
                    _rn_labels=_tr_s["Total Room Night"].apply(lambda v: f"{int(v):,}" if v>=max(1,_rn_max*.05) else "")
                    _fig_rn.add_trace(go.Scatter(x=_tr_s["_mon_label"],y=_tr_s["Total Room Night"],mode="lines+markers+text",text=_rn_labels,textposition="top center",textfont=dict(size=10,color="#0D9488"),line=dict(color="#0D9488",width=2.5,shape="spline"),marker=dict(size=8,color="#2DD4BF"),fill="tozeroy",fillcolor="rgba(13,148,136,.08)",hovertemplate="<b>%{x}</b><br>Room Night: <b>%{y:,.0f}</b><extra></extra>",cliponaxis=False))
                    _max_rn=_tr_s.loc[_tr_s["Total Room Night"].idxmax()]
                    _fig_rn.add_annotation(x=_max_rn["_mon_label"],y=_max_rn["Total Room Night"],text=f"▲ Peak: {int(_max_rn['Total Room Night']):,}",showarrow=True,arrowhead=2,arrowcolor="#0D9488",ax=0,ay=-32,font=dict(size=10,color="#0D9488"),bgcolor="rgba(240,253,250,.9)",bordercolor="#0D9488",borderpad=4)
                    _fig_rn.update_layout(hovermode="x unified",height=280,xaxis=dict(tickangle=-30),showlegend=False,margin=dict(l=8,r=8,t=12,b=8))
                    st.plotly_chart(theme(_fig_rn),use_container_width=True)
                else:
                    st.info("Kolom Total Room Night tidak tersedia.")

            ct3,ct4=st.columns(2)
            with ct3:
                if "Profit" in df_view.columns:
                    _dt_pr=_dt.copy(); _dt_pr["Profit"]=pd.to_numeric(_dt_pr.get("Profit",0),errors="coerce").fillna(0)
                    _pr_s=(_dt_pr.groupby(["_mon_label","_mon_num"],as_index=False)["Profit"].sum().sort_values("_mon_num"))
                    _pr_s["Profit"]=pd.to_numeric(_pr_s["Profit"],errors="coerce").fillna(0)
                    _pr_total=float(_pr_s["Profit"].sum()); _pr_peak=_pr_s.loc[_pr_s["Profit"].idxmax()] if not _pr_s.empty else None
                    st.markdown('<div style="background:#fff;border:1px solid #E2E8F0;border-radius:12px;padding:16px 18px 4px;box-shadow:0 1px 3px rgba(0,0,0,.05);">'
                                '<div style="font-size:.72rem;font-weight:700;color:#0F172A;margin-bottom:3px;">💹 Tren Profit Bulanan</div>'
                                '<div style="font-size:.57rem;color:#94A3B8;margin-bottom:10px;">Total profit per bulan · IDR</div>'
                                +_mini_stats([("Total Profit",compact_num(_pr_total)),("Margin avg",f"{pm_val:.1f}%" if pm_val is not None else "—"),("Peak",_pr_peak["_mon_label"] if _pr_peak is not None else "—")])
                                +'</div>', unsafe_allow_html=True)
                    _pr_max=_pr_s["Profit"].abs().max()
                    _pr_labels=_pr_s["Profit"].apply(lambda v: compact_num(v) if abs(v)>=max(1,_pr_max*.05) else "")
                    _fig_pr=go.Figure()
                    _fig_pr.add_trace(go.Scatter(x=_pr_s["_mon_label"],y=_pr_s["Profit"],mode="lines+markers+text",text=_pr_labels,textposition="top center",textfont=dict(size=10,color="#134E4A"),line=dict(color="#134E4A",width=2.5,shape="spline"),marker=dict(size=8,color="#2DD4BF"),fill="tozeroy",fillcolor="rgba(19,78,74,.07)",hovertemplate="<b>%{x}</b><br>Profit: <b>%{y:,.0f}</b><extra></extra>",cliponaxis=False))
                    if not _pr_s.empty and _pr_s["Profit"].max()>0:
                        _max_pr=_pr_s.loc[_pr_s["Profit"].idxmax()]
                        _fig_pr.add_annotation(x=_max_pr["_mon_label"],y=_max_pr["Profit"],text=f"▲ Peak: {compact_num(_max_pr['Profit'])}",showarrow=True,arrowhead=2,arrowcolor="#134E4A",ax=0,ay=-32,font=dict(size=10,color="#134E4A"),bgcolor="rgba(255,244,238,.95)",bordercolor="#134E4A",borderpad=4)
                    _fig_pr.update_layout(hovermode="x unified",height=280,xaxis=dict(tickangle=-30),showlegend=False,margin=dict(l=8,r=8,t=12,b=8))
                    st.plotly_chart(theme(_fig_pr),use_container_width=True)
                else: st.info("Kolom Profit tidak tersedia.")
            with ct4:
                if "Hotel_City" in df_view.columns:
                    _dt_cy=_dt.dropna(subset=["Hotel_City"]) if "Hotel_City" in _dt.columns else pd.DataFrame()
                    if not _dt_cy.empty:
                        _cy_s=(_dt_cy.groupby(["_mon_label","_mon_num"],as_index=False)["Hotel_City"].nunique().rename(columns={"Hotel_City":"Kota Unik"}).sort_values("_mon_num"))
                        _cy_s["Kota Unik"]=pd.to_numeric(_cy_s["Kota Unik"],errors="coerce").fillna(0).astype(int)
                        _cy_peak=_cy_s.loc[_cy_s["Kota Unik"].idxmax()] if not _cy_s.empty else None
                        _cy_avg=int(_cy_s["Kota Unik"].mean()) if not _cy_s.empty else 0
                        st.markdown('<div style="background:#fff;border:1px solid #E2E8F0;border-radius:12px;padding:16px 18px 4px;box-shadow:0 1px 3px rgba(0,0,0,.05);">'
                                    '<div style="font-size:.72rem;font-weight:700;color:#0F172A;margin-bottom:3px;">🗺️ Tren Kota Unik Bulanan</div>'
                                    '<div style="font-size:.57rem;color:#94A3B8;margin-bottom:10px;">Jumlah kota hotel unik per bulan</div>'
                                    +_mini_stats([("Max Kota",str(int(_cy_s["Kota Unik"].max())) if not _cy_s.empty else "—"),("Peak",f'{_cy_peak["_mon_label"]} · {int(_cy_peak["Kota Unik"])}' if _cy_peak is not None else "—"),("Avg/bln",str(_cy_avg))])
                                    +'</div>', unsafe_allow_html=True)
                        _cy_max=_cy_s["Kota Unik"].max()
                        _cy_labels=_cy_s["Kota Unik"].apply(lambda v: f"{int(v):,}" if v>=max(1,_cy_max*.05) else "")
                        _fig_cy=go.Figure()
                        _fig_cy.add_trace(go.Scatter(x=_cy_s["_mon_label"],y=_cy_s["Kota Unik"],mode="lines+markers+text",text=_cy_labels,textposition="top center",textfont=dict(size=10,color="#134E4A"),line=dict(color="#134E4A",width=2.5,shape="spline"),marker=dict(size=8,color="#A78BFA"),fill="tozeroy",fillcolor="rgba(19,78,74,.07)",hovertemplate="<b>%{x}</b><br>Kota Unik: <b>%{y:,.0f}</b><extra></extra>",cliponaxis=False))
                        if _cy_peak is not None:
                            _fig_cy.add_annotation(x=_cy_peak["_mon_label"],y=_cy_peak["Kota Unik"],text=f"▲ Peak: {int(_cy_peak['Kota Unik']):,}",showarrow=True,arrowhead=2,arrowcolor="#134E4A",ax=0,ay=-32,font=dict(size=10,color="#134E4A"),bgcolor="rgba(243,238,255,.95)",bordercolor="#134E4A",borderpad=4)
                        _fig_cy.update_layout(hovermode="x unified",height=280,xaxis=dict(tickangle=-30),showlegend=False,margin=dict(l=8,r=8,t=12,b=8))
                        st.plotly_chart(theme(_fig_cy),use_container_width=True)
                else: st.info("Kolom Hotel_City tidak tersedia.")

        # ── Distribusi & Analisis ─────────────────────────────────────────────
        st.markdown('<div style="height:4px"></div>', unsafe_allow_html=True)
        gsec("🏢 Distribusi &amp; Analisis")
        ch1,ch2=st.columns(2)
        with ch1:
            inv_to_col = "Normalized_Inv_To" if "Normalized_Inv_To" in df_view.columns else next(
                (c for c in df_view.columns if any(k in c.lower() for k in ["invoice to","invoiceto","bill to","billto","sold to","client"])),None)
            if inv_to_col and "Invoice No" in df_view.columns:
                _df_inv=(df_view[[inv_to_col,"Invoice No"]].dropna(subset=[inv_to_col])
                         .assign(**{inv_to_col:lambda d: d[inv_to_col].astype(str).str.strip()})
                         .pipe(lambda d: d[~d[inv_to_col].isin(["","nan","None","NaN"])]))
                _total_inv=_df_inv["Invoice No"].nunique()
                top10_inv=(_df_inv.groupby(inv_to_col,dropna=True)["Invoice No"].nunique().reset_index()
                           .rename(columns={"Invoice No":"Invoice Unik"}).sort_values("Invoice Unik",ascending=False).head(10))
                top10_inv["Pct"]=(top10_inv["Invoice Unik"]/_total_inv*100).round(1)
                _max_inv_top=int(top10_inv["Invoice Unik"].max())
                RANK_COLORS=[("#0D9488","#F0FDFA","#CCFBF1"),("#0F766E","#F0FDFA","#99F6E4"),("#134E4A","#F0FDFA","#5EEAD4"),("#334155","#F8FAFC","#E2E8F0")]
                rows_html=""
                for i,row in top10_inv.iterrows():
                    ri=top10_inv.index.get_loc(i); name=str(row[inv_to_col]); w=(int(row["Invoice Unik"])/_max_inv_top*100); pct=row["Pct"]; val=f'{int(row["Invoice Unik"]):,}'
                    ci=min(ri,3); bar_color,bg_rank,bd_rank=RANK_COLORS[ci]
                    rank_badge=f'<div style="width:20px;height:20px;border-radius:6px;flex-shrink:0;display:flex;align-items:center;justify-content:center;font-size:.58rem;font-weight:800;background:{bg_rank};color:{bar_color};border:1px solid {bd_rank};">{ri+1}</div>'
                    row_bg="#fff" if ri%2==0 else "#FAFBFC"
                    rows_html+=(f'<div style="display:grid;grid-template-columns:24px 1fr auto;align-items:center;gap:10px;padding:7px 16px;background:{row_bg};border-bottom:1px solid #F1F5F9;"'
                                f' onmouseover="this.style.background=\'#F0FDFA\'" onmouseout="this.style.background=\'{row_bg}\'">'
                                f'{rank_badge}<div style="min-width:0;">'
                                f'<div style="font-size:.68rem;font-weight:600;color:#0F172A;line-height:1.3;margin-bottom:4px;">{name}</div>'
                                f'<div style="display:flex;align-items:center;gap:6px;"><div style="flex:1;height:5px;background:#F1F5F9;border-radius:5px;overflow:hidden;"><div style="width:{w:.1f}%;height:100%;border-radius:5px;background:linear-gradient(90deg,{bar_color},{"#2DD4BF" if ci<3 else "#94A3B8"});"></div></div>'
                                f'<span style="font-size:.54rem;color:#94A3B8;white-space:nowrap;">{pct:.1f}%</span></div></div>'
                                f'<div style="text-align:right;flex-shrink:0;"><span style="font-family:\'Sora\',sans-serif;font-size:.78rem;font-weight:800;color:#0F172A;">{val}</span>'
                                f'<div style="font-size:.5rem;color:#94A3B8;">invoice</div></div></div>')
                st.markdown(
                    '<div style="background:#fff;border:1px solid #E2E8F0;border-radius:12px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.06);">'
                    '<div style="padding:12px 16px 10px;border-bottom:1px solid #E2E8F0;display:flex;align-items:center;justify-content:space-between;background:linear-gradient(90deg,#F0FDFA,#fff);">'
                    '<span style="font-size:.75rem;font-weight:700;color:#0F172A;">🏢 Top 10 Invoice To</span>'
                    f'<span style="font-size:.55rem;color:#94A3B8;font-style:italic;">dari {_total_inv:,} invoice unik</span></div>'
                    +rows_html+'</div>', unsafe_allow_html=True)
            else:
                st.info("Kolom 'Invoice To' tidak ditemukan.")

        with ch2:
            dom_col=next((c for c in df_view.columns if any(k in c.lower() for k in ["domestic","international","destination","dom/int","domint","tipe","type hotel","lokasi"])),None)
            RING_PALETTE=[("#0D9488","rgba(13,148,136,.09)"),("#0F766E","rgba(15,118,110,.09)"),("#14B8A6","rgba(20,184,166,.09)"),("#5EEAD4","rgba(94,234,212,.09)"),("#99F6E4","rgba(153,246,228,.09)")]
            def _fmt_label(n):
                if n>=1_000_000: return f"{n/1_000_000:.1f}M"
                if n>=1_000: return f"{n/1_000:.1f}K"
                return str(n)
            def _build_segs(grp_df,name_col):
                total=int(grp_df["Invoice Unik"].sum()); segs=[]
                for idx,(_,row) in enumerate(grp_df.iterrows()):
                    col,track=RING_PALETTE[min(idx,len(RING_PALETTE)-1)]
                    pct=round(row["Invoice Unik"]/total*100,1) if total>0 else 0
                    segs.append({"label":str(row[name_col]),"sub":f'{int(row["Invoice Unik"]):,} invoice',"value":int(row["Invoice Unik"]),"pct":pct,"color":col,"trackColor":track})
                return segs,total
            def _render_rings(segs,total,caption_text):
                components.html(build_donut_html(segs,_fmt_label(total),f"Berdasarkan invoice unik · {_fmt_label(total)} total"),height=370,scrolling=False)
                st.caption(caption_text)
            if dom_col and "Invoice No" in df_view.columns:
                _df_dom=(df_view[[dom_col,"Invoice No"]].dropna(subset=[dom_col])
                         .assign(**{dom_col:lambda d: d[dom_col].astype(str).str.strip()})
                         .pipe(lambda d: d[~d[dom_col].isin(["","nan","None","NaN"])]))
                dom_grp=_df_dom.groupby(dom_col,dropna=True)["Invoice No"].nunique().reset_index().rename(columns={"Invoice No":"Invoice Unik"})
                if len(dom_grp)>4:
                    top4=dom_grp.nlargest(4,"Invoice Unik").reset_index(drop=True)
                    oth=dom_grp.nlargest(len(dom_grp),"Invoice Unik").iloc[4:]["Invoice Unik"].sum()
                    dom_grp=pd.concat([top4,pd.DataFrame([{dom_col:"Others","Invoice Unik":oth}])],ignore_index=True)
                else: dom_grp=dom_grp.reset_index(drop=True)
                segs,total=_build_segs(dom_grp,dom_col)
                _render_rings(segs,total,f"*Kolom: {dom_col}")
            elif "Product Type" in df_view.columns and "Invoice No" in df_view.columns:
                df_dom=df_view.copy()
                df_dom["Dom_Int"]=df_dom["Product Type"].astype(str).apply(lambda x: "International" if any(k in x.upper() for k in ["INTER","LUAR","ABROAD","OVERSEA"]) else "Domestic")
                dom_grp2=(df_dom.groupby("Dom_Int")["Invoice No"].nunique().reset_index().rename(columns={"Invoice No":"Invoice Unik"}).reset_index(drop=True))
                segs,total=_build_segs(dom_grp2,"Dom_Int")
                _render_rings(segs,total,"*Diklasifikasikan dari kolom Product Type")
            else:
                st.info("Kolom Domestic/International tidak ditemukan.")

        # ── Preview data ──────────────────────────────────────────────────────
        st.markdown('<div style="height:12px"></div>', unsafe_allow_html=True)
        gsec("&#9776; Preview Data")
        rpp=50; tp=max(1,(tr//rpp)+int(tr%rpp>0))
        if "pg" not in st.session_state: st.session_state.pg=0
        if st.session_state.pg>=tp: st.session_state.pg=0
        pc,pm2,pn=st.columns([1,5,1])
        with pc:
            if st.button("Prev",key="btn_prev") and st.session_state.pg>0: st.session_state.pg-=1; st.rerun()
        with pn:
            if st.button("Next",key="btn_next") and st.session_state.pg<tp-1: st.session_state.pg+=1; st.rerun()
        with pm2:
            st.markdown(f'<p style="text-align:center;font-size:.68rem;color:#475569;padding:9px 0;margin:0;">Hal&nbsp;{st.session_state.pg+1}&nbsp;/&nbsp;{tp} &nbsp;·&nbsp; {tr:,} baris</p>', unsafe_allow_html=True)
        s,e=st.session_state.pg*rpp,(st.session_state.pg+1)*rpp
        st.dataframe(df_view.iloc[s:e],use_container_width=True)
        dc,ec=st.columns(2)
        with dc:
            st.download_button("⬇ Download CSV",df_view.to_csv(index=False).encode("utf-8"),"hotel_report.csv","text/csv",use_container_width=True)
        with ec:
            ob=io.BytesIO()
            with pd.ExcelWriter(ob,engine="xlsxwriter") as w: df_view.to_excel(w,index=False,sheet_name="Report")
            st.download_button("⬇ Download Excel",ob.getvalue(),"hotel_report.xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",use_container_width=True)

    # ════════════════════════════════════════════════════════════════
    # TAB 2 — TREN INVOICE (cached)
    # ════════════════════════════════════════════════════════════════
    with tab2:
        ti, tr2 = _cached_invoice_trend(_vh, df_view)
        if ti is not None:
            ca,cb=st.columns([3,2])
            with ca:
                gsec("Tren Invoice Bulanan","📈")
                fig=go.Figure()
                fig.add_trace(go.Scatter(x=ti["Bulan"],y=ti["Invoice"],mode="lines+markers",line=dict(color="#0D9488",width=2.5,shape="spline"),marker=dict(size=9,color="#0D9488"),fill="tozeroy",fillcolor="rgba(13,148,136,.1)",hovertemplate="<b>%{x}</b><br>Invoice: <b>%{y:,.0f}</b><extra></extra>"))
                fig.update_layout(xaxis_title="",yaxis_title="Invoice Unik",hovermode="x unified",height=320)
                st.plotly_chart(theme(fig),use_container_width=True)
            with cb:
                gsec("Ringkasan Bulanan","📋")
                if tr2 is not None:
                    merged=ti[["Bulan","Invoice"]].merge(tr2,on="Bulan",how="left"); merged.columns=["Bulan","Invoice Unik","Room Night"]
                else:
                    merged=ti[["Bulan","Invoice"]].rename(columns={"Invoice":"Invoice Unik"})
                st.dataframe(merged.style.format({c:"{:,.0f}" for c in merged.columns if merged[c].dtype!="O"}).background_gradient(subset=["Invoice Unik"],cmap="Purples"),use_container_width=True,height=320)
            gsec("Volume Invoice per Bulan","📊")
            fig2=px.bar(ti,x="Bulan",y="Invoice",text="Invoice",color="Invoice",color_continuous_scale=["rgba(99,102,241,.3)","#0D9488","#0D9488"])
            fig2.update_traces(texttemplate="%{y:,.0f}",textposition="outside",textfont=dict(size=11,color="#8898AA"),marker_line_width=0,marker_cornerradius=4,cliponaxis=False)
            fig2.update_layout(coloraxis_showscale=False,height=290,xaxis_title="",yaxis_title="")
            st.plotly_chart(theme(fig2),use_container_width=True)
        else:
            st.warning("Kolom Issued Date atau Invoice No tidak ditemukan.")

    # ════════════════════════════════════════════════════════════════
    # TAB 3 — SUPPLIER (cached)
    # ════════════════════════════════════════════════════════════════
    with tab3:
        ss3, d3 = _cached_supplier(_vh, df_view)
        if ss3 is not None:
            ca,cb=st.columns([3,2])
            with ca:
                gsec("Distribusi Supplier","🏢")
                fig3=px.pie(d3,names="Supplier_Name",values="Total Room Night",hole=0.52,color_discrete_sequence=GLASS_PALETTE)
                fig3.update_traces(textinfo="percent+label",textfont=dict(size=12),pull=[0.05]+[0]*(len(d3)-1),marker=dict(line=dict(color="rgba(6,8,24,.8)",width=2)),hovertemplate="<b>%{label}</b><br>Room Night: %{value:,.0f}<br>%{percent}<extra></extra>")
                fig3.update_layout(height=360,legend=dict(orientation="v",yanchor="middle",y=.5,xanchor="left",x=1.02))
                st.plotly_chart(theme(fig3),use_container_width=True)
            with cb:
                gsec("Top Supplier","📊")
                fig3b=px.bar(ss3.head(8),x="Total Room Night",y="Supplier_Name",orientation="h",text="Total Room Night",color="Total Room Night",color_continuous_scale=TEAL_SCALE)
                fig3b.update_traces(texttemplate="%{x:,.0f}",textposition="outside",textfont=dict(size=10,color="#8898AA"),marker_line_width=0,marker_cornerradius=4,cliponaxis=False)
                fig3b.update_layout(yaxis=dict(categoryorder="total ascending"),coloraxis_showscale=False,height=360,xaxis_title="",yaxis_title="")
                st.plotly_chart(theme(fig3b),use_container_width=True)
            st.dataframe(d3.sort_values("Total Room Night",ascending=False).reset_index(drop=True).style.format({"Total Room Night":"{:,.0f}"}).background_gradient(subset=["Total Room Night"],cmap="YlGnBu"),use_container_width=True)
        else:
            st.warning("Kolom Supplier_Name atau Total Room Night tidak tersedia.")

    # ════════════════════════════════════════════════════════════════
    # TAB 4 — PRODUCT TYPE (cached)
    # ════════════════════════════════════════════════════════════════
    with tab4:
        ps4, d4 = _cached_product(_vh, df_view)
        if ps4 is not None:
            ca,cb=st.columns([3,2])
            with ca:
                gsec("Distribusi Product Type","📦")
                fig4=px.pie(d4,names="Product Type",values="Total Room Night",hole=0.52,color_discrete_sequence=GLASS_PALETTE)
                fig4.update_traces(textinfo="percent+label",textfont=dict(size=12),pull=[0.05]+[0]*(len(d4)-1),marker=dict(line=dict(color="rgba(6,8,24,.8)",width=2)),hovertemplate="<b>%{label}</b><br>Room Night: %{value:,.0f}<br>%{percent}<extra></extra>")
                fig4.update_layout(height=360)
                st.plotly_chart(theme(fig4),use_container_width=True)
            with cb:
                gsec("Tabel Product Type","📋")
                st.dataframe(d4.sort_values("Total Room Night",ascending=False).reset_index(drop=True).style.format({"Total Room Night":"{:,.0f}"}).background_gradient(subset=["Total Room Night"],cmap="YlGnBu"),use_container_width=True,height=360)
        else:
            st.warning("Kolom Product Type atau Total Room Night tidak tersedia.")

    # ════════════════════════════════════════════════════════════════
    # TAB 5 — AGENT SCORECARD
    # ════════════════════════════════════════════════════════════════
    with tab5:
        ac=next((c for c in df_view.columns if "agent" in c.lower() or "handler" in c.lower()),None)
        if ac and "Invoice No" in df_view.columns and "Total Room Night" in df_view.columns:
            agent_map={"client-cre-mic-opc":"API-DTM","client-cre-ptrmtt-cp":"API-DTM","farras":"Farras","firda":"Firda","rida.manora":"Rida","meijika":"Meiji","veronica":"Vero","selvy":"Selvy","ade.puspita":"Ade","cbt.admin":"CBT-Admin","shaiful.baldy":"Baldy","muhammad.geraldi":"Gerald","achmad.rifandi":"Fandi","sulistia":"CBT-Tia","aliryodan":"CBT-Ali","rifyal.tumber":"Rifyal"}
            dfa=df_view.copy(); dfa[ac]=dfa[ac].astype(str).str.strip().str.lower().replace(agent_map)
            _null_ac={"nan","none","","nat","<na>","n/a","null"}
            dfa=dfa[~dfa[ac].str.lower().isin(_null_ac)]
            def _classify_pic(name):
                nu=str(name).strip().upper()
                for p in KNOWN_PICS:
                    if p.upper()==nu: return p
                return "Other"
            dfa["PIC_Group"]=dfa[ac].apply(_classify_pic)
            _n_months=1
            if "Issued Date" in dfa.columns:
                _periods=dfa["Issued Date"].dropna().dt.to_period("M").unique(); _n_months=max(len(_periods),1)
            _company_col=None
            if "Normalized_Inv_To" in dfa.columns: _company_col="Normalized_Inv_To"
            else: _company_col=next((c for c in dfa.columns if any(k in c.lower() for k in ["invoice to","invoiceto","bill to","billto","sold to","client"])),None)
            pic_order=KNOWN_PICS+["Other"]; pic_data={}
            for _pic in pic_order:
                _sub=dfa[dfa["PIC_Group"]==_pic]
                if _sub.empty: continue
                _inv_u=int(_sub["Invoice No"].nunique()); _rn=float(_sub["Total Room Night"].sum())
                _sa=float(_sub["Sales AR"].fillna(0).astype(float).sum()) if "Sales AR" in _sub.columns else None
                _pr=float(_sub["Profit"].fillna(0).astype(float).sum()) if "Profit" in _sub.columns else None
                _avg_inv=_inv_u/_n_months; _avg_rn=_rn/_inv_u if _inv_u>0 else 0
                _avg_sa=_sa/_inv_u if (_sa is not None and _inv_u>0) else None
                _avg_pr=_pr/_inv_u if (_pr is not None and _inv_u>0) else None
                _co_count=int(_sub[_company_col].dropna().nunique()) if _company_col else 0
                _avg_pm=None
                if "Profit" in _sub.columns and "Sales AR" in _sub.columns:
                    _pm_sub=_sub.copy(); _pm_sub["_sa_f"]=pd.to_numeric(_pm_sub["Sales AR"],errors="coerce").fillna(0); _pm_sub["_pr_f"]=pd.to_numeric(_pm_sub["Profit"],errors="coerce").fillna(0); _pm_mask=_pm_sub["_sa_f"]!=0
                    if _pm_mask.any(): _avg_pm=float((_pm_sub.loc[_pm_mask,"_pr_f"]/_pm_sub.loc[_pm_mask,"_sa_f"]*100).mean())
                _top_sup="—"; _top_sup_rn=0
                if "Supplier_Name" in _sub.columns:
                    _sup_rn=(_sub.groupby("Supplier_Name",dropna=True)["Total Room Night"].sum().sort_values(ascending=False))
                    _sup_rn=_sup_rn[~_sup_rn.index.astype(str).str.strip().str.upper().isin({"","NAN","NONE","DIRECT TO HOTEL"})]
                    if not _sup_rn.empty: _top_sup=str(_sup_rn.index[0]); _top_sup_rn=int(_sup_rn.iloc[0])
                _total_inv_all=dfa["Invoice No"].nunique(); _total_rn_all=float(dfa["Total Room Night"].sum())
                _inv_pct=(_inv_u/_total_inv_all*100) if _total_inv_all>0 else 0
                _rn_pct=(_rn/_total_rn_all*100) if _total_rn_all>0 else 0
                pic_data[_pic]={"inv_u":_inv_u,"rn":_rn,"sa":_sa,"pr":_pr,"avg_inv":_avg_inv,"avg_rn":_avg_rn,"avg_sa":_avg_sa,"avg_pr":_avg_pr,"avg_pm":_avg_pm,"co_count":_co_count,"top_sup":_top_sup,"top_sup_rn":_top_sup_rn,"inv_pct":_inv_pct,"rn_pct":_rn_pct}

            def _initials(name):
                parts=name.split()
                if len(parts)>=2: return (parts[0][0]+parts[1][0]).upper()
                return name[:2].upper() if len(name)>=2 else name.upper()

            def _build_card2(pic,d):
                is_other=(pic=="Other"); ini="OTH" if is_other else _initials(pic)
                card_cls="pic2-card other" if is_other else "pic2-card"
                inv_u=d["inv_u"]; rn=d["rn"]; sa=d["sa"]; pr=d["pr"]; avg_inv=d["avg_inv"]; avg_rn=d["avg_rn"]; avg_sa=d["avg_sa"]; avg_pr=d["avg_pr"]; avg_pm=d.get("avg_pm"); co=d["co_count"]; inv_pct=d["inv_pct"]; rn_pct=d["rn_pct"]
                sa_str=compact_num(sa) if sa is not None else "—"; pr_str=compact_num(pr) if pr is not None else "—"
                avg_sa_str=compact_num(avg_sa) if avg_sa is not None else "—"; avg_pr_str=compact_num(avg_pr) if avg_pr is not None else "—"
                pm_pct_str,pm_raw="—",None
                if sa is not None and pr is not None and sa>0: pm_raw=pr/sa*100; pm_pct_str=f"{pm_raw:.1f}%"
                avg_pm_str=f"{avg_pm:.1f}%" if avg_pm is not None else "—"
                _photo_uri=_load_avatar_b64(pic) if not is_other else ""
                if _photo_uri: _avatar_html='<div class="p2av p2av-photo"><img src="'+_photo_uri+'" alt="'+pic+'" /></div>'
                else: _avatar_html='<div class="p2av">'+ini+'</div>'
                share_html='<div class="p2-share"><span class="p2-share-dot"></span><span class="p2-share-txt">'+f'{inv_pct:.1f}% inv&thinsp;·&thinsp;{rn_pct:.1f}% RN'+'</span></div>'
                def _bar(pct,muted=False):
                    w=min(float(pct),100); cls="p2-bar-fill muted" if muted else "p2-bar-fill"
                    return '<div class="p2-bar"><div class="'+cls+'" style="width:'+f"{w:.1f}"+'%;"></div></div>'
                def _mrow(icon,label,val,hint="",bar_pct=None):
                    bar_h=_bar(bar_pct) if bar_pct is not None else ""; hint_h=('<div class="p2m-hint">'+hint+'</div>') if hint else ""
                    return '<div class="p2-mrow"><div class="p2m-top"><span class="p2m-icon">'+icon+'</span><span class="p2m-label">'+label+'</span></div><div class="p2m-val">'+val+'</div>'+hint_h+bar_h+'</div>'
                pm_color="#059669" if (pm_raw is not None and pm_raw>=0) else "#DC2626"
                avg_pm_color="#059669" if (avg_pm is not None and avg_pm>=0) else "#DC2626"
                h='<div class="'+card_cls+'">'
                h+='<div class="p2-banner">'+_avatar_html+'<div class="p2-banner-info"><div class="p2-name">'+pic+'</div><div class="p2-role">Hotel Bookers · MTT</div>'+share_html+'</div></div>'
                h+='<div class="p2-body"><div class="p2-section-lbl">📋 Volume Transaksi</div>'
                h+='<div class="p2-mgroup">'+_mrow("🧾","Invoice Unik",f'{inv_u:,}',f'avg {avg_inv:.1f} / bulan',bar_pct=inv_pct)+_mrow("🌙","Room Night",compact_num(rn),f'avg {avg_rn:.1f} RN / inv',bar_pct=rn_pct)+'</div>'
                h+='<div class="p2-section-lbl">💰 Finansial</div>'
                h+='<div class="p2-mgroup">'+_mrow("📦","Sales AR",sa_str,f'avg {avg_sa_str} / inv')+_mrow("💹","Profit",pr_str,f'avg {avg_pr_str} / inv')+'</div>'
                h+=(f'<div class="p2-margin-strip"><div class="p2-margin-left"><div class="p2-margin-lbl">Profit Margin</div>'
                    f'<div class="p2-margin-val" style="color:{pm_color};">{pm_pct_str}</div>'
                    f'<div style="margin-top:6px;padding-top:6px;border-top:1px dashed rgba(13,148,136,.2);">'
                    f'<div class="p2-margin-lbl" style="margin-bottom:2px;">Avg Profit Margin</div>'
                    f'<div style="font-family:\'Sora\',sans-serif;font-size:.78rem;font-weight:700;color:{avg_pm_color};">{avg_pm_str}</div>'
                    f'<div style="font-size:.48rem;color:#94A3B8;margin-top:1px;">rata-rata per transaksi</div></div></div>'
                    f'<div class="p2-margin-right"><div class="p2-margin-lbl">Companies</div><div class="p2-margin-val">{co:,}</div></div></div>')
                h+='</div><div class="p2-footer"><div class="p2-footer-lbl">🏨 Supplier Preference</div>'
                if d["top_sup"]!="—":
                    short_sup=d["top_sup"][:28]+"…" if len(d["top_sup"])>28 else d["top_sup"]
                    h+=f'<div class="p2-sup-row"><span class="p2-sup-name">{short_sup}</span><span class="p2-sup-rn">{d["top_sup_rn"]:,} RN</span></div>'
                else: h+='<span class="p2-no-sup">Tidak ada data supplier</span>'
                h+='</div></div>'
                return h

            gsec("Scorecard PIC Agent","🏅")
            _known_sorted=sorted([p for p in pic_order if p!="Other" and p in pic_data],key=lambda p: pic_data[p]["sa"] if pic_data[p]["sa"] is not None else 0,reverse=True)
            _render_order=_known_sorted+(["Other"] if "Other" in pic_data else [])
            cards_parts=['<div class="pic2-grid">']
            for _pic in _render_order: cards_parts.append(_build_card2(_pic,pic_data[_pic]))
            cards_parts.append('</div>')
            st.markdown("".join(cards_parts),unsafe_allow_html=True)

            st.markdown("<div style='height:16px'></div>",unsafe_allow_html=True)
            gsec("Tabel Ringkasan Scorecard PIC","📋")
            _tbl_rows=[]
            for _pic in _render_order:
                d=pic_data[_pic]; _sa_v=d["sa"] if d["sa"] is not None else 0; _pr_v=d["pr"] if d["pr"] is not None else 0
                _pm_v=(_pr_v/_sa_v*100) if _sa_v>0 else None
                _tbl_rows.append({"pic":_pic,"inv_u":d["inv_u"],"avg_inv":round(d["avg_inv"],1),"rn":int(d["rn"]),"avg_rn":round(d["avg_rn"],1),"sa":_sa_v,"avg_sa":d["avg_sa"] if d["avg_sa"] is not None else 0,"pr":_pr_v,"avg_pr":d["avg_pr"] if d["avg_pr"] is not None else 0,"pm":_pm_v,"co":d["co_count"],"inv_pct":round(d["inv_pct"],1),"rn_pct":round(d["rn_pct"],1),"top_sup":d["top_sup"]})

            _df_tbl_dl=pd.DataFrame([{"PIC":r["pic"],"Invoice Unik":r["inv_u"],"Avg Inv/Bulan":r["avg_inv"],"Room Night":r["rn"],"Avg RN/Inv":r["avg_rn"],"Sales AR":r["sa"],"Avg Sales/Inv":r["avg_sa"],"Profit":r["pr"],"Avg Profit/Inv":r["avg_pr"],"Profit Margin%":f"{r['pm']:.1f}%" if r["pm"] is not None else "—","Companies":r["co"],"% Invoice":r["inv_pct"],"% RN":r["rn_pct"],"Supplier Utama":r["top_sup"]} for r in _tbl_rows])
            _max_sa=max((r["sa"] for r in _tbl_rows),default=1) or 1; _max_rn=max((r["rn"] for r in _tbl_rows),default=1) or 1; _max_inv=max((r["inv_u"] for r in _tbl_rows),default=1) or 1; _max_pr=max((r["pr"] for r in _tbl_rows),default=1) or 1

            def _bar_spark(val,mx,color="#0D9488",bg="#F0FDFA"):
                w=min(val/mx*100,100) if mx>0 else 0
                return f'<div style="background:{bg};border-radius:4px;overflow:hidden;height:4px;width:100%;margin-top:3px;"><div style="width:{w:.1f}%;height:100%;background:{color};border-radius:4px;"></div></div>'
            def _pm_badge(pm):
                if pm is None: return '<span style="color:#94A3B8;font-size:.62rem;">—</span>'
                color="#059669" if pm>=0 else "#DC2626"; bg="#ECFDF5" if pm>=0 else "#FFF1F2"; border="#A7F3D0" if pm>=0 else "#FECACA"; icon="▲" if pm>=0 else "▼"
                return f'<span style="display:inline-block;padding:2px 8px;border-radius:12px;font-size:.62rem;font-weight:700;color:{color};background:{bg};border:1px solid {border};">{icon} {abs(pm):.1f}%</span>'
            def _pct_pill(pct,kind="inv"):
                color="#0D9488" if kind=="inv" else "#0F766E"
                return f'<span style="display:inline-block;padding:1px 7px;border-radius:10px;font-size:.6rem;font-weight:700;color:{color};background:#F0FDFA;border:1px solid #CCFBF1;">{pct:.1f}%</span>'

            th_style='style="padding:9px 14px;font-size:.56rem;font-weight:700;text-transform:uppercase;letter-spacing:.9px;color:#64748B;background:#FAFBFC;border-bottom:2px solid #E2E8F0;white-space:nowrap;"'
            th_r='style="padding:9px 14px;font-size:.56rem;font-weight:700;text-transform:uppercase;letter-spacing:.9px;color:#64748B;background:#FAFBFC;border-bottom:2px solid #E2E8F0;white-space:nowrap;text-align:right;"'
            html_tbl=('<div style="border:1px solid #E2E8F0;border-radius:14px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.06);background:#fff;margin-bottom:16px;"><div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;">'
                      f'<thead><tr><th {th_style}>#</th><th {th_style}>Agent</th><th {th_r}>Sales AR</th><th {th_r}>Profit</th><th {th_r}>Margin</th><th {th_r}>Invoice</th><th {th_r}>Room Night</th><th {th_r}>Avg RN/Inv</th><th {th_r}>Avg Sales/Inv</th><th {th_r}>Companies</th><th {th_r}>% Inv</th><th {th_r}>% RN</th><th {th_style}>Supplier Utama</th></tr></thead><tbody>')
            for rank,r in enumerate(_tbl_rows,1):
                is_other=r["pic"]=="Other"; row_bg="#FAFAFA" if rank%2==0 else "#FFFFFF"
                if is_other: row_bg="#F8FAFC"
                if rank<=3 and not is_other: rank_html=f'<span style="font-size:.85rem;">{["🥇","🥈","🥉"][rank-1]}</span>'
                else: rank_html=f'<span style="display:inline-flex;width:22px;height:22px;border-radius:50%;background:#F1F5F9;font-size:.6rem;font-weight:700;color:#94A3B8;align-items:center;justify-content:center;">{rank}</span>'
                ini="OTH" if is_other else _initials(r["pic"]); av_bg="#94A3B8" if is_other else "#0D9488"
                name_cell=(f'<div style="display:flex;align-items:center;gap:9px;"><div style="width:30px;height:30px;border-radius:50%;background:linear-gradient(135deg,{av_bg},{av_bg}CC);color:#fff;font-size:.62rem;font-weight:800;display:flex;align-items:center;justify-content:center;">{ini}</div>'
                           f'<span style="font-weight:700;font-size:.73rem;color:#0F172A;">{r["pic"]}</span></div>')
                td='style="padding:10px 14px;border-bottom:1px solid #F1F5F9;vertical-align:middle;"'
                tdr='style="padding:10px 14px;border-bottom:1px solid #F1F5F9;vertical-align:middle;text-align:right;"'
                sa_cell=f'<div style="font-size:.75rem;font-weight:700;color:#0F172A;">{compact_num(r["sa"])}</div>'+_bar_spark(r["sa"],_max_sa,"#0D9488","#F0FDFA")
                pr_cell=f'<div style="font-size:.75rem;font-weight:700;color:{"#059669" if r["pr"]>=0 else "#DC2626"};">{compact_num(r["pr"])}</div>'+_bar_spark(max(r["pr"],0),_max_pr,"#059669","#F0FDF4")
                inv_cell=f'<div style="font-size:.73rem;font-weight:700;color:#0F172A;">{r["inv_u"]:,}</div><div style="font-size:.56rem;color:#94A3B8;">avg {r["avg_inv"]:.1f}/bln</div>'+_bar_spark(r["inv_u"],_max_inv,"#0D9488","#F0FDFA")
                rn_cell=f'<div style="font-size:.73rem;font-weight:700;color:#0F172A;">{compact_num(r["rn"])}</div>'+_bar_spark(r["rn"],_max_rn,"#0F766E","#F0FDFA")
                sup_name=r["top_sup"][:22]+"…" if len(str(r["top_sup"]))>22 else str(r["top_sup"])
                sup_cell=f'<span style="font-size:.63rem;color:#0F766E;font-weight:500;">{sup_name}</span>' if r["top_sup"]!="—" else '<span style="font-size:.6rem;color:#94A3B8;font-style:italic;">—</span>'
                html_tbl+=(f'<tr style="background:{row_bg};" onmouseover="this.style.background=\'#F0FDFA\'" onmouseout="this.style.background=\'{row_bg}\'">'
                           f'<td {td} style="padding:10px 14px;border-bottom:1px solid #F1F5F9;text-align:center;">{rank_html}</td><td {td}>{name_cell}</td>'
                           f'<td {tdr}>{sa_cell}</td><td {tdr}>{pr_cell}</td><td {tdr}>{_pm_badge(r["pm"])}</td>'
                           f'<td {tdr}>{inv_cell}</td><td {tdr}>{rn_cell}</td>'
                           f'<td {tdr} style="padding:10px 14px;border-bottom:1px solid #F1F5F9;text-align:right;font-size:.73rem;color:#334155;">{r["avg_rn"]:.1f}</td>'
                           f'<td {tdr} style="padding:10px 14px;border-bottom:1px solid #F1F5F9;text-align:right;font-size:.73rem;color:#334155;">{compact_num(r["avg_sa"])}</td>'
                           f'<td {tdr} style="padding:10px 14px;border-bottom:1px solid #F1F5F9;text-align:right;font-size:.73rem;color:#334155;">{r["co"]:,}</td>'
                           f'<td {tdr}>{_pct_pill(r["inv_pct"],"inv")}</td><td {tdr}>{_pct_pill(r["rn_pct"],"rn")}</td><td {td}>{sup_cell}</td></tr>')
            html_tbl+='</tbody></table></div></div>'
            st.markdown(html_tbl,unsafe_allow_html=True)
            _ob_ag=io.BytesIO()
            with pd.ExcelWriter(_ob_ag,engine="xlsxwriter") as _w: _df_tbl_dl.to_excel(_w,index=False,sheet_name="Scorecard_PIC")
            st.download_button("⬇ Download Tabel Scorecard PIC",_ob_ag.getvalue(),"scorecard_pic_agent.xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",use_container_width=True,key="dl_scorecard_pic")
        else:
            st.warning("Kolom Agent, Invoice No, atau Total Room Night tidak ditemukan.")

    # ════════════════════════════════════════════════════════════════
    # TAB 6 — PTM CORP (cached)
    # ════════════════════════════════════════════════════════════════
    with tab6:
        dfh = _cached_ptm(_vh, df_view)
        if dfh is None:
            st.warning("Kolom Supplier_Name, Hotel_Name, atau Total Room Night tidak ditemukan.")
        elif isinstance(dfh, pd.DataFrame) and dfh.empty:
            st.warning("Tidak ditemukan data Supplier = 'PTM CORP RATE'.")
        else:
            ca,cb=st.columns([3,2])
            with ca:
                gsec("Top Hotel PTM Corp Rate","🏨")
                fh=px.bar(dfh.head(15),x="Total Room Night",y="Hotel_Name",orientation="h",text="Total Room Night",color="Total Room Night",color_continuous_scale=["rgba(252,211,77,.2)","rgba(252,211,77,.6)","#2DD4BF"])
                fh.update_traces(texttemplate="%{x:,.0f}",textposition="outside",textfont=dict(size=11,color="#8898AA"),marker_line_width=0,marker_cornerradius=4,cliponaxis=False)
                fh.update_layout(yaxis=dict(categoryorder="total ascending",automargin=True),coloraxis_showscale=False,height=460,xaxis_title="",yaxis_title="",margin=dict(l=8,r=80,t=30,b=8))
                st.plotly_chart(theme(fh),use_container_width=True)
            with cb:
                gsec("Tabel Hotel PTM","📋")
                st.dataframe(dfh.head(20).reset_index(drop=True).style.format({"Total Room Night":"{:,.0f}"}).background_gradient(subset=["Total Room Night"],cmap="YlGnBu"),use_container_width=True,height=400)
                ob3=io.BytesIO()
                with pd.ExcelWriter(ob3,engine="xlsxwriter") as w: dfh.to_excel(w,index=False,sheet_name="Hotel_PTM")
                st.download_button("⬇ Download Hotel PTM",ob3.getvalue(),"hotel_ptm_corp_rate.xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",use_container_width=True)

    # ════════════════════════════════════════════════════════════════
    # TAB 7 — SUPPLIER CATEGORY (cached)
    # ════════════════════════════════════════════════════════════════
    with tab7:
        cs7, d7 = _cached_cat(_vh, df_view)
        if cs7 is not None:
            ca,cb=st.columns([3,2])
            with ca:
                gsec("Distribusi Kategori Supplier","🎯")
                fc7=px.pie(d7,names="Supplier_Category",values="Total Room Night",hole=0.52,color_discrete_sequence=GLASS_PALETTE)
                fc7.update_traces(textinfo="percent+label",textfont=dict(size=12),pull=[0.05]+[0]*(len(d7)-1),marker=dict(line=dict(color="rgba(6,8,24,.8)",width=2)),hovertemplate="<b>%{label}</b><br>Room Night: %{value:,.0f}<extra></extra>")
                fc7.update_layout(height=380)
                st.plotly_chart(theme(fc7),use_container_width=True)
            with cb:
                gsec("Tabel Kategori","📋")
                st.dataframe(d7.sort_values("Total Room Night",ascending=False).reset_index(drop=True).style.format({"Total Room Night":"{:,.0f}"}).background_gradient(subset=["Total Room Night"],cmap="YlGnBu"),use_container_width=True,height=380)
        else:
            st.warning("Kolom Supplier_Category atau Total Room Night tidak tersedia.")

# ══════════════════════════════════════════════════════════════════════════════
# EMPTY STATE
# ══════════════════════════════════════════════════════════════════════════════
else:
    for k in ["df_raw","upload_hash"]: st.session_state.pop(k,None)
    st.markdown("""
    <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;padding:80px 40px;text-align:center;max-width:480px;margin:60px auto 0;">
      <div style="width:64px;height:64px;margin-bottom:24px;background:#F0FDFA;border:1px solid #CCFBF1;border-radius:16px;display:grid;place-items:center;">
        <svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="#0D9488" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"/><polyline points="13 2 13 9 20 9"/></svg>
      </div>
      <div style="font-family:'DM Sans',sans-serif;font-size:1.05rem;font-weight:700;color:#0F172A;margin-bottom:10px;">Belum ada data</div>
      <p style="font-size:.72rem;color:#94A3B8;line-height:1.9;margin:0 auto 28px;max-width:340px;">
        Upload file Excel Custom Report di sidebar, lalu klik
        <span style="color:#0D9488;font-weight:600;background:#F0FDFA;padding:1px 7px;border-radius:5px;border:1px solid #CCFBF1;">Sync Data</span>
        untuk memuat normalisasi dari Google Drive.
      </p>
      <div style="display:flex;gap:8px;flex-wrap:wrap;justify-content:center;">
        <span style="font-size:.62rem;font-weight:500;padding:6px 14px;border-radius:20px;background:#F0FDFA;color:#0D9488;border:1px solid #CCFBF1;">Custom Report .xlsx</span>
        <span style="font-size:.62rem;font-weight:500;padding:6px 14px;border-radius:20px;background:#F8FAFC;color:#64748B;border:1px solid #E2E8F0;">Google Drive Sync</span>
        <span style="font-size:.62rem;font-weight:500;padding:6px 14px;border-radius:20px;background:#F0FDFA;color:#0D9488;border:1px solid #CCFBF1;">Optimized v9.3</span>
      </div>
    </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# FOOTER
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<div style="margin-top:56px;border-top:1px solid #E2E8F0;background:#fff;">
  <div style="background:#F0FDFA;border-bottom:1px solid #CCFBF1;padding:10px 36px;display:flex;align-items:flex-start;gap:10px;">
    <span style="font-size:.8rem;margin-top:1px;flex-shrink:0;">⚠️</span>
    <p style="margin:0;font-size:.6rem;color:#0F766E;line-height:1.8;">
      <strong style="color:#0D9488;">DISCLAIMER &nbsp;|&nbsp;</strong>
      Data yang ditampilkan bersumber dari file Custom Report yang diunggah dan referensi normalisasi dari Google Drive MTT.
      Seluruh informasi bersifat <em>internal dan rahasia</em> — dilarang disebarluaskan tanpa izin tertulis dari manajemen.
    </p>
  </div>
  <div style="padding:12px 36px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
    <span style="font-size:.6rem;color:#94A3B8;">&copy; 2025 <strong style="color:#0D9488;">Hotel Intelligence</strong> · MTT · All rights reserved</span>
    <span style="font-size:.6rem;color:#94A3B8;">Powered by Streamlit · v9.3 · Optimized Edition</span>
    <span style="font-size:.6rem;color:#94A3B8;">Built by <strong style="color:#0D9488;">Rifyal Tumber</strong> · MTT · 2025</span>
  </div>
</div>""", unsafe_allow_html=True)
