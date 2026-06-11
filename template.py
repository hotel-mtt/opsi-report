# join_opsifin.py — Opsifin v10.0 · Hotel Report Dashboard
# Rifyal Tumber · MTT · 2025
# v10: Optimasi performa — calamine engine (3-8x lebih cepat vs openpyxl),
#      Parquet cache antar-session (instant load file yang sama),
#      GDrive cache persistent ke disk, lazy import calamine.

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import io, requests, re, hashlib, base64, os as _os
# st.components.v1 deprecated - using st.html() instead
from concurrent.futures import ThreadPoolExecutor, as_completed
import pathlib, tempfile

# ── Parquet / GDrive disk-cache ───────────────────────────────────────────────
_CACHE_DIR = pathlib.Path(tempfile.gettempdir()) / "hotel_intel_cache"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)
_GDRIVE_CACHE_DIR = _CACHE_DIR / "gdrive"
_GDRIVE_CACHE_DIR.mkdir(parents=True, exist_ok=True)

st.set_page_config(page_title="Hotel Intelligence · MTT", page_icon="🏨",
                   layout="wide", initial_sidebar_state="expanded")

# ── Constants ─────────────────────────────────────────────────────────────────
GDRIVE_IDS = {
    "hotel_chain":       "1r8dp_Chp-8QWKk_qXDMEehGj_c54U7ka",
    "hotel_city":        "1RQkiBAJJYbdkZngtrVlicYQEBKj3kPYL",
    "hotel_name":        "1paYSVhvvunLCZMKm4EF8TawvyRSHDC19",
    "hotel_supplier":    "11BG3oFaNQNEHXxy7jpWXZ0-Z9P6CBpRx",
    "supplier_category": "1zBudcR8Ia1nK0k4daMAOkOrIgvRO3GQD",
}
GDRIVE_LABELS = {
    "hotel_chain": "Hotel Chain", "hotel_city": "Hotel City",
    "hotel_name": "Hotel Name", "hotel_supplier": "Supplier",
    "supplier_category": "Supplier Category",
}

# DROP_SET: nama kolom PERSIS dari file (setelah di-strip & collapse whitespace)
DROP_SET = frozenset([
    "No", "Branch", "Customer Type", "Customer Name", "Customer Code",
    "PNR", "Base Fare", "Airlines", "Class", "Route",
    "Departure Date", "Departure Time", "Arrival Date", "Arrival Time",
    "NTA", "Airline Code", "Flight No", "Hotel Address", "Hotel Group Chain",
    "Description", "Due Date", "Group Chain", "Source Reference", "Sales Net",
    "Title", "Remark 1", "Remark 2", "Remark 3", "Remark 4", "Remark 5",
    "Remark 6", "Remark 7", "Remark 8", "Remark 9", "Remark 10",
    "Remark 11", "Remark 12",
    "Supplier Code", "Ticket No", "Fare Tax", "IWJR", "Add Charge",
    "Insurance", "PSC", "Other Charge", "Incentive", "Agent Comm",
    "Travel Services", "VAT", "Stamp Fee", "MDR", "Extra Disc", "Rounding",
    "Base Sell", "Currency", "Sales Handler", "Remark", "Source Rescode",
    "Booking Code", "Voucher Number", "Room Type", "Profit %",
])

GLASS_PALETTE = ["#0D9488","#134E4A","#2DD4BF","#5EEAD4","#99F6E4","#CCFBF1","#0F766E","#042F2E"]
TEAL_SCALE    = ["#CCFBF1","#99F6E4","#2DD4BF","#0D9488","#0F766E","#134E4A"]

# Nama-nama ini tampil sebagai kartu individual — sisanya semua jadi "Other"
KNOWN_PICS = ["API-DTM","Ade","Farras","Selvy","Vero","Firda","Meiji","Rida","Gerald","Baldy","Vial"]

AGENT_MAP = {
    "client-cre-mic-opc":   "API-DTM",
    "client-cre-ptrmtt-cp": "API-DTM",
    "api-dtm":              "API-DTM",
    "farras":               "Farras",
    "firda":                "Firda",
    "rida.manora":          "Rida",
    "rida":                 "Rida",
    "meijika":              "Meiji",
    "meiji":                "Meiji",
    "veronica":             "Vero",
    "vero":                 "Vero",
    "selvy":                "Selvy",
    "ade.puspita":          "Ade",
    "ade":                  "Ade",
    "shaiful.baldy":        "Baldy",
    "baldy":                "Baldy",
    "muhammad.geraldi":     "Gerald",
    "geraldi":              "Gerald",
    "gerald":               "Gerald",
    "vial":                 "Vial",
    "Vial":                 "Vial",
    "rifyal.tumber":        "Vial",
    "Rifyal.Tumber":        "Vial",
    "achmad.vial":          "Vial",
}

_WHOLESALERS = frozenset([
    "MG BEDBANK","MG BED BANK","MGBEDBANK","KLIKNBOOK","KLIK N BOOK","KLOOK",
    "HOTELBEDS","HOTEL BEDS","WEBBEDS","WEB BEDS","TOURICO","GTA",
    "JUMBO TOURS","WORLDHOTELS","RESTEL","BONOTEL","RECONLINE",
])
_CORPORATE   = frozenset(["PTM CORP RATE","CORPORATE RATE"])
_DIRECT      = frozenset(["DIRECT TO HOTEL","DIRECT HOTEL","DIRECT"])
_OTA         = frozenset([
    "TRAVELOKA","TRAVELOKA BUSINESS","TIKET.COM","BOOKING.COM","BOOKING COM",
    "AGODA","AGODA CORPORATE","EXPEDIA","HOTELS.COM",
])
CBT_ALIASES  = frozenset([
    "CBT PERTAMINA(HOTEL CM)","CBT PERTAMINA (HOTEL)",
    "PERTAMINA ENERGY TERMINAL (CBT)","CBT PERTAMINA",
])

# ── Utility ───────────────────────────────────────────────────────────────────
def _load_avatar_b64(pic_name):
    base = _os.path.dirname(_os.path.abspath(__file__))
    for ext in [".jpg",".jpeg",".png",".webp"]:
        p = _os.path.join(base, "assets", pic_name + ext)
        if _os.path.isfile(p):
            with open(p,"rb") as f:
                data = base64.b64encode(f.read()).decode()
            mime = "image/jpeg" if ext in [".jpg",".jpeg"] else ("image/png" if ext==".png" else "image/webp")
            return f"data:{mime};base64,{data}"
    return ""

def _is_valid_file(f):
    try: _=f.name; _=f.size; return True
    except: return False

def compute_upload_hash(files):
    valid = [f for f in files if _is_valid_file(f)]
    if not valid: return ""
    h = hashlib.md5()
    for f in sorted(valid, key=lambda x: x.name):
        h.update(f.name.encode()); h.update(str(f.size).encode())
    return h.hexdigest()

def make_view_hash(df):
    try:
        return hashlib.md5(pd.util.hash_pandas_object(df, index=True).values.tobytes()).hexdigest()
    except:
        return hashlib.md5(f"{df.shape}{list(df.columns)}".encode()).hexdigest()

def compact_num(v):
    try:
        v=float(v); a=abs(v)
        if a>=1e9: return f"{v/1e9:.1f}B"
        if a>=1e6: return f"{v/1e6:.1f}M"
        if a>=1e3: return f"{v/1e3:.1f}K"
        return f"{int(v):,}"
    except: return str(v)

def theme(fig):
    fig.update_layout(
        font_family="Open Sans", font_color="#525F7F", font_size=12,
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=12,r=12,t=40,b=12),
        legend=dict(font=dict(size=11),bgcolor="rgba(255,255,255,.8)",
                    bordercolor="rgba(0,0,0,.06)",borderwidth=1),
        hoverlabel=dict(bgcolor="#fff",bordercolor="rgba(0,0,0,.1)",
                        font_size=12,font_color="#32325D"),
    )
    fig.update_xaxes(showgrid=True,gridcolor="rgba(0,0,0,.06)",zeroline=False,
                     tickfont=dict(size=11,color="#8898AA"))
    fig.update_yaxes(showgrid=True,gridcolor="rgba(0,0,0,.06)",zeroline=False,
                     tickfont=dict(size=11,color="#8898AA"))
    return fig

def gsec(title, icon=""):
    lbl = (f'<span style="font-size:.75rem;margin-right:4px;">{icon}</span>{title}') if icon else title
    st.markdown(f'<div class="gsec">{lbl}</div>', unsafe_allow_html=True)

# ── GDrive ────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_gdrive_mapping(file_id):
    """Fetch mapping dari GDrive dengan disk-cache 1 jam agar restart app tetap cepat."""
    import json, time
    cache_file = _GDRIVE_CACHE_DIR / f"{file_id}.json"
    # Cek disk cache (max 1 jam)
    if cache_file.exists() and (time.time() - cache_file.stat().st_mtime) < 3600:
        try:
            data = json.loads(cache_file.read_text(encoding="utf-8"))
            return data["mapping"], data["count"]
        except Exception:
            pass
    try:
        r = requests.get(f"https://drive.google.com/uc?export=download&id={file_id}", timeout=20)
        r.raise_for_status()
        # Gunakan calamine jika tersedia (lebih cepat), fallback ke openpyxl
        try:
            df_map = pd.read_excel(io.BytesIO(r.content), engine="calamine")
        except Exception:
            df_map = pd.read_excel(io.BytesIO(r.content), engine="openpyxl")
        if df_map.shape[1] >= 2:
            mapping = dict(zip(df_map.iloc[:,0].astype(str), df_map.iloc[:,1]))
            count   = len(df_map)
            # Simpan ke disk
            try:
                cache_file.write_text(
                    json.dumps({"mapping": mapping, "count": count}, ensure_ascii=False),
                    encoding="utf-8")
            except Exception:
                pass
            return mapping, count
        return {}, 0
    except Exception as e:
        return None, str(e)

def fetch_all_mappings_parallel():
    nm, ss = {}, {}
    with ThreadPoolExecutor(max_workers=len(GDRIVE_IDS)) as ex:
        futs = {ex.submit(fetch_gdrive_mapping, fid): k for k, fid in GDRIVE_IDS.items()}
        for fut in as_completed(futs):
            k = futs[fut]; mapping, result = fut.result()
            if mapping is None:
                ss[k] = "err"; st.toast(f"✗ {GDRIVE_LABELS[k]}", icon="❌")
            else:
                nm[k] = mapping; ss[k] = "ok"
                st.toast(f"✓ {GDRIVE_LABELS[k]} · {result:,} baris", icon="✅")
    return nm, ss

# ── Excel reader (v10: calamine engine + Parquet cache) ───────────────────────
def _get_excel_engine():
    """Coba calamine dulu (3-8x lebih cepat), fallback ke openpyxl."""
    try:
        import python_calamine  # noqa: F401
        return "calamine"
    except ImportError:
        return "openpyxl"

_EXCEL_ENGINE = _get_excel_engine()

def _read_excel_fast(file_obj):
    """Baca Excel dengan engine terbaik yang tersedia. Gunakan usecols untuk hemat RAM."""
    raw = file_obj.read() if hasattr(file_obj, "read") else open(file_obj, "rb").read()
    buf = io.BytesIO(raw)

    # Pass 1: header saja — deteksi kolom yang perlu di-drop
    try:
        df_hdr = pd.read_excel(buf, nrows=0, engine=_EXCEL_ENGINE)
    except Exception:
        buf.seek(0)
        df_hdr = pd.read_excel(buf, nrows=0, engine="openpyxl")

    col_norm = {c: re.sub(r'\s+', ' ', str(c).strip()) for c in df_hdr.columns}
    keep = [c for c in df_hdr.columns if col_norm[c] not in DROP_SET]

    # Pass 2: baca data hanya kolom yang dibutuhkan
    buf.seek(0)
    try:
        df = pd.read_excel(buf, usecols=keep, engine=_EXCEL_ENGINE)
    except Exception:
        buf.seek(0)
        df = pd.read_excel(buf, usecols=keep, engine="openpyxl")

    df.columns = [re.sub(r'\s+', ' ', str(c).strip()) for c in df.columns]
    return df

def _parquet_cache_path(upload_hash: str) -> pathlib.Path:
    return _CACHE_DIR / f"raw_{upload_hash}.parquet"

def _load_parquet_cache(upload_hash: str):
    """Muat dari Parquet cache jika tersedia (< 24 jam)."""
    import time
    p = _parquet_cache_path(upload_hash)
    if p.exists() and (time.time() - p.stat().st_mtime) < 86400:
        try:
            return pd.read_parquet(p)
        except Exception:
            try: p.unlink()
            except Exception: pass
    return None

def _save_parquet_cache(df: pd.DataFrame, upload_hash: str):
    """Simpan ke Parquet cache untuk akses berikutnya."""
    try:
        # Parquet tidak mendukung period dtype — konversi ke string dulu
        df_save = df.copy()
        for col in df_save.columns:
            if hasattr(df_save[col], "dt") and str(df_save[col].dtype).startswith("period"):
                df_save[col] = df_save[col].astype(str)
        df_save.to_parquet(_parquet_cache_path(upload_hash), index=False)
    except Exception:
        pass  # Cache gagal tidak masalah — app tetap jalan

# ── build_df_raw ──────────────────────────────────────────────────────────────
def build_df_raw(files, norm_maps):
    dfs = []
    for f in files:
        try:
            dfs.append(_read_excel_fast(f))
        except Exception as e:
            st.toast(f"⚠️ Gagal baca {getattr(f,'name','?')}: {e}", icon="⚠️")
    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    # ── Tanggal datetime ───────────────────────────────────────────────────────
    for col in ["Inv Date", "Issued Date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    # Check In / Check Out disimpan sebagai string 'YYYY-MM-DD' di file ini
    for col in ["Check In", "Check Out"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    if "Issued Date" in df.columns:
        df["Issued_Month"] = df["Issued Date"].dt.strftime("%B")
        df["Issued_Year"]  = df["Issued Date"].dt.year.astype("Int16")

    # ── Full Name ──────────────────────────────────────────────────────────────
    # File punya "First Name" (setelah normalisasi double-space) + "Last Name"
    fn = df["First Name"].fillna("").astype(str).str.strip() if "First Name" in df.columns else pd.Series([""] * len(df), index=df.index)
    ln = df["Last Name"].fillna("").astype(str).str.strip()  if "Last Name"  in df.columns else pd.Series([""] * len(df), index=df.index)
    df["Full Name"] = (fn + " " + ln).str.strip().str.upper()
    df.drop(columns=["First Name","Last Name","Title"], errors="ignore", inplace=True)

    # ── Normalized Invoice To ──────────────────────────────────────────────────
    if "Invoice To" in df.columns:
        inv_str = df["Invoice To"].astype(str).str.strip()
        df["Normalized_Inv_To"] = np.where(
            inv_str.str.upper().isin(CBT_ALIASES), "CBT PERTAMINA",
            np.where(inv_str.isin(["","nan","None","NaN"]), "Unknown", inv_str)
        )

    # ── Normalisasi via GDrive mapping ─────────────────────────────────────────
    # hotel_chain: file tidak punya "Hotel Chain", pakai "Hotel Group Chain"
    for map_key, src_col, dst_col in [
        ("hotel_city",     "Hotel City",        "Hotel_City"),
        ("hotel_name",     "Hotel Name",        "Hotel_Name"),
        ("hotel_chain",    "Hotel Group Chain", "Hotel_Chain"),
        ("hotel_supplier", "Supplier Name",     "Supplier_Name"),
    ]:
        if src_col in df.columns:
            src = df[src_col].astype(str).str.strip()
            if norm_maps.get(map_key):
                df[dst_col] = src.map(norm_maps[map_key]).fillna(src)
            else:
                df[dst_col] = src
        elif dst_col not in df.columns:
            df[dst_col] = ""

    if "Supplier Name" in df.columns and "Supplier_Name" not in df.columns:
        df["Supplier_Name"] = df["Supplier Name"].fillna("Unknown")
    if "Supplier_Name" not in df.columns:
        df["Supplier_Name"] = "Unknown"

    # ── Supplier Category (vectorized) ────────────────────────────────────────
    _raw  = df.get("Supplier Name", pd.Series([""] * len(df), index=df.index)).astype(str).str.strip().str.upper()
    _norm = df["Supplier_Name"].astype(str).str.strip().str.upper()

    if norm_maps.get("supplier_category"):
        sc_upper = {str(k).strip().upper(): v for k, v in norm_maps["supplier_category"].items()}
        df["Supplier_Category"] = _raw.map(sc_upper).astype(object)
        mask_nan = df["Supplier_Category"].isna()
        if mask_nan.any():
            df.loc[mask_nan, "Supplier_Category"] = _norm[mask_nan].map(sc_upper)
    else:
        df["Supplier_Category"] = pd.Series([""] * len(df), index=df.index, dtype=object)

    unc = df["Supplier_Category"].isna() | (df["Supplier_Category"].astype(str).str.strip().isin(["","Uncategorized"]))
    if unc.any():
        conds = [
            (_raw.isin(_DIRECT)     | _norm.isin(_DIRECT))     & unc,
            (_raw.isin(_CORPORATE)  | _norm.isin(_CORPORATE))  & unc,
            (_raw.isin(_WHOLESALERS)| _norm.isin(_WHOLESALERS))& unc,
            (_raw.isin(_OTA)        | _norm.isin(_OTA))         & unc,
            _raw.str.contains("BEDBANK|WHOLESAL", regex=True, na=False) & unc,
            _raw.str.contains("DIRECT",  na=False)              & unc,
            _raw.str.contains("CORP.*RATE|RATE.*CORP", regex=True, na=False) & unc,
            _raw.str.contains("CHANNEL MANAGER", na=False)      & unc,
        ]
        choices = ["DIRECT HOTEL","CORPORATE RATE","WHOLESALER","OTA",
                   "WHOLESALER","DIRECT HOTEL","CORPORATE RATE","CHANNEL MANAGER"]
        # Use pd.Series to avoid dtype mismatch
        assigned = pd.Series(
            np.select([c[unc] for c in conds], choices, default="Uncategorized"),
            index=df.index[unc])
        df.loc[unc, "Supplier_Category"] = assigned

    df["Supplier_Category"] = df["Supplier_Category"].fillna("Uncategorized")
    sc_rename = {
        "DIRECT TO HOTEL":"DIRECT HOTEL","DIRECT HOTEL":"DIRECT HOTEL",
        "PTM CORP RATE":"CORPORATE RATE","CORPORATE RATE":"CORPORATE RATE",
        "WHOLESALER":"WHOLESALER","OTA":"OTA","CHANNEL MANAGER":"CHANNEL MANAGER",
    }
    sc_up = df["Supplier_Category"].astype(str).str.strip().str.upper()
    df["Supplier_Category"] = sc_up.map(sc_rename).fillna(df["Supplier_Category"].astype(str).str.strip())

    # ── Total Room Night ───────────────────────────────────────────────────────
    if "Room" in df.columns and "Night" in df.columns:
        df["Total Room Night"] = (
            pd.to_numeric(df["Room"],  errors="coerce").fillna(0) *
            pd.to_numeric(df["Night"], errors="coerce").fillna(0)
        )

    # Drop sisa kolom yang tidak diperlukan
    df.drop(columns=[c for c in DROP_SET if c in df.columns], errors="ignore", inplace=True)
    return df

# ── maybe_rebuild (v10: Parquet cache) ────────────────────────────────────────
def maybe_rebuild_df(uploaded_files, norm_maps):
    if not uploaded_files:
        for k in ["df_raw","upload_hash"]: st.session_state.pop(k, None)
        return False
    fh = compute_upload_hash(uploaded_files)
    nh = hashlib.md5(str(sorted((k,len(v)) for k,v in norm_maps.items())).encode()).hexdigest()
    combined = fh + nh
    if st.session_state.get("upload_hash") == combined and "df_raw" in st.session_state:
        return False  # Sudah ada di session — tidak perlu re-build

    # Coba muat dari Parquet disk-cache (instant jika file sama)
    cached_df = _load_parquet_cache(combined)
    if cached_df is not None:
        st.session_state["df_raw"]      = cached_df
        st.session_state["upload_hash"] = combined
        st.toast("⚡ Data dimuat dari cache (instan)", icon="⚡")
        return True

    # Proses ulang & simpan ke cache
    engine_info = f"engine: {_EXCEL_ENGINE}"
    with st.spinner(f"⏳ Memproses data... ({engine_info})"):
        df = build_df_raw(uploaded_files, norm_maps)
        st.session_state["df_raw"]      = df
        st.session_state["upload_hash"] = combined
        _save_parquet_cache(df, combined)
    return True

# ── Cached per-tab ────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def _cached_invoice_trend(vh, _df):
    if "Issued Date" not in _df.columns or "Invoice No" not in _df.columns:
        return None, None
    dt = _df.dropna(subset=["Issued Date","Invoice No"]).copy()
    dt = dt[~dt["Invoice No"].astype(str).str.strip().isin(["","nan","None","NaN"])]
    dt["_ml"] = dt["Issued Date"].dt.strftime("%b")
    dt["_mn"] = dt["Issued Date"].dt.month
    ti = dt.groupby(["_ml","_mn"])["Invoice No"].nunique().reset_index()
    ti.columns = ["Bulan","MonN","Invoice"]
    ti = ti.sort_values("MonN").reset_index(drop=True)
    tr2 = None
    if "Total Room Night" in dt.columns:
        tr2 = dt.groupby("_ml")["Total Room Night"].sum().reset_index()
        tr2.columns = ["Bulan","Room Night"]
    return ti, tr2

@st.cache_data(show_spinner=False)
def _cached_supplier(vh, _df):
    if "Supplier_Name" not in _df.columns or "Total Room Night" not in _df.columns:
        return None, None
    tmp = (_df[["Supplier_Name","Total Room Night"]]
           .dropna(subset=["Supplier_Name"])
           .assign(Supplier_Name=lambda d: d["Supplier_Name"].astype(str).str.strip())
           .pipe(lambda d: d[~d["Supplier_Name"].isin(["","nan","None","NaN"])]))
    ss = tmp.groupby("Supplier_Name")["Total Room Night"].sum().reset_index().sort_values("Total Room Night", ascending=False)
    tail_sum = ss.iloc[5:]["Total Room Night"].sum() if len(ss) > 5 else 0
    top = ss.head(5)
    d = pd.concat([top, pd.DataFrame([{"Supplier_Name":"Others","Total Room Night":tail_sum}])] if len(ss)>5 else [top], ignore_index=True)
    return ss, d

@st.cache_data(show_spinner=False)
def _cached_product(vh, _df):
    if "Product Type" not in _df.columns or "Total Room Night" not in _df.columns:
        return None, None
    tmp = (_df[["Product Type","Total Room Night"]]
           .dropna(subset=["Product Type"])
           .assign(**{"Product Type": lambda d: d["Product Type"].astype(str).str.strip()})
           .pipe(lambda d: d[~d["Product Type"].isin(["","nan","None","NaN"])]))
    ps = tmp.groupby("Product Type")["Total Room Night"].sum().reset_index().sort_values("Total Room Night", ascending=False)
    tail_sum = ps.iloc[6:]["Total Room Night"].sum() if len(ps) > 6 else 0
    top = ps.head(6)
    d = pd.concat([top, pd.DataFrame([{"Product Type":"Others","Total Room Night":tail_sum}])] if len(ps)>6 else [top], ignore_index=True)
    return ps, d

@st.cache_data(show_spinner=False)
def _cached_ptm(vh, _df):
    if not all(c in _df.columns for c in ["Supplier_Name","Hotel_Name","Total Room Night"]):
        return None
    mask = _df["Supplier_Name"].astype(str).str.upper().str.contains("PTM|CORP RATE", regex=True, na=False)
    dfptm = _df[mask]
    if dfptm.empty:
        return pd.DataFrame()
    tmp = (dfptm[["Hotel_Name","Total Room Night"]]
           .dropna(subset=["Hotel_Name"])
           .assign(Hotel_Name=lambda d: d["Hotel_Name"].astype(str).str.strip())
           .pipe(lambda d: d[~d["Hotel_Name"].isin(["","nan","None","NaN"])]))
    return tmp.groupby("Hotel_Name", as_index=False)["Total Room Night"].sum().sort_values("Total Room Night", ascending=False)

@st.cache_data(show_spinner=False)
def _cached_cat(vh, _df):
    if "Supplier_Category" not in _df.columns or "Total Room Night" not in _df.columns:
        return None, None
    tmp = (_df[["Supplier_Category","Total Room Night"]]
           .dropna(subset=["Supplier_Category"])
           .assign(Supplier_Category=lambda d: d["Supplier_Category"].astype(str).str.strip())
           .pipe(lambda d: d[~d["Supplier_Category"].isin(["","nan","None","NaN"])]))
    cs = tmp.groupby("Supplier_Category")["Total Room Night"].sum().reset_index().sort_values("Total Room Night", ascending=False)
    tail_sum = cs.iloc[5:]["Total Room Night"].sum() if len(cs) > 5 else 0
    top = cs.head(5)
    d = pd.concat([top, pd.DataFrame([{"Supplier_Category":"Others","Total Room Night":tail_sum}])] if len(cs)>5 else [top], ignore_index=True)
    return cs, d

def get_prev_period_metrics(df_raw, df_view):
    try:
        if "Issued Date" not in df_raw.columns or df_view.empty:
            return {}
        curr_min = df_view["Issued Date"].dropna().min()
        curr_max = df_view["Issued Date"].dropna().max()
        if pd.isnull(curr_min) or pd.isnull(curr_max):
            return {}
        delta    = curr_max - curr_min
        prev_max = curr_min - pd.Timedelta(days=1)
        prev_min = prev_max - delta
        raw_min  = df_raw["Issued Date"].dropna().min()
        raw_max  = df_raw["Issued Date"].dropna().max()
        overlap  = max((min(prev_max,raw_max) - max(prev_min,raw_min)).days + 1, 0)
        period   = max((prev_max - prev_min).days + 1, 1)
        if overlap / period < 0.80:
            return {}
        prev = df_raw[(df_raw["Issued Date"] >= prev_min) & (df_raw["Issued Date"] <= prev_max)]
        if len(prev) < 5:
            return {}
        m = {"rows":len(prev), "prev_min":prev_min.strftime("%d %b %Y"), "prev_max":prev_max.strftime("%d %b %Y")}
        m["ui"] = int(prev["Invoice No"].nunique())                        if "Invoice No"       in prev.columns else None
        m["rn"] = int(np.ceil(prev["Total Room Night"].sum()))             if "Total Room Night" in prev.columns else None
        m["sa"] = float(prev["Sales AR"].fillna(0).astype(float).sum())   if "Sales AR"         in prev.columns else None
        m["up"] = int(prev["Full Name"].dropna().nunique())                if "Full Name"        in prev.columns else None
        if "Profit" in prev.columns and "Sales AR" in prev.columns:
            _p = prev["Profit"].fillna(0).astype(float)
            _s = prev["Sales AR"].fillna(0).astype(float)
            mm = _s != 0
            m["pm"] = float((_p[mm]/_s[mm]*100).mean()) if mm.any() else 0.0
        return m
    except:
        return {}

# ── Donut HTML ────────────────────────────────────────────────────────────────
def build_donut_html(segments, total_label, subtitle=""):
    import json
    segs_js = json.dumps(segments, ensure_ascii=False)
    html = """<!DOCTYPE html><html><head><meta charset="UTF-8">
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@700;800&family=DM+Sans:wght@400;600;700&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0;}
body{background:transparent;font-family:'DM Sans',sans-serif;}
.wrap{background:#fff;border:1px solid #E2E8F0;border-radius:16px;padding:18px 20px 14px;width:100%;}
.hdr{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:14px;gap:8px;}
.title{font-family:'Space Grotesk',sans-serif;font-size:.88rem;font-weight:700;color:#0F172A;}
.sub{font-size:.53rem;color:#94A3B8;margin-top:3px;}
.live{display:flex;align-items:center;gap:5px;background:#F0FDF9;border:1px solid #CCFBF1;border-radius:8px;padding:4px 10px;font-size:.54rem;font-weight:700;color:#0D9488;}
.dot{width:6px;height:6px;border-radius:50%;background:#0D9488;animation:pulse 2s infinite;}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.body{display:flex;align-items:center;gap:18px;flex-wrap:wrap;}
.cwrap{position:relative;flex-shrink:0;}
.ctxt{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);text-align:center;pointer-events:none;}
.cnum{font-family:'Space Grotesk',sans-serif;font-size:1.2rem;font-weight:800;color:#0F172A;line-height:1;}
.clbl{font-size:.5rem;font-weight:600;letter-spacing:.8px;text-transform:uppercase;color:#94A3B8;}
.legend{flex:1;min-width:130px;display:flex;flex-direction:column;gap:2px;}
.lrow{display:flex;align-items:center;gap:9px;padding:6px 9px;border-radius:9px;cursor:default;}
.lrow:hover{background:#F0FDF9;}
.lcolor{width:9px;height:9px;border-radius:3px;flex-shrink:0;}
.lbody{flex:1;min-width:0;}
.lname{font-size:.64rem;font-weight:700;color:#0F172A;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-bottom:3px;}
.lbwrap{height:4px;background:#F1F5F9;border-radius:4px;overflow:hidden;margin-bottom:3px;}
.lbar{height:100%;border-radius:4px;width:0;transition:width .9s cubic-bezier(.4,0,.2,1);}
.lmeta{display:flex;justify-content:space-between;align-items:center;}
.lval{font-family:'Space Grotesk',sans-serif;font-size:.59rem;font-weight:700;}
.lpct{font-size:.53rem;font-weight:600;padding:1px 6px;border-radius:10px;}
</style></head><body>
<div class="wrap">
  <div class="hdr">
    <div><div class="title">&#127758; Distribusi Invoice</div><div class="sub">SUBTITLE</div></div>
    <div class="live"><span class="dot"></span>Live</div>
  </div>
  <div class="body">
    <div class="cwrap">
      <svg width="170" height="170" viewBox="0 0 170 170" id="svg"></svg>
      <div class="ctxt"><div class="cnum" id="cnum">-</div><div class="clbl">INVOICE</div></div>
    </div>
    <div class="legend" id="lg"></div>
  </div>
</div>
<script>
const SEGS=SEGS_JS,TOTAL="TOTAL_LBL";
const CX=85,CY=85,R=65,SW=26,GAP=2.5;
const svg=document.getElementById('svg');
document.getElementById('cnum').textContent=TOTAL;
const tp=SEGS.reduce((a,s)=>a+s.pct,0)||100,av=360-GAP*SEGS.length;
function P(cx,cy,r,deg){const rad=(deg-90)*Math.PI/180;return[cx+r*Math.cos(rad),cy+r*Math.sin(rad)];}
function arc(s,e){const ro=R+SW/2,ri=R-SW/2,lg=e-s>180?1:0;
const[x1,y1]=P(CX,CY,ro,s+1),[x2,y2]=P(CX,CY,ro,e-1),[ix1,iy1]=P(CX,CY,ri,e-1),[ix2,iy2]=P(CX,CY,ri,s+1);
return`M ${x1} ${y1} A ${ro} ${ro} 0 ${lg} 1 ${x2} ${y2} L ${ix1} ${iy1} A ${ri} ${ri} 0 ${lg} 0 ${ix2} ${iy2} Z`;}
const tr=document.createElementNS('http://www.w3.org/2000/svg','circle');
tr.setAttribute('cx',CX);tr.setAttribute('cy',CY);tr.setAttribute('r',R);
tr.setAttribute('fill','none');tr.setAttribute('stroke','#F1F5F9');tr.setAttribute('stroke-width',SW);
svg.appendChild(tr);
let sd=0;SEGS.forEach((sg,i)=>{const sw=sg.pct/tp*av,ed=sd+sw;
const p=document.createElementNS('http://www.w3.org/2000/svg','path');
p.setAttribute('fill',sg.color);p.setAttribute('d',arc(sd,ed));
p.style.opacity='0';p.style.transition=`opacity .3s ease ${i*.1}s`;
svg.appendChild(p);sd=ed+GAP;});
setTimeout(()=>svg.querySelectorAll('path').forEach(p=>p.style.opacity='1'),80);
const lg=document.getElementById('lg');
SEGS.forEach((sg,i)=>{const row=document.createElement('div');row.className='lrow';
row.innerHTML=`<div class="lcolor" style="background:${sg.color};"></div>`
+`<div class="lbody"><div class="lname">${sg.label}</div>`
+`<div class="lbwrap"><div class="lbar" id="b${i}" style="background:${sg.color};"></div></div>`
+`<div class="lmeta"><span class="lval" style="color:${sg.color};">${Number(sg.value).toLocaleString('id-ID')}</span>`
+`<span class="lpct" style="background:${sg.color}18;color:${sg.color};">${sg.pct}%</span></div></div>`;
lg.appendChild(row);});
setTimeout(()=>SEGS.forEach((_,i)=>document.getElementById('b'+i).style.width=SEGS[i].pct+'%'),300);
</script></body></html>"""
    html = html.replace("SEGS_JS", segs_js)
    html = html.replace("TOTAL_LBL", str(total_label))
    html = html.replace("SUBTITLE", subtitle)
    return html

# ══════════════════════════════════════════════════════════════════════════════
# CSS
# ══════════════════════════════════════════════════════════════════════════════
_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=DM+Sans:wght@400;600;700;800&family=Sora:wght@600;700;800&display=swap');
:root{--bg:#F4F6F9;--card:#fff;--t1:#0F172A;--t2:#334155;--t3:#64748B;--t4:#94A3B8;
  --p:#0D9488;--p2:#0F766E;--p3:#134E4A;--pl:#F0FDFA;--pm:#CCFBF1;
  --bd:#E2E8F0;--r:.375rem;--r2:.5rem;
  --f:'Inter',sans-serif;--fh:'DM Sans',sans-serif;--fd:'Sora',sans-serif;}
*,*::before,*::after{box-sizing:border-box;}
html,body,[class*="css"]{font-family:var(--f)!important;font-size:13px!important;
  color:var(--t2)!important;background-color:var(--bg)!important;-webkit-font-smoothing:antialiased;}
.stApp,body{background-color:var(--bg)!important;}
.block-container{padding:0!important;max-width:100%!important;overflow-x:hidden!important;}
.main .block-container,[data-testid="stMainBlockContainer"]{padding:24px 38px 80px!important;max-width:100%!important;}
section[data-testid="stMain"]>div{padding:24px 38px 80px!important;}
[data-testid="collapsedControl"],[data-testid="stSidebarCollapseButton"],
button[data-testid="baseButton-header"],#MainMenu,footer,header{display:none!important;}
::-webkit-scrollbar{width:5px;height:5px;}
::-webkit-scrollbar-thumb{background:#CDD0D5;border-radius:10px;}
/* HEADER */
.ghdr{background:#fff;padding:0 36px;height:60px;display:flex;align-items:center;
  justify-content:space-between;position:sticky;top:0;z-index:500;border-bottom:1px solid var(--bd);}
.ghdr-brand{display:flex;align-items:center;gap:12px;}
.ghdr-logo{width:34px;height:34px;background:var(--pl);border-radius:var(--r2);
  display:grid;place-items:center;flex-shrink:0;border:1px solid var(--pm);}
.ghdr-name{font-family:var(--fh);font-size:.93rem;font-weight:700;color:var(--t1);}
.ghdr-name span{color:var(--p);font-weight:500;}
.ghdr-sub{font-size:.58rem;color:var(--t4);margin-top:2px;letter-spacing:.5px;text-transform:uppercase;}
.ghdr-right{display:flex;align-items:center;gap:8px;}
.ghdr-live{display:flex;align-items:center;gap:6px;font-size:.6rem;font-weight:600;
  color:var(--p);padding:5px 12px;border-radius:20px;background:var(--pl);border:1px solid var(--pm);}
.ghdr-dot{width:6px;height:6px;border-radius:50%;background:var(--p);animation:lb 2s ease-in-out infinite;}
@keyframes lb{0%,100%{opacity:1}50%{opacity:.4}}
.ghdr-pill{font-size:.6rem;font-weight:600;color:var(--t3);padding:5px 12px;
  border-radius:20px;background:var(--bg);border:1px solid var(--bd);}
/* TICKER */
.gticker{background:var(--pl);border-bottom:1px solid var(--pm);padding:6px 0;overflow:hidden;position:relative;}
.gticker::before,.gticker::after{content:'';position:absolute;top:0;width:80px;height:100%;z-index:2;}
.gticker::before{left:0;background:linear-gradient(90deg,var(--pl),transparent);}
.gticker::after{right:0;background:linear-gradient(270deg,var(--pl),transparent);}
.gticker-track{display:inline-block;white-space:nowrap;animation:tick 65s linear infinite;
  font-size:.58rem;letter-spacing:.8px;text-transform:uppercase;}
.gticker-track:hover{animation-play-state:paused;}
.ti{color:var(--t4);}.ti.hi{color:var(--p);font-weight:600;}
.tsep{margin:0 24px;color:var(--pm);}
@keyframes tick{from{transform:translateX(0)}to{transform:translateX(-50%)}}
/* SIDEBAR */
[data-testid="stSidebar"]{background:#fff!important;border-right:1px solid var(--bd)!important;
  min-width:256px!important;max-width:256px!important;}
[data-testid="stSidebar"]>div:first-child{padding:0!important;}
.sb-top{padding:20px 18px 16px;border-bottom:1px solid var(--bd);display:flex;align-items:center;gap:11px;}
.sb-logo{width:32px;height:32px;background:var(--pl);border-radius:var(--r2);
  display:grid;place-items:center;flex-shrink:0;border:1px solid var(--pm);}
.sb-name{font-family:var(--fh);font-size:.85rem;font-weight:700;color:var(--t1);}
.sb-name span{color:var(--p);font-weight:500;}
.sb-ver{font-size:.56rem;color:var(--t4);margin-top:2px;}
.sb-sec{padding:16px 18px 6px;font-size:.58rem;font-weight:600;color:var(--t4)!important;
  text-transform:uppercase;letter-spacing:1.5px;}
.sb-div{height:1px;background:var(--bd);margin:4px 16px;}
.sync-row{display:flex;align-items:center;justify-content:space-between;
  padding:6px 18px;border-radius:var(--r);margin:1px 6px;}
.sync-row:hover{background:var(--bg);}
.sync-lbl{font-size:.7rem;color:var(--t2);font-weight:500;}
.tag{font-size:.56rem;font-weight:600;padding:2px 9px;border-radius:20px;}
.tok{background:#F0FDF4;color:#16A34A;border:1px solid #BBF7D0;}
.terr{background:#FFF1F2;color:#DC2626;border:1px solid #FECACA;}
.twait{background:#F8FAFC;color:var(--t4);border:1px solid var(--bd);}
/* BUTTONS */
[data-testid="stButton"]>button{background:var(--p)!important;color:#fff!important;border:none!important;
  border-radius:var(--r)!important;font-size:.72rem!important;font-weight:600!important;
  padding:9px 20px!important;transition:all .15s!important;}
[data-testid="stButton"]>button:hover{background:var(--p2)!important;transform:translateY(-1px)!important;}
[data-testid="stDownloadButton"]>button{background:#fff!important;color:var(--t2)!important;
  border:1px solid var(--bd)!important;border-radius:var(--r)!important;
  font-size:.7rem!important;font-weight:500!important;padding:9px 20px!important;}
/* TABS */
[data-testid="stTabs"] [data-baseweb="tab-list"]{background:var(--bg)!important;border:1px solid var(--bd)!important;
  border-radius:var(--r2)!important;gap:2px!important;padding:4px!important;margin-bottom:24px;}
[data-testid="stTabs"] [data-baseweb="tab"]{font-size:.71rem!important;font-weight:500!important;
  color:var(--t3)!important;padding:8px 18px!important;border-bottom:none!important;border-radius:var(--r)!important;}
[data-testid="stTabs"] [aria-selected="true"]{color:var(--t1)!important;font-weight:600!important;
  background:#fff!important;box-shadow:0 1px 3px rgba(0,0,0,.08)!important;}
/* MISC */
.gsec{display:flex;align-items:center;gap:10px;font-size:.65rem;font-weight:600;color:var(--t3);
  text-transform:uppercase;letter-spacing:.8px;margin:8px 0 16px;}
.gsec::after{content:'';flex:1;height:1px;background:var(--bd);}
.norm-bar{display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin-bottom:20px;
  padding:9px 14px;background:#fff;border:1px solid var(--bd);border-radius:var(--r);}
.norm-cap{font-size:.56rem;font-weight:600;color:var(--t4);text-transform:uppercase;
  letter-spacing:1.5px;margin-right:6px;}
.npill{font-size:.6rem;font-weight:500;padding:3px 11px;border-radius:20px;
  background:var(--bg);border:1px solid var(--bd);color:var(--t4);}
.npill.on{background:var(--pl);border-color:var(--pm);color:var(--p2);}
[data-testid="stDataFrame"]{border:1px solid var(--bd)!important;border-radius:var(--r)!important;}
[data-testid="stDataFrame"] th{background:#FAFAFA!important;font-size:.64rem!important;
  font-weight:600!important;color:var(--t4)!important;text-transform:uppercase!important;}
[data-testid="stDataFrame"] td{font-size:.71rem!important;color:var(--t2)!important;}
/* AGENT CARDS */
.p2-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(290px,1fr));gap:20px;margin-bottom:32px;}
.p2-card{background:#fff;border:1px solid #D1D9E0;border-radius:20px;overflow:hidden;
  display:flex;flex-direction:column;
  transition:box-shadow .25s,transform .25s;cursor:default;
  box-shadow:0 2px 8px rgba(0,0,0,.07);}
.p2-card:hover{transform:translateY(-5px);box-shadow:0 20px 48px -8px rgba(13,148,136,.22);}
.p2-banner{background:linear-gradient(135deg,#0D9488 0%,#064E3B 100%);
  padding:22px 20px 18px;display:flex;align-items:center;gap:16px;
  position:relative;overflow:hidden;}
.p2-banner::after{content:'';position:absolute;right:-20px;top:-20px;width:100px;height:100px;
  background:rgba(255,255,255,.06);border-radius:50%;}
.p2-card.oth .p2-banner{background:linear-gradient(135deg,#475569 0%,#1E293B 100%);}
.p2av{width:60px;height:60px;border-radius:50%;display:flex;align-items:center;justify-content:center;
  font-family:var(--fd);font-size:1.1rem;font-weight:800;color:#fff;
  background:rgba(255,255,255,.18);border:3px solid rgba(255,255,255,.35);flex-shrink:0;z-index:1;}
.p2av.photo{background:#E2E8F0;padding:0;overflow:hidden;border:3px solid rgba(255,255,255,.55);}
.p2av.photo img{width:100%;height:100%;object-fit:cover;object-position:center 8%;
  transform:scale(1.35);transform-origin:center 20%;border-radius:50%;}
.p2-bi{flex:1;min-width:0;z-index:1;}
.p2-name{font-family:var(--fh);font-size:1.15rem;font-weight:800;color:#fff;
  line-height:1.2;letter-spacing:-.3px;}
.p2-role{font-size:.62rem;color:rgba(255,255,255,.65);margin-top:3px;font-weight:500;}
.p2-sh{display:inline-flex;align-items:center;gap:5px;margin-top:9px;
  background:rgba(255,255,255,.15);border:1px solid rgba(255,255,255,.25);
  border-radius:20px;padding:4px 10px;}
.p2-sh-dot{width:5px;height:5px;border-radius:50%;background:#2DD4BF;}
.p2-sh-txt{font-size:.62rem;font-weight:600;color:rgba(255,255,255,.95);white-space:nowrap;}
.p2-body{padding:16px 18px 8px;display:flex;flex-direction:column;flex:1;}
.p2-slbl{font-size:.6rem;font-weight:700;text-transform:uppercase;letter-spacing:1.2px;
  color:var(--t3);margin:12px 0 8px;display:flex;align-items:center;gap:8px;}
.p2-slbl::after{content:'';flex:1;height:1px;background:#E8EEF3;}
.p2-mg{display:grid;grid-template-columns:1fr 1fr;gap:2px;background:#E8EEF3;
  border-radius:12px;overflow:hidden;border:1px solid #E8EEF3;}
.p2-mr{background:#fff;padding:12px 14px 10px;display:flex;flex-direction:column;gap:2px;}
.p2-mr:hover{background:#F0FDF9;}
.p2m-top{display:flex;align-items:center;gap:5px;margin-bottom:3px;}
.p2m-ic{font-size:.75rem;}
.p2m-lb{font-size:.58rem;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:var(--t4);}
.p2m-v{font-family:var(--fd);font-size:1.3rem;font-weight:800;color:var(--t1);
  letter-spacing:-.6px;line-height:1;}
.p2m-h{font-size:.58rem;color:var(--t3);margin-top:2px;font-weight:500;}
.p2-bar{height:4px;background:#EEF2FF;border-radius:10px;overflow:hidden;margin-top:6px;}
.p2-bf{height:100%;border-radius:10px;background:linear-gradient(90deg,#0D9488,#2DD4BF);
  transition:width .7s cubic-bezier(.4,0,.2,1);}
.p2-card.oth .p2-bf{background:linear-gradient(90deg,#94A3B8,#CBD5E1);}
.p2-ms{display:grid;grid-template-columns:1fr 1fr;gap:2px;
  background:#BAE6FD;border-radius:12px;border:1px solid #BAE6FD;overflow:hidden;margin-top:12px;}
.p2-card.oth .p2-ms{background:#E2E8F0;border-color:#CBD5E1;}
.p2-ml,.p2-mr2{background:#F0F9FF;padding:11px 14px;}
.p2-card.oth .p2-ml,.p2-card.oth .p2-mr2{background:#F8FAFC;}
.p2-mr2{text-align:right;}
.p2-mslb{font-size:.58rem;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#0369A1;}
.p2-card.oth .p2-mslb{color:var(--t3);}
.p2-msv{font-family:var(--fd);font-size:1.2rem;font-weight:800;line-height:1.1;
  margin-top:3px;color:var(--t1);}
.p2-ft{padding:12px 18px 16px;border-top:1px solid #EEF2F7;background:#FAFCFE;}
.p2-ftlb{font-size:.6rem;font-weight:700;text-transform:uppercase;letter-spacing:1px;
  color:var(--t4);margin-bottom:7px;}
.p2-sr{display:flex;align-items:center;justify-content:space-between;gap:8px;
  background:var(--pl);border:1px solid var(--pm);border-radius:10px;padding:8px 12px;}
.p2-card.oth .p2-sr{background:#F1F5F9;border-color:var(--bd);}
.p2-sn{font-size:.68rem;font-weight:600;color:var(--p2);overflow:hidden;
  text-overflow:ellipsis;white-space:nowrap;flex:1;}
.p2-srn{font-size:.63rem;font-weight:700;color:var(--p);background:#fff;
  padding:3px 9px;border-radius:12px;border:1px solid var(--pm);flex-shrink:0;}
</style>
"""
st.markdown(_CSS, unsafe_allow_html=True)

st.markdown("""<script>
(function(){
  const M="38px",sels=['[data-testid="stMainBlockContainer"]','.main .block-container'];
  function fix(){sels.forEach(s=>document.querySelectorAll(s).forEach(el=>{
    el.style.setProperty('padding-left',M,'important');
    el.style.setProperty('padding-right',M,'important');
    el.style.setProperty('max-width','100%','important');
  }));}
  fix();new MutationObserver(fix).observe(document.body,{childList:true,subtree:true});
})();
</script>""", unsafe_allow_html=True)

# ── Header & Ticker ───────────────────────────────────────────────────────────
st.markdown("""
<div class="ghdr">
  <div class="ghdr-brand">
    <div class="ghdr-logo">
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#0D9488"
           stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
        <path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/>
        <polyline points="9 22 9 12 15 12 15 22"/>
      </svg>
    </div>
    <div>
      <div class="ghdr-name">Hotel <span>Intelligence</span></div>
      <div class="ghdr-sub">MTT &nbsp;·&nbsp; Opsifin &nbsp;·&nbsp; Travel Analytics</div>
    </div>
  </div>
  <div class="ghdr-right">
    <span class="ghdr-pill">v10.0</span>
    <div class="ghdr-live"><span class="ghdr-dot"></span>Live</div>
  </div>
</div>
<div class="gticker"><div class="gticker-track">
  <span class="ti hi">Hotel Intelligence Platform</span><span class="tsep">·</span>
  <span class="ti">Invoice &amp; Supplier Analytics</span><span class="tsep">·</span>
  <span class="ti hi">Agent Scorecard Dashboard</span><span class="tsep">·</span>
  <span class="ti">Supplier Category Intelligence</span><span class="tsep">·</span>
  <span class="ti hi">MTT Travel Analytics · v9.3 · 2025</span><span class="tsep">·</span>
  <span class="ti hi">Hotel Intelligence Platform</span><span class="tsep">·</span>
  <span class="ti">Invoice &amp; Supplier Analytics</span><span class="tsep">·</span>
  <span class="ti hi">Agent Scorecard Dashboard</span><span class="tsep">·</span>
  <span class="ti">Supplier Category Intelligence</span><span class="tsep">·</span>
  <span class="ti hi">MTT Travel Analytics · v9.3 · 2025</span><span class="tsep">·</span>
</div></div>""", unsafe_allow_html=True)

# ── Pre-sidebar: rebuild ──────────────────────────────────────────────────────
_up_raw = st.session_state.get("main_upload") or []
_up     = [f for f in _up_raw if _is_valid_file(f)]
_nm     = st.session_state.get("norm_maps", {})
if _up:
    maybe_rebuild_df(_up, _nm)
elif not _up_raw:
    for k in ["df_raw","upload_hash"]:
        st.session_state.pop(k, None)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div class="sb-top">
      <div class="sb-logo">
        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="#0D9488"
             stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/>
          <polyline points="9 22 9 12 15 12 15 22"/>
        </svg>
      </div>
      <div>
        <div class="sb-name">Hotel <span>Report</span></div>
        <div class="sb-ver">Opsifin · MTT · v10.0</div>
      </div>
    </div>""", unsafe_allow_html=True)

    st.markdown('<div class="sb-sec">Data Utama</div>', unsafe_allow_html=True)
    st.file_uploader("Upload Custom Report (.xlsx)", type=["xlsx"],
                     accept_multiple_files=True, key="main_upload",
                     label_visibility="collapsed")

    # ── Info engine & cache ───────────────────────────────────────────────────
    _cache_files = list(_CACHE_DIR.glob("raw_*.parquet"))
    _cache_mb    = sum(f.stat().st_size for f in _cache_files) / 1_048_576
    _engine_icon = "⚡" if _EXCEL_ENGINE == "calamine" else "🐢"
    st.markdown(
        f'''<div style="margin:4px 18px 2px;padding:8px 12px;background:#F0FDFA;
            border:1px solid #CCFBF1;border-radius:8px;font-size:.58rem;line-height:1.8;">
          <b style="color:#0D9488;">{_engine_icon} Engine:</b>
          <span style="color:#0F766E;">{_EXCEL_ENGINE}</span><br>
          <b style="color:#0D9488;">📦 Parquet cache:</b>
          <span style="color:#64748B;">{len(_cache_files)} file · {_cache_mb:.1f} MB</span>
        </div>''', unsafe_allow_html=True)
    if _cache_files and st.button("🗑 Hapus Cache", use_container_width=True, key="btn_clear_cache"):
        for f in _cache_files:
            try: f.unlink()
            except Exception: pass
        for f in list(_GDRIVE_CACHE_DIR.glob("*.json")):
            try: f.unlink()
            except Exception: pass
        for k in ["df_raw","upload_hash","norm_maps","sync_state"]:
            st.session_state.pop(k, None)
        st.toast("🗑 Cache dihapus", icon="🗑")
        st.rerun()

    st.markdown('<div class="sb-div"></div><div class="sb-sec">Normalisasi · Google Drive</div>', unsafe_allow_html=True)
    _ss = st.session_state.get("sync_state", {})
    for k, lbl in GDRIVE_LABELS.items():
        s  = _ss.get(k, "wait")
        tc = {"ok":"tag tok","err":"tag terr","wait":"tag twait"}[s]
        tt = {"ok":"Synced","err":"Error","wait":"Pending"}[s]
        st.markdown(f'<div class="sync-row"><span class="sync-lbl">{lbl}</span><span class="{tc}">{tt}</span></div>',
                    unsafe_allow_html=True)

    st.markdown('<div style="height:12px"></div>', unsafe_allow_html=True)
    if st.button("🔄  Sync Data", use_container_width=True, key="btn_sync"):
        nm2, ns2 = fetch_all_mappings_parallel()
        st.session_state["sync_state"] = ns2
        st.session_state["norm_maps"]  = nm2
        for k in ["df_raw","upload_hash"]:
            st.session_state.pop(k, None)
        if all(v=="ok" for v in ns2.values()):
            st.toast("✅ Semua data normalisasi berhasil!", icon="✅")
        else:
            failed = [GDRIVE_LABELS[k] for k,v in ns2.items() if v!="ok"]
            st.toast(f"⚠️ Gagal: {', '.join(failed)}", icon="⚠️")
        st.rerun()

    st.markdown('<div class="sb-div"></div><div class="sb-sec">Filter Data</div>', unsafe_allow_html=True)
    if "df_raw" in st.session_state:
        _r = st.session_state["df_raw"]
        if "Issued_Year" in _r.columns:
            yr = sorted(_r["Issued_Year"].dropna().unique().tolist())
            st.multiselect("Tahun", yr, key="f_years")
        if "Inv Date" in _r.columns and _r["Inv Date"].notna().any():
            _imin = _r["Inv Date"].min().date()
            _imax = _r["Inv Date"].max().date()
            _id_raw = st.session_state.get("f_inv")
            if _id_raw and hasattr(_id_raw,"__len__") and len(_id_raw)==2:
                try: _id = [max(_id_raw[0],_imin), min(_id_raw[1],_imax)]
                except: _id = [_imin, _imax]
            else: _id = [_imin, _imax]
            st.date_input("Periode Inv Date", value=_id, key="f_inv",
                          min_value=_imin, max_value=_imax)
        if "Check In" in _r.columns and _r["Check In"].notna().any():
            _cmin = _r["Check In"].min().date()
            _cmax = _r["Check In"].max().date()
            _cd_raw = st.session_state.get("f_ci")
            if _cd_raw and hasattr(_cd_raw,"__len__") and len(_cd_raw)==2:
                try: _cd = [max(_cd_raw[0],_cmin), min(_cd_raw[1],_cmax)]
                except: _cd = [_cmin, _cmax]
            else: _cd = [_cmin, _cmax]
            st.date_input("Check In Range", value=_cd, key="f_ci",
                          min_value=_cmin, max_value=_cmax)
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

    # ── Filter ─────────────────────────────────────────────────────────────────
    df_view = df_raw.copy()
    sel_y = st.session_state.get("f_years", [])
    if sel_y and "Issued_Year" in df_view.columns:
        df_view = df_view[df_view["Issued_Year"].isin(sel_y)]
    sel_i = st.session_state.get("f_inv", [])
    if "Inv Date" in df_view.columns and isinstance(sel_i,(list,tuple)) and len(sel_i)==2:
        df_view = df_view[
            (df_view["Inv Date"] >= pd.to_datetime(sel_i[0])) &
            (df_view["Inv Date"] <= pd.to_datetime(sel_i[1]))
        ]
    sel_c = st.session_state.get("f_ci", [])
    if "Check In" in df_view.columns and isinstance(sel_c,(list,tuple)) and len(sel_c)==2:
        df_view = df_view[
            (df_view["Check In"] >= pd.to_datetime(sel_c[0])) &
            (df_view["Check In"] <= pd.to_datetime(sel_c[1]))
        ]

    _vh = make_view_hash(df_view)

    # Normalisasi pill
    _ss2 = st.session_state.get("sync_state", {})
    _pm_norm = {
        "Hotel Chain":   _ss2.get("hotel_chain")=="ok",
        "Hotel City":    _ss2.get("hotel_city")=="ok",
        "Hotel Name":    _ss2.get("hotel_name")=="ok",
        "Supplier":      _ss2.get("hotel_supplier")=="ok",
        "Supplier Cat":  _ss2.get("supplier_category")=="ok",
    }
    ph = " ".join(
        f'<span class="npill {"on" if v else ""}">{k}</span>'
        for k, v in _pm_norm.items()
    )
    st.markdown(f'<div class="norm-bar"><span class="norm-cap">Norm</span>{ph}</div>',
                unsafe_allow_html=True)

    tab1,tab2,tab3,tab4,tab5,tab6,tab7 = st.tabs(
        ["Summary","Tren Invoice","Supplier","Product Type","Agent","PTM Corp","Kategori"])

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 — SUMMARY
    # ══════════════════════════════════════════════════════════════════════════
    with tab1:
        _tr  = len(df_view)
        _tc  = len(df_view.columns)
        _ui  = int(df_view["Invoice No"].nunique())                      if "Invoice No"       in df_view.columns else 0
        _rn  = int(np.ceil(df_view["Total Room Night"].sum()))           if "Total Room Night" in df_view.columns else 0
        _sa  = float(df_view["Sales AR"].fillna(0).astype(float).sum()) if "Sales AR"         in df_view.columns else 0.0
        _up  = int(df_view["Full Name"].replace("",np.nan).dropna().nunique()) if "Full Name" in df_view.columns else 0
        _pmv = None
        if "Profit" in df_view.columns and "Sales AR" in df_view.columns:
            _pp = df_view["Profit"].fillna(0).astype(float)
            _ps = df_view["Sales AR"].fillna(0).astype(float)
            _mm = _ps != 0
            _pmv = float((_pp[_mm]/_ps[_mm]*100).mean()) if _mm.any() else 0.0
        _aging = None
        if "Check In" in df_view.columns and "Inv Date" in df_view.columns:
            _ag = df_view.dropna(subset=["Check In","Inv Date"]).copy()
            _ag["_d"] = (_ag["Check In"] - _ag["Inv Date"]).dt.days
            _agp = _ag[_ag["_d"] >= 0]
            if not _agp.empty: _aging = float(_agp["_d"].mean())
        _tsup = int(df_view["Supplier_Name"].dropna().nunique()) if "Supplier_Name" in df_view.columns else 0
        _thot = int(df_view["Hotel_Name"].dropna().nunique())    if "Hotel_Name"    in df_view.columns else 0
        _tcit = int(df_view["Hotel_City"].dropna().nunique())    if "Hotel_City"    in df_view.columns else 0
        _tpic = int(df_view["Agent"].dropna().nunique())         if "Agent"         in df_view.columns else 0
        prev  = get_prev_period_metrics(df_raw, df_view)

        def _badge(curr, pv, size="lg"):
            neu = '<span style="display:inline-flex;align-items:center;gap:3px;font-size:.53rem;font-weight:600;padding:2px 7px;border-radius:20px;background:#F8FAFC;color:#94A3B8;border:1px solid #E2E8F0;">── No ref</span>'
            try:
                if curr is None or pv is None: return neu
                c = float(curr); p = float(pv)
                if p == 0: return neu
                pct = (c-p)/abs(p)*100
                is_up = pct > 0
                arr = "▲" if pct > 0 else "▼"
                bg  = "#ECFDF5" if is_up else "#FFF1F2"
                col = "#059669" if is_up else "#DC2626"
                bdr = "#A7F3D0" if is_up else "#FECACA"
                fs  = "font-size:.53rem;" if size=="sm" else "font-size:.58rem;"
                return f'<span style="display:inline-flex;align-items:center;gap:3px;{fs}font-weight:700;padding:2px 8px;border-radius:20px;background:{bg};color:{col};border:1px solid {bdr};">{arr} {abs(pct):.1f}%</span>'
            except:
                return neu

        def _ctr(v):
            if v is None: return "N/A"
            f = float(v); a = abs(f)
            if a >= 1e9: return f'<span style="font-family:\'Sora\',sans-serif;">{f/1e9:.1f}B</span>'
            if a >= 1e6: return f'<span style="font-family:\'Sora\',sans-serif;">{f/1e6:.1f}M</span>'
            if a >= 1e3: return f'<span style="font-family:\'Sora\',sans-serif;">{f/1e3:.1f}K</span>'
            return f'<span style="font-family:\'Sora\',sans-serif;">{int(f):,}</span>'

        def _hero(icon, label, val_html, sub, badge, accent="linear-gradient(90deg,#0D9488,#134E4A)"):
            return (
                f'<div style="background:#fff;border:1px solid #E2E8F0;border-radius:14px;overflow:hidden;'
                f'transition:box-shadow .2s,transform .2s;" '
                f'onmouseover="this.style.transform=\'translateY(-3px)\';this.style.boxShadow=\'0 12px 32px -6px rgba(13,148,136,.16)\'" '
                f'onmouseout="this.style.transform=\'\';this.style.boxShadow=\'\'">'
                f'<div style="height:3px;background:{accent};"></div>'
                f'<div style="padding:18px 20px 16px;">'
                f'<div style="width:38px;height:38px;border-radius:10px;display:grid;place-items:center;'
                f'margin-bottom:12px;font-size:1.1rem;background:#F0FDFA;border:1px solid #CCFBF1;">{icon}</div>'
                f'<div style="font-size:.55rem;font-weight:700;letter-spacing:1.2px;text-transform:uppercase;'
                f'color:#94A3B8;margin-bottom:6px;">{label}</div>'
                f'<div style="font-size:2rem;font-weight:800;line-height:1;letter-spacing:-1.5px;'
                f'color:#0F172A;margin-bottom:4px;">{val_html}</div>'
                f'<div style="font-size:.58rem;color:#94A3B8;margin-bottom:10px;">{sub}</div>'
                f'{badge}</div></div>'
            )

        def _cell(label, val, hint, badge=""):
            return (
                f'<div style="padding:14px 18px 12px;border-right:1px solid #E2E8F0;transition:background .12s;" '
                f'onmouseover="this.style.background=\'#F8FDFC\'" onmouseout="this.style.background=\'\'">'
                f'<span style="font-size:.53rem;font-weight:700;letter-spacing:.9px;text-transform:uppercase;'
                f'color:#94A3B8;display:block;margin-bottom:5px;">{label}</span>'
                f'<span style="font-family:\'Sora\',sans-serif;font-size:1.3rem;font-weight:800;color:#0F172A;'
                f'display:block;margin-bottom:3px;">{val}</span>'
                f'<span style="font-size:.54rem;color:#94A3B8;display:block;margin-bottom:4px;">{hint}</span>'
                f'{badge}</div>'
            )

        # Prev period bar
        if prev:
            _pd = f"&nbsp;·&nbsp;{prev.get('prev_min','')} – {prev.get('prev_max','')}"
            st.markdown(
                f'<div style="padding:8px 14px;background:#fff;border:1px solid #E2E8F0;border-radius:10px;'
                f'margin-bottom:20px;display:flex;align-items:center;gap:8px;">'
                f'<span style="width:7px;height:7px;border-radius:50%;background:#0D9488;display:inline-block;"></span>'
                f'<span style="font-size:.58rem;color:#0F766E;font-weight:600;">'
                f'✓ Perbandingan aktif{_pd}&nbsp;·&nbsp;{prev.get("rows",0):,} baris</span></div>',
                unsafe_allow_html=True)
        else:
            st.markdown(
                '<div style="padding:8px 14px;background:#F8FAFC;border:1px solid #E2E8F0;border-radius:10px;margin-bottom:20px;">'
                '<span style="font-size:.58rem;color:#94A3B8;font-weight:600;">'
                'ⓘ Atur filter periode untuk mengaktifkan perbandingan vs periode sebelumnya</span></div>',
                unsafe_allow_html=True)

        # Hero cards row
        _pm_html = (f'<span style="font-family:\'Sora\',sans-serif;">{_pmv:.1f}%</span>'
                    if _pmv is not None else "N/A")
        st.markdown(
            '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:14px;">'
            + _hero("📄","Invoice Unik",_ctr(_ui),"Total transaksi invoice unik",_badge(_ui,prev.get("ui")))
            + _hero("💰","Sales AR",_ctr(_sa),"Total nilai penjualan (IDR)",_badge(_sa,prev.get("sa")),"linear-gradient(90deg,#134E4A,#0D9488)")
            + _hero("📈","Avg Profit Margin",_pm_html,"Rata-rata margin keuntungan",_badge(_pmv,prev.get("pm")),"linear-gradient(90deg,#0D9488,#2DD4BF)")
            + '</div>',
            unsafe_allow_html=True)

        # Volume grid
        _ag_html = f"{_aging:.1f} hari" if _aging else "N/A"
        st.markdown(
            '<div style="display:grid;grid-template-columns:repeat(4,1fr);background:#fff;'
            'border:1px solid #E2E8F0;border-radius:12px;overflow:hidden;'
            'box-shadow:0 1px 3px rgba(0,0,0,.05);margin-bottom:14px;">'
            '<div style="grid-column:1/-1;padding:8px 16px 6px;font-size:.53rem;font-weight:700;'
            'letter-spacing:1.1px;text-transform:uppercase;color:#94A3B8;'
            'border-bottom:1px solid #E2E8F0;background:#FAFBFC;">Volume &amp; Trafik</div>'
            + _cell("Room Night",f"{_rn:,}","Total malam kamar",_badge(_rn,prev.get("rn"),size="sm"))
            + _cell("Pax Unik",f"{_up:,}","Nama tamu unik",_badge(_up,prev.get("up"),size="sm"))
            + _cell("Avg Aging Invoice",_ag_html,"Check In − Inv Date")
            + _cell("Total Baris",f"{_tr:,}","Baris data aktif")
            + '</div>',
            unsafe_allow_html=True)

        # Master data grid
        st.markdown(
            '<div style="display:grid;grid-template-columns:repeat(5,1fr);background:#fff;'
            'border:1px solid #E2E8F0;border-radius:12px;overflow:hidden;'
            'box-shadow:0 1px 3px rgba(0,0,0,.05);margin-bottom:20px;">'
            '<div style="grid-column:1/-1;padding:8px 16px 6px;font-size:.53rem;font-weight:700;'
            'letter-spacing:1.1px;text-transform:uppercase;color:#94A3B8;'
            'border-bottom:1px solid #E2E8F0;background:#FAFBFC;">Master Data</div>'
            + _cell("Total Supplier",f"{_tsup:,}","Supplier unik")
            + _cell("Total Hotel",f"{_thot:,}","Hotel unik")
            + _cell("Total City",f"{_tcit:,}","Kota hotel unik")
            + _cell("Total Agent",f"{_tpic:,}","Agent unik")
            + _cell("Kolom Aktif",f"{_tc:,}","Field tersedia")
            + '</div>',
            unsafe_allow_html=True)

        # ── Tren bulanan ───────────────────────────────────────────────────────
        if "Issued Date" in df_view.columns and "Invoice No" in df_view.columns:
            _dt = df_view.dropna(subset=["Issued Date","Invoice No"]).copy()
            _dt["_ml"] = _dt["Issued Date"].dt.strftime("%b %Y")
            _dt["_mn"] = _dt["Issued Date"].dt.to_period("M").apply(lambda p: p.ordinal)
            _ti2 = (_dt.groupby(["_ml","_mn"],as_index=False)["Invoice No"]
                       .nunique().rename(columns={"Invoice No":"Inv"}).sort_values("_mn"))
            has_rn = "Total Room Night" in df_view.columns
            if has_rn:
                _tr2s = _dt.groupby(["_ml","_mn"],as_index=False)["Total Room Night"].sum().sort_values("_mn")

            def _mini(items):
                parts = []
                for lbl, val in items:
                    parts.append(
                        f'<div><span style="font-size:.5rem;font-weight:700;text-transform:uppercase;'
                        f'letter-spacing:.8px;color:#94A3B8;">{lbl}</span><br>'
                        f'<span style="font-family:\'Sora\',sans-serif;font-size:.88rem;'
                        f'font-weight:800;color:#0F172A;">{val}</span></div>')
                return ('<div style="display:flex;gap:16px;margin-bottom:12px;'
                        'padding-bottom:12px;border-bottom:1px solid #F1F5F9;">'
                        + "".join(parts) + '</div>')

            def _linec(df_l, x, y, color="#0D9488", fill="rgba(13,148,136,.08)", h=280):
                _mx = df_l[y].max(); _th = max(1, _mx*.05)
                _lbl = df_l[y].apply(lambda v: f"{int(v):,}" if v >= _th else "")
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=df_l[x], y=df_l[y], mode="lines+markers+text",
                    text=_lbl, textposition="top center",
                    textfont=dict(size=10,color=color),
                    line=dict(color=color,width=2.5,shape="spline"),
                    marker=dict(size=8,color=color),
                    fill="tozeroy", fillcolor=fill,
                    hovertemplate=f"<b>%{{x}}</b><br>{y}: <b>%{{y:,.0f}}</b><extra></extra>",
                    cliponaxis=False))
                if not df_l.empty:
                    pk = df_l.loc[df_l[y].idxmax()]
                    fig.add_annotation(
                        x=pk[x], y=pk[y],
                        text=f"▲ Peak: {int(pk[y]):,}",
                        showarrow=True,arrowhead=2,arrowcolor=color,ax=0,ay=-32,
                        font=dict(size=10,color=color),
                        bgcolor="rgba(240,253,250,.9)",bordercolor=color,borderpad=4)
                fig.update_layout(hovermode="x unified",height=h,
                                  xaxis=dict(tickangle=-30),showlegend=False,
                                  margin=dict(l=8,r=8,t=12,b=8))
                return fig

            gsec("📈 Tren Bulanan")
            c1, c2 = st.columns(2)
            with c1:
                pk = _ti2.loc[_ti2["Inv"].idxmax()]
                st.markdown(
                    '<div style="background:#fff;border:1px solid #E2E8F0;border-radius:12px;padding:16px 18px 4px;">'
                    '<div style="font-size:.72rem;font-weight:700;color:#0F172A;margin-bottom:3px;">📄 Tren Invoice Bulanan</div>'
                    + _mini([("Total",compact_num(int(_ti2["Inv"].sum()))),
                              ("Peak",f'{pk["_ml"]} · {compact_num(int(pk["Inv"]))}'),
                              ("Avg/bln",compact_num(int(_ti2["Inv"].mean())))])
                    + '</div>', unsafe_allow_html=True)
                st.plotly_chart(theme(_linec(_ti2,"_ml","Inv")), use_container_width=True)
            with c2:
                if has_rn:
                    pk2 = _tr2s.loc[_tr2s["Total Room Night"].idxmax()]
                    st.markdown(
                        '<div style="background:#fff;border:1px solid #E2E8F0;border-radius:12px;padding:16px 18px 4px;">'
                        '<div style="font-size:.72rem;font-weight:700;color:#0F172A;margin-bottom:3px;">🌙 Tren Room Night Bulanan</div>'
                        + _mini([("Total",compact_num(int(_tr2s["Total Room Night"].sum()))),
                                  ("Peak",f'{pk2["_ml"]} · {compact_num(int(pk2["Total Room Night"]))}'),
                                  ("Avg/bln",compact_num(int(_tr2s["Total Room Night"].mean())))])
                        + '</div>', unsafe_allow_html=True)
                    st.plotly_chart(theme(_linec(_tr2s,"_ml","Total Room Night")), use_container_width=True)
                else:
                    st.info("Kolom Total Room Night tidak tersedia.")

            c3, c4 = st.columns(2)
            with c3:
                if "Profit" in df_view.columns:
                    _dt3 = _dt.copy()
                    _dt3["Profit"] = pd.to_numeric(_dt3.get("Profit", pd.Series(dtype=float)), errors="coerce").fillna(0)
                    _pr_s = _dt3.groupby(["_ml","_mn"],as_index=False)["Profit"].sum().sort_values("_mn")
                    st.markdown(
                        '<div style="background:#fff;border:1px solid #E2E8F0;border-radius:12px;padding:16px 18px 4px;">'
                        '<div style="font-size:.72rem;font-weight:700;color:#0F172A;margin-bottom:3px;">💹 Tren Profit Bulanan</div>'
                        + _mini([("Total",compact_num(_pr_s["Profit"].sum())),
                                  ("Margin avg",f"{_pmv:.1f}%" if _pmv else "—")])
                        + '</div>', unsafe_allow_html=True)
                    st.plotly_chart(theme(_linec(_pr_s,"_ml","Profit",color="#134E4A",fill="rgba(19,78,74,.07)")), use_container_width=True)
                else:
                    st.info("Kolom Profit tidak tersedia.")
            with c4:
                if "Hotel_City" in df_view.columns:
                    _dt4 = _dt.dropna(subset=["Hotel_City"]).copy() if "Hotel_City" in _dt.columns else pd.DataFrame()
                    if not _dt4.empty:
                        _cy_s = (_dt4.groupby(["_ml","_mn"],as_index=False)["Hotel_City"]
                                 .nunique().rename(columns={"Hotel_City":"Kota"}).sort_values("_mn"))
                        st.markdown(
                            '<div style="background:#fff;border:1px solid #E2E8F0;border-radius:12px;padding:16px 18px 4px;">'
                            '<div style="font-size:.72rem;font-weight:700;color:#0F172A;margin-bottom:3px;">🗺️ Tren Kota Unik</div>'
                            + _mini([("Max",str(int(_cy_s["Kota"].max()))),("Avg/bln",str(int(_cy_s["Kota"].mean())))])
                            + '</div>', unsafe_allow_html=True)
                        st.plotly_chart(theme(_linec(_cy_s,"_ml","Kota",color="#134E4A",fill="rgba(19,78,74,.07)")), use_container_width=True)
                else:
                    st.info("Kolom Hotel_City tidak tersedia.")

        # ── Distribusi ─────────────────────────────────────────────────────────
        gsec("🏢 Distribusi &amp; Analisis")
        dch1, dch2 = st.columns(2)
        with dch1:
            inv_col = ("Normalized_Inv_To" if "Normalized_Inv_To" in df_view.columns
                       else ("Invoice To" if "Invoice To" in df_view.columns else None))
            if inv_col and "Invoice No" in df_view.columns:
                _dfi = (df_view[[inv_col,"Invoice No"]]
                        .dropna(subset=[inv_col])
                        .assign(**{inv_col: lambda d: d[inv_col].astype(str).str.strip()})
                        .pipe(lambda d: d[~d[inv_col].isin(["","nan","None","NaN","Unknown"])]))
                _tot_inv = _dfi["Invoice No"].nunique()
                _top10 = (_dfi.groupby(inv_col)["Invoice No"].nunique().reset_index()
                          .rename(columns={"Invoice No":"Inv"})
                          .sort_values("Inv", ascending=False).head(10))
                _top10["Pct"] = (_top10["Inv"] / _tot_inv * 100).round(1)
                _mx = int(_top10["Inv"].max())
                _RC = [("#0D9488","#F0FDFA","#CCFBF1"),("#0F766E","#F0FDFA","#99F6E4"),
                       ("#134E4A","#F0FDFA","#5EEAD4"),("#334155","#F8FAFC","#E2E8F0")]
                rows = ""
                for pos, (_, row) in enumerate(_top10.iterrows()):
                    name = str(row[inv_col]); w = int(row["Inv"])/_mx*100
                    ci = min(pos, 3); bc, bg, bd = _RC[ci]
                    rbg = "#fff" if pos%2==0 else "#FAFBFC"
                    rows += (
                        f'<div style="display:grid;grid-template-columns:22px 1fr auto;align-items:center;'
                        f'gap:9px;padding:7px 14px;background:{rbg};border-bottom:1px solid #F1F5F9;" '
                        f'onmouseover="this.style.background=\'#F0FDFA\'" onmouseout="this.style.background=\'{rbg}\'">'
                        f'<div style="width:20px;height:20px;border-radius:6px;display:flex;align-items:center;'
                        f'justify-content:center;font-size:.58rem;font-weight:800;background:{bg};color:{bc};border:1px solid {bd};">{pos+1}</div>'
                        f'<div><div style="font-size:.68rem;font-weight:600;color:#0F172A;margin-bottom:4px;">{name}</div>'
                        f'<div style="display:flex;align-items:center;gap:6px;">'
                        f'<div style="flex:1;height:5px;background:#F1F5F9;border-radius:5px;overflow:hidden;">'
                        f'<div style="width:{w:.1f}%;height:100%;background:linear-gradient(90deg,{bc},#2DD4BF);border-radius:5px;"></div></div>'
                        f'<span style="font-size:.54rem;color:#94A3B8;">{row["Pct"]:.1f}%</span></div></div>'
                        f'<div style="text-align:right;">'
                        f'<span style="font-family:\'Sora\',sans-serif;font-size:.78rem;font-weight:800;color:#0F172A;">{int(row["Inv"]):,}</span>'
                        f'<div style="font-size:.5rem;color:#94A3B8;">invoice</div></div></div>')
                st.markdown(
                    '<div style="background:#fff;border:1px solid #E2E8F0;border-radius:12px;overflow:hidden;">'
                    '<div style="padding:12px 16px 10px;border-bottom:1px solid #E2E8F0;'
                    'display:flex;align-items:center;justify-content:space-between;'
                    'background:linear-gradient(90deg,#F0FDFA,#fff);">'
                    '<span style="font-size:.75rem;font-weight:700;color:#0F172A;">🏢 Top 10 Invoice To</span>'
                    f'<span style="font-size:.55rem;color:#94A3B8;">dari {_tot_inv:,} unik</span></div>'
                    + rows + '</div>',
                    unsafe_allow_html=True)
            else:
                st.info("Kolom Invoice To tidak ditemukan.")

        with dch2:
            # Domestic vs International dari Product Type
            _RCOLS = ["#0D9488","#0F766E","#14B8A6","#5EEAD4","#99F6E4"]
            if "Product Type" in df_view.columns and "Invoice No" in df_view.columns:
                _dfd = (df_view[["Product Type","Invoice No"]]
                        .dropna(subset=["Product Type"])
                        .assign(**{"Product Type": lambda d: d["Product Type"].astype(str).str.strip()})
                        .pipe(lambda d: d[~d["Product Type"].isin(["","nan","None","NaN"])]))
                _dg = (_dfd.groupby("Product Type")["Invoice No"].nunique().reset_index()
                       .rename(columns={"Invoice No":"Inv"}).sort_values("Inv", ascending=False))
                _dgt = int(_dg["Inv"].sum())
                _fmt = lambda n: (f"{n/1e6:.1f}M" if n>=1e6 else (f"{n/1e3:.1f}K" if n>=1e3 else str(n)))
                segs = []
                for idx, (_, row) in enumerate(_dg.iterrows()):
                    pct = round(row["Inv"]/_dgt*100, 1) if _dgt > 0 else 0
                    segs.append({
                        "label": str(row["Product Type"]),
                        "value": int(row["Inv"]),
                        "pct":   pct,
                        "color": _RCOLS[min(idx, len(_RCOLS)-1)]
                    })
                _donut_html = build_donut_html(segs, _fmt(_dgt), f"Berdasarkan invoice unik · {_fmt(_dgt)} total")
                st.html(f'<div style="height:370px;overflow:hidden;">' + _donut_html + '</div>')
                st.caption("*Domestic vs International dari kolom Product Type")
            else:
                st.info("Kolom Product Type tidak tersedia.")

        # ── Preview Data ───────────────────────────────────────────────────────
        gsec("&#9776; Preview Data")
        _rpp = 50
        _tp  = max(1, (_tr//_rpp) + int(_tr%_rpp > 0))
        if "pg" not in st.session_state: st.session_state.pg = 0
        if st.session_state.pg >= _tp: st.session_state.pg = 0
        pc, pm2, pn = st.columns([1,5,1])
        with pc:
            if st.button("Prev", key="btn_prev") and st.session_state.pg > 0:
                st.session_state.pg -= 1; st.rerun()
        with pn:
            if st.button("Next", key="btn_next") and st.session_state.pg < _tp-1:
                st.session_state.pg += 1; st.rerun()
        with pm2:
            st.markdown(
                f'<p style="text-align:center;font-size:.68rem;color:#475569;padding:9px 0;margin:0;">'
                f'Hal&nbsp;{st.session_state.pg+1}&nbsp;/&nbsp;{_tp} &nbsp;·&nbsp; {_tr:,} baris</p>',
                unsafe_allow_html=True)
        _s = st.session_state.pg * _rpp
        _e = _s + _rpp
        st.dataframe(df_view.iloc[_s:_e], width="stretch")
        _dc, _ec = st.columns(2)
        with _dc:
            st.download_button("⬇ Download CSV",
                               df_view.to_csv(index=False).encode("utf-8"),
                               "hotel_report.csv","text/csv",width="stretch")
        with _ec:
            _ob = io.BytesIO()
            with pd.ExcelWriter(_ob, engine="xlsxwriter") as _w:
                df_view.to_excel(_w, index=False, sheet_name="Report")
            st.download_button("⬇ Download Excel", _ob.getvalue(),
                               "hotel_report.xlsx",
                               "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               width="stretch")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2 — TREN INVOICE
    # ══════════════════════════════════════════════════════════════════════════
    with tab2:
        ti2, tr2c = _cached_invoice_trend(_vh, df_view)
        if ti2 is not None:
            ca, cb = st.columns([3,2])
            with ca:
                gsec("Tren Invoice Bulanan","📈")
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=ti2["Bulan"], y=ti2["Invoice"], mode="lines+markers",
                    line=dict(color="#0D9488",width=2.5,shape="spline"),
                    marker=dict(size=9,color="#0D9488"),
                    fill="tozeroy",fillcolor="rgba(13,148,136,.1)",
                    hovertemplate="<b>%{x}</b><br>Invoice: <b>%{y:,.0f}</b><extra></extra>"))
                fig.update_layout(xaxis_title="",yaxis_title="Invoice Unik",
                                  hovermode="x unified",height=320)
                st.plotly_chart(theme(fig), use_container_width=True)
            with cb:
                gsec("Ringkasan Bulanan","📋")
                if tr2c is not None:
                    _m = ti2[["Bulan","Invoice"]].merge(tr2c, on="Bulan", how="left")
                    _m.columns = ["Bulan","Invoice Unik","Room Night"]
                else:
                    _m = ti2[["Bulan","Invoice"]].rename(columns={"Invoice":"Invoice Unik"})
                _num_cols_m = [c for c in _m.columns if pd.api.types.is_numeric_dtype(_m[c])]
                _sty_m = _m.style.format({c: "{:,.0f}" for c in _num_cols_m})
                if "Invoice Unik" in _num_cols_m:
                    _sty_m = _sty_m.apply(
                        lambda s: [f"background-color: rgba(88,28,220,{0.05 + 0.55*(float(v)-float(s.min()))/(float(s.max())-float(s.min())+1e-9):.2f}); color: #0F172A" for v in s]
                        if s.max() > s.min() else [""] * len(s),
                        subset=["Invoice Unik"])
                st.dataframe(_sty_m, width="stretch", height=320)
            gsec("Volume Invoice per Bulan","📊")
            fig2 = px.bar(ti2, x="Bulan", y="Invoice", text="Invoice", color="Invoice",
                          color_continuous_scale=["rgba(99,102,241,.3)","#0D9488","#0D9488"])
            fig2.update_traces(texttemplate="%{y:,.0f}", textposition="outside",
                               textfont=dict(size=11,color="#8898AA"),
                               marker_line_width=0, marker_cornerradius=4, cliponaxis=False)
            fig2.update_layout(coloraxis_showscale=False, height=290, xaxis_title="", yaxis_title="")
            st.plotly_chart(theme(fig2), use_container_width=True)
        else:
            st.warning("Kolom Issued Date atau Invoice No tidak ditemukan.")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3 — SUPPLIER
    # ══════════════════════════════════════════════════════════════════════════
    with tab3:
        ss3, d3 = _cached_supplier(_vh, df_view)
        if ss3 is not None:
            ca, cb = st.columns([3,2])
            with ca:
                gsec("Distribusi Supplier","🏢")
                fig3 = px.pie(d3, names="Supplier_Name", values="Total Room Night",
                              hole=0.52, color_discrete_sequence=GLASS_PALETTE)
                fig3.update_traces(textinfo="percent+label", textfont=dict(size=11),
                                   pull=[0.05]+[0]*(len(d3)-1),
                                   marker=dict(line=dict(color="rgba(6,8,24,.8)",width=2)),
                                   hovertemplate="<b>%{label}</b><br>Room Night: %{value:,.0f}<br>%{percent}<extra></extra>")
                fig3.update_layout(height=360, legend=dict(orientation="v",yanchor="middle",y=.5,xanchor="left",x=1.02))
                st.plotly_chart(theme(fig3), use_container_width=True)
            with cb:
                gsec("Top Supplier","📊")
                fig3b = px.bar(ss3.head(10), x="Total Room Night", y="Supplier_Name",
                               orientation="h", text="Total Room Night", color="Total Room Night",
                               color_continuous_scale=TEAL_SCALE)
                fig3b.update_traces(texttemplate="%{x:,.0f}", textposition="outside",
                                    textfont=dict(size=10,color="#8898AA"),
                                    marker_line_width=0, marker_cornerradius=4, cliponaxis=False)
                fig3b.update_layout(yaxis=dict(categoryorder="total ascending"),
                                    coloraxis_showscale=False, height=380, xaxis_title="", yaxis_title="")
                st.plotly_chart(theme(fig3b), use_container_width=True)
            gsec("Tabel Lengkap Supplier")
            st.dataframe(
                ss3.reset_index(drop=True)
                .style.format({"Total Room Night":"{:,.0f}"})
                .apply(lambda s: [f"background-color: rgba(13,148,136,{0.05 + 0.55*(float(v)-float(s.min()))/(float(s.max())-float(s.min())+1e-9):.2f}); color: #0F172A" for v in s] if pd.to_numeric(s, errors='coerce').notna().any() else [""] * len(s), subset=["Total Room Night"]),
                width="stretch")
        else:
            st.warning("Kolom Supplier_Name atau Total Room Night tidak tersedia.")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 4 — PRODUCT TYPE
    # ══════════════════════════════════════════════════════════════════════════
    with tab4:
        ps4, d4 = _cached_product(_vh, df_view)
        if ps4 is not None:
            ca, cb = st.columns([3,2])
            with ca:
                gsec("Distribusi Product Type","📦")
                fig4 = px.pie(d4, names="Product Type", values="Total Room Night",
                              hole=0.52, color_discrete_sequence=GLASS_PALETTE)
                fig4.update_traces(textinfo="percent+label", textfont=dict(size=12),
                                   pull=[0.05]+[0]*(len(d4)-1),
                                   marker=dict(line=dict(color="rgba(6,8,24,.8)",width=2)),
                                   hovertemplate="<b>%{label}</b><br>Room Night: %{value:,.0f}<br>%{percent}<extra></extra>")
                fig4.update_layout(height=360)
                st.plotly_chart(theme(fig4), use_container_width=True)
            with cb:
                gsec("Tabel Product Type","📋")
                st.dataframe(
                    d4.reset_index(drop=True)
                    .style.format({"Total Room Night":"{:,.0f}"})
                    .apply(lambda s: [f"background-color: rgba(13,148,136,{0.05 + 0.55*(float(v)-float(s.min()))/(float(s.max())-float(s.min())+1e-9):.2f}); color: #0F172A" for v in s] if pd.to_numeric(s, errors='coerce').notna().any() else [""] * len(s), subset=["Total Room Night"]),
                    width="stretch", height=360)
        else:
            st.warning("Kolom Product Type atau Total Room Night tidak tersedia.")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 5 — AGENT SCORECARD
    # ══════════════════════════════════════════════════════════════════════════
    with tab5:
        if "Agent" in df_view.columns and "Invoice No" in df_view.columns and "Total Room Night" in df_view.columns:
            dfa = df_view.copy()
            dfa["Agent"] = (dfa["Agent"].astype(str).str.strip().str.lower()
                            .map(lambda x: AGENT_MAP.get(x, x.title())))
            _null_ag = {"nan","none","","nat","<na>","n/a","null","-"}
            dfa = dfa[~dfa["Agent"].str.lower().isin(_null_ag)]

            def _pic_group(name):
                nu = str(name).strip()
                for p in KNOWN_PICS:
                    if p.lower() == nu.lower():
                        return p
                return "Other"

            dfa["PIC"] = dfa["Agent"].apply(_pic_group)

            _n_months = 1
            if "Issued Date" in dfa.columns:
                _periods = dfa["Issued Date"].dropna().dt.to_period("M").unique()
                _n_months = max(len(_periods), 1)
            _ccol = ("Normalized_Inv_To" if "Normalized_Inv_To" in dfa.columns
                     else ("Invoice To" if "Invoice To" in dfa.columns else None))

            _known_with_data = [p for p in KNOWN_PICS if p in dfa["PIC"].unique()]
            _has_other = "Other" in dfa["PIC"].unique()
            _order = _known_with_data + (["Other"] if _has_other else [])

            pic_data = {}
            for _pic in _order:
                _s = dfa[dfa["PIC"] == _pic]
                if _s.empty: continue
                _iu = int(_s["Invoice No"].nunique())
                _rn = float(_s["Total Room Night"].sum())
                _sa = float(_s["Sales AR"].fillna(0).astype(float).sum()) if "Sales AR" in _s.columns else 0.0
                _pr = float(_s["Profit"].fillna(0).astype(float).sum())   if "Profit"   in _s.columns else 0.0
                _avg_pm = None
                if "Profit" in _s.columns and "Sales AR" in _s.columns:
                    sf = pd.to_numeric(_s["Sales AR"], errors="coerce").fillna(0)
                    pf = pd.to_numeric(_s["Profit"],   errors="coerce").fillna(0)
                    mm = sf != 0
                    if mm.any(): _avg_pm = float((pf[mm]/sf[mm]*100).mean())
                _top_s = "\u2014"; _top_rn = 0
                if "Supplier_Name" in _s.columns:
                    _srn = (_s.groupby("Supplier_Name")["Total Room Night"].sum()
                            .sort_values(ascending=False))
                    _srn = _srn[~_srn.index.astype(str).str.strip().isin({"","nan","None","NaN","Unknown"})]
                    if not _srn.empty: _top_s = str(_srn.index[0]); _top_rn = int(_srn.iloc[0])
                _ait = dfa["Invoice No"].nunique()
                _art = float(dfa["Total Room Night"].sum())
                pic_data[_pic] = {
                    "iu":_iu,"rn":_rn,"sa":_sa,"pr":_pr,
                    "avg_inv":_iu/_n_months,
                    "avg_rn":_rn/_iu if _iu > 0 else 0,
                    "avg_sa":_sa/_iu if _iu > 0 else 0,
                    "avg_pr":_pr/_iu if _iu > 0 else 0,
                    "avg_pm":_avg_pm,
                    "co":(int(_s[_ccol].dropna().nunique()) if _ccol else 0),
                    "top_s":_top_s, "top_rn":_top_rn,
                    "ip":(_iu/_ait*100 if _ait > 0 else 0),
                    "rp":(_rn/_art*100 if _art > 0 else 0),
                }

            def _ini(n):
                p = str(n).split()
                if len(p) >= 2: return (p[0][0]+p[1][0]).upper()
                return str(n)[:2].upper() if len(str(n)) >= 2 else str(n).upper()

            def _bar_html(pct):
                w = min(float(pct), 100)
                return '<div class="p2-bar"><div class="p2-bf" style="width:' + f"{w:.1f}" + '%;"></div></div>'

            def _mrow_html(icon, label, val, hint="", bp=None):
                bh = _bar_html(bp) if bp is not None else ""
                hh = '<div class="p2m-h">' + hint + '</div>' if hint else ""
                return (
                    '<div class="p2-mr">'
                    '<div class="p2m-top">'
                    '<span class="p2m-ic">' + icon + '</span>'
                    '<span class="p2m-lb">' + label + '</span>'
                    '</div>'
                    '<div class="p2m-v">' + val + '</div>'
                    + hh + bh + '</div>'
                )

            def _build_card(pic, d):
                is_other = (pic == "Other")
                ini  = _ini(pic)
                cls  = "p2-card oth" if is_other else "p2-card"
                sa_s = compact_num(d["sa"]) if d["sa"] else "\u2014"
                pr_s = compact_num(d["pr"]) if d["pr"] else "\u2014"
                as_s = compact_num(d["avg_sa"]) if d["avg_sa"] else "\u2014"
                ap_s = compact_num(d["avg_pr"]) if d["avg_pr"] else "\u2014"
                pm_raw = None; pm_s = "\u2014"
                if d["sa"] and d["pr"] and d["sa"] > 0:
                    pm_raw = d["pr"] / d["sa"] * 100
                    pm_s   = f"{pm_raw:.1f}%"
                apm_s = f"{d['avg_pm']:.1f}%" if d.get("avg_pm") is not None else "\u2014"
                photo = _load_avatar_b64(pic) if not is_other else ""
                if photo:
                    av = '<div class="p2av photo"><img src="' + photo + '" alt="' + pic + '"/></div>'
                else:
                    av = '<div class="p2av">' + ini + '</div>'
                sh_txt = f'{d["ip"]:.1f}% inv \u00b7 {d["rp"]:.1f}% RN'
                sh = ('<div class="p2-sh">'
                      '<span class="p2-sh-dot"></span>'
                      '<span class="p2-sh-txt">' + sh_txt + '</span>'
                      '</div>')
                pm_c  = "#059669" if (pm_raw is not None and pm_raw  >= 0) else "#DC2626"
                apm_c = "#059669" if (d.get("avg_pm") is not None and d["avg_pm"] >= 0) else "#DC2626"

                top_block = (
                    '<div class="' + cls + '">'
                    '<div class="p2-banner">'
                    + av +
                    '<div class="p2-bi">'
                    '<div class="p2-name">' + pic + '</div>'
                    '<div class="p2-role">Hotel Bookers \u00b7 MTT</div>'
                    + sh +
                    '</div></div>'
                )
                body_block = (
                    '<div class="p2-body">'
                    '<div class="p2-slbl">&#x1F4CB; Volume</div>'
                    '<div class="p2-mg">'
                    + _mrow_html("\U0001F9FE","Invoice",f'{d["iu"]:,}',f'avg {d["avg_inv"]:.1f}/bln',d["ip"])
                    + _mrow_html("\U0001F319","Room Night",compact_num(d["rn"]),f'avg {d["avg_rn"]:.1f}/inv',d["rp"])
                    + '</div>'
                    '<div class="p2-slbl">&#x1F4B0; Finansial</div>'
                    '<div class="p2-mg">'
                    + _mrow_html("\U0001F4E6","Sales AR",sa_s,"avg " + as_s + "/inv")
                    + _mrow_html("\U0001F4C8","Profit",pr_s,"avg " + ap_s + "/inv")
                    + '</div>'
                    '<div class="p2-ms">'
                    '<div class="p2-ml">'
                    '<div class="p2-mslb">Profit Margin</div>'
                    '<div class="p2-msv" style="color:' + pm_c + ';">' + pm_s + '</div>'
                    '<div style="margin-top:7px;padding-top:7px;border-top:1px dashed rgba(13,148,136,.22);">'
                    '<div class="p2-mslb" style="margin-bottom:2px;">Avg PM</div>'
                    '<div style="font-family:\'Sora\',sans-serif;font-size:.82rem;font-weight:700;color:' + apm_c + ';">' + apm_s + '</div>'
                    '</div></div>'
                    '<div class="p2-mr2">'
                    '<div class="p2-mslb">Companies</div>'
                    '<div class="p2-msv">' + f'{d["co"]:,}' + '</div>'
                    '</div></div>'
                    '</div>'
                )
                if d["top_s"] != "\u2014":
                    sh2 = d["top_s"][:26]+"…" if len(d["top_s"]) > 26 else d["top_s"]
                    ft_inner = ('<div class="p2-sr">'
                                '<span class="p2-sn">' + sh2 + '</span>'
                                '<span class="p2-srn">' + f'{d["top_rn"]:,}' + ' RN</span>'
                                '</div>')
                else:
                    ft_inner = '<span style="font-size:.65rem;color:#94A3B8;font-style:italic;">\u2014</span>'

                footer_block = (
                    '<div class="p2-ft">'
                    '<div class="p2-ftlb">&#x1F3E8; Supplier Preference</div>'
                    + ft_inner +
                    '</div>'
                )
                return top_block + body_block + footer_block + '</div>'

            _known_sorted = sorted(
                _known_with_data,
                key=lambda p: pic_data[p]["sa"] if p in pic_data else 0,
                reverse=True)
            _final_order = _known_sorted + (["Other"] if _has_other and "Other" in pic_data else [])

            gsec("Scorecard PIC Agent","🏅")
            st.markdown(
                '<div class="p2-grid">'
                + "".join(_build_card(p, pic_data[p]) for p in _final_order)
                + '</div>',
                unsafe_allow_html=True)

            gsec("Tabel Ringkasan Scorecard","📋")
            _rows = []
            for p in _final_order:
                d = pic_data[p]
                _sa2 = d["sa"] or 0; _pr2 = d["pr"] or 0
                _rows.append({
                    "PIC": p,
                    "Invoice": d["iu"],
                    "Avg Inv/Bln": round(d["avg_inv"],1),
                    "Room Night": int(d["rn"]),
                    "Avg RN/Inv": round(d["avg_rn"],1),
                    "Sales AR": _sa2,
                    "Avg Sales/Inv": round(d["avg_sa"],0),
                    "Profit": _pr2,
                    "Avg Profit/Inv": round(d["avg_pr"],0),
                    "Profit Margin": f"{_pr2/_sa2*100:.1f}%" if _sa2 > 0 else "\u2014",
                    "Companies": d["co"],
                    "% Inv": round(d["ip"],1),
                    "% RN": round(d["rp"],1),
                    "Top Supplier": d["top_s"],
                })
            _df_sc = pd.DataFrame(_rows)
            st.dataframe(
                _df_sc.style.format({c:"{:,.0f}" for c in ["Invoice","Room Night","Sales AR","Profit"]}),
                width="stretch")
            _ob_sc = io.BytesIO()
            with pd.ExcelWriter(_ob_sc, engine="xlsxwriter") as _w:
                _df_sc.to_excel(_w, index=False, sheet_name="Scorecard")
            st.download_button("⬇ Download Scorecard", _ob_sc.getvalue(),
                               "scorecard_agent.xlsx",
                               "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               width="stretch")
        else:
            st.warning("Kolom Agent, Invoice No, atau Total Room Night tidak ditemukan.")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 6 — PTM CORP
    # ══════════════════════════════════════════════════════════════════════════
    with tab6:
        dfh = _cached_ptm(_vh, df_view)
        if dfh is None:
            st.warning("Kolom Supplier_Name, Hotel_Name, atau Total Room Night tidak ditemukan.")
        elif isinstance(dfh, pd.DataFrame) and dfh.empty:
            st.warning("Tidak ditemukan data Supplier PTM/Corp Rate.")
        else:
            ca, cb = st.columns([3,2])
            with ca:
                gsec("Top Hotel PTM Corp Rate","🏨")
                fh = px.bar(dfh.head(15), x="Total Room Night", y="Hotel_Name",
                            orientation="h", text="Total Room Night", color="Total Room Night",
                            color_continuous_scale=["rgba(252,211,77,.2)","rgba(252,211,77,.6)","#2DD4BF"])
                fh.update_traces(texttemplate="%{x:,.0f}", textposition="outside",
                                 textfont=dict(size=11,color="#8898AA"),
                                 marker_line_width=0, marker_cornerradius=4, cliponaxis=False)
                fh.update_layout(yaxis=dict(categoryorder="total ascending",automargin=True),
                                 coloraxis_showscale=False, height=460,
                                 xaxis_title="", yaxis_title="",
                                 margin=dict(l=8,r=80,t=30,b=8))
                st.plotly_chart(theme(fh), use_container_width=True)
            with cb:
                gsec("Tabel Hotel PTM","📋")
                st.dataframe(
                    dfh.head(20).reset_index(drop=True)
                    .style.format({"Total Room Night":"{:,.0f}"})
                    .apply(lambda s: [f"background-color: rgba(13,148,136,{0.05 + 0.55*(float(v)-float(s.min()))/(float(s.max())-float(s.min())+1e-9):.2f}); color: #0F172A" for v in s] if pd.to_numeric(s, errors='coerce').notna().any() else [""] * len(s), subset=["Total Room Night"]),
                    width="stretch", height=400)
                _ob3 = io.BytesIO()
                with pd.ExcelWriter(_ob3, engine="xlsxwriter") as _w:
                    dfh.to_excel(_w, index=False, sheet_name="Hotel_PTM")
                st.download_button("⬇ Download", _ob3.getvalue(), "hotel_ptm.xlsx",
                                   "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                   width="stretch")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 7 — KATEGORI SUPPLIER
    # ══════════════════════════════════════════════════════════════════════════
    with tab7:
        cs7, d7 = _cached_cat(_vh, df_view)
        if cs7 is not None:
            ca, cb = st.columns([3,2])
            with ca:
                gsec("Distribusi Kategori Supplier","🎯")
                fc7 = px.pie(d7, names="Supplier_Category", values="Total Room Night",
                             hole=0.52, color_discrete_sequence=GLASS_PALETTE)
                fc7.update_traces(textinfo="percent+label", textfont=dict(size=12),
                                  pull=[0.05]+[0]*(len(d7)-1),
                                  marker=dict(line=dict(color="rgba(6,8,24,.8)",width=2)),
                                  hovertemplate="<b>%{label}</b><br>Room Night: %{value:,.0f}<extra></extra>")
                fc7.update_layout(height=380)
                st.plotly_chart(theme(fc7), use_container_width=True)
            with cb:
                gsec("Tabel Kategori","📋")
                st.dataframe(
                    cs7.reset_index(drop=True)
                    .style.format({"Total Room Night":"{:,.0f}"})
                    .apply(lambda s: [f"background-color: rgba(13,148,136,{0.05 + 0.55*(float(v)-float(s.min()))/(float(s.max())-float(s.min())+1e-9):.2f}); color: #0F172A" for v in s] if pd.to_numeric(s, errors='coerce').notna().any() else [""] * len(s), subset=["Total Room Night"]),
                    width="stretch", height=380)
        else:
            st.warning("Kolom Supplier_Category atau Total Room Night tidak tersedia.")

# ══════════════════════════════════════════════════════════════════════════════
# EMPTY STATE
# ══════════════════════════════════════════════════════════════════════════════
else:
    for k in ["df_raw","upload_hash"]:
        st.session_state.pop(k, None)
    st.markdown("""
    <div style="display:flex;flex-direction:column;align-items:center;padding:80px 40px;
                text-align:center;max-width:480px;margin:60px auto 0;">
      <div style="width:64px;height:64px;margin-bottom:24px;background:#F0FDFA;
                  border:1px solid #CCFBF1;border-radius:16px;display:grid;place-items:center;">
        <svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="#0D9488"
             stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">
          <path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"/>
          <polyline points="13 2 13 9 20 9"/>
        </svg>
      </div>
      <div style="font-family:'DM Sans',sans-serif;font-size:1.05rem;font-weight:700;
                  color:#0F172A;margin-bottom:10px;">Belum ada data</div>
      <p style="font-size:.72rem;color:#94A3B8;line-height:1.9;margin:0 auto 28px;">
        Upload file Excel Custom Report di sidebar kiri, lalu klik
        <span style="color:#0D9488;font-weight:600;background:#F0FDFA;padding:1px 7px;
                     border-radius:5px;border:1px solid #CCFBF1;">Sync Data</span>
        untuk normalisasi dari Google Drive.
      </p>
      <div style="display:flex;gap:8px;flex-wrap:wrap;justify-content:center;">
        <span style="font-size:.62rem;padding:6px 14px;border-radius:20px;
              background:#F0FDFA;color:#0D9488;border:1px solid #CCFBF1;">Custom Report .xlsx</span>
        <span style="font-size:.62rem;padding:6px 14px;border-radius:20px;
              background:#F8FAFC;color:#64748B;border:1px solid #E2E8F0;">Google Drive Sync</span>
        <span style="font-size:.62rem;padding:6px 14px;border-radius:20px;
              background:#F0FDFA;color:#0D9488;border:1px solid #CCFBF1;">v10.0</span>
      </div>
    </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# FOOTER
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<div style="margin-top:56px;border-top:1px solid #E2E8F0;background:#fff;">
  <div style="background:#F0FDFA;border-bottom:1px solid #CCFBF1;padding:10px 36px;display:flex;gap:10px;">
    <span style="font-size:.8rem;flex-shrink:0;">⚠️</span>
    <p style="margin:0;font-size:.6rem;color:#0F766E;line-height:1.8;">
      <strong style="color:#0D9488;">DISCLAIMER · </strong>
      Data bersifat internal dan rahasia — dilarang disebarluaskan tanpa izin tertulis dari manajemen MTT.
    </p>
  </div>
  <div style="padding:12px 36px;display:flex;justify-content:space-between;flex-wrap:wrap;gap:8px;">
    <span style="font-size:.6rem;color:#94A3B8;">&copy; 2025 <strong style="color:#0D9488;">Hotel Intelligence</strong> · MTT</span>
    <span style="font-size:.6rem;color:#94A3B8;">Powered by Streamlit · v10.0</span>
    <span style="font-size:.6rem;color:#94A3B8;">Built by <strong style="color:#0D9488;">Rifyal Tumber</strong> · MTT · 2025</span>
  </div>
</div>""", unsafe_allow_html=True)
