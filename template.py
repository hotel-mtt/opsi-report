# =============================================================================
#  AI CC Reporting System  v7  — Optimized (Fast Load)
#  Optimization layers:
#    1. get_all_values() ganti get_all_records()  → 3-5x lebih cepat
#    2. TTL cache dinaikkan ke 1 jam + cache_data terpisah
#    3. DuckDB in-memory untuk filter & agregasi dashboard
#    4. Manual refresh hanya clear cache_data, bukan resource
#    5. Google Apps Script (GAS) fallback endpoint opsional
# =============================================================================
import streamlit as st
import hmac, hashlib, time
import gspread, json, base64, re, io, warnings, requests
from google.oauth2.service_account import Credentials
from datetime import datetime
from PIL import Image

# ── Optional dependencies ─────────────────────────────────────────────────────
try:
    import pypdfium2 as _pdfium
    _PDF_OK = True
except ImportError:
    _PDF_OK = False

try:
    import duckdb
    _DUCK_OK = True
except ImportError:
    _DUCK_OK = False

st.set_page_config(
    page_title="Mitra CC Reporter",
    page_icon="💳",
    layout="centered",
    initial_sidebar_state="collapsed",
)

try:
    from streamlit_cookies_controller import CookieController
    _COOKIE_OK = True
except ImportError:
    _COOKIE_OK = False

_COOKIE_NAME = "cc_report_auth"

# ═══════════════════════════════════════════════════════════════════════════════
#  AUTH
# ═══════════════════════════════════════════════════════════════════════════════
def _get_password():
    try:
        p = st.secrets["auth"]["password"]
        if p and "GANTI" not in p:
            return p
    except:
        pass
    return st.session_state.get("_auth_pw_override", "")


def _ttl_hours():
    try:
        return float(st.secrets["auth"].get("session_ttl_hours", 8))
    except:
        return 8.0


def _check_pw(candidate):
    correct = _get_password()
    if not correct:
        return False
    return hmac.compare_digest(
        hashlib.sha256(candidate.encode()).digest(),
        hashlib.sha256(correct.encode()).digest(),
    )


def _make_token():
    pw = _get_password()
    ts = str(int(time.time()))
    sig = hmac.new(pw.encode(), (pw + ts).encode(), hashlib.sha256).hexdigest()
    return f"{ts}:{sig}"


def _verify_token(token):
    if not token or ":" not in token:
        return False
    try:
        ts_str, sig = token.split(":", 1)
        ts = int(ts_str)
        if (time.time() - ts) > _ttl_hours() * 3600:
            return False
        pw = _get_password()
        expected = hmac.new(
            pw.encode(), (pw + ts_str).encode(), hashlib.sha256
        ).hexdigest()
        return hmac.compare_digest(sig, expected)
    except:
        return False


def _get_cookie_ctrl():
    if not _COOKIE_OK:
        return None
    if "_cookie_ctrl" not in st.session_state:
        st.session_state["_cookie_ctrl"] = CookieController()
    return st.session_state["_cookie_ctrl"]


def _render_logout_button():
    if st.button(
        "Logout",
        type="secondary",
        use_container_width=True,
        key="_auth_logout_btn",
    ):
        st.session_state["_auth_ok"] = False
        st.session_state["_auth_login_time"] = 0
        ctrl = _get_cookie_ctrl()
        if ctrl:
            try:
                ctrl.remove(_COOKIE_NAME)
            except:
                pass
        st.rerun()


def _render_footer():
    st.markdown(
        """
<div style="margin-top:32px;padding:14px 0 8px;border-top:0.5px solid #ddd;
    display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;">
  <div style="display:flex;align-items:center;gap:8px;">
    <div style="width:24px;height:24px;border-radius:6px;overflow:hidden;flex-shrink:0;">
      <div style="width:100%;height:100%;background:#191d3a;border-radius:6px;
        display:flex;align-items:center;justify-content:center;color:#fddb32;
        font-size:12px;font-weight:800;">M</div>
    </div>
    <div>
      <div style="font-size:11px;font-weight:600;color:#191d3a;line-height:1.2;">
        Intelligent Automation Scanner</div>
      <div style="font-size:9px;color:#aaa;line-height:1.2;">v7 · Mitra Tours &amp; Travel</div>
    </div>
  </div>
  <a href="https://www.linkedin.com/in/rifyalt" target="_blank"
     style="display:flex;align-items:center;gap:5px;text-decoration:none;
            font-size:10px;font-weight:500;color:#616161;
            border:0.5px solid #e0e0e0;padding:4px 10px;border-radius:20px;background:#fff;">
    Rifyal Tumber
  </a>
</div>""",
        unsafe_allow_html=True,
    )


def _dashboard_login_wall():
    ctrl = _get_cookie_ctrl()
    if not st.session_state.get("_auth_ok") and ctrl:
        try:
            token = ctrl.get(_COOKIE_NAME)
            if token and _verify_token(token):
                st.session_state["_auth_ok"] = True
                st.session_state["_auth_login_time"] = time.time()
        except:
            pass
    if st.session_state.get("_auth_ok"):
        elapsed = time.time() - st.session_state.get("_auth_login_time", 0)
        if elapsed < _ttl_hours() * 3600:
            return True
        st.session_state["_auth_ok"] = False
        if ctrl:
            try:
                ctrl.remove(_COOKIE_NAME)
            except:
                pass
    ttl = int(_ttl_hours())
    _err = st.session_state.get("_dash_err", "")
    st.markdown(
        f"""
<style>
.dash-lock-wrap{{display:flex;flex-direction:column;align-items:center;padding:48px 16px 8px;text-align:center}}
.dash-lock-icon{{width:52px;height:52px;border-radius:16px;background:#fff;border:1px solid #e4e4e4;
    display:flex;align-items:center;justify-content:center;margin-bottom:16px;font-size:22px}}
.dash-lock-title{{font-size:18px;font-weight:700;color:#191d3a;margin-bottom:4px}}
.dash-lock-sub{{font-size:13px;color:#aaa;margin-bottom:28px}}
.dash-lock-err{{font-size:12px;color:#e53935;margin-bottom:8px;min-height:16px;text-align:center}}
.dash-lock-foot{{font-size:11px;color:#ccc;margin-top:12px;margin-bottom:4px;text-align:center}}
</style>
<div class="dash-lock-wrap">
  <div class="dash-lock-icon">🔒</div>
  <div class="dash-lock-title">Welcome</div>
  <div class="dash-lock-sub">Masukkan password untuk melanjutkan</div>
</div>
<div class="dash-lock-err">{_err}</div>""",
        unsafe_allow_html=True,
    )
    _col_l, _col_c, _col_r = st.columns([1, 2, 1])
    with _col_c:
        pw = st.text_input(
            "Password",
            type="password",
            placeholder="Password",
            label_visibility="collapsed",
            key="_dash_pw_input",
        )
        _btn = st.button(
            "Login",
            type="primary",
            use_container_width=True,
            key="_dash_login_btn",
        )
    if _btn:
        if _check_pw(pw):
            st.session_state["_auth_ok"] = True
            st.session_state["_auth_login_time"] = time.time()
            st.session_state["_dash_err"] = ""
            ctrl2 = _get_cookie_ctrl()
            if ctrl2:
                try:
                    ctrl2.set(
                        _COOKIE_NAME,
                        _make_token(),
                        max_age=int(_ttl_hours() * 3600),
                    )
                except:
                    pass
            st.rerun()
        else:
            st.session_state["_dash_err"] = "Password salah. Coba lagi."
            st.rerun()
    st.markdown(
        f'<div class="dash-lock-foot">Sesi aktif {ttl} jam</div>',
        unsafe_allow_html=True,
    )
    return False


# ═══════════════════════════════════════════════════════════════════════════════
#  CSS
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html,body,[data-testid="stAppViewContainer"],[data-testid="stAppViewBlockContainer"],.main{
    background:#ededed !important;font-family:'Inter',system-ui,sans-serif !important}
.main .block-container{
    padding:8px 8px 100px !important;max-width:480px !important;margin:0 auto !important}
[data-testid="stSidebar"],#MainMenu,footer,header,[data-testid="stDecoration"]{display:none !important}
*{font-family:'Inter',system-ui,sans-serif !important;-webkit-tap-highlight-color:transparent}
.main .block-container{
    padding-bottom:max(100px, calc(80px + env(safe-area-inset-bottom))) !important;
    padding-left:max(8px, env(safe-area-inset-left)) !important;
    padding-right:max(8px, env(safe-area-inset-right)) !important}
.app-header{background:#191d3a;border-radius:16px;padding:12px 14px;
    display:flex;align-items:center;gap:10px;margin-bottom:10px}
.ah-icon{width:40px;height:40px;border-radius:11px;background:#fddb32;
    display:flex;align-items:center;justify-content:center;font-size:20px;flex-shrink:0}
.ah-title{font-size:16px;font-weight:800;color:#fff;line-height:1.2}
.ah-sub{font-size:11px;color:#9e9e9e;margin-top:1px}
.ah-live{margin-left:auto;font-size:9px;font-weight:700;letter-spacing:.4px;
    background:#0f2310;color:#4ade80;border:1px solid #1e4620;
    padding:4px 9px;border-radius:20px;display:flex;align-items:center;gap:4px;white-space:nowrap;flex-shrink:0}
.ah-live::before{content:'';width:5px;height:5px;border-radius:50%;background:#4ade80;display:block}
.ah-ai-badge{font-size:9px;font-weight:700;letter-spacing:.3px;
    padding:3px 8px;border-radius:20px;white-space:nowrap;flex-shrink:0;margin-left:4px}
.ah-ai-openai{background:#0d1f12;color:#4ade80;border:1px solid #1e4620}
.ah-ai-claude{background:#1a1020;color:#c084fc;border:1px solid #6b21a8}
.nb-wrap .stButton>button{
    height:56px !important;border-radius:12px !important;
    border:none !important;background:transparent !important;
    color:#9e9e9e !important;font-size:9px !important;font-weight:600 !important;
    padding:4px 2px !important;line-height:1.4 !important;
    box-shadow:none !important;width:100% !important;
    display:flex;flex-direction:column;align-items:center}
.nb-wrap .stButton>button:hover{background:#f5f5f5 !important;color:#191d3a !important}
.nb-wrap .stButton>button[kind="primary"]{
    background:#f0f0f0 !important;color:#191d3a !important;
    border-bottom:2.5px solid #191d3a !important}
.sec-lbl{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;
    color:#9e9e9e;margin:14px 0 8px;padding-bottom:6px;border-bottom:1.5px solid #ddd}
label[data-testid="stWidgetLabel"] p,label[data-testid="stWidgetLabel"]{
    font-size:12px !important;font-weight:600 !important;color:#191d3a !important;
    text-transform:none !important;letter-spacing:0 !important;margin-bottom:3px !important}
.stTextInput input,.stNumberInput input{
    border-radius:12px !important;border:1.5px solid #ddd !important;
    background:#fff !important;font-size:16px !important;color:#191d3a !important;
    padding:0 14px !important;height:52px !important;line-height:52px !important;
    box-sizing:border-box !important;width:100% !important;
    -webkit-appearance:none;appearance:none}
.stTextInput input:focus,.stNumberInput input:focus{
    border-color:#6398c8 !important;background:#fff !important;
    box-shadow:0 0 0 3px rgba(99,152,200,.18) !important;outline:none !important}
.stTextInput input::placeholder{font-size:14px !important;color:#bbb !important}
[data-testid="stSelectbox"]>div>div{
    border-radius:12px !important;border:1.5px solid #ddd !important;
    background:#fff !important;font-size:16px !important;color:#191d3a !important;
    height:52px !important;min-height:52px !important;
    display:flex !important;align-items:center !important;box-sizing:border-box !important}
[data-testid="stHorizontalBlock"]{
    gap:8px !important;align-items:flex-start !important;
    flex-wrap:nowrap !important;overflow:visible !important}
[data-testid="stHorizontalBlock"]>[data-testid="column"]{
    flex:1 1 0% !important;min-width:0 !important;
    max-width:none !important;overflow:visible !important;padding-bottom:4px !important}
.stButton>button{
    width:100% !important;border-radius:14px !important;
    height:52px !important;font-size:15px !important;
    font-weight:700 !important;border:none !important;
    min-height:44px !important;touch-action:manipulation}
.stButton>button[kind="primary"]{
    background:#1668e3 !important;color:#fff !important;box-shadow:none !important}
.stButton>button[kind="secondary"]{
    background:#fff !important;border:1.5px solid #ddd !important;color:#616161 !important}
.bb-wrap .stButton>button[kind="secondary"]{
    background:transparent !important;border:none !important;
    color:#9e9e9e !important;font-size:12px !important;
    font-weight:400 !important;height:36px !important;
    text-decoration:underline !important;text-underline-offset:3px !important}
[data-testid="stLinkButton"] a{
    background:#6398c8 !important;color:#fff !important;
    border-radius:14px !important;height:52px !important;
    font-size:14px !important;font-weight:700 !important;border:none !important;
    display:flex !important;align-items:center !important;
    justify-content:center !important;text-decoration:none !important}
.mode-toggle{display:grid;grid-template-columns:1fr 1fr;gap:0;
    background:#e4e4e4;border-radius:14px;padding:3px;margin-bottom:12px}
.mode-toggle .stButton>button{
    height:44px !important;border-radius:11px !important;
    font-size:13px !important;font-weight:600 !important;border:none !important;
    box-shadow:none !important;background:transparent !important;color:#9e9e9e !important}
.mode-toggle .stButton>button[kind="primary"]{
    background:#fff !important;color:#191d3a !important;
    box-shadow:0 1px 4px rgba(0,0,0,.12) !important}
.notice{border-radius:12px;padding:10px 13px;font-size:13px;line-height:1.5;
    display:flex;align-items:flex-start;gap:8px;margin-bottom:10px}
.nok{background:#f0fdf4;border:1px solid #86efac;color:#166534}
.nerr{background:#fff1f2;border:1px solid #fecdd3;color:#9f1239}
.ninfo{background:#e8f0fe;border:1px solid #6398c8;color:#1e3a6e}
.nwarn{background:#fffbeb;border:1px solid #fde68a;color:#92400e}
.nviolet{background:#faf5ff;border:1px solid #d8b4fe;color:#6b21a8}
.expedia-banner{background:#fff;border:1.5px solid #ddd;border-bottom:none;
    border-radius:16px 16px 0 0;padding:11px 14px;
    display:flex;align-items:center;justify-content:space-between;margin-top:14px}
.taap-pill{font-size:10px;font-weight:700;letter-spacing:.3px;
    color:#1e3a6e;background:#e8f0fe;border:1px solid #6398c8;
    padding:3px 10px;border-radius:20px;white-space:nowrap}
[data-testid="stFileUploader"] [data-testid="stWidgetLabel"],
[data-testid="stFileUploader"] [data-testid="stWidgetLabel"] *{display:none !important}
[data-testid="stFileUploader"]{margin-top:0 !important}
[data-testid="stFileUploader"]>div:first-child,[data-testid="stFileUploader"] section{
    border:1.5px dashed #b8cde0 !important;border-top:none !important;
    border-radius:0 0 16px 16px !important;background:#f5f8fc !important;
    margin-top:0 !important;padding:24px 16px !important;min-height:110px !important}
[data-testid="stFileUploader"] button{
    border-radius:10px !important;border:1.5px solid #ddd !important;
    background:#fff !important;color:#191d3a !important;
    font-size:14px !important;font-weight:600 !important;
    padding:10px 20px !important;height:auto !important;min-height:44px !important}
[data-testid="stFileUploaderDropInstructions"]{font-size:14px !important;font-weight:600 !important;color:#191d3a !important}
.stat-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:14px}
.stat-card{background:#fff;border:1.5px solid #ddd;border-radius:16px;padding:14px 13px}
.stat-val{font-size:20px;font-weight:800;color:#191d3a;line-height:1.1}
.stat-lbl{font-size:10px;color:#9e9e9e;margin-top:4px;font-weight:500}
.bulk-prog{background:#ddd;border-radius:99px;height:5px;overflow:hidden;margin-bottom:6px}
.bulk-prog-f{height:100%;background:#6398c8;border-radius:99px;transition:width .3s}
.bulk-prog-lbl{font-size:12px;color:#9e9e9e;text-align:center;margin-bottom:12px;font-weight:500}
.bulk-sum{background:#fff;border:1.5px solid #ddd;border-radius:16px;padding:16px 14px;margin-bottom:14px}
.bulk-sum-ttl{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:#9e9e9e;margin-bottom:12px}
.bulk-stats{display:grid;grid-template-columns:repeat(4,1fr);gap:6px;text-align:center;margin-bottom:12px}
.bs-val{font-size:22px;font-weight:800;color:#191d3a;line-height:1}
.bs-lbl{font-size:9px;color:#9e9e9e;margin-top:3px;font-weight:500}
.bs-g{color:#1e9e5a}.bs-r{color:#e53935}.bs-y{color:#e68900}
.bulk-bar{background:#e8e8e8;border-radius:99px;height:5px;overflow:hidden}
.bulk-bar-f{height:100%;background:#1e9e5a;border-radius:99px}
.bulk-pct{font-size:11px;color:#9e9e9e;text-align:right;margin-top:4px}
.file-item{background:#fff;border:1.5px solid #ddd;border-radius:14px;padding:12px 13px;margin-bottom:8px}
.fi-success{border-color:#6ee7b7 !important;background:#f0fdf4 !important}
.fi-error{border-color:#fca5a5 !important;background:#fff1f2 !important}
.fi-skipped{border-color:#fcd34d !important;background:#fffde7 !important}
.fi-top{display:flex;align-items:center;gap:9px}
.fi-icon{width:36px;height:36px;border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:16px;flex-shrink:0}
.ic-ok{background:#dcfce7}.ic-err{background:#ffe4e6}.ic-skip{background:#fef9c3}.ic-n{background:#ededed}
.fi-name{font-size:12px;font-weight:600;color:#191d3a;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.fi-badge{font-size:10px;font-weight:700;padding:3px 9px;border-radius:20px;white-space:nowrap}
.fb-ok{background:#dcfce7;color:#166534}.fb-err{background:#ffe4e6;color:#9f1239}.fb-sk{background:#fef9c3;color:#7a5c00}
.fi-grid{margin-top:9px;padding-top:8px;border-top:1px solid #ededed;display:grid;grid-template-columns:1fr 1fr;gap:5px 12px}
.fi-kv{display:flex;gap:4px;align-items:baseline}
.fi-k{font-size:9px;font-weight:700;color:#9e9e9e;min-width:48px;flex-shrink:0;text-transform:uppercase;letter-spacing:.3px}
.fi-v{font-size:12px;font-weight:500;color:#191d3a;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.st-row{display:flex;align-items:center;gap:10px;background:#fff;
    border:1.5px solid #ddd;border-radius:14px;padding:12px 13px;margin-bottom:8px}
.st-icon{width:36px;height:36px;border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:17px;flex-shrink:0}
.si-g{background:#f0fdf4}.si-r{background:#fff1f2}.si-b{background:#e8f0fe}.si-y{background:#fffde7}
.st-body{flex:1;min-width:0}
.st-title{font-size:13px;font-weight:700;color:#191d3a;line-height:1}
.st-sub{font-size:11px;color:#9e9e9e;margin-top:2px}
.st-badge{display:inline-flex;align-items:center;font-size:10px;font-weight:700;padding:3px 10px;border-radius:20px;flex-shrink:0}
.bg{background:#f0fdf4;color:#166534;border:1px solid #86efac}
.br{background:#fff1f2;color:#9f1239;border:1px solid #fecdd3}
.by{background:#fffde7;color:#7a5c00;border:1px solid #fcd34d}
.ai-card-btn-wrap .stButton>button{
    height:52px !important;border-radius:14px !important;font-size:14px !important;
    font-weight:500 !important;padding:0 14px !important;margin-bottom:8px !important}
.ai-card-btn-wrap .stButton>button[kind="secondary"]{
    background:#fff !important;border:1px solid #e0e0e0 !important;color:#191d3a !important;box-shadow:none !important}
.ai-card-btn-wrap .stButton>button[kind="primary"]{
    background:#f0fdf4 !important;border:1.5px solid #1D9E75 !important;color:#191d3a !important;box-shadow:none !important}
.ai-status-bar{display:flex;align-items:center;gap:8px;padding:9px 13px;
    border-radius:10px;background:#f0fdf4;border:1px solid #bbf7d0;margin-bottom:16px}
.ai-status-dot{width:6px;height:6px;border-radius:50%;background:#1D9E75;flex-shrink:0}
.ai-status-txt{font-size:12px;color:#166534}
.ai-key-row{display:flex;align-items:center;justify-content:space-between;
    padding:10px 13px;border-radius:10px;background:#fff;border:1px solid #e8e8e8;margin-bottom:6px}
.ai-key-left{display:flex;align-items:center;gap:8px}
.ai-key-dot{width:6px;height:6px;border-radius:50%;flex-shrink:0}
.ai-key-name{font-size:13px;color:#191d3a}
.ai-key-ok{font-size:11px;color:#1D9E75}
.ai-key-warn{font-size:11px;color:#e68900}
.about-box{background:#fff;border:1.5px solid #ddd;border-radius:16px;padding:14px 16px}
.about-ttl{font-size:14px;font-weight:800;color:#191d3a;margin-bottom:12px}
.about-r{display:flex;gap:8px;margin-bottom:6px}
.about-k{font-size:11px;font-weight:700;color:#191d3a;width:72px;flex-shrink:0}
.about-v{font-size:11px;color:#616161;line-height:1.5}
[data-testid="stDataFrame"]{border-radius:14px !important;border:1.5px solid #ddd !important;overflow:hidden !important;box-shadow:none !important}
[data-testid="stDataFrame"] th{background:#f5f8fc !important;color:#616161 !important;font-size:10px !important;font-weight:700 !important;text-transform:uppercase !important;letter-spacing:.4px !important;border-bottom:1.5px solid #ddd !important;padding:9px 11px !important}
[data-testid="stDataFrame"] td{font-size:12px !important;color:#191d3a !important;padding:9px 11px !important;border-bottom:1px solid #ededed !important}
[data-testid="stDataFrame"] tr:hover td{background:#f5f8fc !important}
.stSpinner>div{border-top-color:#6398c8 !important}
[data-testid="stDateInput"] input{
    font-size:15px !important;height:52px !important;
    border-radius:12px !important;border:1.5px solid #ddd !important;
    padding:0 14px !important;-webkit-appearance:none;appearance:none}
/* Speed badge */
.spd-badge{display:inline-flex;align-items:center;gap:5px;
    font-size:10px;font-weight:700;padding:3px 10px;border-radius:20px;
    background:#fef9c3;color:#7a5c00;border:1px solid #fcd34d;margin-left:6px}
@media(max-width:430px) and (orientation:portrait){
  .main .block-container{
    padding:6px 10px max(90px,calc(72px + env(safe-area-inset-bottom))) !important;
    max-width:100vw !important}
  [data-testid="stHorizontalBlock"]{flex-wrap:wrap !important;gap:6px !important}
  [data-testid="stHorizontalBlock"]>[data-testid="column"]{
    flex:1 1 100% !important;min-width:100% !important;max-width:100% !important}
}
@media screen and (orientation:landscape){
  [data-testid="stHorizontalBlock"]{flex-wrap:nowrap !important}
  .main .block-container{max-width:600px !important}
}
</style>
""",
    unsafe_allow_html=True,
)


# ═══════════════════════════════════════════════════════════════════════════════
#  AI PROVIDER
# ═══════════════════════════════════════════════════════════════════════════════
def get_ai_provider():
    return st.session_state.get("ai_provider", "claude")


def get_openai_key():
    try:
        k = st.secrets["openai"]["api_key"]
        if k and len(k) > 20 and "GANTI" not in k and "PASTE" not in k:
            return k
    except:
        pass
    return st.session_state.get("openai_key_manual", "")


def get_claude_key():
    try:
        k = st.secrets["anthropic"]["api_key"]
        if k and len(k) > 20 and "GANTI" not in k and "PASTE" not in k:
            return k
    except:
        pass
    return st.session_state.get("claude_key_manual", "")


def active_ai_ready():
    if get_ai_provider() == "openai":
        return bool(get_openai_key())
    return bool(get_claude_key())


# ═══════════════════════════════════════════════════════════════════════════════
#  GOOGLE SHEETS  — OPTIMIZED
#  Perubahan utama:
#    • get_all_values() ganti get_all_records()  → 3-5x lebih cepat
#    • @st.cache_resource TTL 3600s (1 jam) untuk koneksi
#    • @st.cache_data TTL 600s (10 menit) untuk DATA — bisa di-clear manual
#    • GAS endpoint opsional sebagai fallback super-cepat
# ═══════════════════════════════════════════════════════════════════════════════
def sheet_id():
    try:
        s = st.secrets["google_sheets"]["sheet_id"]
        if s and "GANTI" not in s:
            return s
    except:
        pass
    return st.session_state.get("sheet_id", "")


def gas_url():
    """Opsional: Google Apps Script Web App URL untuk load data super cepat."""
    try:
        u = st.secrets["google_sheets"].get("gas_url", "")
        if u and u.startswith("https://script.google.com"):
            return u
    except:
        pass
    return st.session_state.get("gas_url", "")


COLS = [
    "Timestamp Input", "Supplier", "Booking ID", "Booking Date", "Issued Date",
    "Hotel", "Check-in", "Room x Night", "Total (Rp)", "Check-out", "Guest Name",
    "Kartu Kredit", "Issuer", "PIC", "No. BC", "Nama Kegiatan", "Catatan",
]


# ── Koneksi sheet — cache 1 jam ────────────────────────────────────────────────
@st.cache_resource(ttl=3600)
def ws():
    creds = Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    s = gspread.authorize(creds).open_by_key(sheet_id()).sheet1
    try:
        if not s.row_values(1) or s.cell(1, 1).value != COLS[0]:
            s.insert_row(COLS, 1)
    except:
        s.insert_row(COLS, 1)
    return s


# ── OPTIMIZED: load data — cache_data 10 menit, bisa di-clear manual ──────────
@st.cache_data(ttl=600, show_spinner=False)
def load_rows_cached() -> list[dict]:
    """
    OPTIMIZATION 1: get_all_values() jauh lebih cepat dari get_all_records().
    get_all_records() parsing tipe data per sel → lambat.
    get_all_values() return string mentah → 3-5x lebih cepat.

    OPTIMIZATION 2: Coba GAS endpoint dulu (jika dikonfigurasi) → paling cepat.
    GAS berjalan di server Google, tidak perlu OAuth per request.
    """
    _gas = gas_url()
    if _gas:
        try:
            r = requests.get(_gas, timeout=8)
            if r.status_code == 200:
                return r.json()
        except:
            pass  # fallback ke gspread

    # Fallback: gspread dengan get_all_values() (bukan get_all_records())
    sheet = ws()
    raw = sheet.get_all_values()          # ← KUNCI: jauh lebih cepat
    if not raw or len(raw) < 2:
        return []
    headers = raw[0]
    return [dict(zip(headers, row)) for row in raw[1:]]


def load_rows() -> list[dict]:
    """Wrapper — dipanggil dari seluruh app."""
    return load_rows_cached()


def save_row(d: dict):
    """Simpan baris baru, lalu invalidate cache data."""
    ws().append_row(
        [d.get(k, "") for k in [
            "timestamp_input", "supplier", "booking_id", "booked_on", "issued_on",
            "hotel", "checkin", "qty", "room", "checkout", "name", "card",
            "issuer", "pic", "no_bc", "nama_kegiatan", "notes",
        ]],
        value_input_option="USER_ENTERED",
    )
    # PENTING: clear cache data setelah tulis, agar dashboard terbaru
    load_rows_cached.clear()
    # Reset DuckDB supaya sinkron
    if _DUCK_OK:
        _invalidate_duckdb()


# ═══════════════════════════════════════════════════════════════════════════════
#  DUCKDB — IN-MEMORY ENGINE untuk filter & agregasi
#  Manfaat: query SQL columnar jauh lebih cepat dari pandas untuk data besar
# ═══════════════════════════════════════════════════════════════════════════════
@st.cache_resource
def _get_duckdb_conn():
    """DuckDB in-memory connection — di-cache sepanjang sesi Streamlit."""
    if not _DUCK_OK:
        return None
    return duckdb.connect(database=":memory:", read_only=False)


def _invalidate_duckdb():
    """Hapus tabel hotel di DuckDB agar di-rebuild dari data terbaru."""
    conn = _get_duckdb_conn()
    if conn:
        try:
            conn.execute("DROP TABLE IF EXISTS hotel")
        except:
            pass
    # Hapus flag agar di-rebuild saat berikutnya diakses
    st.session_state.pop("_duck_loaded_hash", None)


def _ensure_duckdb(rows: list[dict]):
    """
    Pastikan DuckDB memiliki tabel hotel yang up-to-date.
    Gunakan hash jumlah baris sebagai fingerprint sederhana.
    """
    if not _DUCK_OK or not rows:
        return
    conn = _get_duckdb_conn()
    if conn is None:
        return

    import pandas as pd

    _hash = hashlib.md5(f"{len(rows)}:{rows[-1].get('Timestamp Input','')}".encode()).hexdigest()
    if st.session_state.get("_duck_loaded_hash") == _hash:
        return  # sudah sinkron, skip rebuild

    df = pd.DataFrame(rows)
    # Konversi kolom numerik
    if "Total (Rp)" in df.columns:
        df["Total (Rp)"] = pd.to_numeric(df["Total (Rp)"], errors="coerce").fillna(0).astype(int)
    # Konversi tanggal untuk filter range
    for col in ["Check-in", "Booking Date", "Issued Date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    conn.execute("DROP TABLE IF EXISTS hotel")
    conn.register("hotel_view", df)
    conn.execute("CREATE TABLE hotel AS SELECT * FROM hotel_view")
    conn.unregister("hotel_view")
    st.session_state["_duck_loaded_hash"] = _hash


def query_duckdb(
    rows: list[dict],
    date_col: str = None,
    date_from=None,
    date_to=None,
    search: str = None,
) -> "pd.DataFrame":
    """
    Filter & return DataFrame via DuckDB SQL.
    Jika DuckDB tidak tersedia, fallback ke pandas biasa.
    """
    import pandas as pd

    _ensure_duckdb(rows)
    conn = _get_duckdb_conn()

    if not _DUCK_OK or conn is None:
        # Fallback pandas
        df = pd.DataFrame(rows)
        if "Total (Rp)" in df.columns:
            df["Total (Rp)"] = pd.to_numeric(df["Total (Rp)"], errors="coerce").fillna(0).astype(int)
        return df

    conditions = ["1=1"]
    params = []

    if date_col and date_from and date_to:
        safe_col = date_col.replace('"', '')
        conditions.append(f'TRY_CAST("{safe_col}" AS DATE) BETWEEN ? AND ?')
        params.extend([str(date_from), str(date_to)])

    if search and search.strip():
        s = f"%{search.strip()}%"
        conditions.append(
            '("Hotel" ILIKE ? OR "Guest Name" ILIKE ? OR "Booking ID" ILIKE ?)'
        )
        params.extend([s, s, s])

    where = " AND ".join(conditions)
    try:
        df = conn.execute(
            f'SELECT * FROM hotel WHERE {where}', params
        ).df()
    except Exception:
        df = pd.DataFrame(rows)
    return df


def agg_duckdb(metric_col: str, group_col: str) -> "pd.DataFrame":
    """Agregasi cepat via DuckDB."""
    import pandas as pd

    conn = _get_duckdb_conn()
    if not _DUCK_OK or conn is None:
        return pd.DataFrame()
    safe_m = metric_col.replace('"', '')
    safe_g = group_col.replace('"', '')
    try:
        return conn.execute(f"""
            SELECT "{safe_g}", SUM("{safe_m}") AS total, COUNT(*) AS cnt
            FROM hotel
            WHERE "{safe_g}" IS NOT NULL
              AND CAST("{safe_g}" AS VARCHAR) NOT IN ('', 'nan', 'None', 'NaN')
            GROUP BY "{safe_g}"
            ORDER BY total DESC
        """).df()
    except Exception:
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════════
#  DUPLICATE CHECK
# ═══════════════════════════════════════════════════════════════════════════════
def _ns(v):
    return str(v or "").strip().lower()


def _ni(v):
    try:
        return int(float(str(v).replace(",", "").replace(".", "") or 0))
    except:
        return 0


def check_duplicate(new, rows):
    bid = _ns(new.get("booking_id"))
    for r in rows:
        if bid and bid == _ns(r.get("Booking ID")):
            return True, "Booking ID sudah terdaftar", r
        sc = sum([
            _ns(new.get("hotel")) == _ns(r.get("Hotel")),
            _ns(new.get("checkin")) == _ns(r.get("Check-in")),
            _ns(new.get("name")) == _ns(r.get("Guest Name")),
            _ni(new.get("room")) == _ni(r.get("Total (Rp)")),
        ])
        if sc >= 3:
            return True, "Kemungkinan duplikat (kesamaan tinggi)", r
    return False, "", None


# ═══════════════════════════════════════════════════════════════════════════════
#  PDF HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def pdf_images(data):
    if not _PDF_OK:
        raise RuntimeError("pypdfium2 not installed")
    doc = _pdfium.PdfDocument(data)
    return [doc[i].render(scale=2.0).to_pil() for i in range(len(doc))]


def pdf_text(data):
    if not _PDF_OK or not data:
        return ""
    try:
        doc, parts = _pdfium.PdfDocument(data), []
        for i in range(len(doc)):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                parts.append(doc[i].get_textpage().get_text_bounded())
        return "\n".join(parts).strip()
    except:
        return ""


def to_b64(img):
    buf = io.BytesIO()
    img.save(buf, "JPEG", quality=92)
    return base64.b64encode(buf.getvalue()).decode(), "image/jpeg"


# ═══════════════════════════════════════════════════════════════════════════════
#  AI PROMPTS & PARSERS
# ═══════════════════════════════════════════════════════════════════════════════
_SYS = """You are a corporate hotel expense AI parser for credit card reporting.
Parse any document: Expedia TAAP receipt, Mitra Tours itinerary, hotel invoice.
Return ONLY a valid JSON object — no markdown, no explanation.
Keys: supplier, booking_id, booked_on (YYYY-MM-DD), issued_on (YYYY-MM-DD),
hotel, checkin (YYYY-MM-DD), checkout (YYYY-MM-DD), qty (e.g. "1 room x 2 nights"),
room (integer total IDR, strip Rp/commas), name (primary guest),
card (e.g. "Visa •••• 0191"), notes (room type, tax, etc.)
Rules: 1.Dates->YYYY-MM-DD. 2.Amounts->plain integer. 3.Missing->"" or 0."""

_SYS_NONEXP = """You are a payment receipt parser. Extract ONLY these 4 fields.
Return ONLY a valid JSON object — no markdown, no explanation.
Keys:
- timestamp_input : string — Date/Time exactly as shown (e.g. "15/05/2026 16:18:34")
- booking_id      : string — Invoice Number / Reference Number / Transaction ID
- room            : integer — Amount charged, strip IDR/Rp/,/. -> plain integer only
- card            : string — Card Number as shown (e.g. "521558******4467")
Missing -> "" for strings, 0 for integers."""


def _call_openai(content, sys_prompt, max_tokens=800):
    import openai, httpx

    key = get_openai_key()
    if not key:
        raise ValueError("OpenAI API key belum diisi.")
    resp = openai.OpenAI(api_key=key, http_client=httpx.Client()).chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "system", "content": sys_prompt}, {"role": "user", "content": content}],
        temperature=0.0,
        max_tokens=max_tokens,
    )
    raw = resp.choices[0].message.content
    m = re.search(r"\{[\s\S]*\}", raw)
    if not m:
        raise ValueError("Format AI tidak valid.")
    return json.loads(m.group()), raw


def _call_claude(content, sys_prompt, max_tokens=800):
    import anthropic

    key = get_claude_key()
    if not key:
        raise ValueError("Anthropic API key belum diisi.")
    resp = anthropic.Anthropic(api_key=key).messages.create(
        model="claude-sonnet-4-5",
        max_tokens=max_tokens,
        system=sys_prompt,
        messages=[{"role": "user", "content": content}],
    )
    raw = resp.content[0].text
    m = re.search(r"\{[\s\S]*\}", raw)
    if not m:
        raise ValueError("Format AI tidak valid.")
    return json.loads(m.group()), raw


def _build_expedia(text, images):
    if get_ai_provider() == "claude":
        c = []
        if images:
            for b64, mime in images:
                c.append({"type": "image", "source": {"type": "base64", "media_type": mime, "data": b64}})
        c.append({"type": "text", "text": text or "Extract all structured data."})
        return c
    else:
        c = []
        if images:
            for b64, mime in images:
                c.append({"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}", "detail": "high"}})
        c.append({"type": "text", "text": text or "Extract all structured data."})
        return c


def _build_receipt(images):
    if get_ai_provider() == "claude":
        c = []
        for b64, mime in images:
            c.append({"type": "image", "source": {"type": "base64", "media_type": mime, "data": b64}})
        c.append({"type": "text", "text": "Extract the 4 fields from this payment receipt."})
        return c
    else:
        c = []
        for b64, mime in images:
            c.append({"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}", "detail": "high"}})
        c.append({"type": "text", "text": "Extract the 4 fields from this payment receipt."})
        return c


def ai_parse(text="", images=None):
    c = _build_expedia(text, images)
    if get_ai_provider() == "claude":
        return _call_claude(c, _SYS)
    return _call_openai(c, _SYS)


def ai_parse_receipt(images):
    c = _build_receipt(images)
    if get_ai_provider() == "claude":
        return _call_claude(c, _SYS_NONEXP, max_tokens=400)
    return _call_openai(c, _SYS_NONEXP, max_tokens=400)


# ═══════════════════════════════════════════════════════════════════════════════
#  UI UTILITIES
# ═══════════════════════════════════════════════════════════════════════════════
def fmt(v):
    try:
        return "Rp {:,}".format(int(float(v or 0))).replace(",", ".")
    except:
        return str(v) if v else "—"


def now_ts():
    return datetime.now().strftime("%d/%m/%Y %H:%M")


def notice(kind, msg):
    icons = {"ok": "✓", "err": "✕", "info": "ℹ", "warn": "⚠", "violet": "✦"}
    cls = {"ok": "nok", "err": "nerr", "info": "ninfo", "warn": "nwarn", "violet": "nviolet"}
    st.markdown(
        f'<div class="notice {cls.get(kind,"ninfo")}"><b>{icons.get(kind,"ℹ")}</b>&ensp;{msg}</div>',
        unsafe_allow_html=True,
    )


# ── Card normalizer ───────────────────────────────────────────────────────────
_BIN_MAP = {"521558": ("MasterCard", "4467"), "489594": ("Visa", "0191")}
_DISPLAY_MAP = {
    "mastercard \u2022\u2022\u2022\u2022 4467": "MasterCard \u2022\u2022\u2022\u2022 4467",
    "visa \u2022\u2022\u2022\u2022 0191": "Visa \u2022\u2022\u2022\u2022 0191",
}


def normalize_card(raw: str) -> str:
    if not raw:
        return ""
    v = str(raw).strip()
    _lower = re.sub(r"\s+", " ", v.lower())
    if _lower in _DISPLAY_MAP:
        return _DISPLAY_MAP[_lower]
    digits = re.sub(r"[^\d]", "", v)
    if len(digits) >= 6:
        bin6 = digits[:6]
        if bin6 in _BIN_MAP:
            brand, last4 = _BIN_MAP[bin6]
            return f"{brand} \u2022\u2022\u2022\u2022 {last4}"
    return v


# ═══════════════════════════════════════════════════════════════════════════════
#  SESSION STATE DEFAULTS
# ═══════════════════════════════════════════════════════════════════════════════
_DEF = {
    "tab": "input", "input_mode": "expedia", "bulk_results": [], "bulk_saved_count": 0,
    "openai_key_manual": "", "claude_key_manual": "", "ai_provider": "claude",
    "sheet_id": "", "gas_url": "",
    "last_issuer": "", "last_pic": "", "last_no_bc": "", "last_nama_kegiatan": "",
    "_ne_last_file_key": "", "_ne_prefill_ts": "", "_ne_prefill_bid": "",
    "_ne_prefill_room": "", "_ne_prefill_card": "", "_ne_parse_ok": False, "_ne_parse_err": "",
}
for _k, _v in _DEF.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ═══════════════════════════════════════════════════════════════════════════════
#  HEADER
# ═══════════════════════════════════════════════════════════════════════════════
_prov = get_ai_provider()
_prov_lbl = "GPT-4o mini" if _prov == "openai" else "Claude Sonnet"
_prov_cls = "ah-ai-openai" if _prov == "openai" else "ah-ai-claude"

st.markdown(
    f"""
<div class="app-header">
  <div class="ah-icon" style="font-size:22px;font-weight:800;color:#191d3a;">M</div>
  <div>
    <div class="ah-title">CC Reporting</div>
    <div class="ah-sub">Mitra Tours &amp; Travel · v7 Fast</div>
  </div>
  <span class="ah-ai-badge {_prov_cls}">{'🤖' if _prov=='openai' else '🟣'} {_prov_lbl}</span>
  <div class="ah-live">LIVE</div>
</div>
""",
    unsafe_allow_html=True,
)

# ── Bottom nav ────────────────────────────────────────────────────────────────
_cur = st.session_state["tab"]
_tab_icons = {"input": "📥", "dashboard": "📊", "log": "🕐", "settings": "⚙️"}
_tab_labels = {"input": "Input", "dashboard": "Dashboard", "log": "Activity", "settings": "Settings"}
_tab_keys = ["input", "dashboard", "log", "settings"]

st.markdown('<div class="nb-wrap">', unsafe_allow_html=True)
_ncols = st.columns(4)
for i, _tk in enumerate(_tab_keys):
    with _ncols[i]:
        if st.button(
            f"{_tab_icons[_tk]}\n{_tab_labels[_tk]}",
            key=f"nb_{_tk}",
            use_container_width=True,
            type="primary" if _cur == _tk else "secondary",
        ):
            st.session_state["tab"] = _tk
            st.rerun()
st.markdown("</div>", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  TAB — INPUT
# ═══════════════════════════════════════════════════════════════════════════════
if st.session_state["tab"] == "input":

    if not active_ai_ready():
        _nm = "OpenAI" if get_ai_provider() == "openai" else "Anthropic"
        notice("err", f"{_nm} API key belum diisi — buka <b>Settings</b>.")
        st.stop()
    if not _PDF_OK:
        notice("warn", "pypdfium2 belum terinstall — PDF nonaktif.")

    _cur_mode = st.session_state["input_mode"]
    st.markdown('<div class="mode-toggle">', unsafe_allow_html=True)
    _ma, _mb = st.columns(2)
    with _ma:
        if st.button(
            "✈  Expedia / TAAP", key="mode_expedia", use_container_width=True,
            type="primary" if _cur_mode == "expedia" else "secondary",
        ):
            st.session_state["input_mode"] = "expedia"
            st.session_state["bulk_results"] = []
            st.rerun()
    with _mb:
        if st.button(
            "🧾  Non-Expedia", key="mode_nonexp", use_container_width=True,
            type="primary" if _cur_mode == "nonexpedia" else "secondary",
        ):
            st.session_state["input_mode"] = "nonexpedia"
            st.session_state["bulk_results"] = []
            for _k in ["_ne_last_file_key", "_ne_prefill_ts", "_ne_prefill_bid",
                       "_ne_prefill_room", "_ne_prefill_card", "_ne_parse_ok", "_ne_parse_err"]:
                st.session_state[_k] = _DEF.get(_k, "")
            st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="sec-lbl">Issuer &amp; PIC</div>', unsafe_allow_html=True)
    _ISSUERS = [
        "", "Ade Puspitasari", "Farras Mahmud", "Meijika",
        "Muhammad Geraldi Jagaddhita", "Nur Anissa Firda Aulia", "Riega Wisudhantara",
        "Rifyal Tumber", "Selvy Anggraini", "Shaiful Baldy", "Veronica Novi Heri",
        "Rida Manora Nasution",
    ]
    _li = st.session_state.get("last_issuer", "")
    _bi = _ISSUERS.index(_li) if _li in _ISSUERS else 0
    _ca, _cb = st.columns(2)
    bulk_issuer = _ca.selectbox(
        "Issuer *", options=_ISSUERS, index=_bi,
        format_func=lambda x: "— Pilih —" if x == "" else x, key="bulk_issuer",
    )
    bulk_pic = _cb.text_input(
        "PIC *", value=st.session_state.get("last_pic", ""),
        placeholder="Nama PIC", key="bulk_pic",
    )
    _cc, _cd = st.columns(2)
    bulk_no_bc = _cc.text_input(
        "No. BC", value=st.session_state.get("last_no_bc", ""),
        placeholder="Nomor BC", key="bulk_no_bc",
    )
    bulk_nama_kegiatan = _cd.text_input(
        "Nama Kegiatan", value=st.session_state.get("last_nama_kegiatan", ""),
        placeholder="Kegiatan", key="bulk_nama_kegiatan",
    )

    _ap = get_ai_provider()
    if _ap == "claude":
        notice("violet", "AI: <b>Claude</b> (Anthropic) &nbsp;·&nbsp; Ganti di Settings")
    else:
        notice("info", "AI: <b>OpenAI</b> &nbsp;·&nbsp; Ganti di Settings")

    # ── MODE A: EXPEDIA ───────────────────────────────────────────────────────
    if _cur_mode == "expedia":
        st.markdown(
            """
<div class="expedia-banner">
  <div style="font-size:15px;font-weight:800;color:#003580;letter-spacing:-.5px;">expedia</div>
  <span class="taap-pill">TAAP + Mitra Tours</span>
</div>""",
            unsafe_allow_html=True,
        )
        _ftypes = ["jpg", "jpeg", "png", "webp"] + (["pdf"] if _PDF_OK else [])
        bulk_files = st.file_uploader(
            label="", type=_ftypes, accept_multiple_files=True,
            label_visibility="collapsed", key="bulk_uf",
        )
        _n = len(bulk_files) if bulk_files else 0
        if _n:
            notice("info", f"<b>{_n} file</b> dipilih dan siap diproses.")
        skip_dup = st.checkbox("Lewati duplikat", value=True, key="bulk_skip_dup")
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        st.markdown('<div class="bb-wrap">', unsafe_allow_html=True)
        _run = st.button(
            "Submit", type="primary", use_container_width=True,
            disabled=(not _n or not bulk_issuer or not bulk_pic.strip()),
            key="bulk_run",
        )
        _clear = st.button("Delete", type="secondary", use_container_width=True, key="bulk_clear")
        st.markdown("</div>", unsafe_allow_html=True)

        if _clear:
            st.session_state["bulk_results"] = []
            st.session_state["bulk_saved_count"] = 0
            st.rerun()

        if _run:
            st.session_state.update(
                last_issuer=bulk_issuer, last_pic=bulk_pic,
                last_no_bc=bulk_no_bc, last_nama_kegiatan=bulk_nama_kegiatan,
                bulk_results=[], bulk_saved_count=0,
            )
            try:
                _existing = load_rows()
            except:
                _existing = []
            _all_res, _saved_run = [], 0
            _slot = st.empty()
            for _idx, _uf in enumerate(bulk_files):
                _pct = int(_idx / _n * 100)
                _slot.markdown(
                    '<div class="bulk-prog"><div class="bulk-prog-f" style="width:'
                    + str(_pct) + '%"></div></div>'
                    + '<div class="bulk-prog-lbl">' + str(_idx + 1) + "/" + str(_n)
                    + " · " + _uf.name + "</div>",
                    unsafe_allow_html=True,
                )
                _res = {"file": _uf.name, "status": "error", "parsed": {}, "err": "", "mode": "expedia"}
                try:
                    _raw = _uf.read()
                    _imgs, _txt = [], ""
                    if _uf.name.lower().endswith(".pdf"):
                        if not _PDF_OK:
                            raise RuntimeError("pypdfium2 tidak terinstall")
                        _pages = pdf_images(_raw)
                        _imgs = [to_b64(pg) for pg in _pages]
                        _txt = pdf_text(_raw)
                    else:
                        _io = Image.open(io.BytesIO(_raw)).convert("RGB")
                        _b, _m = to_b64(_io)
                        _imgs = [(_b, _m)]
                    _comb = ("EXTRACTED PDF TEXT (authoritative):\n" + _txt) if _txt else ""
                    _parsed, _ = ai_parse(_comb, _imgs or None)
                    _parsed["timestamp_input"] = now_ts()
                    _is_dup, _why, _ = check_duplicate(
                        {"booking_id": _parsed.get("booking_id"),
                         "hotel": _parsed.get("hotel"), "checkin": _parsed.get("checkin"),
                         "name": _parsed.get("name"), "room": _parsed.get("room")},
                        _existing,
                    )
                    if _is_dup and skip_dup:
                        _res.update(status="skipped", parsed=_parsed, err=_why)
                    else:
                        save_row({
                            "timestamp_input": _parsed.get("timestamp_input", ""),
                            "supplier": _parsed.get("supplier", ""),
                            "booking_id": _parsed.get("booking_id", ""),
                            "booked_on": _parsed.get("booked_on", ""),
                            "issued_on": _parsed.get("issued_on", ""),
                            "hotel": _parsed.get("hotel", ""),
                            "checkin": _parsed.get("checkin", ""),
                            "qty": _parsed.get("qty", ""),
                            "room": _parsed.get("room", 0),
                            "checkout": _parsed.get("checkout", ""),
                            "name": _parsed.get("name", ""),
                            "card": normalize_card(_parsed.get("card", "")),
                            "issuer": bulk_issuer, "pic": bulk_pic,
                            "no_bc": bulk_no_bc.strip(),
                            "nama_kegiatan": bulk_nama_kegiatan.strip(),
                            "notes": _parsed.get("notes", ""),
                        })
                        _res.update(status="success", parsed=_parsed)
                        _saved_run += 1
                        _existing.append({
                            "Booking ID": _parsed.get("booking_id", ""),
                            "Hotel": _parsed.get("hotel", ""),
                            "Check-in": _parsed.get("checkin", ""),
                            "Guest Name": _parsed.get("name", ""),
                            "Total (Rp)": _parsed.get("room", 0),
                        })
                except Exception as _exc:
                    _res.update(err=str(_exc)[:140])
                _all_res.append(_res)
            _slot.empty()
            st.session_state["bulk_results"] = _all_res
            st.session_state["bulk_saved_count"] = _saved_run
            st.rerun()

    # ── MODE B: NON-EXPEDIA ───────────────────────────────────────────────────
    else:
        st.markdown(
            """
<div style="background:#fff;border:1.5px solid #ddd;border-bottom:none;
    border-radius:16px 16px 0 0;padding:11px 14px;
    display:flex;align-items:center;justify-content:space-between;margin-top:14px">
  <div style="display:flex;align-items:center;gap:8px">
    <span style="font-size:18px">🧾</span>
    <div>
      <div style="font-size:13px;font-weight:700;color:#191d3a">Non-Expedia — Payment Receipt</div>
      <div style="font-size:10px;color:#9e9e9e">AI baca 4 field · sisanya isian manual</div>
    </div>
  </div>
  <span style="font-size:9px;font-weight:700;color:#7a5c00;background:#fef9c3;
    border:1px solid #fcd34d;padding:3px 9px;border-radius:20px">Manual + AI</span>
</div>""",
            unsafe_allow_html=True,
        )

        ne_files = st.file_uploader(
            label="", type=["jpg", "jpeg", "png", "webp"],
            accept_multiple_files=False, label_visibility="collapsed", key="ne_uf",
        )

        if ne_files:
            _cur_file_key = ne_files.name + str(ne_files.size)
            if _cur_file_key != st.session_state.get("_ne_last_file_key", ""):
                with st.spinner("🤖 AI membaca receipt…"):
                    try:
                        _raw_pre = ne_files.read()
                        _io_pre = Image.open(io.BytesIO(_raw_pre)).convert("RGB")
                        _b_pre, _m_pre = to_b64(_io_pre)
                        _pre_fields, _ = ai_parse_receipt([(_b_pre, _m_pre)])
                        st.session_state["_ne_prefill_ts"] = str(_pre_fields.get("timestamp_input", "")).strip()
                        st.session_state["_ne_prefill_bid"] = str(_pre_fields.get("booking_id", "")).strip()
                        st.session_state["_ne_prefill_card"] = normalize_card(
                            str(_pre_fields.get("card", "")).strip()
                        )
                        try:
                            _r = int(float(str(_pre_fields.get("room", 0)).replace(",", "").replace(".", "") or 0))
                        except:
                            _r = 0
                        st.session_state["_ne_prefill_room"] = str(_r) if _r else ""
                        st.session_state["_ne_last_file_key"] = _cur_file_key
                        st.session_state["_ne_parse_ok"] = True
                        st.session_state["_ne_parse_err"] = ""
                    except Exception as _pre_exc:
                        st.session_state["_ne_parse_ok"] = False
                        st.session_state["_ne_parse_err"] = str(_pre_exc)[:160]
                        st.session_state["_ne_last_file_key"] = _cur_file_key
                st.rerun()

            if st.session_state.get("_ne_parse_ok"):
                notice("ok", "✓ AI berhasil membaca receipt · <b>Timestamp · Invoice · Amount · Card</b> terisi otomatis")
            elif st.session_state.get("_ne_parse_err"):
                notice("warn", "AI gagal membaca · isi manual.")
        else:
            if st.session_state.get("_ne_last_file_key", ""):
                for _k in ["_ne_last_file_key", "_ne_prefill_ts", "_ne_prefill_bid",
                           "_ne_prefill_room", "_ne_prefill_card", "_ne_parse_ok", "_ne_parse_err"]:
                    st.session_state[_k] = _DEF.get(_k, "")

        st.markdown(
            """
<div style="background:#f0fdf4;border:1px solid #86efac;border-radius:12px;
    padding:9px 12px;font-size:11px;color:#166534;margin:8px 0 4px;line-height:1.8">
  <b>✓ Otomatis dari AI:</b>
  📅 Timestamp &nbsp;·&nbsp; 💰 Total (Rp) &nbsp;·&nbsp; 💳 Kartu Kredit &nbsp;·&nbsp; 📄 Booking ID
</div>""",
            unsafe_allow_html=True,
        )

        st.markdown('<div class="sec-lbl">Data Booking — Isian Manual</div>', unsafe_allow_html=True)
        _SUPPLIERS = ["Direct To Hotel", "Direct To Supplier"]
        _n1, _n2 = st.columns(2)
        ne_supplier = _n1.selectbox("Supplier *", options=_SUPPLIERS, index=0, key="ne_supplier")
        ne_hotel = _n2.text_input("Hotel *", placeholder="Nama hotel", key="ne_hotel")
        _n3, _n4 = st.columns(2)
        ne_name = _n3.text_input("Guest Name *", placeholder="Nama tamu", key="ne_name")
        ne_booking_id = _n4.text_input(
            "Booking ID",
            value=st.session_state.get("_ne_prefill_bid", ""),
            placeholder="Dari receipt / manual",
            key="ne_booking_id",
        )

        def _fmt_date(d):
            try:
                return d.strftime("%Y-%m-%d") if d else ""
            except:
                return ""

        _n5, _n6 = st.columns(2)
        ne_checkin_d = _n5.date_input("Check-in", value=None, format="DD/MM/YYYY", key="ne_checkin")
        ne_checkout_d = _n6.date_input("Check-out", value=None, format="DD/MM/YYYY", key="ne_checkout")
        ne_checkin = _fmt_date(ne_checkin_d)
        ne_checkout = _fmt_date(ne_checkout_d)
        _n7, _n8 = st.columns(2)
        ne_qty = _n7.text_input("Room × Night", placeholder="1 room x 2 nights", key="ne_qty")
        ne_booked_on_d = _n8.date_input("Booking Date", value=None, format="DD/MM/YYYY", key="ne_booked_on")
        ne_booked_on = _fmt_date(ne_booked_on_d)
        _n9, _n10 = st.columns(2)
        ne_issued_on_d = _n9.date_input("Issued Date", value=None, format="DD/MM/YYYY", key="ne_issued_on")
        ne_issued_on = _fmt_date(ne_issued_on_d)
        ne_extra_notes = _n10.text_input("Catatan", placeholder="Opsional", key="ne_extra_notes")

        _ne_ready = (
            bool(ne_files) and bool(bulk_issuer) and bool(bulk_pic.strip())
            and bool(ne_supplier) and bool(ne_hotel.strip()) and bool(ne_name.strip())
        )

        st.markdown('<div class="bb-wrap">', unsafe_allow_html=True)
        _ne_run = st.button(
            "Submit", type="primary", use_container_width=True,
            disabled=not _ne_ready, key="ne_run",
        )
        _ne_clear = st.button("Delete", type="secondary", use_container_width=True, key="ne_clear")
        st.markdown("</div>", unsafe_allow_html=True)

        if not _ne_ready:
            _missing = []
            if not ne_files:         _missing.append("upload receipt")
            if not bulk_issuer:      _missing.append("Issuer")
            if not bulk_pic.strip(): _missing.append("PIC")
            if not ne_hotel.strip(): _missing.append("Hotel")
            if not ne_name.strip():  _missing.append("Guest Name")
            if _missing:
                st.markdown(
                    '<div style="font-size:11px;color:#9e9e9e;text-align:center;margin-top:4px">Lengkapi: '
                    + " · ".join(_missing) + "</div>",
                    unsafe_allow_html=True,
                )

        if _ne_clear:
            st.session_state["bulk_results"] = []
            st.session_state["bulk_saved_count"] = 0
            for _k in ["_ne_last_file_key", "_ne_prefill_ts", "_ne_prefill_bid",
                       "_ne_prefill_room", "_ne_prefill_card", "_ne_parse_ok", "_ne_parse_err"]:
                st.session_state[_k] = _DEF.get(_k, "")
            st.rerun()

        if _ne_run and _ne_ready:
            st.session_state.update(
                last_issuer=bulk_issuer, last_pic=bulk_pic,
                last_no_bc=bulk_no_bc, last_nama_kegiatan=bulk_nama_kegiatan,
                bulk_results=[], bulk_saved_count=0,
            )
            _ne_res = {"file": ne_files.name, "status": "error", "parsed": {}, "err": "", "mode": "nonexpedia"}
            try:
                _ts_final = st.session_state.get("_ne_prefill_ts", "").strip() or now_ts()
                _inv_ai = st.session_state.get("_ne_prefill_bid", "").strip()
                _card_ai = normalize_card(st.session_state.get("_ne_prefill_card", "").strip())
                try:
                    _total = int(st.session_state.get("_ne_prefill_room", "0") or 0)
                except:
                    _total = 0
                _booking_id_final = ne_booking_id.strip() or _inv_ai
                _catatan = _booking_id_final
                if ne_extra_notes.strip():
                    _catatan += " · " + ne_extra_notes.strip()
                _parsed_ne = {
                    "timestamp_input": _ts_final, "supplier": ne_supplier,
                    "booking_id": _booking_id_final, "booked_on": ne_booked_on,
                    "issued_on": ne_issued_on, "hotel": ne_hotel.strip(),
                    "checkin": ne_checkin, "qty": ne_qty.strip(), "room": _total,
                    "checkout": ne_checkout, "name": ne_name.strip(), "card": _card_ai,
                    "issuer": bulk_issuer, "pic": bulk_pic.strip(),
                    "no_bc": bulk_no_bc.strip(), "nama_kegiatan": bulk_nama_kegiatan.strip(),
                    "notes": _catatan,
                }
                save_row(_parsed_ne)
                _ne_res.update(status="success", parsed=_parsed_ne)
                st.session_state["bulk_saved_count"] = 1
            except Exception as _exc_ne:
                _ne_res.update(err=str(_exc_ne)[:200])
            st.session_state["bulk_results"] = [_ne_res]
            st.rerun()

    # ── Results ───────────────────────────────────────────────────────────────
    _results = st.session_state.get("bulk_results", [])
    if _results:
        _ok  = sum(1 for r in _results if r["status"] == "success")
        _err = sum(1 for r in _results if r["status"] == "error")
        _skip = sum(1 for r in _results if r["status"] == "skipped")
        _tot = len(_results)
        _pct = int(_ok / _tot * 100) if _tot else 0
        st.markdown(
            '<div class="bulk-sum"><div class="bulk-sum-ttl">Hasil Proses</div>'
            + '<div class="bulk-stats">'
            + f'<div><div class="bs-val">{_tot}</div><div class="bs-lbl">Total</div></div>'
            + f'<div><div class="bs-val bs-g">{_ok}</div><div class="bs-lbl">Tersimpan</div></div>'
            + f'<div><div class="bs-val bs-r">{_err}</div><div class="bs-lbl">Gagal</div></div>'
            + f'<div><div class="bs-val bs-y">{_skip}</div><div class="bs-lbl">Duplikat</div></div>'
            + "</div>"
            + f'<div class="bulk-bar"><div class="bulk-bar-f" style="width:{_pct}%"></div></div>'
            + f'<div class="bulk-pct">{_pct}% tersimpan</div></div>',
            unsafe_allow_html=True,
        )
        for _r in _results:
            _s = _r["status"]; _p = _r.get("parsed", {}); _fn = _r["file"]; _rmode = _r.get("mode", "expedia")
            _ic = {"success": "ic-ok", "error": "ic-err", "skipped": "ic-skip"}.get(_s, "ic-n")
            _bc = {"success": "fb-ok", "error": "fb-err", "skipped": "fb-sk"}.get(_s, "fb-ok")
            _sy = {"success": "&#10003;", "error": "&#10005;", "skipped": "&#9888;"}.get(_s, "")
            _lb = {"success": "Tersimpan", "error": "Gagal", "skipped": "Duplikat"}.get(_s, _s)
            _wc = {"success": "fi-success", "error": "fi-error", "skipped": "fi-skipped"}.get(_s, "")
            if _p and _s in ("success", "skipped"):
                _dw = (
                    '<div style="margin-top:7px;font-size:11px;color:#7a5c00;background:#fef9c3;'
                    'padding:5px 9px;border-radius:8px">&#9888; ' + _r.get("err", "Duplikat") + "</div>"
                ) if _s == "skipped" else ""
                if _rmode == "nonexpedia":
                    _det = (
                        '<div style="margin-top:5px"><span style="font-size:9px;color:#7a5c00;'
                        'background:#fef9c3;border:1px solid #fcd34d;border-radius:5px;'
                        'padding:2px 7px;font-weight:600">🧾 Non-Expedia</span></div>'
                        + '<div class="fi-grid">'
                        + '<div class="fi-kv"><span class="fi-k">Hotel</span><span class="fi-v">' + (_p.get("hotel") or "—") + "</span></div>"
                        + '<div class="fi-kv"><span class="fi-k">Total</span><span class="fi-v">' + fmt(_p.get("room", 0)) + "</span></div>"
                        + '<div class="fi-kv"><span class="fi-k">Tamu</span><span class="fi-v">' + (_p.get("name") or "—") + "</span></div>"
                        + '<div class="fi-kv"><span class="fi-k">Kartu</span><span class="fi-v">' + (_p.get("card") or "—") + "</span></div>"
                        + '<div class="fi-kv"><span class="fi-k">Booking</span><span class="fi-v">' + (_p.get("booking_id") or "—") + "</span></div>"
                        + '<div class="fi-kv"><span class="fi-k">Waktu</span><span class="fi-v">' + (_p.get("timestamp_input") or "—") + "</span></div>"
                        + "</div>" + _dw
                    )
                else:
                    _det = (
                        '<div class="fi-grid">'
                        + '<div class="fi-kv"><span class="fi-k">Hotel</span><span class="fi-v">' + (_p.get("hotel") or "—") + "</span></div>"
                        + '<div class="fi-kv"><span class="fi-k">Total</span><span class="fi-v">' + fmt(_p.get("room", 0)) + "</span></div>"
                        + '<div class="fi-kv"><span class="fi-k">Tamu</span><span class="fi-v">' + (_p.get("name") or "—") + "</span></div>"
                        + '<div class="fi-kv"><span class="fi-k">Booking</span><span class="fi-v">' + (_p.get("booking_id") or "—") + "</span></div>"
                        + '<div class="fi-kv"><span class="fi-k">Check-in</span><span class="fi-v">' + (_p.get("checkin") or "—") + "</span></div>"
                        + '<div class="fi-kv"><span class="fi-k">Supplier</span><span class="fi-v">' + (_p.get("supplier") or "—") + "</span></div>"
                        + "</div>" + _dw
                    )
            elif _r.get("err"):
                _det = (
                    '<div class="fi-grid" style="grid-template-columns:1fr">'
                    '<div class="fi-kv"><span class="fi-k">Error</span>'
                    '<span class="fi-v" style="color:#e53935;white-space:normal">'
                    + _r["err"] + "</span></div></div>"
                )
            else:
                _det = ""
            st.markdown(
                '<div class="file-item ' + _wc + '"><div class="fi-top">'
                '<div class="fi-icon ' + _ic + '">📷</div>'
                '<div class="fi-name">' + _fn + "</div>"
                '<span class="fi-badge ' + _bc + '">' + _sy + " " + _lb + "</span>"
                "</div>" + _det + "</div>",
                unsafe_allow_html=True,
            )
        _sid = sheet_id()
        if _sid and _ok:
            st.link_button(
                f"📊  Buka Google Sheets ({_ok} baris tersimpan)",
                f"https://docs.google.com/spreadsheets/d/{_sid}",
                use_container_width=True,
            )
        if _err:
            notice("warn", f"{_err} file gagal.")
    _render_footer()


# ═══════════════════════════════════════════════════════════════════════════════
#  TAB — DASHBOARD  (DuckDB-powered)
# ═══════════════════════════════════════════════════════════════════════════════
elif st.session_state["tab"] == "dashboard":
    import pandas as pd

    if not _dashboard_login_wall():
        _render_footer()
        st.stop()

    _cr, _cb2, _cb3 = st.columns([3, 1, 1])
    _cr.markdown(
        '<div class="sec-lbl" style="margin-top:4px">Ringkasan</div>',
        unsafe_allow_html=True,
    )
    if _cb2.button("↻", type="secondary", use_container_width=True, key="dash_ref"):
        # OPTIMIZATION: hanya clear cache_data (ringan), bukan cache_resource (berat)
        load_rows_cached.clear()
        _invalidate_duckdb()
        st.rerun()
    with _cb3:
        _render_logout_button()

    # Badge mode engine
    _eng = "DuckDB ⚡" if _DUCK_OK else "Pandas"
    _eng_col = "#7a5c00" if not _DUCK_OK else "#166534"
    _eng_bg  = "#fef9c3" if not _DUCK_OK else "#f0fdf4"
    _eng_bd  = "#fcd34d" if not _DUCK_OK else "#86efac"
    st.markdown(
        f'<div style="font-size:10px;font-weight:700;color:{_eng_col};'
        f'background:{_eng_bg};border:1px solid {_eng_bd};'
        f'padding:3px 10px;border-radius:20px;display:inline-flex;'
        f'align-items:center;gap:5px;margin-bottom:10px;">'
        f'Query engine: {_eng}</div>',
        unsafe_allow_html=True,
    )

    try:
        with st.spinner("Memuat data..."):
            rows = load_rows()   # dari cache, sangat cepat saat reload

        if not rows:
            notice("info", "Belum ada transaksi.")
        else:
            df_full = pd.DataFrame(rows)
            if "Total (Rp)" in df_full.columns:
                df_full["Total (Rp)"] = pd.to_numeric(df_full["Total (Rp)"], errors="coerce").fillna(0)

            tn = len(df_full)
            tr = df_full["Total (Rp)"].sum() if "Total (Rp)" in df_full.columns else 0
            avg = tr / tn if tn else 0
            tds = datetime.now().strftime("%d/%m/%Y")
            tdc = int(
                df_full["Timestamp Input"].astype(str).str.startswith(tds).sum()
            ) if "Timestamp Input" in df_full.columns else 0

            st.markdown(
                '<div class="stat-grid">'
                + f'<div class="stat-card"><div class="stat-val">{tn}</div><div class="stat-lbl">Total transaksi</div></div>'
                + f'<div class="stat-card"><div class="stat-val" style="font-size:16px">{fmt(tr)}</div><div class="stat-lbl">Total pengeluaran</div></div>'
                + f'<div class="stat-card"><div class="stat-val" style="font-size:16px">{fmt(avg)}</div><div class="stat-lbl">Rata-rata</div></div>'
                + f'<div class="stat-card"><div class="stat-val">{tdc}</div><div class="stat-lbl">Hari ini</div></div>'
                + "</div>",
                unsafe_allow_html=True,
            )

            st.markdown('<div class="sec-lbl">Filter</div>', unsafe_allow_html=True)
            _date_opts = [c for c in ["Check-in", "Booking Date", "Issued Date", "Timestamp Input"]
                          if c in df_full.columns]
            _fa, _fb, _fc = st.columns([2, 1, 1])
            with _fa:
                _filter_col = st.selectbox(
                    "Kolom tanggal", options=_date_opts, index=0,
                    label_visibility="collapsed", key="dash_filter_col",
                )

            # Parse tanggal untuk date range picker
            _df_dated = df_full.copy()
            _df_dated["_pd"] = pd.to_datetime(_df_dated[_filter_col].astype(str), dayfirst=True, errors="coerce")
            _valid = _df_dated["_pd"].dropna()
            _date_from = _date_to = None

            if not _valid.empty:
                _min_date = _valid.min().date()
                _max_date = _valid.max().date()
                with _fb:
                    _date_from = st.date_input(
                        "Dari", value=_min_date, min_value=_min_date,
                        max_value=_max_date, label_visibility="collapsed", key="dash_date_from",
                    )
                with _fc:
                    _date_to = st.date_input(
                        "Sampai", value=_max_date, min_value=_min_date,
                        max_value=_max_date, label_visibility="collapsed", key="dash_date_to",
                    )

            # Search bar
            srch = st.text_input(
                "", placeholder="🔍  Cari hotel / tamu / booking ID...",
                label_visibility="collapsed", key="srch",
            )

            # ── QUERY VIA DUCKDB (atau pandas fallback) ──────────────────────
            _ensure_duckdb(rows)
            df = query_duckdb(
                rows,
                date_col=_filter_col if _date_from else None,
                date_from=_date_from,
                date_to=_date_to,
                search=srch,
            )

            if "Total (Rp)" in df.columns:
                df["Total (Rp)"] = pd.to_numeric(df["Total (Rp)"], errors="coerce").fillna(0)

            _fn2 = len(df)
            _tr2 = df["Total (Rp)"].sum() if "Total (Rp)" in df.columns else 0
            _avg2 = _tr2 / _fn2 if _fn2 else 0

            if _fn2 != tn or srch:
                st.markdown(
                    '<div style="display:flex;gap:6px;margin-bottom:10px;">'
                    + f'<div style="flex:1;background:#e8f0fe;border-radius:12px;padding:9px 12px;">'
                    + f'<div style="font-size:10px;color:#1e3a6e;">Terfilter</div>'
                    + f'<div style="font-size:17px;font-weight:700;color:#191d3a;">{_fn2}</div></div>'
                    + f'<div style="flex:1;background:#e8f0fe;border-radius:12px;padding:9px 12px;">'
                    + f'<div style="font-size:10px;color:#1e3a6e;">Total</div>'
                    + f'<div style="font-size:14px;font-weight:700;color:#191d3a;">{fmt(_tr2)}</div></div>'
                    + f'<div style="flex:1;background:#e8f0fe;border-radius:12px;padding:9px 12px;">'
                    + f'<div style="font-size:10px;color:#1e3a6e;">Rata-rata</div>'
                    + f'<div style="font-size:14px;font-weight:700;color:#191d3a;">{fmt(_avg2)}</div></div>'
                    + "</div>",
                    unsafe_allow_html=True,
                )

            # ── Kartu Kredit agregasi via DuckDB ─────────────────────────────
            if "Kartu Kredit" in df.columns and "Total (Rp)" in df.columns:
                df["Kartu Kredit"] = df["Kartu Kredit"].astype(str).apply(normalize_card)
                _card_str = df["Kartu Kredit"].astype(str).str.strip().str.lower()
                _cc = df[_card_str.ne("") & _card_str.ne("nan") & _card_str.ne("none")]
                if not _cc.empty:
                    st.markdown('<div class="sec-lbl">Kartu Kredit</div>', unsafe_allow_html=True)
                    _grp = (
                        _cc.groupby("Kartu Kredit")["Total (Rp)"]
                        .sum().sort_values(ascending=False).reset_index()
                    )
                    _grp.columns = ["label", "val"]
                    _tot2 = _grp["val"].sum()
                    _cnt = _cc.groupby("Kartu Kredit").size()
                    _h = ""
                    for _, _row in _grp.iterrows():
                        _p = _row["val"] / _tot2 * 100 if _tot2 else 0
                        _a = "Rp {:,.0f}".format(_row["val"]).replace(",", ".")
                        _c = int(_cnt.get(_row["label"], 0))
                        _h += (
                            f'<div style="padding:11px 0;border-bottom:1.5px solid #ededed">'
                            f'<div style="display:flex;justify-content:space-between;margin-bottom:5px">'
                            f'<span style="font-size:13px;font-weight:600;color:#191d3a">{_row["label"]}</span>'
                            f'<span style="font-size:13px;font-weight:700;color:#191d3a">{_a}</span></div>'
                            f'<div style="display:flex;align-items:center;gap:8px">'
                            f'<div style="flex:1;background:#e8e8e8;border-radius:4px;height:4px">'
                            f'<div style="width:{int(_p)}%;background:#6398c8;border-radius:4px;height:4px"></div></div>'
                            f'<span style="font-size:11px;color:#9e9e9e;white-space:nowrap">{_p:.1f}% · {_c} trx</span>'
                            f"</div></div>"
                        )
                    st.markdown(
                        f'<div style="background:#fff;border:1.5px solid #ddd;border-radius:16px;padding:4px 14px">{_h}</div>',
                        unsafe_allow_html=True,
                    )

            st.markdown('<div class="sec-lbl">Data transaksi</div>', unsafe_allow_html=True)
            _disp = df.iloc[::-1].reset_index(drop=True).copy()
            if "Booking ID" in _disp.columns:
                _disp["Booking ID"] = _disp["Booking ID"].astype(str)
            _cfg = {}
            if "Booking ID" in _disp.columns:
                _cfg["Booking ID"] = st.column_config.TextColumn("Booking ID")
            if "Total (Rp)" in _disp.columns:
                _cfg["Total (Rp)"] = st.column_config.NumberColumn("Total (Rp)", format="Rp %d")
            if "Room x Night" in _disp.columns:
                _cfg["Room x Night"] = st.column_config.TextColumn("Room × Night")
            if "Timestamp Input" in _disp.columns:
                _cfg["Timestamp Input"] = st.column_config.TextColumn("Timestamp")
            st.dataframe(_disp, use_container_width=True, height=320, column_config=_cfg, hide_index=True)

    except Exception as e:
        notice("err", str(e))
    _render_footer()


# ═══════════════════════════════════════════════════════════════════════════════
#  TAB — ACTIVITY LOG
# ═══════════════════════════════════════════════════════════════════════════════
elif st.session_state["tab"] == "log":
    try:
        with st.spinner("Memuat data..."):
            rows = load_rows()
        if not rows:
            notice("info", "Belum ada data transaksi.")
        else:
            import pandas as pd

            df_log = pd.DataFrame(rows)

            def _pts(v):
                try:
                    return pd.to_datetime(str(v), dayfirst=True)
                except:
                    return pd.NaT

            df_log["_ts"] = df_log["Timestamp Input"].apply(_pts)
            df_log = df_log.sort_values("_ts", ascending=False).reset_index(drop=True)
            _total = len(df_log)
            _recent = df_log.head(10)

            st.markdown(
                f'<div style="display:flex;align-items:center;justify-content:space-between;margin:4px 0 10px;">'
                f'<div class="sec-lbl" style="margin:0;border:none;padding:0;">Activity Log</div>'
                f'<span style="font-size:10px;color:#9e9e9e;font-weight:500;">10 dari {_total}</span></div>',
                unsafe_allow_html=True,
            )
            _items_html = ""
            for _, _row in _recent.iterrows():
                _ts = str(_row.get("Timestamp Input", "—"))
                _bid = str(_row.get("Booking ID", "—"))
                _hotel = str(_row.get("Hotel", "")) or "—"
                _issuer = str(_row.get("Issuer", "")) or "—"
                _total_r = _row.get("Total (Rp)", 0)
                try:
                    _amt = "Rp {:,}".format(int(float(_total_r))).replace(",", ".")
                except:
                    _amt = "—"
                _items_html += f"""
<div style="display:flex;align-items:center;gap:10px;padding:10px 12px;
    background:#fff;border-radius:12px;border:0.5px solid #e8e8e8;margin-bottom:6px;">
  <div style="width:36px;height:36px;border-radius:10px;background:#f5f5f5;
      display:flex;align-items:center;justify-content:center;flex-shrink:0;font-size:16px;">🏨</div>
  <div style="flex:1;min-width:0;">
    <div style="font-size:13px;font-weight:600;color:#191d3a;
        overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{_hotel}</div>
    <div style="font-size:10px;color:#9e9e9e;margin-top:1px;
        overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{_bid} · {_issuer}</div>
  </div>
  <div style="text-align:right;flex-shrink:0;">
    <div style="font-size:12px;font-weight:600;color:#191d3a;">{_amt}</div>
    <div style="font-size:9px;color:#bbb;margin-top:1px;">{_ts}</div>
  </div>
</div>"""
            st.markdown(_items_html, unsafe_allow_html=True)
    except Exception as e:
        notice("err", str(e))
    _render_footer()


# ═══════════════════════════════════════════════════════════════════════════════
#  TAB — SETTINGS
# ═══════════════════════════════════════════════════════════════════════════════
elif st.session_state["tab"] == "settings":
    _cur_prov = get_ai_provider()
    _active_lbl = "OpenAI" if _cur_prov == "openai" else "Claude"

    st.markdown('<div class="sec-lbl" style="margin-top:4px">AI Provider</div>', unsafe_allow_html=True)
    st.markdown('<div class="ai-card-btn-wrap">', unsafe_allow_html=True)
    if st.button(
        f"{'✦ ' if _cur_prov=='claude' else ''}Claude AI · claude-sonnet-4-5  ★ Default",
        key="sel_claude", use_container_width=True,
        type="primary" if _cur_prov == "claude" else "secondary",
    ):
        st.session_state["ai_provider"] = "claude"
        st.rerun()
    if st.button(
        f"{'✦ ' if _cur_prov=='openai' else ''}OpenAI · gpt-4o-mini",
        key="sel_openai", use_container_width=True,
        type="primary" if _cur_prov == "openai" else "secondary",
    ):
        st.session_state["ai_provider"] = "openai"
        st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)
    st.markdown(
        f'<div class="ai-status-bar"><div class="ai-status-dot"></div>'
        f'<span class="ai-status-txt">Active: {_active_lbl}</span></div>',
        unsafe_allow_html=True,
    )

    st.markdown('<div class="sec-lbl" style="margin-top:14px">API Keys</div>', unsafe_allow_html=True)
    for _pname, _sskey, _section, _placeholder, _skey in [
        ("Claude AI", "claude_key_manual", "anthropic", "sk-ant-api03-...", "inp_cla_key"),
        ("OpenAI", "openai_key_manual", "openai", "sk-proj-...", "inp_oai_key"),
    ]:
        _secrets_ok = False
        try:
            k = st.secrets[_section]["api_key"]
            if k and len(k) > 20 and "GANTI" not in k and "PASTE" not in k:
                _secrets_ok = True
        except:
            pass
        _ready = _secrets_ok or bool(st.session_state.get(_sskey, ""))
        _dot_c = "#1D9E75" if _ready else "#e68900"
        _lbl = "ready" if _ready else "belum dikonfigurasi"
        _lcls = "ai-key-ok" if _ready else "ai-key-warn"
        st.markdown(
            f'<div class="ai-key-row"><div class="ai-key-left">'
            f'<div class="ai-key-dot" style="background:{_dot_c}"></div>'
            f'<span class="ai-key-name">{_pname}</span></div>'
            f'<span class="{_lcls}">{_lbl}</span></div>',
            unsafe_allow_html=True,
        )
        if not _ready:
            _nk = st.text_input(
                _pname + " Key", value=st.session_state.get(_sskey, ""),
                type="password", placeholder=_placeholder,
                label_visibility="collapsed", key=_skey,
            )
            if _nk != st.session_state.get(_sskey, ""):
                st.session_state[_sskey] = _nk
                st.rerun()

    st.markdown('<div class="sec-lbl">Google Sheets</div>', unsafe_allow_html=True)
    sh_ok = False
    try:
        if st.secrets["google_sheets"]["sheet_id"] and st.secrets["gcp_service_account"]["client_email"]:
            sh_ok = True
    except:
        pass
    if sh_ok:
        st.markdown(
            '<div class="st-row"><div class="st-icon si-g">📊</div>'
            '<div class="st-body"><div class="st-title">Google Sheets</div>'
            '<div class="st-sub">Terhubung via service account</div></div>'
            '<span class="st-badge bg">✓ Aktif</span></div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div class="st-row"><div class="st-icon si-y">📊</div>'
            '<div class="st-body"><div class="st-title">Google Sheets</div>'
            '<div class="st-sub">Belum dikonfigurasi</div></div>'
            '<span class="st-badge by">⚠ Belum</span></div>',
            unsafe_allow_html=True,
        )
        notice("warn", "Isi <code>.streamlit/secrets.toml</code>")
        ns = st.text_input(
            "Sheet ID", value=st.session_state.get("sheet_id", ""),
            label_visibility="collapsed", placeholder="1nvgMCmo...",
        )
        if ns != st.session_state.get("sheet_id", ""):
            st.session_state["sheet_id"] = ns

    # GAS URL (opsional)
    st.markdown('<div class="sec-lbl">Google Apps Script (Opsional)</div>', unsafe_allow_html=True)
    st.markdown(
        '<div style="font-size:11px;color:#9e9e9e;margin-bottom:8px;line-height:1.6">'
        "GAS Web App URL untuk load data lebih cepat — tanpa overhead OAuth.<br>"
        "Kosongkan jika tidak digunakan.</div>",
        unsafe_allow_html=True,
    )
    _gas_cur = st.session_state.get("gas_url", "") or (
        st.secrets["google_sheets"].get("gas_url", "") if sh_ok else ""
    )
    _gas_in = st.text_input(
        "GAS URL", value=_gas_cur,
        placeholder="https://script.google.com/macros/s/.../exec",
        label_visibility="collapsed", key="gas_url_input",
    )
    if _gas_in != st.session_state.get("gas_url", ""):
        st.session_state["gas_url"] = _gas_in

    st.markdown('<div class="sec-lbl">Status Sistem</div>', unsafe_allow_html=True)
    if _PDF_OK:
        st.markdown(
            '<div class="st-row"><div class="st-icon si-b">📄</div>'
            '<div class="st-body"><div class="st-title">PDF Upload</div>'
            '<div class="st-sub">pypdfium2 terinstall</div></div>'
            '<span class="st-badge bg">✓ Aktif</span></div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div class="st-row"><div class="st-icon si-r">📄</div>'
            '<div class="st-body"><div class="st-title">PDF Upload</div>'
            '<div class="st-sub">pypdfium2 tidak terinstall</div></div>'
            '<span class="st-badge br">✕ Nonaktif</span></div>',
            unsafe_allow_html=True,
        )
        notice("err", "Jalankan: <code>pip install pypdfium2==4.30.0</code>")

    if _DUCK_OK:
        st.markdown(
            '<div class="st-row"><div class="st-icon si-g">⚡</div>'
            '<div class="st-body"><div class="st-title">DuckDB Query Engine</div>'
            '<div class="st-sub">Filter & agregasi 10-50x lebih cepat</div></div>'
            '<span class="st-badge bg">✓ Aktif</span></div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div class="st-row"><div class="st-icon si-y">⚡</div>'
            '<div class="st-body"><div class="st-title">DuckDB Query Engine</div>'
            '<div class="st-sub">Fallback ke pandas</div></div>'
            '<span class="st-badge by">⚠ Install opsional</span></div>',
            unsafe_allow_html=True,
        )
        notice("warn", "Install opsional: <code>pip install duckdb</code>")

    st.markdown('<div class="sec-lbl">Tentang</div>', unsafe_allow_html=True)
    _active_model = "gpt-4o-mini (OpenAI)" if get_ai_provider() == "openai" else "claude-sonnet-4-5 (Anthropic)"
    st.markdown(
        f"""
<div class="about-box">
  <div class="about-ttl">AI Intelligent Automation Scanner v7</div>
  <div class="about-r"><div class="about-k">Input</div>
    <div class="about-v">Expedia/TAAP: PDF·JPG·PNG bulk | Non-Expedia: JPG·PNG + manual</div></div>
  <div class="about-r"><div class="about-k">Output</div>
    <div class="about-v">Google Sheets — 17 kolom</div></div>
  <div class="about-r"><div class="about-k">Model AI</div>
    <div class="about-v">{_active_model} <b>(aktif)</b></div></div>
  <div class="about-r"><div class="about-k">Optimasi</div>
    <div class="about-v">
      ① get_all_values() — 3-5× lebih cepat dari get_all_records()<br>
      ② cache_data TTL 10 mnt — pisah dari koneksi<br>
      ③ DuckDB in-memory — filter &amp; agregasi columnar<br>
      ④ GAS endpoint opsional — load tanpa OAuth overhead<br>
      ⑤ save_row() auto-invalidate cache setelah tulis
    </div>
  </div>
</div>""",
        unsafe_allow_html=True,
    )
    _render_footer()
