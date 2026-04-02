import streamlit as st
import pandas as pd
import numpy as np
import io

# =========================
# PAGE CONFIG
# =========================
st.set_page_config(
    page_title="OPSIGO Dashboard",
    layout="wide"
)

st.title("📊 OPSIGO Report Dashboard")

# =========================
# LOAD DATA
# =========================
@st.cache_data
def load_data(file):
    try:
        df = pd.read_excel(file)
        df.columns = df.columns.str.strip()
        return df
    except Exception as e:
        st.error(f"Gagal membaca file: {e}")
        return pd.DataFrame()

uploaded_file = st.file_uploader("📂 Upload File Excel", type=["xlsx"])

# =========================
# MAIN APP
# =========================
if uploaded_file:

    df = load_data(uploaded_file)

    if df.empty:
        st.warning("Data kosong atau gagal dibaca")
        st.stop()

    st.success("✅ File berhasil di-load")

    # =========================
    # CLEANING
    # =========================
    date_cols = ["Check In", "Check Out", "Issued Date"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # =========================
    # SIDEBAR FILTER
    # =========================
    st.sidebar.header("🔎 Filter Data")

    # Filter perusahaan
    if "Nama Perusahaan" in df.columns:
        company_list = sorted(df["Nama Perusahaan"].dropna().unique())
        selected_company = st.sidebar.multiselect(
            "Nama Perusahaan",
            company_list,
            default=company_list
        )
        df = df[df["Nama Perusahaan"].isin(selected_company)]

    # Filter tanggal
    if "Check In" in df.columns:
        min_date = df["Check In"].min()
        max_date = df["Check In"].max()

        if pd.notna(min_date) and pd.notna(max_date):
            date_range = st.sidebar.date_input(
                "Filter Check In",
                value=[min_date, max_date]
            )

            if len(date_range) == 2:
                start_date, end_date = date_range
                df = df[
                    (df["Check In"] >= pd.to_datetime(start_date)) &
                    (df["Check In"] <= pd.to_datetime(end_date))
                ]

    # =========================
    # METRICS
    # =========================
    st.subheader("📌 Summary")

    col1, col2, col3 = st.columns(3)

    col1.metric("Total Data", len(df))

    if "Amount" in df.columns:
        col2.metric("Total Amount", f"{df['Amount'].sum():,.0f}")
    else:
        col2.metric("Total Amount", "-")

    if "Booking ID" in df.columns:
        col3.metric("Total Booking", df["Booking ID"].nunique())
    else:
        col3.metric("Total Booking", "-")

    # =========================
    # DATA TABLE
    # =========================
    st.subheader("📋 Data Preview")

    st.dataframe(
        df,
        width="stretch",
        height=400
    )

    # =========================
    # CHART
    # =========================
    if "Nama Perusahaan" in df.columns and "Amount" in df.columns:
        import plotly.express as px

        st.subheader("📊 Total Amount per Perusahaan")

        summary = df.groupby("Nama Perusahaan")["Amount"].sum().reset_index()

        fig = px.bar(
            summary,
            x="Nama Perusahaan",
            y="Amount",
            text_auto=True
        )

        st.plotly_chart(fig, use_container_width=True)

    # =========================
    # HEATMAP BOOKING
    # =========================
    if "Check In" in df.columns:
        import plotly.express as px

        st.subheader("🔥 Heatmap Booking")

        df["Month"] = df["Check In"].dt.month
        df["Year"] = df["Check In"].dt.year

        value_col = "Booking ID" if "Booking ID" in df.columns else df.columns[0]

        pivot = df.pivot_table(
            index="Month",
            columns="Year",
            values=value_col,
            aggfunc="count"
        )

        if not pivot.empty:
            fig_heatmap = px.imshow(
                pivot,
                text_auto=True,
                aspect="auto"
            )
            st.plotly_chart(fig_heatmap, use_container_width=True)

    # =========================
    # DOWNLOAD EXCEL
    # =========================
    st.subheader("⬇️ Download Report")

    output = io.BytesIO()

    try:
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Report")

        st.download_button(
            label="Download Excel",
            data=output.getvalue(),
            file_name="OPSIGO_Report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        st.error(f"Gagal export Excel: {e}")

else:
    st.info("📂 Silakan upload file Excel untuk mulai")
