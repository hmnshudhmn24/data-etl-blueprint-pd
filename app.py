
import streamlit as st
import pandas as pd
import io
import difflib

st.set_page_config(page_title="🛠️ ETL Blueprint Generator", layout="wide")
st.title("🛠️ Data Transformation as Code (ETL Blueprint)")

uploaded_file = st.file_uploader("Upload your CSV file", type=["csv"])
if uploaded_file:
    df = pd.read_csv(uploaded_file)
    st.subheader("Original Data Preview")
    st.dataframe(df.head())

    transformation_log = []
    code_snippets = []
    df_transformed = df.copy()

    st.sidebar.header("Transformations")

    if st.sidebar.checkbox("Drop Columns"):
        cols_to_drop = st.sidebar.multiselect("Select columns to drop", df.columns)
        if cols_to_drop:
            df_transformed.drop(columns=cols_to_drop, inplace=True)
            transformation_log.append(f"Dropped columns: {cols_to_drop}")
            code_snippets.append(f"df.drop(columns={cols_to_drop}, inplace=True)")

    if st.sidebar.checkbox("Fill NA Values"):
        fill_col = st.sidebar.selectbox("Select column to fill NAs", df.columns)
        fill_val = st.sidebar.text_input("Value to fill with")
        if fill_val:
            df_transformed[fill_col].fillna(fill_val, inplace=True)
            transformation_log.append(f"Filled NAs in '{fill_col}' with '{fill_val}'")
            code_snippets.append(f"df['{fill_col}'].fillna('{fill_val}', inplace=True)")

    if st.sidebar.checkbox("Group by + Aggregate"):
        group_col = st.sidebar.selectbox("Group by column", df.columns)
        agg_col = st.sidebar.selectbox("Aggregate column", df.columns)
        agg_func = st.sidebar.selectbox("Aggregate function", ["sum", "mean", "max", "min"])
        if st.sidebar.button("Apply Groupby"):
            df_transformed = df_transformed.groupby(group_col)[agg_col].agg(agg_func).reset_index()
            transformation_log.append(f"Grouped by '{group_col}', aggregated '{agg_col}' with '{agg_func}'")
            code_snippets.append(f"df = df.groupby('{group_col}')['{agg_col}'].agg('{agg_func}').reset_index()")

    st.subheader("Transformed Data Preview")
    st.dataframe(df_transformed.head())

    if transformation_log:
        st.subheader("📜 Transformation Log")
        for log in transformation_log:
            st.write("🔹 " + log)

    if code_snippets:
        st.subheader("💾 Generated ETL Script")
        final_script = "import pandas as pd\n\ndf = pd.read_csv('your_file.csv')\n" + "\n".join(code_snippets)
        st.code(final_script, language='python')
        if st.download_button("📥 Download ETL Script", data=final_script, file_name="etl_script.py"):
            st.success("Script downloaded!")
