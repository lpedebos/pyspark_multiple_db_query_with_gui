import streamlit as st
from utils.sidebar import make_sidebar, init_theme

init_theme()

if not st.session_state.get("logged_in", False):
  st.switch_page("streamlit_app.py")

make_sidebar()

st.write(
    """
# ☝️ First query

This is the query that will run on distributed databases.

The same query will run on all databases.
So, be aware that all databases must have the same structure (schema, tables, columns, etc), not necessarily the same data.

"""
)
st.write("")
st.write("")

input1 = st.text_area("Type bellow and hit the save button.")

if st.button("Save query"):
    with open("first_query.sql", "w") as f:
        f.write(input1)
    st.success("First query saved with success!")
