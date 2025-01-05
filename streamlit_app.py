import streamlit as st
from time import sleep
from navigation import make_sidebar

make_sidebar()

st.title("Welcome to The Pyspark PostgreSQL Data Extractor (PPDE)")

st.divider()

st.write("Please log in to continue (username `postgres`, password `rules`).")

username = st.text_input("Username")
password = st.text_input("Password", type="password")

if st.button("Log in", type="primary"):
    if username == "postgres" and password == "rules":
        st.session_state.logged_in = True
        st.success("Logged in successfully!")
        sleep(0.5)
        st.switch_page("pages/welcome.py")
    else:
        st.error("Incorrect username or password")
st.divider()
url = "https://github.com/lpedebos/pyspark_multiple_db_query"
st.write("[🐈‍⬛   Github reference page](%s)." % url)

