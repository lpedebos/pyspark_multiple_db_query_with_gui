import streamlit as st
from utils.sidebar import init_theme

st.set_page_config(page_title="Login", page_icon="🔐")

init_theme()

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

st.title("🔐 Login")

if not st.session_state.logged_in:
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        if username == "admin" and password == "password":  # Replace with real logic
            st.session_state.logged_in = True
            st.success("Login successful!")
            st.switch_page("pages/welcome.py")
        else:
            st.error("Invalid credentials")
else:
    st.switch_page("pages/welcome.py")
