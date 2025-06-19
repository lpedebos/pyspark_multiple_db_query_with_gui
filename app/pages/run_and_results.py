import streamlit as st
from utils.sidebar import make_sidebar, init_theme
import subprocess
import sys
import logging

from contextlib import contextmanager, redirect_stdout
from io import StringIO 
from time import sleep
from threading import current_thread

if not st.session_state.get("logged_in", False):
    st.switch_page("streamlit_app.py")


init_theme()

make_sidebar()

st.write(
    """
# ✌️ Run and results

Hit the button bellow to execute the queries.

After that you will be able to download the file.

"""
)
st.write("")
st.write("")



if st.button("Execute queries"):
    with st.spinner('Executing... it can take a while'):
        subprocess.run([f"{sys.executable}", "main.py"])
        st.success("Execution finished")
        with open('results.csv', 'r') as f:
            st.download_button(label='Download CSV', data=f, file_name='results.csv', mime="text/csv") 


