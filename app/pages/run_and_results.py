import streamlit as st
from utils.sidebar import make_sidebar, init_theme
import subprocess
import sys
import time
import logging
import os


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

        # Elementos dinâmicos
        log_box = st.empty()
        progress_bar = st.progress(0)

        # Abrindo subprocesso em modo streaming
        process = subprocess.Popen(
            [sys.executable, "-u", "main.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )

        log_text = ""
        step_count = 0

        # Lê cada linha assim que ela aparece
        for line in process.stdout:
            log_text += line
            step_count += 1

            # Atualiza caixa de log
            log_box.text(log_text)

            # Atualiza barra de progresso (exemplo simples)
            progress_bar.progress(min(step_count % 100, 100) / 100)

            time.sleep(0.05)  # opcional, apenas para suavizar a UI

        process.wait()
        progress_bar.progress(100)

        st.success("Execution finished")
        with open(os.path.join(os.getcwd(), 'results.csv'), 'r') as f:
            st.download_button(label='Download Results in CSV', data=f, file_name='results.csv', mime="text/csv") 