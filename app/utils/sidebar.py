import streamlit as st

def init_theme():
    ms = st.session_state
    if "themes" not in ms:
        ms.themes = {
            "current_theme": "light",
            "refreshed": True,
            "light": {
                "theme.base": "dark",
                "theme.backgroundColor": "black",
                "theme.primaryColor": "#c98bdb",
                "theme.secondaryBackgroundColor": "#5591f5",
                "theme.textColor": "white",
                "button_face": "🌜"
            },
            "dark": {
                "theme.base": "light",
                "theme.backgroundColor": "white",
                "theme.primaryColor": "#5591f5",
                "theme.secondaryBackgroundColor": "#82E1D7",
                "theme.textColor": "#0a1464",
                "button_face": "🌞"
            }
        }

    if not ms.themes["refreshed"]:
        ms.themes["refreshed"] = True
        st.rerun()

def change_theme():
    ms = st.session_state
    previous = ms.themes["current_theme"]
    theme_dict = ms.themes["light"] if previous == "light" else ms.themes["dark"]

    for k, v in theme_dict.items():
        if k.startswith("theme"):
            st._config.set_option(k, v)

    ms.themes["refreshed"] = False
    ms.themes["current_theme"] = "dark" if previous == "light" else "light"

def make_sidebar():
    with st.sidebar:
        st.title("Pyspark PostgreSQL data extractor")

        if st.session_state.get("logged_in", False):
            st.page_link("pages/welcome.py", label="Welcome", icon="🙋‍♂️")
            st.page_link("pages/first_query.py", label="First Query", icon="☝️")
            st.page_link("pages/second_query.py", label="Second Query", icon="✌️")
            st.page_link("pages/run_and_results.py", label="Run and Results", icon="🍾")
            st.page_link("pages/settings.py", label="Settings", icon="⚙️")

            if st.button("Log out"):
                st.session_state.logged_in = False
                st.switch_page("streamlit_app.py")

        st.write("---")
        btn_face = st.session_state.themes["light"]["button_face"] if st.session_state.themes["current_theme"] == "light" else st.session_state.themes["dark"]["button_face"]
        st.button(btn_face, on_click=change_theme)
