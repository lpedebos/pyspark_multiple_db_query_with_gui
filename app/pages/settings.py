import streamlit as st
import json
import os

CONFIG_PATH = "config.json"

def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    return {"databases": []}

def save_config(config):
    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=4)

# Load config into session state
if "config" not in st.session_state:
    st.session_state.config = load_config()

st.title("⚙️ Settings - Database Connections")

databases = st.session_state.config["databases"]

# Display each database entry
for i, db in enumerate(databases):
    st.subheader(f"Database {i + 1}")
    db["url"] = st.text_input(f"URL {i + 1}", value=db["url"], key=f"url_{i}")
    db["user"] = st.text_input(f"User {i + 1}", value=db["user"], key=f"user_{i}")
    db["password"] = st.text_input(f"Password {i + 1}", value=db["password"], key=f"password_{i}", type="password")

    if st.button(f"Remove Database {i + 1}", key=f"remove_{i}"):
        databases.pop(i)
        st.session_state.config["databases"] = databases
        save_config(st.session_state.config)
        st.experimental_rerun()

# Add new database
if st.button("➕ Add New Database"):
    databases.append({"url": "", "user": "", "password": ""})
    st.session_state.config["databases"] = databases


# Save changes with validation
if st.button("💾 Save Changes"):
    invalid = False
    for i, db in enumerate(databases):
        if not db["url"].strip() or not db["user"].strip() or not db["password"].strip():
            st.warning(f"Database {i + 1} has empty fields. Please fill in all fields.")
            invalid = True
            break

    if not invalid:
        save_config(st.session_state.config)
        st.success("Configuration saved successfully!")
