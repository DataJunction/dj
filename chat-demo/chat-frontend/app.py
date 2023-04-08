import streamlit as st
import requests
import json
import time
import os
import logging
import pandas as pd

API_ENDPOINT = os.environ["API_ENDPOINT"]
DEFAULT_NUMBER_OF_METRICS = 5
DEFAULT_RELEVANCE_THRESHOLD = .5

logging.basicConfig(level=logging.INFO)

st.set_page_config(page_title="DJ", page_icon="https://avatars.githubusercontent.com/u/46006761?s=50&v=4")

f"""
## Welcome to DJ Chat Demo!

Enter natural language questions to query metric data.

"""

def sidebar():
    # Sidebar
    st.sidebar.header("Options")

    top_k_query = st.sidebar.slider(
        "Max number of metrics to query",
        min_value=1,
        max_value=30,
        value=DEFAULT_NUMBER_OF_METRICS,
        step=1,
        key='num_answers',
    )

    thresh_query = st.sidebar.slider(
        "Minimum metric relevance",
        min_value=0.0,
        max_value=1.0,
        value=DEFAULT_RELEVANCE_THRESHOLD,
        step=.05,
        key='rel_thresh',
    )


last_query = ''
def main():
    if "results" not in st.session_state:
        st.session_state["recs"] = []
        st.session_state["results"] = None
        st.session_state['query'] = ''
        st.session_state['query_id'] = ''
        st.session_state["searched"] = False
        st.session_state["query_response_time"] = None

    def fetch_query():
        start = time.time()
        response = requests.get(f"{API_ENDPOINT}/query/{st.session_state.query}?n={st.session_state.num_answers}&rel={st.session_state.rel_thresh}")
        if response.status_code==200:
            data = response.json()
            st.session_state.results = pd.DataFrame(data['results'][0]['rows'])
            st.session_state.query_response_time = time.time()-start
            st.session_state.searched = True
            logging.info(f"Results for '{st.session_state.query}' fetched in {st.session_state.query_response_time:.2}s")
        else:
            st.session_state.results = None
            st.session_state.query_response_time = None
            logging.error(f"Failed to fetch results for '{st.session_state.query}'")

    search_input, search_container = st.columns([6, 1])
    search_input.text_input("", placeholder="Ask for data...", max_chars=100, key='query')
    search_button = search_container.container()
    search_button.write("   ")
    search_button.write("   ")
    search_button.button("Run", on_click=fetch_query)

    if st.session_state.query.strip():

        if st.session_state.query != last_query:
            fetch_query()

        if st.session_state.results is not None:
            f"""
            ```
            Results for '{st.session_state.query}' 
            fetched in {st.session_state.query_response_time:.2}s
            ```
            """
            st.write(st.session_state.results)
            st.bar_chart(st.session_state.results)
sidebar()
main()