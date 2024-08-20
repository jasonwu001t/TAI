import streamlit as st
from stframe import stframe

def main():
    handler = stframe()

    if handler.login():
        handler.show_main_page()
        handler.logout()

if __name__ == "__main__":
    main()
