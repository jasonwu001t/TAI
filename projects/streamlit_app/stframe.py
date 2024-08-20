import streamlit as st
import pandas as pd

class stframe:
    def __init__(self):
        # Initialize any shared variables or states here if needed
        pass

    # Authentication Module
    def authenticate(self, username, password):
        USER_CREDENTIALS = {
            'user1': 'password1',
            'user2': 'password2',
            # Add more users if needed
        }
        if username in USER_CREDENTIALS and USER_CREDENTIALS[username] == password:
            return True
        return False

    def login(self):
        if 'authenticated' not in st.session_state:
            st.session_state.authenticated = False

        if not st.session_state.authenticated:
            st.title("Login")
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")

            if st.button("Login"):
                if self.authenticate(username, password):
                    st.session_state.authenticated = True
                    st.success("Login successful!")
                    
                    # Set query params to trigger a rerun
                    st.query_params = {"authenticated": "true"}
                else:
                    st.error("Invalid username or password")
            return False  # Not authenticated yet
        return True  # Already authenticated

    def logout(self):
        if st.button("Logout"):
            st.session_state.authenticated = False
            st.query_params = {}  # Clear query params to reset state
            st.experimental_set_query_params()  # Redirect to login by clearing query params

    # Main Page Module
    def show_main_page(self):
        st.title("Main Page")
        st.write("Welcome to the main page!")
        st.write("This is where you can put the content of your app.")

        # Sample content
        st.header("Sample Data")
        st.write("Here is some sample data that could be displayed on your main page.")

        st.bar_chart({
            "Category A": [3, 6, 9, 12],
            "Category B": [2, 4, 8, 10],
            "Category C": [1, 7, 5, 9]
        })

        # Option to log out
        st.write("---")
        st.write("You can log out using the button below.")

if __name__ == "__main__":
    handler = stframe()

    # Simulate a Streamlit app within the __main__ section for testing purposes
    if handler.login():
        handler.show_main_page()
        handler.logout()
