import streamlit as st
import requests
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import jwt
from datetime import datetime, timedelta
import os

# Configure the page
st.set_page_config(
    page_title="ETL Implementation App",
    layout="wide"
)

# API URL - Get from environment variable or use default
# This allows the URL to be configured differently in Docker
# API_URL = os.environ.get("API_URL", "http://127.0.0.1:8000")
API_URL = os.environ.get("API_URL", "https://etl-tutorial.onrender.com")

# Initialize session state variables if they don't exist
if 'jwt_token' not in st.session_state:
    st.session_state.jwt_token = None
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'username' not in st.session_state:
    st.session_state.username = None
if 'roles' not in st.session_state:
    st.session_state.roles = []

def login(username, password):
    """Handle user login and token storage"""
    try:
        # Create form data for login
        form_data = {'username': username, 'password': password}
        
        # Make the API request
        st.write(f"Attempting to connect to: {API_URL}/token")
        response = requests.post(f"{API_URL}/token", data=form_data, timeout=10)
        
        if response.status_code == 200:
            token_data = response.json()
            st.session_state.jwt_token = token_data['access_token']
            
            # Decode JWT token to get user info
            payload = jwt.decode(
                st.session_state.jwt_token, 
                options={"verify_signature": False}  # We're just extracting data, not verifying
            )
            
            st.session_state.username = payload.get('name', username)
            st.session_state.roles = payload.get('roles', [])
            st.session_state.logged_in = True
            
            return True
        else:
            st.error(f"Login failed with status code: {response.status_code}")
            if response.text:
                st.error(f"Response: {response.text}")
            return False
    except Exception as e:
        st.error(f"Login failed: {str(e)}")
        return False

def logout():
    """Handle user logout"""
    st.session_state.jwt_token = None
    st.session_state.logged_in = False
    st.session_state.username = None
    st.session_state.roles = []
    st.rerun()

def fetch_data():
    """Fetch data based on user role"""
    if not st.session_state.logged_in:
        return None
    
    # Determine which data endpoint to use based on roles
    data_url = '/data/common'  # Default
    if 'manager' in st.session_state.roles:
        data_url = '/data/manager'
    elif 'employee' in st.session_state.roles:
        data_url = '/data/employee'
    
    # Fetch data from API
    try:
        headers = {'Authorization': f'Bearer {st.session_state.jwt_token}'}
        response = requests.get(f"{API_URL}{data_url}", headers=headers, timeout=10)
        
        if response.status_code == 200:
            try:
                # Try to parse as JSON
                return response.json()
            except:
                # If parsing fails, return raw text
                return response.text
        else:
            return f"Access Denied (Status code: {response.status_code})"
    except Exception as e:
        return f"Error fetching data: {str(e)}"

def login_page():
    """Display the login page"""
    st.title("ETL Implementation App")
    
    # Create a card-like container for the login form
    with st.container():
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.subheader("Login")
            with st.form("login_form"):
                username = st.text_input("Username:")
                password = st.text_input("Password:", type="password")
                submit_button = st.form_submit_button("Login")
                
                if submit_button:
                    if login(username, password):
                        st.success("Login successful!")
                        st.rerun()
                    else:
                        st.error("Login failed. Please check your credentials.")

def display_manager_data(data):
    """Display visualizations for manager role"""
    try:
        # Parse the JSON data
        parsed_data = data if isinstance(data, list) else json.loads(data)
        
        # Extract the separate data components
        employee_data = parsed_data[0]
        total_data = parsed_data[1]
        
        # Convert to DataFrames
        employee_df = pd.DataFrame(employee_data)
        total_df = pd.DataFrame(total_data)
        
        # Create two columns for visualizations
        col1, col2 = st.columns([8, 4])
        
        with col1:
            # Bar chart for salary by category
            fig_bar = go.Figure()
            
            # Add bars for each pay category
            fig_bar.add_trace(go.Bar(
                x=employee_df['Name'],
                y=employee_df['Hourly_Pay'],
                name='Hourly Pay',
                marker_color='#f2cbae'
            ))
            fig_bar.add_trace(go.Bar(
                x=employee_df['Name'],
                y=employee_df['Travel_Pay'],
                name='Travel Pay',
                marker_color='#ebb4d3'
            ))
            fig_bar.add_trace(go.Bar(
                x=employee_df['Name'],
                y=employee_df['Weather_Pay'],
                name='Weather Pay',
                marker_color='#2b2a65'
            ))
            
            fig_bar.update_layout(
                title='Salary Paid by Category',
                barmode='group',
                xaxis_title='Employee',
                yaxis_title='Amount'
            )
            
            st.plotly_chart(fig_bar, use_container_width=True)
        
        with col2:
            # Pie chart for total salary
            fig_pie = px.pie(
                total_df,
                values='Total_Pay',
                names='Name',
                title='Total Salary Received'
            )
            st.plotly_chart(fig_pie, use_container_width=True)
            
    except Exception as e:
        st.error(f"Error displaying manager data: {str(e)}")
        st.write("Raw data:", data)

def display_employee_data(data):
    """Display visualizations for employee role"""
    try:
        # Parse the JSON data
        parsed_data = data if isinstance(data, list) else json.loads(data)
        
        # Extract the separate data components
        monthly_data = parsed_data[0]
        total_data = parsed_data[1]
        
        # Convert to DataFrames
        monthly_df = pd.DataFrame(monthly_data)
        total_df = pd.DataFrame(total_data)
        
        # Define the month order
        month_order = {
            'January': 1, 'February': 2, 'March': 3, 'April': 4,
            'May': 5, 'June': 6, 'July': 7, 'August': 8,
            'September': 9, 'October': 10, 'November': 11, 'December': 12
        }
        
        # Sort the monthly data by month
        monthly_df['month_num'] = monthly_df['date'].map(month_order)
        monthly_df = monthly_df.sort_values('month_num')
        
        # Create two columns for visualizations
        col1, col2 = st.columns([8, 4])
        
        with col1:
            # Bar chart for monthly pay by category
            fig_bar = go.Figure()
            
            # Add bars for each pay category
            fig_bar.add_trace(go.Bar(
                x=monthly_df['date'],
                y=monthly_df['Hourly_Pay'],
                name='Hourly Pay',
                marker_color='#f2cbae'
            ))
            fig_bar.add_trace(go.Bar(
                x=monthly_df['date'],
                y=monthly_df['Travel_Pay'],
                name='Travel Pay',
                marker_color='#ebb4d3'
            ))
            fig_bar.add_trace(go.Bar(
                x=monthly_df['date'],
                y=monthly_df['Weather_Pay'],
                name='Weather Pay',
                marker_color='#2b2a65'
            ))
            
            fig_bar.update_layout(
                title='Monthly Pay by Category',
                barmode='group',
                xaxis_title='Month',
                yaxis_title='Amount'
            )
            
            st.plotly_chart(fig_bar, use_container_width=True)
        
        with col2:
            # Pie chart for hours worked per month
            fig_pie = px.pie(
                total_df,
                values='Total_Hours',
                names='date',
                title='Hours Worked per Month'
            )
            st.plotly_chart(fig_pie, use_container_width=True)
            
    except Exception as e:
        st.error(f"Error displaying employee data: {str(e)}")
        st.write("Raw data:", data)

def dashboard_page():
    """Display the dashboard page"""
    # Add a logout button on the top right
    col1, col2 = st.columns([9, 1])
    with col2:
        if st.button("Logout"):
            logout()
    
    # Display user info
    st.subheader(f"Logged in as {st.session_state.username}")
    
    # Fetch and display data
    data = fetch_data()
    
    if data:
        if 'manager' in st.session_state.roles:
            display_manager_data(data)
        elif 'employee' in st.session_state.roles:
            display_employee_data(data)
        else:
            # For common/default case
            st.write("Data Display:")
            st.write(data)
    else:
        st.error("Please login to view data.")

# Main application logic
def main():
    if st.session_state.logged_in:
        dashboard_page()
    else:
        login_page()

if __name__ == "__main__":
    main()