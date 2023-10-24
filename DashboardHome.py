import streamlit as st
from streamlit_option_menu import option_menu as om
import mysql.connector
import pyodbc as odbc

#imports for EDA
import pandas as pd
import numpy as np
import plotly as plt

#imports for ML
import sklearn as sk



st.set_page_config(page_title="DatAI", page_icon="icon.png", layout="wide")
st.title("Dashboard")
st.text("Welcome to DatAI open-source tool for data science jobs.")

action = st.selectbox("Choose an action", ["Connect to my database", "Upload Dataset"])
st.markdown('<hr style="border: 1px solid lightgray;">', unsafe_allow_html=True)

db_placeholder = st.empty()
db_successful_placeholder = st.empty()
success_message = st.empty()

with st.sidebar:
    st.write("This is your sidebar content")

if action == "Connect to my database":
    
    with db_placeholder.container():

        col1, col2 = st.columns(2)

        # Column 1: Database User and Password
        with col1:
            db_user = st.text_input("Database Username")
            db_password = st.text_input("Database Password", type="password")
            db_name = st.text_input("Database Name")

        # Column 2: Database Action (Selectbox)
        with col2:
            db_action = st.selectbox("Choose your database:", ["Microsoft SQL Server", "PostgreSQL", "MySQL"])
            db_host = st.text_input("Host")
            db_port = st.text_input("Port")
            


        if db_action == "Microsoft SQL Server":
            # Add code to connect to SQLite database
            if st.button("Connect to Database"):
                if db_user and db_password:
                    try:
                        connection_string = f"""
                                DRIVER=ODBC Driver 17 for SQL Server;
                                SERVER=DESKTOP-AR1L4FP\\SQLEXPRESS;
                                DATABASE=Asptest;
                                Trusted_Connection=yes;  
                            """
                        conn = odbc.connect(connection_string)
                        st.write(conn)
                        st.success("Database connection successful")
                        
                        
                        cursor = conn.cursor()

                        # Execute an SQL query to fetch data from the 'categories' table
                        cursor.execute("SELECT * FROM categories;")
                        
                        # Fetch all rows
                        rows = cursor.fetchall()

                        # Display the data in a Streamlit table
                        st.write("Data from 'categories' table:")
                        for row in rows:
                            st.write(f"{row[0]} has a :{row[1]}: {row[2]}")

                      
                        
                        # You can add further logic to display data or perform database operations here

                    except mysql.connector.Error as err:
                        st.error(f"Database connection error: {err}")
                else:
                    st.error("Please enter username and password")
        elif db_action == "PostgreSQL":
            # Add code to connect to PostgreSQL database
            st.write("You selected PostgreSQL. Implement the connection code here.")
        elif db_action == "MySQL":
            # Add code to connect to MySQL database
            if st.button("Connect to Database"):
                if db_user and db_password:
                    try:
                        connection = mysql.connector.connect(
                            host="localhost",
                            port="3306",
                            user=str(db_user),  # Use the entered username
                            password=str(db_password),  # Use the entered password
                            database="sql_hr"
                        )
                        curs = connection.cursor()
                        st.write(curs)
                        st.success("Database connection successful")
                        
                        query = f"SELECT * FROM accounts"
                        curs.execute(query)
                        st.write(curs)
                        data = curs.fetchall()
                        df = pd.DataFrame(data, columns=["account_id", "account_name", "account_balance"])
                        success_message.success("Database connection successful")
                        db_successful_placeholder.dataframe(df) #st.dataframe(df)
                        db_placeholder.empty()
                        # You can add further logic to display data or perform database operations here

                    except mysql.connector.Error as err:
                        st.error(f"Database connection error: {err}")
                else:
                    st.error("Please enter username and password")
            

        
        
        
        # Button to connect to the database
        
                      
    
elif action == "Upload Dataset":
    # File uploader for dataset upload
    uploaded_file = st.file_uploader("Upload your dataset file", type=["csv", "txt", "json"])

    if uploaded_file is not None:
        # Process the uploaded dataset
        st.write(f"File uploaded: {uploaded_file.name}")

# Define a function to display table rows (you can call this function as needed)
