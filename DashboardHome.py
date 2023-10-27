import streamlit as st
import mysql.connector
import pyodbc as odbc
import psycopg2
import pandas as pd
import time

# Initialize your database connection
isMicSql = False
isPgSql = False
isMySql = False
tables = []  # Initialize an empty list for tables

default_postgresql_host = "localhost"
default_postgresql_port = "5432"

st.set_page_config(page_title="DatAI", page_icon="icon.png", layout="wide")
st.title("Dashboard 📊")
st.text("Welcome to DatAI 💜 open-source tool for data science jobs.")

action = st.selectbox("Choose an action", ["🔗 Connect to my database", "📤 Upload Dataset"])
st.markdown('<hr style="border: 1px solid lightgray;">', unsafe_allow_html=True)

db_placeholder = st.empty()
db_table_selectbox = st.empty()
db_successful_placeholder = st.empty()
success_message = st.empty()

# Database connection details
db_host = None
db_port = None
db_user = None
db_password = None
db_name = None
db_server = None
conn = None

if 'is_table' not in st.session_state:
    st.session_state.is_table = False
if 'is_connected_micsql' not in st.session_state:
    st.session_state.is_connected_micsql = False
if 'is_connected_pgsql' not in st.session_state:
    st.session_state.is_connected_pgsql = False  
if 'is_connected_mysql' not in st.session_state:
    st.session_state.is_connected_mysql = False 
               

def init_connections_mysql(dbhost, dbport, dbuser, dbpassword, dbname):
    try:
        connection = mysql.connector.connect(
            host=dbhost,
            port=dbport,
            user=dbuser,
            password=dbpassword,
            database=dbname
        )
        if 'is_table' not in st.session_state:
            st.session_state.is_table = True
        else:
            st.session_state.is_table = True
        if 'is_connected_mysql' not in st.session_state:
            st.session_state.is_connected_mysql = True
        else:
            st.session_state.is_connected_mysql = True


        
        success_conn = st.success("Database successfully connected.")
        time.sleep(2)
        success_conn.empty()
        return connection
    except mysql.connector.Error as err:
        st.error(f"Database connection error: {err}")

def init_connections_pgsql(dbhost, dbport, dbuser, dbpassword, dbname):
    try:
        connection = psycopg2.connect(
            host=dbhost,
            port=dbport,
            user=dbuser,
            password=dbpassword,
            database=dbname
        )
        if 'is_table' not in st.session_state:
            st.session_state.is_table = True
        else:
            st.session_state.is_table = True
        if 'is_connected_pgsql' not in st.session_state:
            st.session_state.is_connected_pgsql = True
        else:
            st.session_state.is_connected_pgsql = True
        

       
        success_conn = st.success("Database successfully connected.")
        time.sleep(2)
        success_conn.empty()  
        return connection
    except psycopg2.Error as err:
        st.error(f"Database connection error: {err}")

def init_connections_micsql(server, name):
    try:
        connection_string = f"""
            DRIVER=ODBC Driver 17 for SQL Server;
            SERVER={{{server}}};
            DATABASE={{{name}}};
            Trusted_Connection=yes;
        """
        conn = odbc.connect(connection_string)
        if 'is_table' not in st.session_state:
            st.session_state.is_table = True
        else:
            st.session_state.is_table = True
        if 'is_connected_micsql' not in st.session_state:
            st.session_state.is_connected_micsql = True
        else:
            st.session_state.is_connected_micsql = True
        

        
        success_conn = st.success("Database successfully connected.")
        time.sleep(2)
        success_conn.empty()
        return conn
    except odbc.Error as err:
        st.error(f"Database connection error: {err}")
        st.session_state.is_connected = False
        st.session_state.is_table = False

def db_connection_layout():
    with db_placeholder.container():
        st.subheader('Database Connection')
        left_credentials, right_credentials = st.columns(2)
        right_credentials.selectbox("Choose your database:", ["Microsoft SQL Server", "PostgreSQL", "MySQL"], key="selected_db")
        
        isMicSql = True if st.session_state.selected_db == "Microsoft SQL Server" else False
        isPgSql = True if st.session_state.selected_db == "PostgreSQL" else False
        isMySql = True if st.session_state.selected_db == "MySQL" else False

        if isMicSql:
            db_server = left_credentials.text_input("Server Name")
            db_name = left_credentials.text_input("Database Name")
        elif isPgSql:
            db_user = left_credentials.text_input("Database Username")
            db_password = left_credentials.text_input("Database Password", type="password")
            db_name = left_credentials.text_input("Database Name")
            db_host = right_credentials.text_input("Host", default_postgresql_host)
            db_port = right_credentials.text_input("Port", default_postgresql_port)
        elif isMySql:
            db_user = left_credentials.text_input("Database Username")
            db_password = left_credentials.text_input("Database Password", type="password")
            db_name = left_credentials.text_input("Database Name")
            db_host = right_credentials.text_input("Host")
            db_port = right_credentials.text_input("Port")
        
        connect_db_btn = st.button("Connect Database")
        
        if connect_db_btn:
            if isMicSql:
                if db_server and db_name:
                    with st.spinner('Connecting to Database. Please wait...'):
                        time.sleep(2)
                    conn = init_connections_micsql(db_server, db_name)
                    if 'db_conn' not in st.session_state:
                        st.session_state['db_conn'] = conn
                    else:
                        st.session_state['db_conn'] = conn
                        
                        return conn
                else:
                    st.error("Please enter server and database name.")
               
            elif isPgSql:
                if db_user and db_password and db_name:
                    with st.spinner('Connecting to Database. Please wait...'):
                        time.sleep(2)
                    conn = init_connections_pgsql(db_host, db_port, db_user, db_password, db_name)

                    if 'db_conn' not in st.session_state:
                        st.session_state['db_conn'] = conn
                    else:
                        st.session_state['db_conn'] = conn                    
                    return conn
                else:
                     st.error("Please complete or correct your credentials.")
            elif isMySql:
                if db_user and db_password and db_name and db_host and db_port:
                    with st.spinner('Connecting to Database. Please wait...'):
                        time.sleep(2)
                    conn = init_connections_mysql(db_host, db_port, db_user, db_password, db_name)

                    if 'db_conn' not in st.session_state:
                        st.session_state['db_conn'] = conn
                    else:
                        st.session_state['db_conn'] = conn
                    return conn
                else:
                     st.error("Please complete or correct your credentials.")
    

def upload_dataset_layout():
    uploaded_file = st.file_uploader("Upload your dataset file please", type=["csv", "txt", "json"])

if action == "🔗 Connect to my database":
    
    db_connection_layout()
    if st.session_state.is_connected_micsql:
        db_placeholder.empty()
        
        conn = st.session_state.db_conn
        cursor = conn.cursor()

        # Execute an SQL query to fetch data from the 'categories' table
        table_query = "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE';"
        cursor.execute(table_query)

        # Fetch all rows
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables]

        
        selected_table = st.selectbox("Select a table:", table_names)

        column_query = f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{selected_table}';"
        cursor.execute(column_query)
        #column_names = [desc[0] for desc in cursor.description]
        columns = cursor.fetchall()
        column_names = [column[3] for column in columns]
        

        if selected_table:
            with st.expander(f"Data for Table: {selected_table}", expanded=True):

                data_query = f"SELECT * FROM {selected_table};"
                cursor.execute(data_query)
                table_data = cursor.fetchall()
                #st.write(list(table_data[0]))
                data = [list(d) for d in table_data]


                if table_data:
                    table_data_df = pd.DataFrame(data, columns=column_names)
                    st.dataframe(table_data_df)

    elif st.session_state.is_connected_pgsql:
        db_placeholder.empty()
        
        conn = st.session_state.db_conn
        cursor = conn.cursor()

        # Execute an SQL query to fetch data from the 'categories' table
        table_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;"
        cursor.execute(table_query)

        # Fetch all rows
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables]

        
        selected_table = st.selectbox("Select a table:", table_names)

        column_query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{selected_table}'"
        
        cursor.execute(column_query)
        #column_names = [desc[0] for desc in cursor.description]
        columns = cursor.fetchall()
        
        column_names = [column[0] for column in columns]

        if selected_table:
            with st.expander(f"Data for Table: {selected_table}", expanded=True):

                data_query = f"SELECT * FROM {selected_table};"
                cursor.execute(data_query)
                table_data = cursor.fetchall()
                #st.write(list(table_data[0]))
                data = [list(d) for d in table_data]


                if table_data:
                    table_data_df = pd.DataFrame(data, columns=column_names)
                    st.dataframe(table_data_df)

    elif st.session_state.is_connected_mysql:
        db_placeholder.empty()
        
        conn = st.session_state.db_conn
        cursor = conn.cursor()

        # Execute an SQL query to fetch data from the 'categories' table
        table_query = "SHOW TABLES"
        cursor.execute(table_query)

        # Fetch all rows
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables]
        
        selected_table = st.selectbox("Select a table:", table_names)

        column_query = f"DESCRIBE {selected_table}"
        
        cursor.execute(column_query)
        #column_names = [desc[0] for desc in cursor.description]
        columns = cursor.fetchall()
        
        column_names = [column[0] for column in columns]

        if selected_table:
            with st.expander(f"Data for Table: {selected_table}", expanded=True):

                data_query = f"SELECT * FROM {selected_table};"
                cursor.execute(data_query)
                table_data = cursor.fetchall()
                #st.write(list(table_data[0]))
                data = [list(d) for d in table_data]


                if table_data:
                    table_data_df = pd.DataFrame(data, columns=column_names)
                    st.dataframe(table_data_df)

elif action == "📤 Upload Dataset":
    st.write("You selected dataset")
    upload_dataset_layout()