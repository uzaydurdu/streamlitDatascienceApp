import mysql.connector 
import streamlit as st

connection = mysql.connector.connect(
    host="localhost",
    port="3306",
    user="root",
    password="33VR46ci_21",
    database="sql_hr"
)

curs = connection.cursor()

def display_table_rows():

    query = f"SELECT * FROM accounts"
    curs.execute('select * from accounts')
    data = curs.fetchall()
    return data

def display_tables(conn):
    cursor = conn.cursor()

    # Execute an SQL query to fetch data from the 'categories' table
    table_query = "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE';"
    cursor.execute(table_query)

    # Fetch all rows
    tables = cursor.fetchall()
    table_names = [table[0] for table in tables]

    
    
    # Use st.selectbox for table selection
    selected_table = st.selectbox("Select a table:", table_names)

    if selected_table:
        with st.expander(f"Data for Table: {selected_table}"):
            # Define a function to load table data dynamically
            data_query = f"SELECT * FROM {selected_table};"
            cursor.execute(data_query)
            table_data = cursor.fetchall()

            # Create a DataFrame from the data
            if table_data:
                table_data_df = pd.DataFrame(table_data)
                st.dataframe(table_data_df)

def init_connections_mysql():
    try:
        connection = mysql.connector.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name
        )
        return connection
    except mysql.connector.Error as err:
        st.error(f"Database connection error: {err}")

def init_connections_pgsql():
    try:
        connection = psycopg2.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name
        )
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
        return conn
    except odbc.Error as err:
        st.error(f"Database connection error: {err}")

def db_form():
    with db_placeholder.container():
        st.subheader('Connect Database')
        col1, col2 = st.columns(2)
        with col2:
            db_action = st.selectbox("Choose your database:", ["Microsoft SQL Server", "PostgreSQL", "MySQL"])
            if db_action != "Microsoft SQL Server" and db_action != "PostgreSQL":
                db_host = st.text_input("Host")
                db_port = st.text_input("Port")
            if db_action == "PostgreSQL":
                db_host = st.text_input("Host", default_postgresql_host)
                db_port = st.text_input("Port", default_postgresql_port)
        # Column 1: Database User and Password
        with col1:
            if db_action != "Microsoft SQL Server":
                db_user = st.text_input("Database Username")
                db_password = st.text_input("Database Password", type="password")
                db_name = st.text_input("Database Name")
            else:
                db_server = st.text_input("Server Name")
                db_name = st.text_input("Database Name")
        conn_button = st.button("Connect Database")
        if conn_button:
            if db_action == "Microsoft SQL Server":
                if db_server and db_name:
                    conn = init_connections_micsql(db_server, db_name)
                    st.write("Connection successful")
                    return conn
                else:
                    st.error("Please enter server and database name.")
            elif db_action == "PostgreSQL":
                if db_user and db_password and db_name and db_host:
                    conn = init_connections_pgsql()
                    display_tables(conn)
                    st.write("Connection successful")
                else:
                    st.error("Please enter all required information.")
            elif db_action == "MySQL":
                if db_user and db_password and db_name and db_host and db_port:
                    conn = init_connections_mysql()
                    display_tables(conn)
                    st.write("Connection successful")
                else:
                    st.error("Please enter all required information.")

with st.sidebar:
    st.write("This is your sidebar content")

if action == "Connect to my database":
    conn = db_form()
    if conn:
        display_tables(conn)

elif action == "Upload Dataset":
    # File uploader for dataset upload
    uploaded_file = st.file_uploader("Upload your dataset file", type=["csv", "txt", "json"])

    if uploaded_file is not None:
        # Process the uploaded dataset
        st.write(f"File uploaded: {uploaded_file.name}")