import streamlit as st
import mysql.connector
import pyodbc as odbc
import psycopg2
import pandas as pd
import numpy as np
import plotly as plt
import sklearn as sk
import time
import re
from scipy.stats import shapiro
from scipy import stats
from decimal import Decimal

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
               
def get_density(df):
    non_zero_count = np.count_nonzero(df)
    total_count = np.product(df.shape)

    return non_zero_count / total_count

def get_nan(df):
    nan_count = df.isna().sum().sum()

    return nan_count

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

def run_query(conn, query):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        return data
    except Exception as e:
        pass

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

        
        selected_table = st.selectbox("Select a table", table_names)

        column_query = f"SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{selected_table}';"
        cursor.execute(column_query)
        #column_names = [desc[0] for desc in cursor.description]
        columns = cursor.fetchall()
        column_names = [column[3] for column in columns]
        

        if selected_table:
            with st.expander(f"Data for Table: {selected_table}", expanded=True):
                left_table, right_query = st.columns(2)
                data_query = f"SELECT * FROM {selected_table};"
                cursor.execute(data_query)
                table_data = cursor.fetchall()
                #st.write(list(table_data[0]))
                data = [list(d) for d in table_data]


                if table_data:
                    table_data_df = pd.DataFrame(data, columns=column_names)
                    left_table.dataframe(table_data_df)
                    query = right_query.text_area("Run some SQL query here")
                    query_btn = right_query.button("Run Query")

                    if query_btn:
                        result = run_query(conn, query)
                        if result:
                            with st.spinner('Query is running. Please wait...'):
                                time.sleep(2)
                            success_query = right_query.success("Query has been successfully run.")
                            right_query.write(result)
                            time.sleep(4)
                            success_query.empty()
                        else:
                            right_query.error("Please check and correct your SQL query.")
        

        
        

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

        
        selected_table = st.selectbox("Select a table", table_names)

        column_query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{selected_table}'"
        
        cursor.execute(column_query)
        #column_names = [desc[0] for desc in cursor.description]
        columns = cursor.fetchall()
        
        column_names = [column[0] for column in columns]

        if selected_table:
            with st.expander(f"Data for Table: {selected_table}", expanded=True):
                left_table, right_query = st.columns(2)
                data_query = f"SELECT * FROM {selected_table};"
                cursor.execute(data_query)
                table_data = cursor.fetchall()
                #st.write(list(table_data[0]))
                data = [list(d) for d in table_data]


                if table_data:
                    table_data_df = pd.DataFrame(data, columns=column_names)
                    left_table.dataframe(table_data_df)
                    query = right_query.text_area("Run some SQL query here")
                    query_btn = right_query.button("Run Query")

                    if query_btn:
                        result = run_query(conn, query)
                        if result:
                            with st.spinner('Query is running. Please wait...'):
                                time.sleep(2)
                            success_query = right_query.success("Query has been successfully run.")
                            right_query.write(result)
                            time.sleep(4)
                            success_query.empty()
                        else:
                            right_query.error("Please check and correct your SQL query.")

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
        
        selected_table = st.selectbox("Select a table", table_names)

        column_query = f"DESCRIBE {selected_table}"
        
        cursor.execute(column_query)
        #column_names = [desc[0] for desc in cursor.description]
        columns = cursor.fetchall()
        
        column_names = [column[0] for column in columns]

        if selected_table:
            with st.expander(f"Data for Table: {selected_table}", expanded=True):
                left_table, right_query = st.columns(2)
                data_query = f"SELECT * FROM {selected_table};"
                cursor.execute(data_query)
                table_data = cursor.fetchall()
                #st.write(list(table_data[0]))
                data = [list(d) for d in table_data]


                if table_data:
                    table_data_df = pd.DataFrame(data, columns=column_names)
                    left_table.dataframe(table_data_df)
                    query = right_query.text_area("Run some SQL query here")
                    query_btn = right_query.button("Run Query")

                    if query_btn:
                        result = run_query(conn, query)
                        if result:
                            with st.spinner('Query is running. Please wait...'):
                                time.sleep(2)
                            success_query = right_query.success("Query has been successfully run.")
                            right_query.write(result)
                            time.sleep(4)
                            success_query.empty()
                        else:
                            right_query.error("Please check and correct your SQL query.")

            tab_metric1, tab_metric2, tab_metric3, tab_metric4, tab_metric5, tab_metric6 = st.columns(6)

            if table_data:
                tab_metric1.metric("#Rows", value=table_data_df.shape[0])
                tab_metric2.metric("#Attributes", value=table_data_df.shape[1])
                tab_metric3.metric("Total Size", value=table_data_df.size)
                nans = get_nan(table_data_df)
                tab_metric4.metric("#Null Values", value=nans)
                density = get_density(table_data_df)
                tab_metric5.metric("Density", value=density, delta=(1-density))
                tab_metric6.metric("Sparsity", value=(1-density), delta=-density)

        

        if column_names:
            selected_column = st.selectbox("Select a column to see statistical inferences", column_names)
            col_metric1, col_metric2, col_metric3, col_metric4, col_metric5, col_metric6 = st.columns(6)
            col_metric1.metric("#Unique Values", value=table_data_df[selected_column].nunique())
            col_metric2.metric("#Null Values", value=table_data_df[selected_column].isna().sum())
            data_type =  table_data_df.dtypes[selected_column]
            pattern = r'^[0-9]+$'
            if str(data_type) == 'object':
                col_metric3.metric("Column Type", value="string")
                try:
                    value = str(table_data_df[selected_column][0])
                    value = value.replace(".", "").replace(",", "")
                    if value.isdigit():
                        decimal_value = Decimal(value)
                        int_value = int(value)
                        float_value = float(value)
                        num_column = pd.to_numeric(table_data_df[selected_column], errors='coerce')
                    else:
                        decimal_value = False
                        int_value = False
                        float_value = False
                    
                except (ValueError, TypeError):
                    pass
                if decimal_value or int_value or float_value:
                    # The value is numeric
                    try:
                        col_metric4.metric("Numeric", value="Yes")
                        # Perform the Shapiro-Wilk test on the entire column
                        stat, p = shapiro(table_data_df[selected_column].dropna())
                        sig_threshold = 0.05

                        if p > sig_threshold:
                            col_metric5.metric("Data Distribution", value="Normal")
                        else:
                            col_metric5.metric("Data Distribution", value="Non-Normal")

                        col_metric6.metric("Mean", value=table_data_df[selected_column].mean())
                        col_metric1.metric("Most Frequent/Mode", value=float(table_data_df[selected_column].mode()[0]))
                        col_metric2.metric("Median", value=table_data_df[selected_column].median())
                        variance_value = num_column.var()
                        formatted_variance = "{:.2f}".format(variance_value)
                        col_metric3.metric("Variance", value=formatted_variance)
                        std_value = num_column.std()
                        formatted_std = "{:.2f}".format(std_value)
                        col_metric4.metric("Standard Deviation", value=formatted_std)
                        col_metric5.metric("Data Range", value=float(num_column.max() - num_column.min()))
                        iqr_value = num_column.quantile(0.75) - num_column.quantile(0.25)
                        col_metric6.metric("Interquartile Range (IQR)", value=float(iqr_value))
                        skew_value = num_column.skew()
                        formatted_skew = "{:.4f}".format(skew_value)
                        col_metric1.metric("Skewness", value=formatted_skew)
                        kurtosis_value = num_column.kurtosis()
                        formatted_kurtosis = "{:.4f}".format(kurtosis_value)
                        col_metric2.metric("Kurtosis", value=formatted_kurtosis)

                        col_size = len(num_column)
                        if col_size > 10:
                            sample_fraction = 0.2
                            sample_size = int(col_size * sample_fraction)
                            random_sample = num_column.sample(n=sample_size)
                            confidence_interval = stats.norm.interval(0.95, loc=random_sample.mean(), scale=random_sample.std()/np.sqrt(len(random_sample)))
                            col_metric3.metric("Confidence Interval Lower", value=f'{confidence_interval[0]:.2f}')
                            col_metric4.metric("Confidence Interval Upper", value=f'{confidence_interval[1]:.2f}')
                    except ValueError:
                        col_metric4.metric("Numeric", value="No")
                else:
                    # The value is not numeric
                    col_metric4.metric("Numeric", value="No")
            elif str(data_type) == 'int64':
                col_metric3.metric("Column Type", value="integer")
                stat, p = shapiro(table_data_df[selected_column])
                sig_threshold = 0.05
                if p > sig_threshold:
                    col_metric4.metric("Data Distribution", value="Normal")
                else:
                    col_metric4.metric("Data Distribution", value="Non-Normal")

                col_metric5.metric("Mean", value=table_data_df[selected_column].mean())
                col_metric6.metric("Most Frequent/Mode", value=float(table_data_df[selected_column].mode()[0]))
                col_metric1.metric("Median", value=table_data_df[selected_column].median())
                variance_value = table_data_df[selected_column].var()
                formatted_variance = "{:.2f}".format(variance_value)
                col_metric2.metric("Variance", value=formatted_variance)
                std_value = table_data_df[selected_column].std()
                formatted_std = "{:.2f}".format(std_value)
                col_metric3.metric("Standard Deviation", value=formatted_std)
                col_metric4.metric("Data Range", value=float(table_data_df[selected_column].max() - table_data_df[selected_column].min()))
                iqr_value = table_data_df[selected_column].quantile(0.75) - table_data_df[selected_column].quantile(0.25)
                col_metric5.metric("Interquartile Range (IQR)", value=float(iqr_value))
                skew_value = table_data_df[selected_column].skew()
                formatted_skew = "{:.4f}".format(skew_value)
                col_metric6.metric("Skewness", value=formatted_skew)
                kurtosis_value = table_data_df[selected_column].kurtosis()
                formatted_kurtosis = "{:.4f}".format(kurtosis_value)
                col_metric1.metric("Kurtosis", value=formatted_kurtosis)

                col_size = len(table_data_df[selected_column])
                if col_size > 10:
                    sample_fraction = 0.2
                    sample_size = int(col_size * sample_fraction)
                    random_sample = table_data_df[selected_column].sample(n=sample_size)
                    confidence_interval = stats.norm.interval(0.95, loc=random_sample.mean(), scale=random_sample.std()/np.sqrt(len(random_sample)))
                    col_metric2.metric("Confidence Interval Lower", value=f'{confidence_interval[0]:.2f}')
                    col_metric3.metric("Confidence Interval Upper", value=f'{confidence_interval[1]:.2f}')
            elif str(data_type) == 'datetime64[ns]':
                col_metric3.metric("Column Type", value="date")
                date_col = pd.to_datetime(table_data_df[selected_column])
                date_span = date_col.max() - date_col.min()

                col_metric4.metric("Min Date", value=date_col.min())
                col_metric5.metric("Max Date", value=date_col.max())
                col_metric6.metric("Date Span", value=date_span)
            elif str(data_type) == 'float64':
                col_metric3.metric("Column Type", value="float")
                stat, p = shapiro(table_data_df[selected_column])
                sig_threshold = 0.05
                if p > sig_threshold:
                    col_metric4.metric("Data Distribution", value="Normal")
                else:
                    col_metric4.metric("Data Distribution", value="Non-Normal")
            else:
                col_metric3.metric("Column Type", value=str(data_type))
        
            



elif action == "📤 Upload Dataset":
    st.write("You selected dataset")
    upload_dataset_layout()