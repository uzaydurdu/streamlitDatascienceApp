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

