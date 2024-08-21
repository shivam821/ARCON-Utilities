import streamlit as st
import os
import pyodbc
from datetime import datetime

st.title('DB Executor')

tab1, tab2 = st.tabs(['Backup', 'Upgrade'])

# Function to connect to the MSSQL database
def connect_to_mssql(server, username, password, database=None):
    connection_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'UID={username};'
        f'PWD={password};'
    )
    if database:
        connection_str += f'DATABASE={database};'
    return pyodbc.connect(connection_str)

with tab1:
    st.header('Backup')
    
    backup_server = st.text_input('Enter Server:', key='backup_server')
    backup_username = st.text_input('Enter Username:', key='backup_username')
    backup_password = st.text_input('Enter Password:', type='password', key='backup_password')
    backup_database = st.text_input('Enter Database:', key='backup_database')
    backup_main_file = st.text_input('Enter Backup Folder Path:', key='backup_main_file')
    
    if st.button('Backup'):
        if not backup_server or not backup_username or not backup_password or not backup_database or not backup_main_file:
            st.error('Please enter all the required data.')
        else:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = f"{backup_database}_backup_{timestamp}.bak"
            backup_file_path = os.path.join(backup_main_file, backup_file)
            
            # Ensure the directory exists
            os.makedirs(backup_main_file, exist_ok=True)
            
            def execute_backup():
                try:
                    conn = connect_to_mssql(backup_server, backup_username, backup_password)
                    cursor = conn.cursor()
                    backup_query = f"BACKUP DATABASE [{backup_database}] TO DISK = '{backup_file_path}'"
                    cursor.execute(backup_query)
                    conn.commit()
                    st.success(f'Successfully created backup: {backup_file_path}')
                except Exception as e:
                    st.error(f'Error occurred during backup: {str(e)}')
                finally:
                    conn.close()
            
            execute_backup()

with tab2:
    st.header('Upgrade')
    
    upgrade_server = st.text_input('Enter Server:', key='upgrade_server')
    upgrade_username = st.text_input('Enter Username:', key='upgrade_username')
    upgrade_password = st.text_input('Enter Password:', type='password', key='upgrade_password')
    upgrade_database = st.text_input('Enter Database:', key='upgrade_database')
    upgrade_main_file = st.text_input('Enter Folder Path:', key='upgrade_main_file')
    
    if st.button('Upgrade'):
        if not upgrade_server or not upgrade_username or not upgrade_password or not upgrade_database or not upgrade_main_file:
            st.error('Please enter all the required data.')
        else:
            def execute_sql_file(conn, file_path):
                try:
                    with open(file_path, 'r') as file:
                        sql_script = file.read()
                    cursor = conn.cursor()
                    cursor.execute(sql_script)
                    conn.commit()
                    st.success(f'Successfully executed {file_path}')
                except Exception as e:
                    st.error(f'Error occurred while executing {file_path}: {str(e)}')
            
            try:
                conn = connect_to_mssql(upgrade_server, upgrade_username, upgrade_password, upgrade_database)
                file_execution = False
                for root, dirs, files in os.walk(upgrade_main_file):
                    for file in files:
                        if file.endswith('.sql'):
                            file_path = os.path.join(root, file)
                            execute_sql_file(conn, file_path)
                            file_execution = True
                
                if not file_execution:
                    st.error('No SQL files found or execution was unsuccessful.')
                else:
                    st.success('Upgrade successful')
            except Exception as e:
                st.error(f'Error during upgrade process: {str(e)}')
            finally:
                conn.close()
