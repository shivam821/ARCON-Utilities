import streamlit as st
import os
import subprocess
from datetime import datetime

st.title('DB Executor')

tab1, tab2 = st.tabs(['Backup', 'Upgrade'])

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
                # Command with properly quoted path
                command = f"sqlcmd -S {backup_server} -U {backup_username} -P {backup_password} -Q \"BACKUP DATABASE [{backup_database}] TO DISK='{backup_file_path}'\""
                try:
                    result = subprocess.run(command, shell=True, text=True, capture_output=True, check=True)
                    st.success(f'Successfully created backup: {backup_file_path}')
                    print(result.stdout)
                except subprocess.CalledProcessError as e:
                    st.error(f'Error occurred during backup: {e.stderr}')
                    print(f'Error occurred during backup: {e.stderr}')
            
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
            def execute_sql_file(file_path):
                command = f"sqlcmd -S {upgrade_server} -U {upgrade_username} -P {upgrade_password} -d {upgrade_database} -i \"{file_path}\""
                try:
                    result = subprocess.run(command, shell=True, text=True, capture_output=True, check=True)
                    st.success(f'Successfully executed {file_path}')
                    print(result.stdout)
                except subprocess.CalledProcessError as e:
                    st.error(f'Error occurred while executing {file_path}: {e.stderr}')
                    print(f'Error occurred while executing {file_path}: {e.stderr}')
            
            file_execution = False
            for root, dirs, files in os.walk(upgrade_main_file):
                for file in files:
                    if file.endswith('.sql'):
                        file_path = os.path.join(root, file)
                        execute_sql_file(file_path)
                        file_execution = True
            
            if not file_execution:
                st.error('No SQL files found or execution was unsuccessful.')
            else:
                st.success('Upgrade successful')
