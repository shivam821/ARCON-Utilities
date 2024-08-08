import streamlit as st
import os
import subprocess
from datetime import datetime

st.title('DB Executor')

# Create a sidebar for navigation
option = st.sidebar.selectbox('Select Tab', ['Backup', 'Upgrade'])

if option == 'Backup':
    st.header('Backup')
    
    backup_server_ip = st.text_input('Enter MySQL Server IP:', key='backup_server_ip')
    backup_server_port = st.text_input('Enter MySQL Server Port:', key='backup_server_port', value='3306')
    backup_username = st.text_input('Enter Username:', key='backup_username')
    backup_password = st.text_input('Enter Password:', type='password', key='backup_password')
    backup_database = st.text_input('Enter Database:', key='backup_database')
    backup_main_file = st.text_input('Enter Backup Folder Path:', key='backup_main_file')
    
    if st.button('Backup'):
        if not backup_server_ip or not backup_server_port or not backup_username or not backup_password or not backup_database or not backup_main_file:
            st.error('Please enter all the required data.')
        else:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = f"{backup_database}_backup_{timestamp}.sql"
            backup_file_path = os.path.join(backup_main_file, backup_file)
            
            # Ensure the directory exists
            os.makedirs(backup_main_file, exist_ok=True)
            
            def execute_backup():
                # Replace with the full path to mysqldump if necessary
                command = f"/usr/bin/mysqldump -h {backup_server_ip} -P {backup_server_port} -u {backup_username} -p{backup_password} {backup_database} > \"{backup_file_path}\""
                try:
                    result = subprocess.run(command, shell=True, text=True, capture_output=True, check=True)
                    st.success(f'Successfully created backup: {backup_file_path}')
                except subprocess.CalledProcessError as e:
                    st.error(f'Error occurred during backup: {e.stderr}')
                
            execute_backup()

elif option == 'Upgrade':
    st.header('Upgrade')
    
    upgrade_server_ip = st.text_input('Enter MySQL Server IP:', key='upgrade_server_ip')
    upgrade_server_port = st.text_input('Enter MySQL Server Port:', key='upgrade_server_port', value='3306')
    upgrade_username = st.text_input('Enter Username:', key='upgrade_username')
    upgrade_password = st.text_input('Enter Password:', type='password', key='upgrade_password')
    upgrade_database = st.text_input('Enter Database:', key='upgrade_database')
    upgrade_main_file = st.text_input('Enter Folder Path:', key='upgrade_main_file')
    
    if st.button('Upgrade'):
        if not upgrade_server_ip or not upgrade_server_port or not upgrade_username or not upgrade_password or not upgrade_database or not upgrade_main_file:
            st.error('Please enter all the required data.')
        else:
            def execute_sql_file(file_path):
                # Command with remote server details
                command = f"mysql -h {upgrade_server_ip} -P {upgrade_server_port} -u {upgrade_username} -p{upgrade_password} {upgrade_database} < \"{file_path}\""
                try:
                    result = subprocess.run(command, shell=True, text=True, capture_output=True, check=True)
                    st.success(f'Successfully executed {file_path}')
                except subprocess.CalledProcessError as e:
                    st.error(f'Error occurred while executing {file_path}: {e.stderr}')
            
            file_execution = False
            for root, dirs, files in os.walk(upgrade_main_file):
                for file in files:
                    if file.endswith('.sql'):
                        file_path = os.path.join(root, file)
                        st.write(f'Processing file: {file_path}')  # Log file being processed
                        execute_sql_file(file_path)
                        file_execution = True
            
            if not file_execution:
                st.error('No SQL files found or execution was unsuccessful.')
            else:
                st.success('Upgrade successful')
