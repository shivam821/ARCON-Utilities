import os
import subprocess
import streamlit as st
from datetime import datetime

def download_and_install_sqlcmd():
    # Check if sqlcmd is already installed
    try:
        result = subprocess.run(["sqlcmd", "-?"], capture_output=True, text=True, check=True)
        st.write("sqlcmd is already installed.")
    except subprocess.CalledProcessError:
        # Download the SQL Server Command Line Tools from the official Microsoft website
        url = "https://go.microsoft.com/fwlink/?linkid=2163764"
        installer_path = "sqlcmd.msi"
        
        # Download the installer
        st.write("Downloading sqlcmd installer...")
        os.system(f"curl -L -o {installer_path} {url}")
        
        # Run the installer
        st.write("Installing sqlcmd...")
        os.system(f"msiexec /i {installer_path} /quiet")
        
        st.write("sqlcmd installed successfully.")

# Ensure sqlcmd is installed before proceeding
download_and_install_sqlcmd()

st.title('DB Executor')

tab1, tab2 = st.tabs(['Backup', 'Upgrade'])

def test_sql_connection(server, username, password):
    command = f"sqlcmd -S {server} -U {username} -P {password} -Q \"SELECT @@VERSION\""
    try:
        result = subprocess.run(command, shell=True, text=True, capture_output=True, check=True)
        st.success("Successfully connected to the SQL Server.")
        st.text(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        st.error(f"Connection failed: {e.stderr}")
        return False

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
            # Test the SQL Server connection
            if test_sql_connection(backup_server, backup_username, backup_password):
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
                        st.text(result.stdout)
                    except subprocess.CalledProcessError as e:
                        st.error(f'Error occurred during backup: {e.stderr}')
                        st.text(f'Error occurred during backup: {e.stderr}')
                
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
            # Test the SQL Server connection
            if test_sql_connection(upgrade_server, upgrade_username, upgrade_password):
                def execute_sql_file(file_path):
                    command = f"sqlcmd -S {upgrade_server} -U {upgrade_username} -P {upgrade_password} -d {upgrade_database} -i \"{file_path}\""
                    try:
                        result = subprocess.run(command, shell=True, text=True, capture_output=True, check=True)
                        st.success(f'Successfully executed {file_path}')
                        st.text(result.stdout)
                    except subprocess.CalledProcessError as e:
                        st.error(f'Error occurred while executing {file_path}: {e.stderr}')
                        st.text(f'Error occurred while executing {file_path}: {e.stderr}')
                
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
