import streamlit as st
import subprocess
import os
from datetime import datetime

def install_sqlserver_module():
    try:
        # Absolute path to PowerShell
        powershell_path = r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"
        
        # Install the SQLServer module if not already installed
        install_command = "Install-Module -Name SqlServer -Force -AllowClobber -Scope CurrentUser"
        result = subprocess.run([powershell_path, "-Command", install_command], capture_output=True, text=True)
        
        if result.returncode == 0:
            st.write("SqlServer module installed successfully.")
        else:
            st.error(f"Error installing SqlServer module: {result.stderr}")
    except Exception as e:
        st.error(f"Exception occurred during module installation: {str(e)}")

def perform_backup(server, username, password, database, backup_path):
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = os.path.join(backup_path, f"{database}_backup_{timestamp}.bak")
        
        powershell_command = f"""
        $secure_password = ConvertTo-SecureString '{password}' -AsPlainText -Force;
        $credential = New-Object System.Management.Automation.PSCredential ('{username}', $secure_password);
        Invoke-Sqlcmd -ServerInstance '{server}' -Database '{database}' -Credential $credential -Query "BACKUP DATABASE [{database}] TO DISK = '{backup_file}'";
        """
        result = subprocess.run([powershell_path, "-Command", powershell_command], capture_output=True, text=True)
        
        if result.returncode == 0:
            st.success(f"Successfully created backup: {backup_file}")
        else:
            st.error(f"Error occurred during backup: {result.stderr}")
    except Exception as e:
        st.error(f"Exception occurred during backup: {str(e)}")

def execute_sql_files(server, username, password, database, folder_path):
    try:
        powershell_command_template = f"""
        $secure_password = ConvertTo-SecureString '{password}' -AsPlainText -Force;
        $credential = New-Object System.Management.Automation.PSCredential ('{username}', $secure_password);
        Invoke-Sqlcmd -ServerInstance '{server}' -Database '{database}' -Credential $credential -InputFile "{{file_path}}";
        """
        
        file_execution = False
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.endswith('.sql'):
                    powershell_command = powershell_command_template.replace("{{file_path}}", os.path.join(root, file))
                    result = subprocess.run([powershell_path, "-Command", powershell_command], capture_output=True, text=True)
                    
                    if result.returncode == 0:
                        st.success(f"Successfully executed {file}")
                        file_execution = True
                    else:
                        st.error(f"Error occurred while executing {file}: {result.stderr}")
        
        if not file_execution:
            st.error('No SQL files found or execution was unsuccessful.')
        else:
            st.success('Upgrade successful')
    except Exception as e:
        st.error(f"Exception occurred during SQL execution: {str(e)}")

# Streamlit App
st.title('DB Executor')

# Install the SqlServer module at the start
install_sqlserver_module()

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
            os.makedirs(backup_main_file, exist_ok=True)
            perform_backup(backup_server, backup_username, backup_password, backup_database, backup_main_file)

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
            execute_sql_files(upgrade_server, upgrade_username, upgrade_password, upgrade_database, upgrade_main_file)
