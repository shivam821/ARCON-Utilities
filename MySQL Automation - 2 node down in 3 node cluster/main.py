import subprocess
import time
import socket
import mysql.connector
from mysql.connector import Error
import os

# Global variables
production_database_server_ip = input('Enter the production database server IP: ')
high_availability_database_ip = input('Enter the high availability database IP: ')
near_dr_database_ip = input('Enter the near DR database IP: ')
dr_database_ip = input('Enter the DR database IP: ')

print()

database_cluster_1 = [production_database_server_ip, high_availability_database_ip, near_dr_database_ip]
database_cluster_2 = [dr_database_ip]

check_interval = 60
log_file_path = 'logs/log.log'
current_primary_file = '/tmp/current_primary_node'

def ensure_log_folder_exists():
    log_folder = os.path.dirname(log_file_path)
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)

def log_event(message):
    ensure_log_folder_exists()
    with open(log_file_path, 'a') as f:
        f.write(f'{time.strftime("%Y-%m-%d %H:%M:%S")} - {message}\n')
    print(message)  # Optional: Print to console

def get_hostname_ip(ip_address):
    try:
        hostname_of_ip = socket.gethostbyaddr(ip_address)
        return hostname_of_ip[0]
    except socket.herror as e:
        return f'Error: {e}'

def connect_mysql_database_server(host, port, user, password):
    try:
        connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password
        )
        if connection.is_connected():
            log_event(f'Connected to server {host}')
            return connection
        else:
            log_event(f'Not able to connect to server {host}')
            return None
    except Error as e:
        log_event(f'Not able to connect to server {host}: {e}')
        return None

def ping_database_cluster(server):
    result = subprocess.run(['ping', '-c', '1', server], capture_output=True, text=True)
    if result.returncode == 0:
        return True, result.stdout
    else:
        return False, result.stderr

def promotion_commands(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("SET GLOBAL read_only = OFF;")
        cursor.execute("STOP SLAVE; RESET SLAVE ALL;")
        connection.commit()
        cursor.close()
        log_event('Promotion commands executed successfully.')
    except Error as e:
        log_event(f'Error executing promotion commands: {e}')

def demotion_commands(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("SET GLOBAL read_only = ON;")
        cursor.execute("STOP SLAVE; RESET SLAVE ALL;")
        connection.commit()
        cursor.close()
        log_event('Demotion commands executed successfully.')
    except Error as e:
        log_event(f'Error executing demotion commands: {e}')

def failover_to_secondary(server_ip):
    connection = connect_mysql_database_server(server_ip, 3306, 'root', 'root@123')
    if connection:
        demotion_commands(connection)
        log_event(f'Demoted {server_ip} to secondary')
        connection.close()

def failback_to_primary(server_ip):
    connection = connect_mysql_database_server(server_ip, 3306, 'root', 'root@123')
    if connection:
        promotion_commands(connection)
        log_event(f'Promoted {server_ip} to primary')
        connection.close()

def track_current_primary():
    if os.path.exists(current_primary_file):
        with open(current_primary_file, 'r') as f:
            return f.read().strip()
    return None

def update_current_primary(ip):
    with open(current_primary_file, 'w') as f:
        f.write(ip)

def main_1():
    all_down_database_cluster_1 = True
    for i in database_cluster_1:
        success, output = ping_database_cluster(i)
        if success:
            log_event(f'Successfully pinged server: {i}')
            log_event(output)
            display_hostname = get_hostname_ip(i)
            log_event(f'The hostname for IP address {i} is {display_hostname}')
            all_down_database_cluster_1 = False
        else:
            log_event(f'Failed to ping server: {i}')
            log_event(output)
            display_hostname = get_hostname_ip(i)
            log_event(f'The hostname for IP address {i} is {display_hostname}')
        
    current_primary = track_current_primary()

    if all_down_database_cluster_1:
        log_event('Database Cluster 1 is Down')
        if current_primary and current_primary in database_cluster_1:
            failover_to_secondary(current_primary)
        
        log_event('Failover actions starting')
        failback_to_primary(dr_database_ip)
        update_current_primary(dr_database_ip)
        
    else:
        log_event('Database Cluster 1 is Up')
        if current_primary == dr_database_ip:
            failback_to_primary(production_database_server_ip)
            failback_to_primary(near_dr_database_ip)
            update_current_primary(production_database_server_ip)

def main_2():
    all_down_database_cluster_2 = True
    for i in database_cluster_2:
        success, output = ping_database_cluster(i)
        if success:
            log_event(f'Successfully pinged server: {i}')
            log_event(output)
            display_hostname = get_hostname_ip(i)
            log_event(f'The hostname for IP address {i} is {display_hostname}')
            all_down_database_cluster_2 = False
        else:
            log_event(f'Failed to ping server: {i}')
            log_event(output)
            display_hostname = get_hostname_ip(i)
            log_event(f'The hostname for IP address {i} is {display_hostname}')

    current_primary = track_current_primary()

    if all_down_database_cluster_2:
        log_event('Database Cluster 2 is Down')
        if current_primary and current_primary == dr_database_ip:
            failback_to_primary(production_database_server_ip)
            failback_to_primary(near_dr_database_ip)
            failover_to_secondary(dr_database_ip)
        
    else:
        log_event('Database Cluster 2 is Up')
        if current_primary == dr_database_ip:
            failback_to_primary(dr_database_ip)
            update_current_primary(None)

while True:
    main_1()
    main_2()
    time.sleep(check_interval)
