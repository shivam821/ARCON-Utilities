import subprocess
import time
import socket
import mysql.connector
from mysql.connector import Error

# Global variables
production_database_server_ip = input('Enter the production database server IP: ')
high_availability_database_ip = input('Enter the high availability database IP: ')
near_dr_database_ip = input('Enter the near DR database IP: ')
dr_database_ip = input('Enter the DR database IP: ')

print()

database_cluster_1 = [production_database_server_ip, high_availability_database_ip, near_dr_database_ip]
database_cluster_2 = [dr_database_ip]

check_interval = 60

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
            print(f'Connected to server {host}')
            return connection
        else:
            return None
    except Error as e:
        print(f'Not able to connect server {host}: {e}')
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
        print('Promotion commands executed successfully.')
    except Error as e:
        print(f'Error executing promotion commands: {e}')

def demotion_commands(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("SET GLOBAL read_only = ON;")
        cursor.execute("STOP SLAVE; RESET SLAVE ALL;")
        connection.commit()
        cursor.close()
        print('Demotion commands executed successfully.')
    except Error as e:
        print(f'Error executing demotion commands: {e}')

def failover_to_secondary_production_database_server():
    connection = connect_mysql_database_server(production_database_server_ip, 3306, 'root', 'root@123')
    if connection:
        demotion_commands(connection)
        print(f'Demoted {production_database_server_ip} to secondary')
        connection.close()

def failover_to_secondary_near_dr_database_server():
    connection = connect_mysql_database_server(near_dr_database_ip, 3306, 'root', 'root@123')
    if connection:
        demotion_commands(connection)
        print(f'Demoted {near_dr_database_ip} to secondary')
        connection.close()

def failover_to_secondary_dr_database_server():
    connection = connect_mysql_database_server(dr_database_ip, 3306, 'root', 'root@123')
    if connection:
        demotion_commands(connection)
        print(f'Demoted {dr_database_ip} to secondary')
        connection.close()

def failback_to_primary_production_database_server():
    connection = connect_mysql_database_server(production_database_server_ip, 3306, 'root', 'root@123')
    if connection:
        promotion_commands(connection)
        print(f'Promoted {production_database_server_ip} to primary')
        connection.close()

def failback_to_primary_near_dr_database_server():
    connection = connect_mysql_database_server(near_dr_database_ip, 3306, 'root', 'root@123')
    if connection:
        promotion_commands(connection)
        print(f'Promoted {near_dr_database_ip} to primary')
        connection.close()

def failback_to_primary_dr_database_server():
    connection = connect_mysql_database_server(dr_database_ip, 3306, 'root', 'root@123')
    if connection:
        promotion_commands(connection)
        print(f'Promoted {dr_database_ip} to primary')
        connection.close()

def main_1():
    all_down_database_cluster_1 = True
    for i in database_cluster_1:
        success, output = ping_database_cluster(i)
        if success:
            print(f'Successfully pinged server: {i}')
            display_hostname = get_hostname_ip(i)
            print(f'The hostname for IP address {i} is {display_hostname}')
            all_down_database_cluster_1 = False
        else:
            print(f'Failed to ping server: {i}')
            display_hostname = get_hostname_ip(i)
            print(f'The hostname for IP address {i} is {display_hostname}')
        
    if all_down_database_cluster_1:
        print('Database Cluster 1 is Down')
        # Connect to servers to ensure availability
        connect_mysql_database_server(production_database_server_ip, 3306, 'root', 'root@123')
        connect_mysql_database_server(near_dr_database_ip, 3306, 'root', 'root@123')

        print()
        failover_to_secondary_production_database_server()
        failover_to_secondary_near_dr_database_server()
        failback_to_primary_dr_database_server()
        print()
    else:
        print('Database Cluster 1 is Up')
        connect_mysql_database_server(production_database_server_ip, 3306, 'root', 'root@123')
        connect_mysql_database_server(near_dr_database_ip, 3306, 'root', 'root@123')
        print()

def main_2():
    all_down_database_cluster_2 = True
    for i in database_cluster_2:
        success, output = ping_database_cluster(i)
        if success:
            print(f'Successfully pinged server: {i}')
            display_hostname = get_hostname_ip(i)
            print(f'The hostname for IP address {i} is {display_hostname}')
            all_down_database_cluster_2 = False
        else:
            print(f'Failed to ping server: {i}')
            display_hostname = get_hostname_ip(i)
            print(f'The hostname for IP address {i} is {display_hostname}')

    if all_down_database_cluster_2:
        print('Database Cluster 2 is Down')
        connect_mysql_database_server(dr_database_ip, 3306, 'root', 'root@123')
        print()
        print('Demoting Database Cluster 2 to secondary')
        print()
        failback_to_primary_production_database_server()
        failback_to_primary_near_dr_database_server()
        failover_to_secondary_dr_database_server()
    else:
        print('Database Cluster 2 is Up')
        connect_mysql_database_server(dr_database_ip, 3306, 'root', 'root@123')
        print()

while True:
    main_1()
    main_2()
    time.sleep(check_interval)
