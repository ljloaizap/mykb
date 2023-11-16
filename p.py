import socket


def use_socket_with_port():
    '''PH'''
    host = '127.0.0.1'  # Replace with the target host or IP address
    port = 5432  # Replace with the target port

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)  # Set a timeout for the connection attempt
            s.connect((host, port))
        print(f"Service is available on {host}:{port}")
    except (ConnectionRefusedError, socket.timeout):
        print(f"Service is NOT available on {host}:{port}")


def use_socket():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)  # Set a timeout for the connection attempt
            # Use port 0 for a connection attempt without specifying a port
            s.connect(('127.0.0.1', 0))
        return True  # Connection succeeded, service is available
    except (ConnectionRefusedError, socket.timeout):
        return False  # Connection


use_socket()
