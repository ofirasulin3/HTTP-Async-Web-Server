import datetime
import time
import socket



def check_error(data,conn):

    all_http_requests = ["GET", "POST", "DELETE", "OPTIONS", "HEAD","PUT","CONNECT" ,"TRACE"]


    try:
        http_data = hw1_utils.decode_http(data)
        response_proto = 'HTTP/1.1'
        response_status = ""
        request = http_data["Request"].split('\n')[0]
        URL = request.split(' ')[1]
        request_name = request.split(' ')[0]
        if request_name not in all_http_requests:
            # RETURN BAD REQUEST 400
            response_status = '400'
            response = str.encode(response_proto)
            response += b' '
            response += str.encode(response_status)
            response += b' '
            response += str.encode("BAD REQUEST")
            response += str.encode("\r\n")
            conn.sendall(response)
            return 400
        if request_name != "GET" or request_name != "POST" or request_name != "DELETE ":
            response_status = '501'
            response = str.encode(response_proto)
            response += b' '
            response += str.encode(response_status)
            response += b' '
            response += str.encode("Not Implemented")
            response += str.encode("\r\n")
            conn.sendall(response)
            return 501
    except:
        #HERE is 500
        response_status = '500'
        response = str.encode(response_proto)
        response += b' '
        response += str.encode(response_status)
        response += b' '
        response += str.encode("Internal Server Error")
        response += str.encode("\r\n")
        conn.sendall(response)
        return 500


if __name__ == "__main__":

    config_file  = open("config.py", "r")
    timeout_param = config_file.readlines()[1].split(" ")[2]



    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('localhost', SERVER_PORT))
            s.listen(5)
            conn, addr = s.accept()
            with conn:
                timeout_start = time.time()
                while time.time() < timeout_start + timeout_param:
                    data = conn.recv(DATA_MAX_SIZE)








