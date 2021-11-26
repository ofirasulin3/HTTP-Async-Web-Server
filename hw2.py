# import datetime
import json
import sqlite3
import time
import socket
import hw2_utils


# def check_error(data, conn):
    # all_http_requests = ["GET", "POST", "DELETE", "OPTIONS", "HEAD", "PUT", "CONNECT", "TRACE"]
    #
    # http_data = hw2_utils.decode_http(data)
    # response_proto = 'HTTP/1.1'
    # response_status = ""
    # request = http_data["Request"].split('\n')[0]
    # URL = request.split(' ')[1]
    # request_name = request.split(' ')[0]
    # if request_name not in all_http_requests:
    #     # RETURN BAD REQUEST 400
    #     response_status = '400'
    #     response_message = "BAD REQUEST"
    # if request_name != "GET" or request_name != "POST" or request_name != "DELETE ":
    #     response_status = '501'
    #     response_message = "Not Implemented"
    #
    # # if user is unauthorized or its not the admin, return 401 Udnauthorized
    # # 401 unauthorized
    # # 403 forbidden
    #
    # # 409 conflict
    #
    # # 200 OK
    #
    # response = str.encode(response_proto)
    # response += b' '
    # response += str.encode(response_status)
    # response += b' '
    # response += str.encode(response_message)
    # response += str.encode("\r\n")
    # conn.sendall(response)
    # return response_status


def mime_parsing(key):
    f = open("mime.json", "rt")
    d = json.load(f)
    list = d["mime-mapping"]
    for item in list:
        if item["extension"] == key:
            return item["mime-type"]
    return -1

if __name__ == "__main__":

    # getting parameters from config file
    config_file = open("config.py", "r")
    lines = config_file.readlines()
    timeout_param = lines[1].split(" ")[2]
    SERVER_PORT = lines[0].split(" ")[2]
    admin_username = lines[2].split(" ")[3].split('\'')[1]
    admin_pass = lines[2].split(" ")[5].split('\'')[1]

    # conn_db = sqlite3.connect('users.db')

    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(('localhost', int(SERVER_PORT)))
                s.listen(5)
                conn, addr = s.accept()
                with conn:
                    timeout_start = time.time()
                    while time.time() < timeout_start + timeout_param:
                        data = conn.recv(4096)

                        if not data:
                            # in any other error, return status 500
                            response_status = '500'
                            response = str.encode(response_proto)
                            response += b' '
                            response += str.encode(response_status)
                            response += b' '
                            response += str.encode("EMPTY DATA")
                            response += str.encode("\r\n")
                            conn.sendall(response)
                            break

                        all_http_requests = ["GET", "POST", "DELETE", "OPTIONS", "HEAD", "PUT", "CONNECT", "TRACE"]

                        http_data = hw2_utils.decode_http(data)
                        response_proto = 'HTTP/1.1'
                        response_status = ""
                        request = http_data["Request"].split('\n')[0]
                        URL = request.split(' ')[1]
                        request_type = request.split(' ')[0]

                        is_error = False
                        if request_type not in all_http_requests:
                            is_error = True
                            # RETURN BAD REQUEST 400
                            response_status = '400'
                            response_message = "BAD REQUEST"
                        if request_type != "GET" or request_type != "POST" or request_type != "DELETE ":
                            is_error = True
                            response_status = '501'
                            response_message = "Not Implemented"

                        # if user is unauthorized or its not the admin, return 401 Udnauthorized

                        # POST request is for Creating a User
                        if request_type == "POST":
                            # http_data_example =
                            # "POST /users HTTP/1.1\r\n
                            # header 1\r\n …
                            # Content-Type: application/x-www-form-urlencoded
                            # Authorization: Basic … …
                            # header K\r\n
                            # username=user1&password=1234"
                            username_to_create = http_data[http_data.index("username=")+9:http_data.index("&")]
                            userpass_to_create = http_data[http_data.index("password=")+9:]





                        # 401 unauthorized
                        # 403 forbidden

                        # 409 conflict

                        response = str.encode(response_proto)
                        response += b' '
                        response += str.encode(response_status)
                        response += b' '
                        response += str.encode(response_message)
                        response += str.encode("\r\n")
                        conn.sendall(response)
                        if is_error:
                            break

                        # Here we are at 200 OK



                        extension = "abs"  # TODO: enter here the right desired extension
                        mime_type = mime_parsing(extension)
                        if mime_type == -1:
                            print("Here should be 404 Not Found")

            except:
                # HERE is 500
                response_proto = 'HTTP/1.1'
                response_status = '500'
                response = str.encode(response_proto)
                response += b' '
                response += str.encode(response_status)
                response += b' '
                response += str.encode("Internal Server Error")
                response += str.encode("\r\n")
                conn.sendall(response)