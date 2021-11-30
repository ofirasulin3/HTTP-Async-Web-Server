import datetime
import json
import time
import socket
import hw2_utils
import base64
import os


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
from hw1_util import check_if_file_exists
from template_parser import parsing


def mime_parsing(key):
    f = open("mime.json", "rt")
    d = json.load(f)
    list = d["mime-mapping"]
    for item in list:
        if item["extension"] == key:
            return item["mime-type"]
    return -1

if __name__ == "__main__":

    parsing("example.dp", "david", True)
    # f_str = open(parsed,"r")
    f_str = open("gen.py", "r")

    f_str = f_str.read()
    # f_str2 = "print('david: ')"
    # print("eval2: \n", eval(f_str2))
    eval(f_str)
    # print("eval: \n", eval(f_str))

    # getting parameters from config file
    config_file = open("config.py", "r")
    lines = config_file.readlines()
    timeout_param = lines[1].split(" ")[2]
    SERVER_PORT = lines[0].split(" ")[2]
    admin_username = lines[2].split(" ")[3].split('\'')[1]
    admin_pass = lines[2].split(" ")[5].split('\'')[1]
    pre_encoded = admin_username+":"+admin_pass
    encoded = base64.b64encode(pre_encoded.encode())
    # print("decoded admin credentials: ", base64.b64decode(encoded))
    # print(b'YWRtaW46YWRtaW4=' == encoded)

    # conn_db = sqlite3.connect('users.db')
    # str_test = "DELETE /users/david HTTP/1.1\r\n"
    # username_to_delete = str_test.split(' ')[1].split('/')[-1]
    # print(username_to_delete)

    # print(hw2_utils.user_credentials_valid("user555", "12345"))

    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(('localhost', int(SERVER_PORT)))
                s.listen(5)
                conn, addr = s.accept()
                with conn:
                    timeout_start = time.time()
                    while time.time() < timeout_start + float(timeout_param):
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
                            if request.split(' ')[1] != "/users":
                                response_status = '501'
                                response_message = "Not Implemented"
                                print("501 Not Implemented")

                            # http_data_example =
                            # "POST /users HTTP/1.1\r\n
                            # header 1\r\n …
                            # Content-Type: application/x-www-form-urlencoded
                            # Authorization: Basic … …
                            # header K\r\n
                            # username=user1&password=1234"
                            username_to_handle = http_data['body'][http_data['body'].index("username=") + 9:http_data['body'].index("&")]
                            print("username_to_create: ", username_to_handle)
                            userpass_to_handle = http_data['body'][http_data['body'].index("password=") + 9:]
                            print("userpass_to_create: ", userpass_to_handle)

                            auth_value = http_data['Authorization']
                            if auth_value:  # TODO: there is no check like this. it sends KeyError and exits.
                                # meaning there is credentials
                                basic_str = "Basic "
                                encoded = auth_value[len(basic_str):]

                                decoded = base64.b64decode(encoded)
                                admin_username_to_check = decoded.split(b':')[0].decode()
                                # print("admin_username_to_check:", admin_username_to_check)
                                admin_password_to_check = decoded.split(b':')[1].decode()
                                # print("admin_password_to_check:", admin_password_to_check)
                                if admin_username_to_check == admin_username \
                                        and admin_password_to_check == admin_pass:
                                    # create the user!
                                    # add username_to_handle and userpass_to_handle to DB
                                    # print("create the user! add to the database")
                                    if hw2_utils.user_exists(username_to_handle):
                                        print("User already exists")
                                        print("Error 409 Conflict")
                                    elif not hw2_utils.user_insert(username_to_handle, userpass_to_handle):
                                        print("DB Error")
                                        print("print 500 Internal Server Error")
                                    else:
                                        print("User Inserted!")
                                        print("Sanity Check: user_exists? ", hw2_utils.user_exists(username_to_handle))
                                else:
                                    # invalid admin credentials
                                    print("invalid admin credentials! send the right error (401/403)")

                            else:  # meaning there are no credentials
                                # In case authorization field doesn't exist,
                                # we will send a response with a form for authentication:
                                response = str.encode(response_proto)
                                response += b' '
                                response_status = '401'
                                response += str.encode(response_status)
                                response += b' '
                                response += str.encode("Unauthorized\r\n")
                                response += b'\r\n'

                                current_date = datetime.datetime.now()
                                response_headers_date = current_date.strftime("%d-%b-%Y (%H:%M:%S.%f)")
                                # response_headers_content_type = "Content-Type: image/png"
                                response_headers_content_len = "Content-Length: 0"
                                response_headers = response_headers_date + "\r\n" + response_headers_content_len + "\r\n"
                                response += str.encode(response_headers)
                                response += str.encode("Connection: keep-alive\r\n")
                                response += str.encode("WWW-Authenticate: Basic realm=\"HW2 realm\"\r\n")
                                response += b'\r\n'  # to separate headers from body
                                conn.sendall(response)

                        if request_type == "DELETE":
                            # DELETE /users/<username> HTTP/1.1\r\n
                            # header
                            # 1\r\n
                            # Authorization: Basic … …
                            # header # K\r\n
                            username_to_delete = http_data['Request'].split(' ')[1].split('/')[-1]
                            print(username_to_delete)
                            auth_value = http_data['Authorization']
                            if auth_value:  # TODO: there is no check like this. it sends KeyError and exits.
                                # print("meaning there is credentials")
                                basic_str = "Basic "
                                encoded = auth_value[len(basic_str):]

                                decoded = base64.b64decode(encoded)
                                admin_username_to_check = decoded.split(b':')[0].decode()
                                admin_password_to_check = decoded.split(b':')[1].decode()
                                if admin_username_to_check == admin_username \
                                        and admin_password_to_check == admin_pass:
                                    # Check if the user exists on DB.
                                    # if user doesn't exists return Error 409 Conflict?
                                    if not hw2_utils.user_exists(username_to_delete):
                                        print(username_to_delete)
                                        print("User:" + username_to_delete + "Doesn't exists")
                                        print("Error 409 Conflict")
                                    # if exists, delete the user! delete username_to_handle from DB
                                    elif not hw2_utils.user_delete(username_to_delete):
                                        print("DB Error")
                                        print("print 500 Internal Server Error")
                                    else:  # deleted successfully
                                        print("send 200 OK")

                                else:
                                    # invalid admin credentials
                                    print("invalid admin credentials! send the right error (401/403)")



                        if request_type == "GET":
                            filename_path = request.split(' ')[1][1:]
                            file = check_if_file_exists(filename_path)
                            if not file:
                                response_status = '404'
                                response = str.encode(response_proto)
                                response += b' '
                                response += str.encode(response_status)
                                response += b' '
                                response += str.encode("Not Found\r\n")
                                conn.sendall(response)
                                break
                            file_extension = filename_path.split('.')[1]
                            file_content_type = mime_parsing(file_extension)

                            if file_extension == "dp":
                                auth_value = http_data['Authorization']
                                if auth_value:
                                    # meaning there is credentials
                                    basic_str = "Basic "
                                    encoded = auth_value[len(basic_str):]
                                    decoded = base64.b64decode(encoded)
                                    admin_username_to_check = decoded.split(b':')[0].decode()
                                    # print("admin_username_to_check:", admin_username_to_check)
                                    admin_password_to_check = decoded.split(b':')[1].decode()
                                    if not hw2_utils.user_credentials_valid(admin_username_to_check, admin_password_to_check):
                                        print("USER DOES NOT EXISTS / WRONG CREDENTIALS")
                                    else:

                                        print("ADD SOME LOGIC TO DYNAMIC PAGES")





                            else:
                                print("regular files logic")
                                response = str.encode(response_proto)
                                response += b' '
                                response_status = '200'  # in case of success (valid) return status 200
                                response += str.encode(response_status)
                                response += b' '
                                response += str.encode("OK\r\n")

                                current_date = datetime.datetime.now()
                                response_headers_date = current_date.strftime("%d-%b-%Y (%H:%M:%S.%f)")
                                response_headers_content_type = "Content-Type: "+file_extension
                                response_headers_content_len = os.path.getsize(filename_path)
                                response_headers = response_headers_date + "\r\n" + response_headers_content_type + "\r\n"
                                response += str.encode(response_headers)
                                response += b'\r\n'  # to separate headers from body
                                output = open(filename_path, 'rb')
                                response += output.read()
                                conn.sendall(response)
                                output.close()












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