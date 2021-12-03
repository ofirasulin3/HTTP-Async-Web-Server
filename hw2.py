import json
import aiofiles as aiofiles
import hw2_utils
import base64
import os
from template_parser import dp_parsing
import asyncio
from aiohttp import web


async def mime_parsing(key):
    async with aiofiles.open("mime.json", 'rt') as f:
        d = json.loads(await f.read())
    # async with aiofiles.open("mime.json", 'rt') as f:
    #     d = json.load(f)
        list = d["mime-mapping"]
        for item in list:
            if item["extension"] == key:
                f.close()
                return item["mime-type"]
        f.close()
        return -1


    # f = open("mime.json", "rt")
    # d = json.load(f)
    # list = d["mime-mapping"]
    # for item in list:
    #     if item["extension"] == key:
    #         return item["mime-type"]
    # return -1

# async def build_html_for_not_found_404(url):
#     html_string = "<!DOCTYPE html> <html> \r\n"
#     html_string += "<head>\r\n"
#     html_string += "<title>404 Not Found</title>\r\n"
#     html_string += "</head>\r\n"
#     html_string += "<body>\r\n"
#     html_string += "<h1>Not Found</h1>\r\n"
#     html_string += "<p>The requested URL " + url + " was not found on the server</p>\r\n"
#     html_string += "</body>\r\n"
#     html_string += "</html>\r\n"
#     return html_string

async def build_html(status, reason, info):
    html_string = "<!DOCTYPE html> <html> \r\n"
    html_string += "<head>\r\n"
    html_string += "<title>" + status + "</title>\r\n"
    html_string += "</head>\r\n"
    html_string += "<body>\r\n"
    html_string += "<h1>" + reason + "</h1>\r\n"
    html_string += "<p>" + info + "</p>\r\n"
    html_string += "</body>\r\n"
    html_string += "</html>\r\n"
    return html_string

# if __name__ == "__main__":
async def main():
    try:
        server = web.Server(handler)
        runner = web.ServerRunner(server)
        await runner.setup()
        site = web.TCPSite(runner, 'localhost', 8001)
        await site.start()
    except Exception as e3:
        print("..e3 is: ", e3)

async def handler(new_request):
    # getting parameters from config file
    # config_file = open("config.py", "r")
    async with aiofiles.open("config.py", 'r') as config_file:
        lines = await config_file.readlines()
        config_file.close()

    timeout_param = lines[1].split(" ")[2]
    SERVER_PORT = lines[0].split(" ")[2]
    admin_username = lines[2].split(" ")[3].split('\'')[1]
    admin_pass = lines[2].split(" ")[5].split('\'')[1]
    pre_encoded = admin_username + ":" + admin_pass
    encoded = base64.b64encode(pre_encoded.encode())
    if not hw2_utils.user_exists(admin_username):
        hw2_utils.user_insert(admin_username, admin_pass)

    try:
        all_http_requests = ["GET", "POST", "DELETE", "OPTIONS", "HEAD", "PUT", "CONNECT", "TRACE"]
        # http_data = hw2_utils.decode_http(data)
        response_proto = 'HTTP/1.1'
        # response_proto = request_.protocol
        response_status = ""
        # request = http_data["Request"].split('\n')[0]

        # URL = request.split(' ')[1]
        URL = new_request.url.path
        request_type = new_request.method
        request = request_type + " " + URL + " " + response_proto
        new_body = await new_request.content.readany()
        # request_type = request.split(' ')[0]

        # if data == b'GET / HTTP/1.1\r\nHost: localhost:8001\r\nConnection: keep-alive\r\nCache-Control: max-age=0\r\nsec-ch-ua: " Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"\r\nsec-ch-ua-mobile: ?0\r\nsec-ch-ua-platform: "Windows"\r\nUpgrade-Insecure-Requests: 1\r\nUser-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36\r\nAccept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9\r\nSec-Fetch-Site: none\r\nSec-Fetch-Mode: navigate\r\nSec-Fetch-User: ?1\r\nSec-Fetch-Dest: document\r\nAccept-Encoding: gzip, deflate, br\r\nAccept-Language: he,en-US;q=0.9,en;q=0.8\r\nCookie: Pycharm-46a7fce2=29a7169a-5c2a-4123-bbb4-2126e2fd8556\r\n\r\n':
        #     response_status = '401'
        #     response = str.encode(response_proto)
        #     response += b' '
        #     response += str.encode(response_status)
        #     response += b' '
        #     response += str.encode("Unauthorized\r\n")
        #     current_date = datetime.datetime.now()
        #     response_headers_date = current_date.strftime("%d-%b-%Y (%H:%M:%S.%f)")
        #     response_headers_content_len = "Content-Length: 0"
        #     response_headers = response_headers_date + "\r\n" + response_headers_content_len + "\r\n"
        #     response += str.encode(response_headers)
        #     response += str.encode("Connection: keep-alive\r\n")
        #     response += str.encode("WWW-Authenticate: Basic realm=\"HW2 realm\"\r\n")
        #     response += b'\r\n'  # to separate headers from body
        #     conn.sendall(response)
        #     break

        if request_type not in all_http_requests:
            return web.Response(body="", status=400, reason="Bad Request",
                                headers={'Content-Length': "0", 'Connection': "close",
                                         "charset": "utf-8"})
        if request_type != 'GET' and request_type != "POST" and request_type != "DELETE":
            return web.Response(body="", status=501, reason="Not Implemented",
                                headers={'Content-Length': "0", 'Connection': "close",
                                         "charset": "utf-8"})

        # POST request - for Creating a User
        if request_type == "POST":
            if request.split(' ')[1] != "/users" and request.split(' ')[1] != "/users/":
                return web.Response(body="", status=400, reason="Bad Request",
                                    headers={'Content-Length': "0", 'Connection': "close",
                                             "charset": "utf-8"})

            body = new_body.decode()
            # body = http_data['body']
            username_to_handle = body[body.index("username=") + 9:body.index("&")]
            userpass_to_handle = body[body.index("password=") + 9:]

            if "Authorization" not in new_request.headers:
                return web.Response(body="", status=401, reason="Unauthorized",
                                    headers={'Content-Length': "0", 'Connection': "keep-alive",
                                             'WWW-Authenticate': "Basic realm=\"HW2 realm\"",
                                             "charset": "utf-8"})

            else:  # meaning there is credentials
                auth_value = new_request.headers['Authorization']
                basic_str = "Basic "
                encoded = auth_value[len(basic_str):]
                if auth_value[0:6] != "Basic ":
                    return web.Response(body="", status=400, reason="Bad Request",
                                        headers={'Content-Length': "0", 'Connection': "close",
                                                 "charset": "utf-8"})
                decoded = base64.b64decode(encoded)
                admin_username_to_check = decoded.split(b':')[0].decode()
                admin_password_to_check = decoded.split(b':')[1].decode()
                if admin_username_to_check == admin_username \
                        and admin_password_to_check == admin_pass:
                    # create the user!
                    # add username_to_handle and userpass_to_handle to DB
                    if hw2_utils.user_exists(username_to_handle):
                        html_string = await build_html("409", "Conflict", "User already exists")
                        return web.Response(body=html_string, status=409, reason="Conflict",
                                            headers={'Content-Length': str(len(html_string)), 'Connection': "close",
                                                     "Content-Type": "text/html",
                                                     "charset": "utf-8"})

                    elif not hw2_utils.user_insert(username_to_handle, userpass_to_handle):
                        return web.Response(body="", status=500, reason="Internal Server Error",
                                            headers={'Content-Length': "0", 'Connection': "close",
                                                     "charset": "utf-8"})
                    else:
                        # User Inserted!
                        return web.Response(body="", status=200, reason="OK",
                                            headers={'Content-Length': "0", 'Connection': "close",
                                                     "charset": "utf-8"})
                elif hw2_utils.user_credentials_valid(admin_username_to_check,
                                                      admin_password_to_check) and admin_username_to_check != admin_username:
                    # regular user trying to insert someone->403
                    html_string = await build_html("403", "Forbidden", "You can't perform this action")
                    return web.Response(body=html_string, status=403, reason="Forbidden",
                                        headers={'Content-Length': str(len(html_string)), 'Connection': "close",
                                                 "Content-Type": "text/html",
                                                 "charset": "utf-8"})
                else:
                    # invalid admin credentials
                    if not hw2_utils.user_credentials_valid(admin_username_to_check,
                                                            admin_password_to_check):
                        html_string = await build_html("401", "Unauthorized", "Invalid Admin Credentials!")
                        return web.Response(body=html_string, status=401, reason="Unauthorized",
                                            headers={'Content-Length': str(len(html_string)), 'Connection': "close",
                                                     "Content-Type": "text/html",
                                                     "charset": "utf-8"})
                    else:
                        return web.Response(body="", status=403, reason="Forbidden",
                                            headers={'Content-Length': "0", 'Connection': "close",
                                                     "charset": "utf-8"})

        if request_type == "DELETE":
            if request.split(' ')[1][0:7] != "/users/" or request.split(' ')[1] == "/users/":
                return web.Response(body="", status=400, reason="Bad Request",
                                    headers={'Content-Length': "0", 'Connection': "close",
                                             "charset": "utf-8"})
            # username_to_delete = http_data['Request'].split(' ')[1].split('/')[-1]
            username_to_delete = URL.split('/')[-1]

            if "Authorization" not in new_request.headers:
                return web.Response(body="", status=401, reason="Unauthorized",
                                    headers={'Content-Length': "0", 'Connection': "keep-alive",
                                             'WWW-Authenticate': "Basic realm=\"HW2 realm\"",
                                             "charset": "utf-8"})
            else:
                # meaning there is credentials
                auth_value = new_request.headers['Authorization']
                basic_str = "Basic "
                if auth_value[0:6] != "Basic ":
                    return web.Response(body="", status=400, reason="Bad Request",
                                        headers={'Content-Length': "0", 'Connection': "close",
                                                 "charset": "utf-8"})
                encoded = auth_value[len(basic_str):]
                decoded = base64.b64decode(encoded)
                admin_username_to_check = decoded.split(b':')[0].decode()
                admin_password_to_check = decoded.split(b':')[1].decode()
                if admin_username_to_check == admin_username \
                        and admin_password_to_check == admin_pass:
                    if username_to_delete == admin_username:
                        html_string = await build_html("409", "Conflict", "User doesn't exist")
                        return web.Response(body=html_string, status=409, reason="Conflict",
                                            headers={'Content-Length': str(len(html_string)), 'Connection': "close",
                                                     "Content-Type": "text/html",
                                                     "charset": "utf-8"})
                    # Check if the user exists on DB.
                    # if user doesn't exists return Error 409 Conflict
                    if not hw2_utils.user_exists(username_to_delete):
                        html_string = await build_html("409", "Conflict", "User doesn't exist")
                        return web.Response(body=html_string, status=409, reason="Conflict",
                                            headers={'Content-Length': str(len(html_string)), 'Connection': "close",
                                                     "Content-Type": "text/html",
                                                     "charset": "utf-8"})
                    # if exists, delete the user! delete username_to_handle from DB
                    elif not hw2_utils.user_delete(username_to_delete):
                        return web.Response(body="", status=500, reason="Internal Server Error",
                                            headers={'Content-Length': "0", 'Connection': "close",
                                                     "charset": "utf-8"})
                    else:  # deleted successfully, send 200 OK
                        return web.Response(body="", status=200, reason="OK",
                                            headers={'Content-Length': "0", 'Connection': "close",
                                                     "charset": "utf-8"})
                elif hw2_utils.user_credentials_valid(admin_username_to_check,
                                                      admin_password_to_check) and admin_username_to_check != admin_username:
                    # regular user trying to delete someone->403
                    html_string = await build_html("403", "Forbidden", "You can't perform this action")
                    return web.Response(body=html_string, status=403, reason="Forbidden",
                                        headers={'Content-Length': str(len(html_string)), 'Connection': "close",
                                                 "Content-Type": "text/html",
                                                 "charset": "utf-8"})
                else:
                    # invalid admin credentials
                    if not hw2_utils.user_credentials_valid(admin_username_to_check,
                                                            admin_password_to_check):
                        html_string = await build_html("401", "Unauthorized", "Invalid Admin Credentials!")
                        return web.Response(body=html_string, status=401, reason="Unauthorized",
                                            headers={'Content-Length': str(len(html_string)), 'Connection': "close",
                                                     "Content-Type": "text/html",
                                                     "charset": "utf-8"})
                    else:
                        return web.Response(body="", status=403, reason="Forbidden",
                                            headers={'Content-Length': "0", 'Connection': "close",
                                                     "charset": "utf-8"})

        if request_type == "GET":
            filename_path = request.split(' ')[1][1:]
            file_exists = await hw2_utils.check_if_file_exists(filename_path)
            if not file_exists:
                html_string = await build_html("404", "Not Found", "The requested URL " + URL + " was not found on the server")
                return web.Response(body=html_string, status=404, reason="Not Found",
                                    headers={'Content-Length': str(len(html_string)), 'Connection': "close",
                                             "Content-Type": "text/html",
                                             "charset": "utf-8"})
            file_extension = filename_path.split('.')[1]
            file_content_type = 0
            if filename_path == "users.db" or filename_path == "config.py":
                # if not hw2_utils.user_credentials_valid(username_to_check, userpassword_to_check):
                #     return web.Response(body="", status=401, reason="Unauthorized",
                #                         headers={'Content-Length': "0", 'Connection': "close",
                #                                  "charset": "utf-8"})
                # else:
                return web.Response(body="", status=403, reason="Forbidden",
                                        headers={'Content-Length': "0", 'Connection': "close",
                                                 "charset": "utf-8"})
            if file_extension != "dp":
                file_content_type = await mime_parsing(file_extension)
                if file_content_type == -1:
                    async with aiofiles.open(filename_path, 'rb') as f:
                        content = await f.read()
                        f.close()
                    return web.Response(body=content, status=200, reason="OK",
                                        headers={'Content-Length': str(os.path.getsize(filename_path)), 'Connection': "close",
                                                 "Content-Type": "text/plain",
                                                 "charset": "utf-8"})
                else:
                    # regular files logic
                    async with aiofiles.open(filename_path, 'rb') as f:
                        content = await f.read()
                        f.close()
                    return web.Response(body=content, status=200, reason="OK",
                                        headers={'Content-Length': str(len(content)), 'Connection': "close",
                                                 "Content-Type": file_extension,
                                                 "charset": "utf-8"})
            else:  # file_extension == "dp":
                username_to_check = None
                authenticated = False
                if "Authorization" in new_request.headers:
                    auth_value = new_request.headers['Authorization']
                    basic_str = "Basic "
                    encoded = auth_value[len(basic_str):]
                    if auth_value[0:6] == "Basic ":
                        decoded = base64.b64decode(encoded)
                        username_to_check2 = decoded.split(b':')[0].decode()
                        userpassword_to_check = decoded.split(b':')[1].decode()
                        if hw2_utils.user_credentials_valid(username_to_check2, userpassword_to_check):
                            authenticated = True
                            username_to_check = username_to_check2
                # Building the Dynamic Page
                multi_dict = new_request.url.query
                params_dict = {}
                for k in set(multi_dict.keys()):
                    k_values = multi_dict.getall(k)
                    if len(k_values) > 1:
                        params_dict[k] = k_values
                    else:
                        params_dict[k] = k_values[0]
                user_dict = {"authenticated": authenticated, "username": username_to_check}
                await dp_parsing(filename_path, user_dict, params_dict)

                async with aiofiles.open("gen.py", 'rb') as f:
                    content = await f.read()
                    f.close()
                return web.Response(body=content, status=200, reason="OK",
                                    headers={'Content-Length': str(len(content)), 'Connection': "close",
                                             "Content-Type": "text/html",
                                             "charset": "utf-8"})

    except Exception as e2:
        print("..e2 is: ", e2)  # HERE is 500
        return web.Response(body="", status=500, reason="Internal Server Error",
                                            headers={'Content-Length': "0", 'Connection': "close",
                                                     "charset": "utf-8"})
try:
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(main())
    loop.run_forever()
    # loop.close()
except Exception as e1:
    print("..e1 is: ", e1)


