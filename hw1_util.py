import socket
import hw1_utils
import os
import datetime
from pdfminer.high_level import extract_text
import glob


# Define socket host and port
SERVER_HOST = '127.0.0.1'
SERVER_PORT = 8888
DATA_MAX_SIZE = 4096


def check_if_file_exists(path_photo):
    return os.path.exists(path_photo)


# assume we got something like: ..../pdfs/
def get_all_files_rec(path):
    return glob.glob(path + '/**/*.pdf', recursive=True)


# gets a pdf file, filters the stopwords and returns a wordcloud photo
def photo_from_pdf(pdf_file_path, pdf_file_name):
    # convert pdf to string
    text = extract_text(pdf_file_path)
    if not text:
        return ""
    text = text.split()
    file = open('stopwords.txt', 'r')

    stop_words_txt = [line.split('\n') for line in file.readlines()]
    stop_words = [item2[0] for item2 in stop_words_txt]

    # filter the stopwords
    filtered_words = [word for word in text if word.lower() not in stop_words]
    result = ' '.join(filtered_words)
    if not result:
        return ""
    # pic_name = "wordclouds_pics/" + pdf_file_path + ".png"
    pic_name2 = pdf_file_path[0:len(pdf_file_path) - 4] + ".png"
    result2 = hw1_utils.generate_wordcloud_to_file(result, pic_name2)
    return result2

    # For showing a wordcloud
    # plt.figure(figsize=(10, 5))
    # plt.imshow(picture, interpolation="bilinear")
    # plt.axis('off')
    # # # plt.savefig(f'wordcloud.png', dpi=300)
    # plt.show()


if __name__ == "__main__":

    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('localhost', SERVER_PORT))
            s.listen(5)
            conn, addr = s.accept()
            with conn:
                while True:
                    data = conn.recv(DATA_MAX_SIZE)
                    response_proto = 'HTTP/1.1'
                    landing_page_requested = False
                    specific_page_requested = False
                    photo_requested = False
                    response = ""
                    if not data:
                        # in any other error, return status 500
                        response_status = '500'
                        response = str.encode(response_proto)
                        response += b' '
                        response += str.encode(response_status)
                        response += b' '
                        response += str.encode("EMPTY DATA\r\n")
                        conn.sendall(response)
                        break

                    # with open ('test_server.txt', 'r') as f:
                    #     file_str = f.read()
                    #     my_str_as_bytes = str.encode(file_str)
                    # http_data = hw1_utils.decode_http(my_str_as_bytes)

                    http_data = hw1_utils.decode_http(data)

                    request = http_data["Request"].split('\n')[0]
                    URL = request.split(' ')[1]

                    # check if the request is GET type
                    # if not, return with status 501
                    request_name = request.split(' ')[0]

                    if request_name != 'GET':
                        response_status = '501'
                        response = str.encode(response_proto)
                        response += b' '
                        response += str.encode(response_status)
                        response += b' '
                        response += str.encode("INVALID REQUEST TYPE")
                        response += str.encode("\r\n")
                        conn.sendall(response)
                        break

                    str_local_host = "localhost:8888"
                    host = http_data["Host"][1:]

                    if host != str_local_host:
                        response_status = '500'
                        response = str.encode(response_proto)
                        response += b' '
                        response += str.encode(response_status)
                        response += b' '
                        response += str.encode("INVALID URL\r\n")
                        conn.sendall(response)
                        break

                    filename_path = "pdfs/"+request.split(' ')[1][1:]
                    # We have a valid url here:
                    if URL == "/":
                        landing_page_requested = True
                    elif URL.split('.')[-1] == "png":
                        photo_requested = True
                        # search for photo
                        exists = check_if_file_exists(filename_path)

                        if not exists:
                            response_status = '404'
                            response = str.encode(response_proto)
                            response += b' '
                            response += str.encode(response_status)
                            response += b' '
                            response += str.encode("PHOTO NOT FOUND\r\n")
                            conn.sendall(response)
                            break

                        # if photo exists, return the photo:
                        # Build the response message:
                        response = str.encode(response_proto)
                        response += b' '
                        response_status = '200'  # in case of success (valid) return status 200
                        response += str.encode(response_status)
                        response += b' '
                        response += str.encode("OK\r\n")

                        current_date = datetime.datetime.now()
                        response_headers_date = current_date.strftime("%d-%b-%Y (%H:%M:%S.%f)")
                        response_headers_content_type = "Content-Type: image/png"
                        # response_headers_content_len = ""
                        response_headers = response_headers_date + "\r\n" + response_headers_content_type + "\r\n"
                        response += str.encode(response_headers)
                        response += b'\r\n'  # to separate headers from body

                        output = open(filename_path, 'rb')
                        response += output.read()
                        conn.sendall(response)

                        output.close()

                    else:
                        specific_page_requested = True

                    if not photo_requested:
                        # not need to check of it's localhost 8888 because it's defined already

                        filepath = filename_path + ".pdf"
                        # if the request if for a wordcloud, check if file exists. if not return 404
                        if not landing_page_requested \
                                and not check_if_file_exists(filepath):
                            response_status = '404'
                            response = str.encode(response_proto)
                            response += b' '
                            response += str.encode(response_status)
                            response += b' '
                            response += str.encode("FILE NOT FOUND\r\n")
                            conn.sendall(response)
                            break

                        if specific_page_requested:
                            # Create the photo

                            item = filepath[5:]
                            picture = photo_from_pdf(filepath, item)
                            if picture == "":
                                # in any other error, return status 500
                                response_status = '500'
                                response = str.encode(response_proto)
                                response += b' '
                                response += str.encode(response_status)
                                response += b' '
                                response += str.encode("EMPTY PDF\r\n")
                                conn.sendall(response)
                                break

                            # Create the wordcloud html page
                            wc_page_html_string = "<!DOCTYPE html> <html> <body>\r\n"
                            wc_page_html_string += "<h1>" + item + "</h1>"
                            pic_name = filename_path[5:]

                            wc_page_html_string += "<table><tr><td><img src = \"/" + pic_name + ".png\"></td></tr> \r\n"

                            # Create the Go Back button
                            wc_page_html_string += "<tr><td><a href=\""
                            for d in range(0, filepath.count('/')+1):
                                wc_page_html_string += "..\\"
                            wc_page_html_string += "\">Go back</a></td></tr>\r\n"
                            wc_page_html_string += "</table></body> </html>\r\n"

                            # Insert the html string to an html file
                            item_name = "pdfs\\" + item + ".html"

                        # here comes the logic:
                        elif landing_page_requested:
                            # Get all files for buttons in Landing Page
                            all_files_paths = get_all_files_rec("pdfs")
                            all_files_buttons = [item[5:] for item in all_files_paths]
                            all_files_levels = [item.count('\\') for item in all_files_paths]
                            pics_names = [item[item.rfind('\\') + 1:] for item in all_files_paths]

                            # Create Landing Page
                            landing_page_html_string = "<!DOCTYPE html> <html> <body>\r\n"
                            landing_page_html_string += "<h1>Landing Page</h1>\r\n"
                            landing_page_html_string += "<h5>Welcome to the landing page!</h5>\r\n"
                            landing_page_html_string += "<h5>Choose a .pdf file to generate a wordcloud from:</h5>\r\n"
                            landing_page_html_string += "<table>\r\n"

                            desired_wc_page_html_string = ""
                            i = 0
                            for item in all_files_buttons:

                                # Insert the html string to an html file
                                item_name = "pdfs\\" + item + ".html"

                                filename_updated_slash = filename_path.replace("/", "\\")

                                # Add a link in the landing page
                                landing_page_html_string += "<tr><td><a href = \""
                                landing_page_html_string += item_name[5:len(item_name)-9] + "\">"
                                landing_page_html_string += item
                                landing_page_html_string += "</a> </td> </tr>\r\n"

                                i += 1
                            landing_page_html_string += "</table>\r\n"
                            landing_page_html_string += "</body> </html>"

                            f = open('LandingPage.html', 'w')
                            message1 = landing_page_html_string
                            f.write(message1)
                            f.close()

                        # Build the response message:
                        response_status = '200'  # in case of success (valid) return status 200
                        response = str.encode(response_proto)
                        response += b' '
                        response += str.encode(response_status)
                        response += b' '
                        response += str.encode("OK\r\n")

                        current_date = datetime.datetime.now()
                        response_headers_date = current_date.strftime("%d-%b-%Y (%H:%M:%S.%f)")
                        response_headers_content_type = "Content-Type: text/html"
                        # response_headers_content_len = ""
                        response_headers = response_headers_date + "\r\n" + response_headers_content_type + "\r\n"
                        response += str.encode(response_headers)
                        response += b'\r\n'  # to separate headers from body

                        if landing_page_requested:
                            output = open('LandingPage.html', 'rb')
                            # we can input the string right away
                            response += output.read()  # sending landing page
                            conn.sendall(response)
                            output.close()
                        else:
                            response += str.encode(wc_page_html_string)
                            conn.sendall(response)
                    break