


'''
This function parses a raw (bytes) HTTP request.
Each header as a corresponding key with the same name.
The first request line (e.g. "GET www.google.com ....") key is 'Request'
input: HTTP raw request, in bytes.
output: A dictionary of the parsed request
'''


def decode_http(http_data):
    http_dict = {}
    fields = http_data.decode('utf-8').split("\r\n")  # convert to string from bytes
    http_dict['Request'] = fields[0]
    fields = fields[1:]
    for field in fields:  # go over the fields
        if not field:
            continue
        key, value = field.split(':', 1)  # split each line by http field name and value
        http_dict[key] = value
    return http_dict