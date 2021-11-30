import sqlite3
import os

def check_if_file_exists(path_photo):
    return os.path.exists(path_photo)

def user_exists(username_to_check):
    conn = None
    try:
        conn = sqlite3.connect('users.db')
        cur = conn.cursor()
        # query = sql.SQL("SELECT username, password"
        #                 " FROM Users"
        #                 " WHERE username = {var}").format(var=sql.Literal(username_to_check))
        # rows_effected, res = conn.execute(str(query))
        # rows_effected, res = conn.execute("SELECT username, password FROM Users WHERE username = (?)", username_to_check)
        cur.execute("SELECT username FROM Users WHERE username=:user_name",
                    {"user_name": username_to_check})
        rows_effected = cur.fetchall()
        conn.commit()
    except Exception as e:
        print("..e is: ", e)
        conn.close()
        return False
    conn.close()
    if len(rows_effected) <= 0:
        return False
    else:
        return True


def user_credentials_valid(username_to_check, password_to_check):
    # if not user_exists(username_to_check):
    #     return False
    conn = None
    try:
        conn = sqlite3.connect('users.db')
        # query = sql.SQL("SELECT username, password"
        #                 " FROM Users"
        #                 " WHERE username = {var1} AND password = {var2}").format(var1=sql.Literal(username_to_check),
        #                                   var2=sql.Literal(password_to_check))
        # rows_effected, res = conn.execute(query)
        cur = conn.cursor()
        cur.execute("SELECT username, password FROM Users WHERE username =:user_name AND password =:pass_word",
                    {"user_name": username_to_check, "pass_word": password_to_check})
        rows_effected = cur.fetchall()
        conn.commit()
    except Exception as e:
        print("..e is: ", e)
        conn.close()
        return False
    conn.close()
    if len(rows_effected) <= 0:
        return False
    else:
        return True


def user_insert(username_to_insert, password_to_insert):
    conn = None
    try:
        conn = sqlite3.connect('users.db')
        # query = sql.SQL("INSERT INTO Users(username, password) VALUES({username}, {password});")\
        #     .format(username=sql.Literal(username_to_insert), password=sql.Literal(password_to_insert))
        # conn.execute(str(query))
        conn.execute("INSERT INTO Users VALUES (?, ?)", (username_to_insert, password_to_insert))

        conn.commit()
    except Exception as e:
        print("e is: ", e)
        conn.close()
        return False
    conn.close()
    return True


def user_delete(username_to_delete):
    # if not user_exists(username_to_check):
    #     return False
    conn = None
    try:
        conn = sqlite3.connect('users.db')
        cur = conn.cursor()
        # query = sql.SQL("DELETE FROM Users WHERE username = {var1}").format(var1=sql.Literal(username_to_delete))
        # rows_effected, _ = conn.execute(str(query))
        cur.execute("DELETE FROM Users WHERE username=:user_name",
                    {"user_name": username_to_delete})
        # rows_effected = cur.fetchall()
        conn.commit()
        if cur.rowcount <= 0:
            conn.close()
            return False
    except Exception as e:
        print("..e is: ", e)
        conn.close()
        return False
    conn.close()
    return True


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
    fields = fields[1:]  # -1 because we don't beed the body
    for field in fields:  # go over the fields
        if not field:
            continue
        if field.find(':') == -1:
            http_dict['body'] = field
            break
        key, value = field.split(':', 1)  # split each line by http field name and value
        http_dict[key] = value
    return http_dict