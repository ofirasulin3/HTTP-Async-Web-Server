

def dp_parsing(dynamic_page_path, username, authenticated, params):
    f = open("gen.py", "w")
    user = {"authenticated": authenticated, "username": username}

    with open(dynamic_page_path) as dp:
        line = dp.readline()
        while line:
            i = 0
            while i < len(line):
                c = line[i]
                if i >= len(line):
                    break
                tmp_exp = ""
                while c != '{':
                    if c != '\n':
                        tmp_exp += c
                    i += 1
                    if i >= len(line):
                        break
                    c = line[i]

                if tmp_exp != "":
                    f.write(tmp_exp + '\n')
                if tmp_exp == "</html>":
                    x = 2
                if i >= len(line):
                        break

                # here I have an opener.
                i += 1
                if i >= len(line):
                    break
                c = line[i]

                i += 1
                if i >= len(line):
                    break
                c = line[i]

                tmp_str = ""
                while c != '%':
                    if c != '\n':
                        tmp_str += c
                    i += 1
                    if i >= len(line):
                        break
                    c = line[i]
                i += 1
                if i >= len(line):
                    break
                c = line[i]

                if c == '}':
                    i += 1
                    if i >= len(line):
                        break
                if tmp_str != "":
                    f.write(eval(tmp_str) + "\n")

                i += 1
            line = dp.readline()

    f.close()
    return f
