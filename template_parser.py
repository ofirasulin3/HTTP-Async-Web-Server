def parsing(dynamic_page, username, authenticated):
    f = open("gen.py", "w")
    # f.write("eval(context)")
    user = {"authenticated": authenticated, "username": username}

    # splitted = dynamic_page.split('{')
    # for
    # for char in dynamic_page:

    # while dynamic_page[i]:
    #     if dynamic_page[i]!='{'
    #         f.write

    with open(dynamic_page) as dp:
        f.write('\"')
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
                        if c != '\"':
                            tmp_exp += c
                        else:
                            tmp_exp += "\'"
                    i += 1
                    if i >= len(line):
                        break
                    c = line[i]

                if tmp_exp != "":
                    f.write('print(\\"' + tmp_exp + '\\")\\n')
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
                        if c != '\"':
                            tmp_str += c
                        else:
                            tmp_str += "\'"
                    i += 1
                    if i >= len(line):
                        break
                    c = line[i]

                if tmp_str != "":
                    f.write(tmp_str + "\\n")

                i += 1
            line = dp.readline()

    # print("created file after parsing is: \n", f)
    f.write('\"')
    f.close()
    return f
