

def dp_parsing(dynamic_page_path, user, params):
    f = open("gen.py", "w")

    tmp_exp = ""
    tmp_str = ""
    inside_exp = False
    with open(dynamic_page_path) as dp:
        line = dp.readline()
        while line:
            i = 0
            while i < len(line) and not inside_exp:
                c = line[i]
                if i >= len(line):
                    break
                # tmp_exp = ""
                while c != '{':
                    if c != '\n':
                        tmp_exp += c
                    i += 1
                    if i >= len(line):
                        break
                    c = line[i]

                if i >= len(line):
                    inside_exp = False
                    continue
                if tmp_exp != "":
                    f.write(tmp_exp + "\n")
                    inside_exp = False
                tmp_exp = ""
                # if i >= len(line):
                #     break

                # here I have an opener.
                inside_exp = True
                i += 1
                if i >= len(line):
                    break
                # c = line[i]

                i += 1
                if i >= len(line):
                    break
                c = line[i]

                # tmp_str = ""
                while c != '%' and inside_exp:
                    if c != '\n':
                        tmp_str += c
                    i += 1
                    if i >= len(line):
                        break
                    c = line[i]
                if i >= len(line):
                    inside_exp = True
                    continue
                i += 1
                if i >= len(line):
                    inside_exp = True
                    continue
                c = line[i]

                if c == '}':
                    i += 1
                    if i >= len(line):
                        break
                if i >= len(line):
                    inside_exp = True
                    continue
                if tmp_str != "":
                    f.write(eval(tmp_str) + "\n")
                    inside_exp = False
                tmp_str = ""
                i += 1
            line = dp.readline()

    f.close()
    return f
