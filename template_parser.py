import re


async def dp_parsing(dynamic_page_path, user, params):
    f = open("gen.py", "w")
    tmp_exp = ""
    tmp_str = ""
    with open(dynamic_page_path) as dp:
        text = dp.read()
        pattern = '{%.*%}'
        s_end = 0
        for match in re.finditer(pattern, text):
            s_start = match.start()
            if s_start - s_end > 0:
                tmp_exp = text[s_end:s_start]
                f.write(tmp_exp + "\n")
            s_end = match.end()
            tmp_str = match.group()
            f.write(eval(tmp_str[2:-2]) + "\n")
        if len(text) - s_end > 0:
            tmp_exp = text[s_end:len(text)]
            f.write(tmp_exp + "\n")
        f.close()
        return f
