import json
from tqdm import tqdm


def invertedIndex2string(s):
    # print(type(s))
    # print(s)
    ss = formatJsonString(s)
    # print(ss)
    # print(type(ss))
    jsonObjects =json.loads(json.loads(ss, strict=False))
    string_list = ["[PAD]"] * jsonObjects["IndexLength"]
    for k,v in jsonObjects['InvertedIndex'].items():
        for vv in v:
            string_list[vv] = k
    return " ".join(string_list)


def formatJsonString(js):
    # s = js[1:-2]
    #
    # s = s.replace('\u0000','')
    s = js.replace('@', 'at')
    # s = s.replace('\\t','')
    # s = s.replace('\\n',' ')
    # s = s.replace('\\r', '')

    # s = s.replace('\\u', '')
    # s = s.replace('\\\\:', ':')
    return s


if __name__ == '__main__':
    with open('en.csv', 'w', encoding='utf-8') as wfile:
        wfile.write( 'PaperId,PaperTitle,abstract\n')
        with open('GisTitleAbstractsEn.txt', 'r', encoding='utf-8') as csvfile:
            lines = csvfile.readlines()
            for i in tqdm(lines):
                row = i.split('\t')
                wfile.write(row[0] + ',@' + row[1] + '@,@' + invertedIndex2string(row[2]) + '@\n')
    print('finished')
