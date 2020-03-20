import sys,json



def read_json(args):
    """parse json
        parameter json file path
        return json data - dictionary
    """
    try:
        read_file = open(args, encoding='utf=8', mode='r')
        jsonstring = ""
        for i in read_file.readlines():
            jsonstring = jsonstring + i
        data=json.loads(jsonstring)
    except json.decoder.JSONDecodeError:
        read_file = open(args, encoding='utf=8', mode='r')
        jsonstring = ""
        for i in read_file.readlines():
            jsonstring = jsonstring + i
        if jsonstring[-1] == "\n" and jsonstring[-2] == ",":
            jsonstring = jsonstring[:-2] + "]}"
        data=json.loads(jsonstring)
    return data

# def parse_hastag_lang(dict_data):




if __name__ == "__main__":
    data=read_json(sys.argv[1])
    print(data['rows'][0]['doc']['entities']['hashtags'])

