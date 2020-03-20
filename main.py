import sys,json
from mpi4py import MPI

class ProcessData():
    def __init__(self,args):
        self.args=args
        self.hashtag=[]
        self.lang=[]
        self.communicator=MPI.COMM_WORLD
        self.communicator_rank=self.communicator.Get_rank()
        self.communicator_size =self.communicator.Get_size()



    def read_json(self):
        """parse json
            parameter json file path
            return json data - dictionary
        """
        read_file = open(self.args, encoding='utf=8', mode='r')
        if self.communicator_rank==0:
            for line in read_file:
                try:
                    if line.endswith('[\n') or line.endswith("]\n"):
                        continue
                    elif line.endswith(',\n'):
                        line = line.replace(line, line[0:len(line) - 2])
                    elif line.endswith('\n'):
                        line= line.replace(line,line[0:len(line)-1])
                    data=json.loads(line)
                    self.hashtag.append(data['doc']['entities']['hashtags'])
                    self.lang.append(data['doc']['lang'])
                except json.decoder.JSONDecodeError:
                    return ("Decoder error line: "+line)
        return self.hashtag,self.lang
        
        # jsonstring = ""
        # for i in read_file.readlines():
        #     jsonstring = jsonstring + i
        # try:
        #     data=json.loads(jsonstring)
        # except json.decoder.JSONDecodeError:
        #     if jsonstring[-1] == "\n" and jsonstring[-2] == ",":
        #         jsonstring = jsonstring[:-2] + "]}"
        #     data=json.loads(jsonstring)
        # read_file.close()
        # return data

# def parse_hastag_lang(dict_data):




if __name__ == "__main__":
    data_obj=ProcessData(sys.argv[1])
    print(data_obj.read_json())
    # data=data_obj.read_json()
    # print(data['rows'][0]['doc']['entities'])

