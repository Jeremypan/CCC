import sys,json
from mpi4py import MPI

class ProcessData():
    def __init__(self,args):
        self.args=args
        self.hashtag=dict()
        self.lang=dict()
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
                        line = line[0:len(line) - 2]
                    elif line.endswith('\n'):
                        line= line[0:len(line)-1]
                    data=json.loads(line)
                    self.statistic_hashtag(data)
                    self.statistic_lang(data)
                except json.decoder.JSONDecodeError:
                    return ("Decoder error line: "+line)
        return sorted(self.hashtag.items(),key=lambda x:x[1],reverse=True),sorted(self.lang.items(),key=lambda x:x[1],reverse=True)

    def statistic_hashtag(self,json_file):
        for data in json_file['doc']['entities']['hashtags']:
            hashtag=data['text']
            if hashtag in list(self.hashtag.keys()):
                self.hashtag[hashtag]+=1
            else:
                self.hashtag[hashtag]=1
    def statistic_lang(self,json_file):
        lang=json_file['doc']['lang']
        if lang in list(self.lang.keys()):
            self.lang[lang]+=1
        else:
            self.lang[lang]=1


# def parse_hastag_lang(dict_data):




if __name__ == "__main__":
    import time
    start_time=time.time()
    data_obj=ProcessData(sys.argv[1])
    print(data_obj.read_json())
    end_time=time.time()
    print('Time cost: '+str(end_time-start_time)+'s')
    # data=data_obj.read_json()
    # print(data['rows'][0]['doc']['entities'])

