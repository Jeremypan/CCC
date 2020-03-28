import sys, json,io
from mpi4py import MPI
from collections import Counter
from datetime import datetime
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

#run command in wins: mpiexec -np 4 python main_final.py

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

# initialize MPI
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()


def seperate_files(file):
    while True:
        files = file.readlines(1024*1024*50*10) #Each time read round 1024+1024*50*2=104857600 bytes = 104.8576 MB
        if not files:
            break
        yield files


sum_hashtag = Counter()
sum_lang = Counter()

start_time = datetime.now().timestamp()
with open('bigTwitter.json', 'r', encoding="utf-8") as read_files:
    for json_data in seperate_files(read_files):
        if rank == 0:
            partition_list = [[] for _ in range(size)]
            for i, partiton in enumerate(json_data):
                partition_list[i % size].append(partiton)
        else:
            json_data, partition_list = None, None

        # send out data through scatter
        scatter = comm.scatter(partition_list, root=0)

        # initialize counter
        hashtag_Counter = Counter()
        lang_Counter = Counter()

        for line in scatter:
            if line.endswith('[\n') or line.endswith("]\n") or line.endswith("}\n"):
                continue
            elif line.endswith(',\n'):
                line = line[0:len(line) - 2]
            elif line.endswith('\n'):
                line = line[0:len(line) - 1]
            try:
                temp=json.loads(line)
            except ValueError:
                print("ValueError Line"+str(line))

            for hashtag in temp['doc']['entities']['hashtags']:
                hashtag_Counter.update([hashtag['text'].lower()])
            lang_Counter.update([temp['doc']['lang']])
        # try:
        #     print(hashtag_Counter)
        # except UnicodeEncodeError:
        #     for i in hashtag_Counter:
        #         print(i)

        data_collection = comm.gather([hashtag_Counter, lang_Counter])

        if rank == 0:
            for data in data_collection:
                sum_hashtag.update(data[0])
                sum_lang.update(data[1])


end_time = datetime.now().timestamp()
if rank==0:
    # from bs4 import BeautifulSoup
    # html_file = open('language_html_table', 'r', encoding='utf-8')
    # soup = BeautifulSoup(html_file, 'lxml')
    # data = soup.select('td')[2:]
    # language = {}
    # for i in range(0, len(data), 2):
    #     language[data[i + 1].string] = data[i].string
    # html_file.close()
    language={'en': 'English (default)', 'ar': 'Arabic', 'bn': 'Bengali', 'cs': 'Czech', 'da': 'Danish', 'de': 'German',
     'el': 'Greek', 'es': 'Spanish', 'fa': 'Persian', 'fi': 'Finnish', 'fil': 'Filipino', 'fr': 'French',
     'he': 'Hebrew', 'hi': 'Hindi', 'hu': 'Hungarian', 'id': 'Indonesian', 'it': 'Italian', 'ja': 'Japanese',
     'ko': 'Korean', 'msa': 'Malay', 'nl': 'Dutch', 'no': 'Norwegian', 'pl': 'Polish', 'pt': 'Portuguese',
     'ro': 'Romanian', 'ru': 'Russian', 'sv': 'Swedish', 'th': 'Thai', 'tr': 'Turkish', 'uk': 'Ukrainian', 'ur': 'Urdu',
     'vi': 'Vietnamese', 'zh-cn': 'Chinese (Simplified)', 'zh-tw': 'Chinese (Traditional)', 'und': 'undefined',
     'zh': 'Chinese'}
    language['und']='undefined'
    language['zh']='Chinese'
    lang_rank_position=1
    print("----------------------------Language_Rank---------------------------------")
    for i,j in sum_lang.most_common(10):
        if i in language.keys():
            print("{0}. {1} ({2}), {3:,}".format(lang_rank_position,language[i],i,j))
        else:
            print("{0}. {1} ({2}), {3:,}".format(lang_rank_position,"unknown", i, j))
        lang_rank_position+=1
    hashtag_rank_position=1
    print("----------------------------Hashtag_Rank---------------------------------")
    print("")
    for i,j in sum_hashtag.most_common(10):
        print("{0}. #{1}, {2:,}".format(hashtag_rank_position,i,j))
        hashtag_rank_position+=1

print("##############################Rank_TimeCost_Sheet#############################")
print("Rank: "+str(rank)+"'s time: " + str(end_time - start_time) + "s")
