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
        files = file.readlines(1024 * 1024*50)
        if not files:
            break
        yield files


sum_hashtag = Counter()
sum_lang = Counter()
if rank==0:
    start_time = datetime.now().timestamp()
with open('smallTwitter.json', 'r', encoding="utf-8") as read_files:
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
            if line.endswith('[\n') or line.endswith("]\n"):
                continue
            elif line.endswith(',\n'):
                line = line[0:len(line) - 2]
            elif line.endswith('\n'):
                line = line[0:len(line) - 1]
            temp = json.loads(line)

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

if rank==0:
    end_time = datetime.now().timestamp()
    print(str(rank)+" time: " + str(end_time - start_time) + "s")
if rank==0:
    from bs4 import BeautifulSoup
    html_file = open('language_html_table', 'r', encoding='utf-8')
    soup = BeautifulSoup(html_file, 'lxml')
    data = soup.select('td')[2:]
    language = {}
    for i in range(0, len(data), 2):
        language[data[i + 1].string] = data[i].string
    language['und']='undefined'
    language['tl']='unknown'
    language['in']='unknown'
    html_file.close()
    lang_rank_position=1
    for i,j in sum_lang.most_common(10):
        print("{0}. {1} ({2}), {3:,}".format(lang_rank_position,language[i],i,j))
        lang_rank_position+=1
    hashtag_rank_position=1
    print("--------------------------------------------------------------------")
    for i,j in sum_hashtag.most_common(10):
        print("{0}. #{1}, {2:,}".format(hashtag_rank_position,i,j))
        hashtag_rank_position+=1

