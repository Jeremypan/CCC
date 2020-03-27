import sys, json
from mpi4py import MPI
from collections import Counter
from datetime import datetime

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

# initialize MPI
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()


def seperate_files(file):
    while True:
        files = file.readlines(1024 * 1024)
        if not files:
            break
        yield files


sum_hashtag = Counter()
sum_lang = Counter()
with open('tinyTwitter.json', 'r', encoding="utf-8") as read_files:
    for json_data in seperate_files(read_files):
        if rank == 0:
            start_time = datetime.now().timestamp()
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
            print(temp)
            for hashtag in temp['doc']['entities']['hashtags']:
                hashtag_Counter.update([hashtag['text'].lower()])
            lang_Counter.update([temp['doc']['lang']])
        try:
            print(hashtag_Counter)
        except UnicodeEncodeError:
            for i in hashtag_Counter:
                print(i)

        data_collection = comm.gather([hashtag_Counter, lang_Counter])

        if rank == 0:
            for data in data_collection:
                sum_hashtag.update(data[0])
                sum_lang.update(data[1])
            end_time = datetime.now().timestamp()

            print("time: " + str(end_time - start_time) + "s")
