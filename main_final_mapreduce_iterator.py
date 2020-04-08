#run command in wins: mpiexec -np 4 python main_final_mapreduce_iterator.py
#load modules for the script
import sys, json,io
from mpi4py import MPI
from collections import Counter
from datetime import datetime

#default encoding utf-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
def seperate_files(file):
    """Create a iterator to read big file
       Parameter: file path
       Yield: parts of files
    """
    while True:
        files = file.readlines(1024*1024*100) #Each time read round 1024*1024*100*4=420MB (around 50 times read bigTwitter)
        if not files:
            break
        yield files



#initial the start time of each process
start_time = datetime.now().timestamp()

#initialize the counter for aggerate record of hastag rank and language rank
total_hashtag = Counter()
total_lang = Counter()

# initialize MPI
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

"""parse json file and take counter of hashtag and language"""

read_files=open('smallTwitter.json', 'r', encoding="utf-8") #open file

""" for loop read file through iterator:

                (big files) ---> put parts of data (ensure size of data is out of memory) through iterator into memory --> parallely parse parts of data --> store result in the total Counter of Hashtag and Language
                                                    ^                                                                                           |
                                                    |                                                                                           |
                                                    | -------------------------------------------------------------------------------------------                       
"""
for json_data in seperate_files(read_files):
    if rank == 0:
        # for process 0 as master to divide data into partition depending on the number of process
        partition_list = [[] for _ in range(size)]
        for i, partiton in enumerate(json_data):
            partition_list[i % size].append(partiton)
    else:
        #other process do not make job of data division
        json_data, partition_list = None, None

    # send out data to other processes through scatter
    scatter = comm.scatter(partition_list, root=0)

    # initialize counter for each process
    hashtag_Counter = Counter()
    lang_Counter = Counter()

    # each process parses json data
    for line in scatter:
        if line.endswith('[\n') or line.endswith("]\n") or line.endswith("}\n"):
            # line end with [\n, ]\n and }\n
            continue
        elif line.endswith(',\n'):
            line = line[0:len(line) - 2]
        elif line.endswith('\n'):
            line = line[0:len(line) - 1]
        # json load the data in string into dictionary
        try:
            temp=json.loads(line)
        except ValueError:
            # Value Error: check the line with value error (json cannot parse)
            print("ValueError Line"+str(line))

        # get hashtage value of dictionary
        for hashtag in temp['doc']['entities']['hashtags']:
            hashtag_Counter.update([hashtag['text'].lower()])

        # get language value of dictionary
        lang_Counter.update([temp['doc']['lang']])

    # try:
    #     print(hashtag_Counter)
    # except UnicodeEncodeError:
    #     for i in hashtag_Counter:
    #         print(i)

    # collect data from each process
    data_collection = comm.gather([hashtag_Counter, lang_Counter])
    # process 0 to combine data into aggerate data
    if rank == 0:
        for data in data_collection:
            total_hashtag.update(data[0])
            total_lang.update(data[1])
read_files.close()


"""other process except 0 stop running here"""
if rank !=0:
    end_time = datetime.now().timestamp()

"""rank 0 of process to display format of ranks"""
if rank==0:
    """read html file to grab the language table"""
    # from bs4 import BeautifulSoup
    # html_file = open('language_html_table', 'r', encoding='utf-8')
    # soup = BeautifulSoup(html_file, 'lxml')
    # data = soup.select('td')[2:]
    # language = {}
    # for i in range(0, len(data), 2):
    #     language[data[i + 1].string] = data[i].string
    # html_file.close()
    ######languange dictionary###########
    language={'en': 'English (default)', 'ar': 'Arabic', 'bn': 'Bengali', 'cs': 'Czech', 'da': 'Danish', 'de': 'German',
     'el': 'Greek', 'es': 'Spanish', 'fa': 'Persian', 'fi': 'Finnish', 'fil': 'Filipino', 'fr': 'French',
     'he': 'Hebrew', 'hi': 'Hindi', 'hu': 'Hungarian', 'id': 'Indonesian', 'it': 'Italian', 'ja': 'Japanese',
     'ko': 'Korean', 'msa': 'Malay', 'nl': 'Dutch', 'no': 'Norwegian', 'pl': 'Polish', 'pt': 'Portuguese',
     'ro': 'Romanian', 'ru': 'Russian', 'sv': 'Swedish', 'th': 'Thai', 'tr': 'Turkish', 'uk': 'Ukrainian', 'ur': 'Urdu',
     'vi': 'Vietnamese', 'zh-cn': 'Chinese (Simplified)', 'zh-tw': 'Chinese (Traditional)', 'und': 'undefined',
     'zh': 'Chinese'}
    language['und']='undefined'
    language['zh']='Chinese'


    #######format to print out rank of hashtag and language
    lang_rank_position=1
    print("----------------------------Language_Rank---------------------------------")
    print("")
    for i,j in total_lang.most_common(10):
        if i in language.keys():
            print("{0}. {1} ({2}), {3:,}".format(lang_rank_position,language[i],i,j))
        else:
            print("{0}. {1} ({2}), {3:,}".format(lang_rank_position,"unknown", i, j))
        lang_rank_position+=1
    hashtag_rank_position=1
    print("----------------------------Hashtag_Rank---------------------------------")
    print("")
    for i,j in total_hashtag.most_common(10):
        print("{0}. #{1}, {2:,}".format(hashtag_rank_position,i,j))
        hashtag_rank_position+=1
    end_time = datetime.now().timestamp()

print("Rank: "+str(rank)+"'s time: " + str(end_time - start_time) + "s")
