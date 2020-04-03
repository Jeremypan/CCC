#run command in wins: mpiexec -np 4 python main_final_mapreduce_iterator.py
#load modules for the script
import sys, json,io,math
from mpi4py import MPI
from collections import Counter
from datetime import datetime
import re

#default encoding utf-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


#initial the start time of each process
start_time = datetime.now().timestamp()



# initialize MPI
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
cores=8 # setup number of cores





read_file=MPI.File.Open(comm,"tinyTwitter.json",amode=MPI.MODE_RDONLY)
file_size=MPI.File.Get_size(read_file)

def rt_language(lang):
    data=json.loads("{"+lang+"}")
    return data["lang"]

if size<2:
    buffer_size = math.ceil(file_size / cores)  # assume 8 cores
    buffer = bytearray(buffer_size)
    total_hashtag_counter=Counter()
    total_language_counter=Counter()
    for i in range(cores):
        offset = buffer_size * i
        read_file.Read_at_all(offset, buffer)
        text = buffer.decode('utf-8')
        hash_tag_list = re.findall("#[^\s^,^\"^!^(\\\\n)]+", text, flags=0)
        language_list = re.findall("\"lang\":\"[\w-]+\"", text, flags=0)
        hash_tag_list = Counter(list(map(lambda x: x.lower(), hash_tag_list)))
        language_list = Counter(list(map(rt_language, language_list)))
        total_hashtag_counter.update(hash_tag_list)
        total_language_counter.update(language_list)
    read_file.Close()

else:
    buffer_size=math.ceil(file_size/(cores))#assume 8 cores
    buffer=bytearray(buffer_size)
     # assume 8 cores
    offset=buffer_size*rank
    read_file.Read_at_all(offset,buffer)
    comm.Barrier() #!!!!important - synchonizaition
    text=buffer.decode('utf-8')
    hash_tag_list=re.findall("#[^\s^,^\"^!^(\\\\n)]+",text,flags=0)
    language_list=re.findall("\"lang\":\"[\w-]+\"",text,flags=0)
    hash_tag_list=Counter(list(map(lambda x:x.lower(),hash_tag_list)))
    # print("Rank: {0}, {1}".format(rank,hash_tag_list.most_common(10)))
    language_list=Counter(list(map(rt_language,language_list)))
    # print("Rank: {0}, {1}".format(rank,language_list.most_common(10)))
    comm.Barrier()
    total_hashtag=comm.gather(hash_tag_list,root=0)
    comm.Barrier()
    total_language=comm.gather(language_list,root=0)
    comm.Barrier()



# from bs4 import BeautifulSoup
    # html_file = open('language_html_table', 'r', encoding='utf-8')
    # soup = BeautifulSoup(html_file, 'lxml')
    # data = soup.select('td')[2:]
    # language = {}
    # for i in range(0, len(data), 2):
    #     language[data[i + 1].string] = data[i].string
    # html_file.close()
    ######languange dictionary###########
if rank==0:
    language={'en': 'English (default)', 'ar': 'Arabic', 'bn': 'Bengali', 'cs': 'Czech', 'da': 'Danish', 'de': 'German',
     'el': 'Greek', 'es': 'Spanish', 'fa': 'Persian', 'fi': 'Finnish', 'fil': 'Filipino', 'fr': 'French',
     'he': 'Hebrew', 'hi': 'Hindi', 'hu': 'Hungarian', 'id': 'Indonesian', 'it': 'Italian', 'ja': 'Japanese',
     'ko': 'Korean', 'msa': 'Malay', 'nl': 'Dutch', 'no': 'Norwegian', 'pl': 'Polish', 'pt': 'Portuguese',
     'ro': 'Romanian', 'ru': 'Russian', 'sv': 'Swedish', 'th': 'Thai', 'tr': 'Turkish', 'uk': 'Ukrainian', 'ur': 'Urdu',
     'vi': 'Vietnamese', 'zh-cn': 'Chinese (Simplified)', 'zh-tw': 'Chinese (Traditional)', 'und': 'undefined',
     'zh': 'Chinese'}
    language['und']='undefined'
    language['zh']='Chinese'


if size<2:
    hashtag_rank_position = 1
    print("----------------------------Language_Rank---------------------------------")
    print("")
    for i,j in total_hashtag_counter.most_common(10):
        print("{0}. {1}, {2:,}".format(hashtag_rank_position, i, j))
        hashtag_rank_position += 1
    ####################################################################################
    lang_rank_position = 1
    print("----------------------------Language_Rank---------------------------------")
    print("")
    for i,j in total_language_counter.most_common(10):
        if i in language.keys():
            print("{0}. {1} ({2}), {3:,}".format(lang_rank_position, language[i], i, j))
        else:
            print("{0}. {1} ({2}), {3:,}".format(lang_rank_position, "unknown", i, j))
        lang_rank_position += 1


else:
    if rank==0:
        Sum_hastag_counter=Counter()
        Sum_language_counter=Counter()
        for i in total_hashtag:
            Sum_hastag_counter.update(i)
        for j in total_language:
            Sum_language_counter.update(j)
        print("----------------------------Hashtag_Rank---------------------------------")
        print("")
        hashtag_rank_position=1
        for i, j in Sum_hastag_counter.most_common(10):
            print("{0}. {1}, {2:,}".format(hashtag_rank_position, i, j))
            hashtag_rank_position += 1
        #####################################################################################
        lang_rank_position = 1
        print("----------------------------Language_Rank---------------------------------")
        print("")
        for i, j in Sum_language_counter.most_common(10):
            if i in language.keys():
                print("{0}. {1} ({2}), {3:,}".format(lang_rank_position, language[i], i, j))
            else:
                print("{0}. {1} ({2}), {3:,}".format(lang_rank_position, "unknown", i, j))
            lang_rank_position += 1

end_time = datetime.now().timestamp()
print("Rank: "+str(rank)+"'s time: " + str(end_time - start_time) + "s")

