#run command in wins: mpiexec -np 4 python main_final_mapreduce_iterator.py
#load modules for the script
import sys, json,io,math,os
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

read_file=open('tinyTwitter.json','rb')
file_size=os.path.getsize('tinyTwitter.json')
if size<2:
    print('test later')
else:
    buffer_size=math.ceil(file_size/size)
    offset=buffer_size*rank
    read_file.seek(offset,1)
    print('Rank: {0} {1}'.format(rank))



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

"""
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
"""
