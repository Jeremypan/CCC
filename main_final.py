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

#iterator
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

# initialize MPI
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()



read_file=open('bigTwitter.json','rb')
file_size=os.path.getsize('bigTwitter.json')

if size<2:
    total_hashtag_counter=Counter()
    total_language_counter=Counter()
    for json_data in seperate_files(read_file):
        for line in json_data:
            line = line.decode('utf-8')
            if line.endswith('[\n') or line.endswith("]\n"):
                # line end with [\n, ]\n and }\n
                continue
            elif line.endswith("}}]}\n"):
                line = line[0:len(line)-3]
            elif line.endswith(',\n'):
                line = line[0:len(line) - 2]
            elif line.endswith('\n'):
                line = line[0:len(line) - 1]
            # json load the data in string into dictionary
            try:
                temp = json.loads(line)
                # get hashtage value of dictionary
                for hashtag in temp['doc']['entities']['hashtags']:
                    total_hashtag_counter.update([hashtag['text'].lower()])
                # get language value of dictionary
                total_language_counter.update([temp['doc']['metadata']['iso_language_code']])
                if 'retweeted_status' in temp['doc'].keys():
                    for hashtag in temp['doc']['retweeted_status']['entities']['hashtags']:
                        total_hashtag_counter.update([hashtag['text'].lower()])
                    total_language_counter.update([temp['doc']['retweeted_status']['metadata']['iso_language_code']])
            except ValueError:
                try:
                    temp = json.loads(line[0:len(line) - 2])
                    for hashtag in temp['doc']['entities']['hashtags']:
                        total_hashtag_counter.update([hashtag['text'].lower()])
                    # get language value of dictionary
                    total_language_counter.update([temp['doc']['metadata']['iso_language_code']])
                    if 'retweeted_status' in temp['doc'].keys():
                        for hashtag in temp['doc']['retweeted_status']['entities']['hashtags']:
                            total_hashtag_counter.update([hashtag['text'].lower()])
                        total_language_counter.update([temp['doc']['retweeted_status']['metadata']['iso_language_code']])
                except ValueError:
                    continue
                # Value Error: check the line with value error (json cannot parse)
                # hash_tag_list = re.findall("#[^\s^,^\"^!^(\\\\n)]+", line, flags=0)
                # language_list = re.findall("\"lang\":\"[\w-]+\"", line, flags=0)
                # total_hashtag_counter.update(Counter(list(map(lambda x: x.lower(), hash_tag_list))))
                # # print("Rank: {0}, {1}".format(rank,hash_tag_list.most_common(10)))
                # total_language_counter.update(Counter(list(map(rt_language, language_list))))
                # print("Rank: {0}, {1}".format(rank,language_list.most_common(10)))

else:
    buffer_size = math.ceil(file_size / (size))
    offset=buffer_size*rank
    end_point=buffer_size+offset
    read_file.seek(offset,0)
    comm.Barrier() #!!!!important - synchonizaition
    hashtag_counter=Counter()
    language_counter=Counter()
    pointer=read_file.tell()
    while pointer<end_point and pointer<file_size:
        line=read_file.readline()
        pointer = read_file.tell()
        line=line.decode('utf-8')
        if line.endswith('[\n') or line.endswith("]\n"):
            # line end with [\n, ]\n and }\n
            continue
        elif line.endswith("}}]}\n"):
            line = line[0:len(line)-3]
        elif line.endswith(',\n'):
            line = line[0:len(line) - 2]
        elif line.endswith("}},\n"):
            line = line[0:len(line) - 2]
        elif line.endswith("\n"):
            line = line[0:len(line) - 1]
        # json load the data in string into dictionary
        try:
            temp=json.loads(line)
            # get hashtage value of dictionary
            for hashtag in temp['doc']['entities']['hashtags']:
                hashtag_counter.update([hashtag['text'].lower()])
            # get language value of dictionary
            language_counter.update([temp['doc']['metadata']['iso_language_code']])
            if 'retweeted_status' in temp['doc'].keys():
                for hashtag in temp['doc']['retweeted_status']['entities']['hashtags']:
                    hashtag_counter.update([hashtag['text'].lower()])
                language_counter.update([temp['doc']['retweeted_status']['metadata']['iso_language_code']])
        except ValueError:
            try:
                temp=json.loads(line[0:len(line)-2])
                for hashtag in temp['doc']['entities']['hashtags']:
                    hashtag_counter.update([hashtag['text'].lower()])
                # get language value of dictionary
                language_counter.update([temp['doc']['metadata']['iso_language_code']])
                if 'retweeted_status' in temp['doc'].keys():
                    for hashtag in temp['doc']['retweeted_status']['entities']['hashtags']:
                        hashtag_counter.update([hashtag['text'].lower()])
                    language_counter.update([temp['doc']['retweeted_status']['metadata']['iso_language_code']])
            except ValueError:
                continue
            # Value Error: check the line with value error (json cannot parse)
            # hash_tag_list=re.findall("#[^\s^,^\"^!^(\\\\n)]+",line,flags=0)
            # language_list=re.findall("\"lang\":\"[\w-]+\"",line,flags=0)
            # hashtag_counter.update(Counter(list(map(lambda x:x.lower(),hash_tag_list))))
            # # print("Rank: {0}, {1}".format(rank,hash_tag_list.most_common(10)))
            # language_counter.update(Counter(list(map(rt_language,language_list))))
            # print("Rank: {0}, {1}".format(rank,language_list.most_common(10)))

    comm.Barrier()
    total_hashtag = comm.gather(hashtag_counter, root=0)
    comm.Barrier()
    total_language = comm.gather(language_counter, root=0)
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
    print("----------------------------Hashtag_Rank---------------------------------")
    print("")
    for i,j in total_hashtag_counter.most_common(10):
        print("{0}. #{1}, {2:,}".format(hashtag_rank_position, i, j))
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
            print("{0}. #{1}, {2:,}".format(hashtag_rank_position, i, j))
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

