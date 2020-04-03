import os
read_file=open('tinyTwitter.json','rb')
file_size=os.path.getsize('tinyTwitter.json')
pointer=0
end=file_size
while pointer<end and pointer<file_size:
    print("Before rl: "+str(pointer))
    read_file.readline()
    pointer=read_file.tell()
    print("After rl: "+str(pointer))