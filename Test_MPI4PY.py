# from mpi4py import MPI
# import numpy as np
#
# comm = MPI.COMM_WORLD
# size = comm.Get_size() # new: gives number of ranks in comm
# rank = comm.Get_rank()
#
# numDataPerRank = 10
# data = None
# if rank == 0:
#     with open("tinyTwitter.json",mode='r',encoding='utf-8') as f:
#         data = f
#     # when size=4 (using -n 4), data = [1.0:40.0]
#
# recvbuf = np.empty(numDataPerRank, dtype='d') # allocate space for recvbuf
# print(recvbuf)
# comm.Scatter(data, recvbuf, root=0)
#
# print('Rank: ',rank, ', recvbuf received: ',recvbuf)
#######################################################################

from mpi4py import MPI
import numpy as np

amode = MPI.MODE_WRONLY|MPI.MODE_CREATE
comm = MPI.COMM_WORLD
fh = MPI.File.Open(comm, "tinyTwitter.json", amode)



# offset = comm.Get_rank()*buffer.nbytes
# fh.Write_at_all(offset, buffer)

fh.Close()