from mpi4py import MPI
comm=MPI.COMM_WORLD
rank=comm.Get_rank()
size=comm.Get_size()


fh=MPI.File.Open(comm,'tinyTwitter.json',amode=MPI.MODE_RDONLY)

status=MPI.Status()
print(status)