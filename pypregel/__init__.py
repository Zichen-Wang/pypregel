import os

from mpi4py import MPI


class Pypregel:
    def __init__(self, reader):
        self.__reader = reader

    def run(self):
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        if rank == 0:
            n = self.__reader.get_num_of_vertices()
            num_of_workers = comm.Get_size() - 1
            batch = n // num_of_workers

            for i in range(0, num_of_workers):
                if i < n % num_of_workers:
                    vertex_list = self.__reader.read(batch + 1)
                else:
                    vertex_list = self.__reader.read(batch)

                comm.send(vertex_list, dest=i+1, tag=0)
        else:
            vertex_list = comm.recv(source=0, tag=0)

        comm.Barrier()

        if rank > 0:
            print(len(vertex_list))
            for v in vertex_list:
                print(v)

        MPI.Finalize()