import numpy as np
import pickle

from mpi4py import MPI


_MASTER_MSG_TAG = 0
_EOF = "$$$"


class _Master:
    def __init__(self, comm, reader, writer):
        self._comm = comm
        self._reader = reader
        self._writer = writer
        self._superstep = 0
        self._num_of_workers = comm.Get_size() - 1

        if self._num_of_workers <= 0:
            raise ValueError("the number of workers should be positive.")

        self._num_of_vertices = reader.read_num_of_vertices()
        self._num_of_active_vertices = 0

        self._split_work()

    def _split_work(self):
        comm = self._comm
        reader = self._reader

        # Master performs input
        comm.bcast((self._num_of_vertices, self._num_of_workers), root=0)
        batch_size = 1

        while True:
            vertex_list = reader.read_batch(batch_size)
            if len(vertex_list) == 0:
                break

            send_list = [[] for _ in range(self._num_of_workers)]

            for v in vertex_list:
                target = v.get_vertex_id() % self._num_of_workers
                send_list[target].append(v)

            for i in range(self._num_of_workers):
                if len(send_list[i]) > 0:
                    comm.send(send_list[i], dest=i + 1, tag=_MASTER_MSG_TAG)

        for i in range(self._num_of_workers):
            comm.send(_EOF, dest=i + 1, tag=_MASTER_MSG_TAG)

    def _aggregate(self, local_aggs):
        pass
        # get global aggregation results from local aggregation results list
        # return result

    '''
    Start to loop through supersteps.
    Master sends superstep, global aggregation result of last superstep.
    Workers receive the aggregation result of last superstep, workers synchronize superstep number.
    Workers loop through current active vertices.
    Workers send local aggregation result to master.
    Global Aggregation is performed in master.
    '''

    def run(self):
        self._superstep = 1
        self._num_of_active_vertices = self._num_of_vertices
        comm = self._comm

        while self._num_of_active_vertices > 0:
            # broadcast global superstep
            comm.bcast(self._superstep, root=0)
            #print("master step" + str(self._superstep))

            # receive global aggregation results (should be improved by tree
            # reduction)

            # reset the number of active vertices
            reduced_active_vertices = np.zeros(1)

            comm.Barrier()

            #print("master before reduce step " + str(self._superstep))

            comm.Reduce(np.zeros(1),
                        reduced_active_vertices,
                        op=MPI.SUM,
                        root=0)

            # increment global superstep
            self._superstep += 1
            self._num_of_active_vertices = reduced_active_vertices[0]

        # broadcast to all workers that the computation is over
        comm.bcast(-1, root=0)

        self._write()

    def _write(self):
        comm = self._comm
        for i in range(self._num_of_workers):
            vertex_list = comm.recv(
                source=i + 1,
                tag=_MASTER_MSG_TAG
            )

            self._writer.write_batch_to_file(vertex_list)
