from mpi4py import MPI
import numpy as np


class _Master:
    def __init__(self, comm, reader):
        self._comm = comm
        self._reader = reader
        self._global_agg = None
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
                    comm.send(send_list[i], dest=i + 1, tag=0)

        end_of_vertex = "$$$"
        for i in range(self._num_of_workers):
            comm.send(end_of_vertex, dest=i + 1, tag=0)

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
            # broadcast global superstep and global aggregation result
            comm.bcast(self._superstep, root=0)
            #print("master step" + str(self._superstep))

            # receive global aggregation results (should be improved by tree reduction)

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
        for i in range(self._num_of_workers):
            comm.send("$$$", dest=i + 1, tag=0)
