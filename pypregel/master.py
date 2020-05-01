import numpy as np

from mpi4py import MPI


# define several Marcos
_MASTER_MSG_TAG = 0

# this is a parameter of this system
_BATCH_SIZE = 1000
_EOF = "$$$"


class _Master:
    """
    _Master is an inner class used to define methods of the master of Pypregel
    """

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
        """
        split a batch into different workers
        :return: None
        """

        comm = self._comm
        reader = self._reader

        # Master broadcasts the configuration information
        comm.bcast((self._num_of_vertices, self._num_of_workers), root=0)

        while True:
            # set a infinite loop and read a batch of vertices
            vertex_list = reader.read_batch(_BATCH_SIZE)

            # if no remaining vertex, then break
            if len(vertex_list) == 0:
                break

            send_list = [[] for _ in range(self._num_of_workers)]

            for v in vertex_list:
                # set the target worker id
                target = v.get_vertex_id() % self._num_of_workers
                send_list[target].append(v)

            for i in range(self._num_of_workers):
                if len(send_list[i]) > 0:
                    comm.send(send_list[i], dest=i + 1, tag=_MASTER_MSG_TAG)

        # end of while

        # tell each worker reading should be over
        for i in range(self._num_of_workers):
            comm.send(_EOF, dest=i + 1, tag=_MASTER_MSG_TAG)

    def run(self):
        """
        start a loop until the number of active vertices is 0
            1. master broadcasts superstep and worker synchronizes it
            2. waiting for workers and reduce the number of active vertices
        """

        self._superstep = 1
        self._num_of_active_vertices = self._num_of_vertices
        comm = self._comm

        while self._num_of_active_vertices > 0:
            # master broadcasts the global superstep
            comm.bcast(self._superstep, root=0)

            # this barrier corresponds to the barrier in workers
            comm.Barrier()

            # reset the number of active vertices
            reduced_active_vertices = np.zeros(1)

            comm.Reduce(np.zeros(1),
                        reduced_active_vertices,
                        op=MPI.SUM,
                        root=0)

            # increase the global superstep by one
            self._superstep += 1

            # set the number of active vertices
            self._num_of_active_vertices = reduced_active_vertices[0]

        # broadcast to all workers that the computation is over
        comm.bcast(-1, root=0)

        # gather results and write to file
        self._write()

    def _write(self):
        """
        gather vertex lists from each worker
        and write them to file
        :return: None
        """

        comm = self._comm

        for i in range(self._num_of_workers):
            # expect a vertex list from each worker
            vertex_list = comm.recv(
                source=i + 1,
                tag=_MASTER_MSG_TAG
            )

            # write this vertex list to file
            self._writer.write_batch_to_file(vertex_list)
