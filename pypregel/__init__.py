from mpi4py import MPI
from pypregel.master import _Master
from pypregel.worker import _Worker


class Pypregel:
    def __init__(self, reader, writer, combiner=None):
        self._comm = MPI.COMM_WORLD
        self.rank = self._comm.Get_rank()
        if self.rank == 0:
            self._master = _Master(self._comm, reader, writer)
        else:
            self._worker = _Worker(self._comm, writer, combiner)

        self._comm.Barrier()

    def run(self):
        if self.rank == 0:
            self._master.run()
        else:
            self._worker.run()
