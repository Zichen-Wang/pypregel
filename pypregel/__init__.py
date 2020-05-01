from mpi4py import MPI
from pypregel.master import _Master
from pypregel.worker import _Worker


class Pypregel:
    """
    Pypregel is the app class and its object is a starter
    """

    def __init__(self, reader, writer, combiner=None, rtt=0.001):
        # MPI is used to pass messages among processes
        self._comm = MPI.COMM_WORLD
        self.rank = self._comm.Get_rank()

        if self.rank == 0:
            self._master = _Master(self._comm, reader, writer)
        else:
            self._worker = _Worker(self._comm, writer, combiner, rtt)

        self._comm.Barrier()

    def run(self):
        """
        run an app: read, compute and write
        :return: None
        """

        if self.rank == 0:
            self._master.run()
        else:
            self._worker.run()
