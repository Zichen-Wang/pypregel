from mpi4py import MPI
from pypregel.master import _Master
from pypregel.worker import _Worker
import time


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
            start_time = time.time()
            self._master.run()
            self._comm.Barrier()
            print("--- %f sec ---" % (time.time() - start_time))
            # gather results and write to file
            self._master.write()
        else:
            self._worker.run()
            self._comm.Barrier()
            # call writer to serialize vertices
            self._worker.write()
