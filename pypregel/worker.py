import threading


class _Worker:
    def __init__(self, comm):
        self._comm = comm
        self._local_superstep = 0
        self._my_id = self._comm.Get_rank()
        self._num_of_vertices = None
        self._num_of_workers = None
        self._num_of_vertices, self._num_of_workers \
            = comm.bcast((self._num_of_vertices, self._num_of_workers), root=0)
        self._vertex_list = []
        self._vertex_active_flag = {}
        self._vertex_active_flag_next_step = {}

        while True:
            vertex_list = comm.recv(source=0, tag=0)
            if vertex_list == "$$$":
                break

            for v in vertex_list:
                v.set_worker(self)
                self._vertex_list.append(v)
                self._vertex_active_flag[v.get_vertex_id()] = False
                self._vertex_active_flag_next_step[v.get_vertex_id()] = False

        self._in_messages = []
        self._out_messages = []

        self._in_lock = threading.Lock()
        self._out_lock = threading.Lock()

    def get_superstep(self):
        return self._local_superstep

    def get_num_of_vertices(self):
        return self._num_of_vertices

    def _vertex_to_worker_id(self, vertex_id):
        return vertex_id % self._num_of_workers + 1

    def debug(self):
        print(self._num_of_vertices, self._num_of_workers)
        for v in self._vertex_list:
            print(v)

    def run(self):
        self.debug()
