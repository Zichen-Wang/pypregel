import threading


class _Worker:
    # Graph partition, properties creation, thread creation are done during initialization
    def __init__(self, comm):
        self._comm = comm
        self._local_superstep = 0
        self._local_agg = None
        self._my_id = self._comm.Get_rank()
        self._read()

        self._num_of_vertices = None
        self._num_of_workers = None

        self._vertex_list = []
        self._active_vertices = []
        self._active_vertices_next_step = []

        self._in_messages = []
        self._out_messages = []

        self._in_lock = threading.Lock()
        self._out_lock = threading.Lock()

    def _read(self):
        comm = self._comm

        self._num_of_vertices, self._num_of_workers \
            = comm.bcast((self._num_of_vertices, self._num_of_workers), root=0)

        while True:
            vertex_list = comm.recv(source=0, tag=0)
            if vertex_list == "$$$":
                break

            for v in vertex_list:
                v.set_worker(self)
                self._vertex_list.append(v)
                self._active_vertices.append(v)

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

    '''
    Start to loop through supersteps.
    Master sends superstep, global aggregation result of last superstep.
    Workers receive the aggregation result of last superstep, workers synchronize superstep number.
    Workers loop through current active vertices.
    Workers send local aggregation result to master.
    Global Aggregation is performed in master.
    '''

    def run(self):
        while True:
            # local superstep synchronization
            self._local_superstep, self._local_agg = \
                self._comm.bcast((self._local_superstep, self._local_superstep), root=0)

            # if the computation is over, break
            if self._local_superstep == -1:
                break

        # receive aggregation result of the last superstep
        self._local_agg = self._comm.recv(source=0, tag=0)

        # loop through current active vertices to compute
        '''
        Deal with in messages
        Perform computation for current step with value passed in
        Deal with out messages
        Pass in aggregator?
        '''
        [vertex.compute() for vertex in self._active_vertices]

        # aggregate local vertices (should be improved by tree reduction)
        #self._comm.send(self._local_agg, dest=0, tag=0)
