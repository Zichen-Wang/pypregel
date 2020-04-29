class _Master:
    def __init__(self, comm, reader):
        self._comm = comm
        self._reader = reader
        self._global_agg = None
        self._super_step = 0
        self._super_step_num = 30
        self._num_of_workers = comm.Get_size() - 1

        if self._num_of_workers <= 0:
            raise ValueError("the number of workers should be positive.")


        # Master performs input
        n = reader.read_num_of_vertices()
        n, self._num_of_workers = comm.bcast((n, self._num_of_workers), root=0)
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

        '''
        Start to loop through supersteps.
        Master sends superstep, global aggregation result of last superstep.
        Workers receive the aggregation result of last superstep, workers synchronize superstep number.
        Workers loop through current active vertices.
        Workers send local aggregation result to master.
        Global Aggregation is performed in master.
        '''

    def _aggregate(self, local_aggs):
        pass
        # get global aggregation results from local aggregation results list
        # return result
    
    def run(self):
        while self._super_step < self._super_step_num:
            # send global superstep
            [self.comm.send(self._super_step, dest=i + 1, tag=0) for i in range(self._num_of_workers)]

            # send global aggregation results
            [self.comm.send(self._global_agg, dest=i + 1, tag=1) for i in range(self._num_of_workers)]

            # receive global aggregation results
            local_aggs = []
            [self.comm.recv(local_aggs, dest=i + 1, tag=1) for i in range(self._num_of_workers)]
            self._global_agg = self._aggregate(local_aggs)

            # increment global superstep
            self._super_step += 1



