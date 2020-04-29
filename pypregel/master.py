class _Master:
    def __init__(self, comm, reader):
        self._comm = comm
        self._reader = reader
        self._super_step = 0
        self._num_of_workers = comm.Get_size() - 1

        if self._num_of_workers <= 0:
            raise ValueError("the number of workers should be positive.")

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

    def run(self):
        pass

