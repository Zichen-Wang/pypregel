class Reader:
    def __init__(self, config, graph):
        self.config_fp = open(config, "r")
        self.graph_fp = open(graph, "r")

    def read_num_of_vertices(self):
        pass

    def read_vertex(self):
        pass

    def read_batch(self, batch_size):
        if batch_size <= 0:
            raise ValueError("batch size should be positive.")

        vertex_list = []
        for _ in range(batch_size):
            v = self.read_vertex()
            if v is None:
                break
            vertex_list.append(v)

        return vertex_list

    def __del__(self):
        self.config_fp.close()
        self.graph_fp.close()
