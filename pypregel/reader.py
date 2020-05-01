class Reader:
    """
    Reader is a public class that user has to extend
    """
    def __init__(self, config, graph):
        """
        constructor takes two file names;
        open two file pointer for user;
        user may also overwrite the constructor
        :param config: str
        :param graph: str
        """

        self.config_fp = open(config, "r")
        self.graph_fp = open(graph, "r")

    def read_num_of_vertices(self):
        """
        user needs to overwrite this method
            and return the total number of vertices
        :return: int
        """

        raise NotImplementedError(
            "Reader read_num_of_vertices() interface not implemented"
        )

    def read_vertex(self):
        """
        user needs to overwrite this method
            and provide the vertex id, the vertex value
            and the list of out edges
        :return: a Vertex object
        """

        raise NotImplementedError(
            "Reader read_vertex() interface not implemented"
        )

    def read_batch(self, batch_size):
        """
        read multiple lines before sending to worker
        :param batch_size: int
        :return: a list of Vertex objects
        """

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
        """
        close file pointers
        :return: None
        """

        self.config_fp.close()
        self.graph_fp.close()
