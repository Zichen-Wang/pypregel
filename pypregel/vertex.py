class Vertex:
    def __init__(self, num_of_vertices, vertex_id, vertex_value, edges):
        self.__num_of_vertices = num_of_vertices
        self.__vertex_id = vertex_id
        self.__vertex_value = float(vertex_value)
        self.__out_edges = edges

    def __str__(self):
        return "%s %s %s %s" % (self.__num_of_vertices, self.__vertex_id, self.__vertex_value, self.__out_edges)
