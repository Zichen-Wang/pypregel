from pypregel.vertex import Vertex


def _default_parse(line, header):
    if header:
        return int(line)
    else:
        line = line.strip().split(':')
        [vertex_id, vertex_value] = line[0].split(',')
        
        edges = []
        if line[1]:
            for e in line[1].split(' '):
                edges.append((e.split(',')[0], e.split(',')[1]))

        return vertex_id, vertex_value, edges


class Reader:
    def __init__(self, vertex_class, filename, user_parse=None):
        if not issubclass(vertex_class, Vertex):
            raise TypeError("vertex_class should extend Vertex")

        self.__fp = open(filename, 'r')
        self.__parse = _default_parse
        self.__Vertex = vertex_class

        if user_parse:
            self.__parse = user_parse

        self.__num_of_vertices = self.__parse(self.__fp.readline(), header=True)

    '''
    header: n

    vertex_id,vertex_value:vertex_id,weight[ vertex_id,weight]*
    '''
    def get_num_of_vertices(self):
        return self.__num_of_vertices

    def read(self, n):
        if n <= 0:
            raise ValueError("n should be positive.")

        cnt = 0
        vertex_list = []
        for line in self.__fp:
            vertex_id, vertex_value, edges = self.__parse(line, header=False)
            vertex_list.append(self.__Vertex(self.__num_of_vertices, vertex_id, vertex_value, edges))
            cnt += 1
            if cnt == n:
                return vertex_list

        return vertex_list

    def close(self):
        self.__fp.close()
