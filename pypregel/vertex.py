class Vertex:
    def __init__(self, vid, value, out_edges):
        if not isinstance(vid, int):
            raise TypeError("vertex id should be integer.")

        self._vid = vid
        self._value = value
        self._out_edges = out_edges
        self._worker = None

    def set_worker(self, worker):
        self._worker = worker

    def has_worker(self):
        return self._worker is not None

    def compute(self):
        raise NotImplementedError(
            "Vertex compute() interface not implemented.")

    def get_vertex_id(self):
        return self._vid

    def get_value(self):
        return self._value

    def set_value(self, value):
        self._value = value

    def superstep(self):
        if not self.has_worker():
            raise AttributeError("Vertex worker not set")
        return self._worker.get_superstep()

    def get_out_edges(self):
        return self._out_edges

    def get_num_of_vertices(self):
        if not self.has_worker():
            raise AttributeError("Vertex worker not set")
        return self._worker.get_num_of_vertices()

    def vote_to_halt(self):
        if not self.has_worker():
            raise AttributeError("Vertex worker not set")
        self._worker.halt(self._vid)

    def send_message_to_vertex(self, dst_vid, msg_value):
        self._worker.send_cur_message(self._vid, dst_vid, msg_value)

    def has_message(self):
        return self._worker.has_cur_message(self._vid)

    def get_message(self):
        return self._worker.get_cur_message(self._vid).get_value()

    def send_message_to_all_neighbors(self, msg_value):
        for e in self._out_edges:
            self.send_message_to_vertex(e.get_dst_vid(), msg_value)


class Edge:
    def __init__(self, dst_vid, value):
        if not isinstance(dst_vid, int):
            raise TypeError("destination id should be integer.")

        self._dst_vid = dst_vid
        self._value = value

    def get_dst_vid(self):
        return self._dst_vid

    def get_value(self):
        return self._value

    def set_dst_vid(self, dst_vid):
        self._dst_vid = dst_vid

    def set_value(self, value):
        self._value = value
