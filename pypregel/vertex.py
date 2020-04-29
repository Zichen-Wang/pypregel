class Vertex:
    def __init__(self, vid, value, out_edges):
        if type(vid) is not int:
            raise TypeError("vertex id should be integer.")

        self._vid = vid
        self._value = value
        self._out_edges = out_edges
        self._worker = None

        # Messages received in last superstep
        self.prev_messages = []
        # Messages received in current superstep
        self.cur_messages = []

    def set_worker(self, worker):
        self._worker = worker

    def has_worker(self):
        return self._worker is None

    def compute(self):
        raise NotImplementedError("Vertex compute() interface not implemented.")

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
        self._worker.deactive(self._vid)

    def send_message_to_vertex(self, dst_id, msg):
        pass

    def send_message_to_all_neighbors(self, msg):
        for e in self._out_edges:
            self.send_message_to_vertex(e.get_dst_id(), msg)

    def __str__(self):
        s = "%d,%d:" % (self._vid, self._value)
        for e in self._out_edges:
            s += str(e) + " "
        return s


class Edge:
    def __init__(self, dst_vid, value):
        if type(dst_vid) is not int:
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

    def __str__(self):
        return "%d %d" % (self._dst_vid, self._value)
