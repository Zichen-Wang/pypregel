class Vertex:
    """
    Vertex is a public class that user should extend
    """

    def __init__(self, vid, value, out_edges):
        if not isinstance(vid, int):
            raise TypeError("vertex id should be integer.")

        self._vid = vid
        self._value = value
        self._out_edges = out_edges
        self._worker = None

    def set_worker(self, worker):
        """
        set the corresponding worker for this vertex
        :param worker: a _Worker object
        :return: None
        """

        self._worker = worker

    def has_worker(self):
        """
        check whether the worker is set
        :return: Boolean
        """

        return self._worker is not None

    def compute(self):
        """
        user should overwrite this method;
        otherwise there would be an Exception
        :return: None/Exception
        """

        raise NotImplementedError(
            "Vertex compute() interface not implemented.")

    def get_vertex_id(self):
        """
        get the vertex id
        :return: int
        """

        return self._vid

    def get_value(self):
        """
        get the vertex value
        :return: a value object; user-defined type
        """

        return self._value

    def set_value(self, value):
        """
        set the vertex value
        :param value: a value object; user-defined type
        :return: None
        """

        self._value = value

    def superstep(self):
        """
        get the current local superstep
        :return: int
        """

        if not self.has_worker():
            raise AttributeError("Vertex worker not set")

        return self._worker.get_superstep()

    def get_out_edges(self):
        """
        get the list of out edges
        :return: list of Edges
        """

        return self._out_edges

    def get_num_of_vertices(self):
        """
        get the total number of vertices
        :return: int
        """

        if not self.has_worker():
            raise AttributeError("Vertex worker not set")

        return self._worker.get_num_of_vertices()

    def vote_to_halt(self):
        """
        this vertex wants to be halt in the next step
        :return: None
        """

        if not self.has_worker():
            raise AttributeError("Vertex worker not set")

        self._worker.halt(self._vid)

    def send_message_to_vertex(self, dst_vid, msg_value):
        """
        send a message to another vertex
        :param dst_vid: destination vertex id
        :param msg_value: a value object; user-defined type
        :return: None
        """

        if not self.has_worker():
            raise AttributeError("Vertex worker not set")

        self._worker.send_cur_message(self._vid, dst_vid, msg_value)

    def has_message(self):
        """
        check whether this vertex has more message of this superstep
        :return: Boolean
        """

        if not self.has_worker():
            raise AttributeError("Vertex worker not set")

        return self._worker.has_cur_message(self._vid)

    def get_message(self):
        """
        get a message of this vertex
        :return: a _Message object
        """

        if not self.has_worker():
            raise AttributeError("Vertex worker not set")

        return self._worker.get_cur_message(self._vid).get_value()

    def send_message_to_all_neighbors(self, msg_value):
        """
        send messages to all neighbors of this vertex
        :param msg_value: a value object; user-defined type
        :return: None
        """

        for e in self._out_edges:
            self.send_message_to_vertex(e.get_dst_vid(), msg_value)


class Edge:
    """
    Edge is public class that user may want to extend
    """

    def __init__(self, dst_vid, value):
        if not isinstance(dst_vid, int):
            raise TypeError("destination id should be an integer.")

        self._dst_vid = dst_vid
        self._value = value

    def get_dst_vid(self):
        """
        get the destination vertex id of this edge
        :return: int
        """

        return self._dst_vid

    def get_value(self):
        """
        get the value (or weight) of this edge
        :return: a value object; user-defined type
        """

        return self._value

    def set_dst_vid(self, dst_vid):
        """
        set the destination vertex id of this edge
        :param dst_vid: int
        :return: None
        """

        self._dst_vid = dst_vid

    def set_value(self, value):
        """
        set the value of this edge
        :param value: a value object; user-defined type
        :return: None
        """

        self._value = value
