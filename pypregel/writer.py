class Writer:
    """
    Write is a public class that user has to extend
    """

    def __init__(self, output_file):
        # the file pointer is private
        # user only needs to define how to translate a vertex

        self._output_file_fp = open(output_file, "w")

    def write_vertex(self, vertex):
        """
        user needs to overwrite this method
            and return a serialized tuple
        :param vertex: a Vertex object
        :return: a tuple (vertex_id, str)
        """

        raise NotImplementedError(
            "Writer write_vertex() interface not implemented"
        )

    def write_batch_to_file(self, vertex_list):
        """
        write a vertex list to file
        :param vertex_list: a list of Vertex objects
        :return: None
        """

        for v in vertex_list:
            self._output_file_fp.write("%d %s\n" % (v[0], v[1]))

    def __del__(self):
        """
        close the file pointer
        :return: None
        """

        self._output_file_fp.close()
