class Writer:
    def __init__(self, output_file):
        self._output_file_fp = open(output_file, "w")

    def write_vertex(self, vertex):
        raise NotImplementedError(
            "Writer write_vertex() interface not implemented"
        )

    def write_batch_to_file(self, vertex_list):
        for v in vertex_list:
            self._output_file_fp.write("%d %s\n" % (v[0], v[1]))

    def __del__(self):
        self._output_file_fp.close()
