import sys

from pypregel import Pypregel
from pypregel.vertex import Vertex, Edge
from pypregel.reader import Reader
from pypregel.writer import Writer


INT_MAX = 1e10
START_VERTEX_ID = 0


class SSSPVertex(Vertex):
    def compute(self):
        if self.superstep() == 1:
            if self.get_vertex_id() == START_VERTEX_ID:
                self.set_value(0)
            else:
                self.set_value(INT_MAX)
        else:
            mm = INT_MAX
            while self.has_message():
                msg = self.get_message()
                mm = min(mm, msg)

            if mm < self.get_value():
                mm = self.get_value()
                for e in self.get_out_edges():
                    self.send_message_to_vertex(e.get_dst_vid(), mm + e.get_value())

        self.vote_to_halt()


class SSSPReader(Reader):
    def read_num_of_vertices(self):
        line = self.config_fp.readline()
        return int(line)

    def read_vertex(self):
        line = self.graph_fp.readline()
        if not line:
            return None

        line = line.strip().split(':')
        vertex_id = int(line[0])

        edges = []
        if line[1]:
            for e in line[1].split(' '):
                edges.append(Edge(int(e.split(',')[0]), int(e.split(',')[1])))

        return SSSPVertex(vertex_id, None, edges)


class SSSPWriter(Writer):
    def write_vertex(self, vertex):
        return vertex.get_vertex_id(), str(vertex.get_value())


def main():
    if len(sys.argv) < 4:
        print("usage: python %s [config] [graph] [out_file]" % sys.argv[0])
        return

    sssp_reader = SSSPReader(sys.argv[1], sys.argv[2])
    sssp_writer = SSSPWriter(sys.argv[3])
    sssp = Pypregel(sssp_reader, sssp_writer)

    sssp.run()


if __name__ == "__main__":
    main()
