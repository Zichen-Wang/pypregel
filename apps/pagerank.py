import sys

from pypregel import Pypregel
from pypregel.vertex import Vertex, Edge
from pypregel.reader import Reader


class PageRankVertex(Vertex):
    def compute(self):
        if self.superstep() >= 1:
            s = 0
            for msg in self.messages:
                s += msg.value

            self.set_value(0.15 / self.get_num_of_vertices() + 0.85 * s)

        if self.superstep() < 30:
            n = len(self.get_out_edges())
            self.send_message_to_all_neighbors(self.get_value() / n)
        else:
            self.vote_to_halt()


class PageRankReader(Reader):
    def read_num_of_vertices(self):
        line = self.config_fp.readline()
        return int(line)

    def read_vertex(self):
        line = self.graph_fp.readline()
        if not line:
            return None

        line = line.strip().split(':')
        vertex_id, vertex_value = line[0].split(',')

        edges = []
        if line[1]:
            for e in line[1].split(' '):
                edges.append(Edge(int(e.split(',')[0]), float(e.split(',')[1])))

        return PageRankVertex(int(vertex_id), float(vertex_value), edges)


def main():
    if len(sys.argv) < 3:
        print("usage: python %s [config] [graph]" % sys.argv[0])
        return

    pagerank_reader = PageRankReader(sys.argv[1], sys.argv[2])
    pagerank = Pypregel(pagerank_reader)

    pagerank.run()


if __name__ == "__main__":
    main()
