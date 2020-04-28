import sys

from pypregel import Pypregel
from pypregel.vertex import Vertex
from pypregel.reader import Reader


class PageRankVertex(Vertex):
    def compute(self):
        if self.superstep() >= 1:
            s = 0
            for msg in self.messages:
                s += msg.value

            self.set_value(0.15 / self.get_num_of_vertex() + 0.85 * s)

        if self.superstep() < 30:
            n = self.out_edge.size()
            self.send_message_to_all_neighbors(self.get_value() / n)
        else:
            self.vote_to_halt()
        

def main():
    if len(sys.argv) < 2:
        print("usage: python %s [file_name]" % sys.argv[0])
        return

    reader = Reader(PageRankVertex, sys.argv[1])
    pagerank = Pypregel(reader)

    pagerank.run()


if __name__ == "__main__":
    main()
