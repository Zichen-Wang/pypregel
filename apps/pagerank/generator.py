# vertex_id1:dst_id1 dst_id2 dst_id3
import random
num_vertices = 10000
num_edges_max = 20
f = open("graph" + str(num_vertices) + ".txt", "w")
vertices = [i for i in range(num_vertices)]
for vertex in vertices:
    f.write(str(vertex) + ':')
    num_edges = random.randint(0, num_edges_max)
    for i in range(num_edges):
        dst = random.randint(0, num_vertices - 1)
        if dst != vertex:
            f.write(str(dst) + ' ')
    f.write('\n')
f.close()
