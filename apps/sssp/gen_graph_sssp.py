# vertex_id0:dst_id0,w0 dst_id1,w1 dst_id2,w2
import random

num_vertices = 5000000
num_edges_max = 20

f = open("graph" + str(num_vertices), "w")

vertices = [i for i in range(num_vertices)]

for vertex in vertices:
    f.write(str(vertex) + ':')
    num_edges = random.randint(0, num_edges_max)

    for i in range(num_edges):
        dst = random.randint(0, num_vertices - 1)
        weight = random.randint(0, 100)
        if dst != vertex:
            f.write(str(dst) + ',' + str(weight) + ' ')

    f.write('\n')

f.close()
