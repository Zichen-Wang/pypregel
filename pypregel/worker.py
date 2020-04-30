import numpy as np
import time

from mpi4py import MPI
from threading import Thread
from queue import SimpleQueue
from collections import deque

from pypregel.message import _Message


_MASTER_MSG_TAG = 0
_USER_MSG_TAG = 1
_EOF = "$$$"
_BUFFER_CAPACITY = 100


class _Worker:
    # Graph partition, properties creation, thread creation are done during
    # initialization
    def __init__(self, comm, writer, combiner):
        self._comm = comm
        self._writer = writer
        self._combiner = combiner

        self._local_superstep = 0
        self._my_id = self._comm.Get_rank()
        # vid -> vertex object
        self._vertex_map = dict()

        self._num_of_vertices = None
        self._num_of_workers = None

        # vid
        self._active_vertices = set()
        self._halt_vertices = set()

        self._read()

        self._out_messages = SimpleQueue()
        self._send_thr = None
        self._sent_messages = 0

        self._in_messages = SimpleQueue()
        self._recv_thr = None
        self._received_messages = 0

        # vid -> message deque
        self._cur_messages = dict()
        self._next_messages = dict()

    def _read(self):
        comm = self._comm

        self._num_of_vertices, self._num_of_workers = comm.bcast(None, root=0)

        while True:
            vertex_list = comm.recv(source=0, tag=_MASTER_MSG_TAG)
            if vertex_list == _EOF:
                break

            for v in vertex_list:
                v.set_worker(self)
                self._vertex_map[v.get_vertex_id()] = v
                self._active_vertices.add(v.get_vertex_id())

    def _write(self):
        comm = self._comm
        vertex_list = []
        for v in self._vertex_map.values():
            vertex_list.append(self._writer.write_vertex(v))

        comm.send(
            vertex_list,
            dest=0,
            tag=_MASTER_MSG_TAG)

    def has_cur_message(self, vid):
        if vid not in self._cur_messages:
            return False

        return len(self._cur_messages[vid]) > 0

    def get_cur_message(self, vid):
        if not self.has_cur_message(vid):
            raise AttributeError("no more message")
        return self._cur_messages[vid].popleft()

    def send_cur_message(self, src_vid, dst_vid, msg_value):
        msg = _Message(src_vid, dst_vid, msg_value)

        dst_worker_id = self._vertex_to_worker_id(dst_vid)
        if dst_worker_id == self._my_id:
            if dst_vid not in self._next_messages:
                self._next_messages[dst_vid] = deque()

            self._next_messages[dst_vid].append(msg)
        else:
            self._out_messages.put(msg)

    def halt(self, vid):
        self._halt_vertices.add(vid)

    def get_superstep(self):
        return self._local_superstep

    def get_num_of_vertices(self):
        return self._num_of_vertices

    def _vertex_to_worker_id(self, vertex_id):
        return vertex_id % self._num_of_workers + 1

    def debug(self):
        print(self._num_of_vertices, self._num_of_workers)
        for v in self._vertex_map.values():
            print(v)

    '''
    Start to loop through supersteps.
    Master sends superstep, global aggregation result of last superstep.
    Workers receive the aggregation result of last superstep, workers synchronize superstep number.
    Workers loop through current active vertices.
    Workers send local aggregation result to master.
    Global Aggregation is performed in master.
    '''

    def run(self):
        # self.debug()
        comm = self._comm
        while True:
            # local superstep synchronization
            self._local_superstep = comm.bcast(None, root=0)

            # if the computation is over, break
            if self._local_superstep == -1:
                break

            # loop through current active vertices to compute
            '''
            Deal with in messages
            Perform computation for current step with value passed in
            Deal with out messages
            Pass in aggregator?
            '''
            self._cur_messages = self._next_messages
            self._next_messages = dict()

            self._send_thr = Thread(target=self._send_worker)
            self._recv_thr = Thread(target=self._recv_worker)
            self._sent_messages = 0
            self._received_messages = 0

            self._send_thr.start()
            self._recv_thr.start()

            for v in self._active_vertices:
                assert v in self._vertex_map
                self._vertex_map[v].compute()

            self._out_messages.put(_EOF)
            self._send_thr.join()

            comm.Barrier()

            comm.send(_EOF, dest=self._my_id, tag=_USER_MSG_TAG)
            self._recv_thr.join()

            print("%d %d %d\n" % (self._local_superstep, self._sent_messages, self._received_messages))

            while True:
                msg = self._in_messages.get()
                if msg == _EOF:
                    break

                dst_vid = msg.get_dst_vid()
                if dst_vid not in self._next_messages:
                    self._next_messages[dst_vid] = deque()

                self._next_messages[dst_vid].append(msg)

            self._halt_vertices -= self._next_messages.keys()
            #print("worker %d step %d: halt %s" % (self._my_id, self._local_superstep, self._halt_vertices))
            self._active_vertices = self._vertex_map.keys() - self._halt_vertices

            size_of_active_vertices = np.zeros(1)
            size_of_active_vertices[0] = len(self._active_vertices)

            comm.Reduce(size_of_active_vertices,
                        np.zeros(1),
                        op=MPI.SUM,
                        root=0)

            #print(
            #    "worker %d finishes %d" %
            #    (self._my_id, self._local_superstep))

        self._write()

    def _send_worker(self):
        comm = self._comm

        # combined_messages: dst_worker_id -> message list
        # combined_messages map buffers local messages
        buffer_size = 0
        combined_messages = dict()

        while True:
            msg = self._out_messages.get()
            if msg == _EOF:
                '''
                if self._combiner:
                    for worker_dst_id in combined_messages:
                        comm.send(
                            combined_messages[worker_dst_id],
                            dest=worker_dst_id,
                            tag=_USER_MSG_TAG
                        )
                '''
                break
            else:
                '''
                if self._combiner:
                    dst_worker_id = self._vertex_to_worker_id(msg.get_dst_vid())
                    if dst_worker_id not in combined_messages:
                        combined_messages[dst_worker_id] = msg
                    else:
                        combined_messages[dst_worker_id] = \
                            self._combiner.combine(combined_messages[dst_worker_id], msg)

                    buffer_size += 1

                    if buffer_size >= _BUFFER_CAPACITY:
                        for worker_dst_id in combined_messages:
                            comm.send(
                                combined_messages[worker_dst_id],
                                dest=worker_dst_id,
                                tag=_USER_MSG_TAG
                            )

                        combined_messages = dict()
                        buffer_size = 0
                else:
                '''
                self._sent_messages += 1
                comm.send(
                    msg,
                    self._vertex_to_worker_id(
                        msg.get_dst_vid()),
                    tag=_USER_MSG_TAG
                )
            # TODO combiner and local buffer list

    def _recv_worker(self):
        comm = self._comm

        while True:
            msg = comm.recv(source=MPI.ANY_SOURCE, tag=_USER_MSG_TAG)
            self._in_messages.put(msg)
            if msg == _EOF:
                break
            self._received_messages += 1
            # TODO combiner
