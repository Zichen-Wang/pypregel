import numpy as np
import time

from mpi4py import MPI
from threading import Thread
from queue import SimpleQueue
from collections import deque

from pypregel.message import _Message


class _Worker:
    # Graph partition, properties creation, thread creation are done during initialization
    def __init__(self, comm):
        self._comm = comm

        self._local_superstep = 0
        self._my_id = self._comm.Get_rank()
        # vid -> vertex object
        self._vertex_map = dict()

        self._num_of_vertices = None
        self._num_of_workers = None

        # vid
        self._active_vertices = set()
        self._vote_halt_vertices = set()

        self._ROOT_TAG = 0
        self._read()

        self._USER_MESSAGE_TAG = 1
        self._init_thread()

        # vid -> message deque
        self._cur_messages = dict()
        self._next_messages = dict()

    def _read(self):
        comm = self._comm

        self._num_of_vertices, self._num_of_workers = comm.bcast(None, root=0)

        while True:
            vertex_list = comm.recv(source=0, tag=self._ROOT_TAG)
            if vertex_list == "$$$":
                break

            for v in vertex_list:
                v.set_worker(self)
                self._vertex_map[v.get_vertex_id()] = v
                self._active_vertices.add(v.get_vertex_id())

    def _init_thread(self):
        # message object deque
        self._stop_thread = False
        self._out_messages = SimpleQueue()
        self._send_thr = Thread(target=self._send_worker, daemon=True)

        self._in_messages = []
        self._recv_thr = Thread(target=self._recv_worker, daemon=True)

        self._send_thr.start()
        self._recv_thr.start()

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
            self._out_messages.put_nowait(msg)

    def halt(self, vid):
        self._vote_halt_vertices.add(vid)

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
        #self.debug()
        comm = self._comm
        while True:
            # local superstep synchronization
            self._local_superstep = comm.bcast(None, root=0)
            print("worker at " + str(self._local_superstep))

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
            self._vote_halt_vertices = set()

            for v in self._active_vertices:
                assert (v in self._vertex_map)
                self._vertex_map[v].compute()

            # aggregate local vertices (should be improved by tree reduction)
            # self._comm.send(self._local_agg, dest=0, tag=0)

            while not self._out_messages.empty():
                time.sleep(0)

            comm.Barrier()

            self._cur_messages = self._next_messages
            messaged_vertices = set()

            while len(self._in_messages) > 0:
                msg = self._in_messages.pop()
                dst_vid = msg.get_dst_vid()
                if dst_vid not in self._cur_messages:
                    self._cur_messages[dst_vid] = deque()

                self._cur_messages[dst_vid].append(msg)
                messaged_vertices.add(dst_vid)

            self._next_messages = dict()

            self._active_vertices = self._vertex_map.keys() - (self._vote_halt_vertices - messaged_vertices)

            size_of_active_vertices = np.zeros(1)
            size_of_active_vertices[0] = len(self._active_vertices)
            comm.Reduce(size_of_active_vertices,
                        np.zeros(1),
                        op=MPI.SUM,
                        root=0)

            print("worker %d %d" % (self._my_id, self._local_superstep))

        for v in self._vertex_map.values():
            print("%d %f" % (v.get_vertex_id(), v.get_value()))

    def _send_worker(self):
        while True:
            msg = self._out_messages.get()
            self._comm.send(msg, self._vertex_to_worker_id(msg.get_src_vid()), tag=self._USER_MESSAGE_TAG)
            # TODO combiner and local buffer list

    def _recv_worker(self):
        while True:
            s = MPI.Status()
            self._comm.probe(status=s)

            if s.tag == self._USER_MESSAGE_TAG:
                msg = self._comm.recv(source=MPI.ANY_SOURCE, tag=self._USER_MESSAGE_TAG)
                self._in_messages.append(msg)
            elif s.tag == self._ROOT_TAG:
                msg = self._comm.recv(source=MPI.ANY_SOURCE, tag=self._ROOT_TAG)
                if msg == "$$$":
                    break

            # TODO combiner
