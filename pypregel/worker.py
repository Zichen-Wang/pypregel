import numpy as np

from mpi4py import MPI
from threading import Thread
from queue import SimpleQueue
from collections import deque

from pypregel.message import _Message


_MASTER_MSG_TAG = 0
_USER_MSG_TAG = 1
_FINISH_MSG_TAG = 2
_EOF = "$$$"


class _Worker:
    # Graph partition, properties creation, thread creation are done during
    # initialization
    def __init__(self, comm, writer):
        self._comm = comm
        self._writer = writer

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

        self._init_thread()

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

    def _init_thread(self):
        # message object deque
        self._out_stop = SimpleQueue()
        self._out_messages = SimpleQueue()
        _send_thr = Thread(target=self._send_worker, daemon=True)

        self._in_messages = []
        _recv_thr = Thread(target=self._recv_worker, daemon=True)

        _send_thr.start()
        _recv_thr.start()

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

            for v in self._active_vertices:
                assert v in self._vertex_map
                self._vertex_map[v].compute()

            self._out_messages.put_nowait(_EOF)
            msg = self._out_stop.get()
            assert msg == _EOF

            comm.Barrier()

            messaged_vertices = set()

            while len(self._in_messages) > 0:
                msg = self._in_messages.pop()
                dst_vid = msg.get_dst_vid()
                if dst_vid not in self._next_messages:
                    self._next_messages[dst_vid] = deque()

                self._next_messages[dst_vid].append(msg)
                messaged_vertices.add(dst_vid)
                if dst_vid in self._halt_vertices:
                    self._halt_vertices.remove(dst_vid)

            self._active_vertices = self._vertex_map.keys() - self._halt_vertices

            size_of_active_vertices = np.zeros(1)
            size_of_active_vertices[0] = len(self._active_vertices)
            comm.Reduce(size_of_active_vertices,
                        np.zeros(1),
                        op=MPI.SUM,
                        root=0)

            print(
                "worker %d finishes %d" %
                (self._my_id, self._local_superstep))

        comm.send(_EOF, dest=self._my_id, tag=_FINISH_MSG_TAG)

        self._write()

    def _send_worker(self):
        comm = self._comm
        stop_flag = False
        while True:
            if stop_flag and self._out_messages.empty():
                self._out_stop.put_nowait(_EOF)
                stop_flag = False

            msg = self._out_messages.get()
            if msg == _EOF:
                stop_flag = True
            else:
                comm.send(
                    msg,
                    self._vertex_to_worker_id(
                        msg.get_dst_vid()),
                    tag=_USER_MSG_TAG)
            # TODO combiner and local buffer list

    def _recv_worker(self):
        comm = self._comm

        while True:
            s = MPI.Status()
            comm.probe(status=s)

            if s.tag == _USER_MSG_TAG:
                msg = comm.recv(source=MPI.ANY_SOURCE, tag=_USER_MSG_TAG)
                self._in_messages.append(msg)
            elif s.tag == _FINISH_MSG_TAG:
                msg = comm.recv(source=MPI.ANY_SOURCE, tag=_FINISH_MSG_TAG)
                assert msg == _EOF
                break

            # TODO combiner
