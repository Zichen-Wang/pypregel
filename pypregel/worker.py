import numpy as np
import time

from mpi4py import MPI
from threading import Thread
from queue import Queue
from collections import deque

from pypregel.message import _Message


# define several Marcos
_MASTER_MSG_TAG = 0
_USER_MSG_TAG = 1
_EOF = "$$$"

# this is a parameter of this system
_BUFFER_CAPACITY = 1000


class _Worker:
    """
    _Worker is an inner class used to define methods of workers of Pypregel
    """

    def __init__(self, comm, writer, combiner, rtt):
        self._comm = comm
        self._writer = writer
        self._combiner = combiner
        self._RTT = rtt

        self._local_superstep = 0
        self._my_id = self._comm.Get_rank()

        # self._vertex_map: vid -> vertex object
        self._vertex_map = dict()

        self._num_of_vertices = None
        self._num_of_workers = None

        # self._active_vertices and self._halt_vertices
        # are sets of vertex id
        self._active_vertices = set()
        self._halt_vertices = set()

        # get the vertex and adjacent lists of vertices
        # belonging to this worker
        self._read()

        # two thread-safe queues are used for
        # sending and receiving threads

        self._out_messages = Queue()
        self._send_thr = None

        self._in_messages = Queue()
        self._recv_thr = None

        # self._cur_messages: vertex_id -> deque of _Message object
        self._cur_messages = dict()

        # self._next_messages: vertex_id -> deque of _Message object
        self._next_messages = dict()

    def _read(self):
        """
        read configuration information and vertex adjacent lists
        :return: None
        """

        comm = self._comm

        # get the configuration information
        self._num_of_vertices, self._num_of_workers = comm.bcast(None, root=0)

        while True:
            # get the adjacent lists
            vertex_list = comm.recv(source=0, tag=_MASTER_MSG_TAG)
            if vertex_list == _EOF:
                break

            for v in vertex_list:
                # set basic properties for each vertex
                v.set_worker(self)
                self._vertex_map[v.get_vertex_id()] = v
                self._active_vertices.add(v.get_vertex_id())

    def write(self):
        """
        invoke user defined writer to serialize a vertex,
        then send vertices to the master
        :return: None
        """

        comm = self._comm
        vertex_list = []
        for v in self._vertex_map.values():
            # serialize each vertex
            vertex_list.append(self._writer.write_vertex(v))

        # send results back to master
        comm.send(
            vertex_list,
            dest=0,
            tag=_MASTER_MSG_TAG)

    def has_cur_message(self, vertex_id):
        """
        check whether this vertex has message
        :param vertex_id: int
        :return: Boolean
        """

        if vertex_id not in self._cur_messages:
            return False

        return len(self._cur_messages[vertex_id]) > 0

    def get_cur_message(self, vertex_id):
        """
        get a piece of message from this vertex
        :param vertex_id:
        :return:
        """

        if not self.has_cur_message(vertex_id):
            raise AttributeError("no more message")

        return self._cur_messages[vertex_id].popleft()

    def send_cur_message(self, src_vid, dst_vid, msg_value):
        """
        send a message from src_vid to dst_vid with value msg_value
        :param src_vid: int, source vertex id
        :param dst_vid: int, destination vertex id
        :param msg_value: message value (user may decide its type)
        :return: None
        """

        # create a message object
        msg = _Message(self._local_superstep, src_vid, dst_vid, msg_value)

        # get the worker vertex id
        dst_worker_id = self._vertex_to_worker_id(dst_vid)

        if dst_worker_id == self._my_id:
            # if belonging to the same worker
            if dst_vid not in self._next_messages:
                self._next_messages[dst_vid] = deque()

            self._next_messages[dst_vid].append(msg)
        else:
            # otherwise, put this message into sending threading queue
            self._out_messages.put(msg)

    def halt(self, vertex_id):
        """
        deactivate a vertex
        :param vertex_id: int
        :return: None
        """

        self._halt_vertices.add(vertex_id)

    def get_superstep(self):
        """
        return the current local superstep
        :return: int
        """

        return self._local_superstep

    def get_num_of_vertices(self):
        """
        return the total number of vertices
        :return: int
        """

        return self._num_of_vertices

    def _vertex_to_worker_id(self, vertex_id):
        """
        map a vertex_id to worker_id
        :param vertex_id: int
        :return: int
        """

        return vertex_id % self._num_of_workers + 1

    def debug(self):
        print(self._num_of_vertices, self._num_of_workers)
        for v in self._vertex_map.values():
            print(v)

    def run(self):
        """
        start a loop until receiving -1 from master:
            1. master broadcasts superstep and worker synchronizes it
            2. create sending and receiving threads
            3. worker loops through current active vertices to compute
            4. worker splits received next-step messages to each vertex
            5. worker count the number of vertices that will be
                active in the next step; reduce the number to the master.
        """

        # self.debug()

        comm = self._comm

        while True:
            # local superstep synchronization
            self._local_superstep = comm.bcast(None, root=0)

            # if the computation is over, break
            if self._local_superstep == -1:
                break

            # set the map of cur messages
            self._cur_messages = self._next_messages

            # reset the map of next messages
            self._next_messages = dict()

            # create and start sending and receiving threads
            self._send_thr = Thread(target=self._send_worker)
            self._recv_thr = Thread(target=self._recv_worker)

            self._send_thr.start()
            self._recv_thr.start()

            # loop through current active vertices to compute
            for v in self._active_vertices:
                assert v in self._vertex_map
                self._vertex_map[v].compute()

            # we need to put an EOF to out_messages
            # to terminate the sending thread
            self._out_messages.put(_EOF)

            self._send_thr.join()

            # all workers finish sending now
            comm.Barrier()

            # there might be some messages on the way to be received
            # by the receiving thread.

            # for most cases, a RTT waiting time is enough

            # user may reset this RTT time
            # the default RTT time is 0.001
            time.sleep(self._RTT)

            # send the worker itself an EOF
            # to terminate the receiving thread
            comm.send(_EOF, dest=self._my_id, tag=_USER_MSG_TAG)

            self._recv_thr.join()

            # iterate the received messages
            while not self._in_messages.empty():
                msg = self._in_messages.get()

                # split a message into the map of next messages
                dst_vid = msg.get_dst_vid()
                if dst_vid not in self._next_messages:
                    self._next_messages[dst_vid] = deque()

                self._next_messages[dst_vid].append(msg)
            # end of while

            # the vertices that received messages should be active in the next step
            self._halt_vertices -= self._next_messages.keys()

            # compute the active vertices set
            self._active_vertices = self._vertex_map.keys() - self._halt_vertices

            # an all-reduce communication is performed
            # master will get the number of active vertices
            size_of_active_vertices = np.zeros(1)
            size_of_active_vertices[0] = len(self._active_vertices)

            comm.Reduce(size_of_active_vertices,
                        np.zeros(1),
                        op=MPI.SUM,
                        root=0)

            print(
                "worker %d finishes %d" %
                (self._my_id, self._local_superstep))

        # end of while

    def _send_worker(self):
        """
        the target function of the sending thread
        """

        comm = self._comm

        # msg_buf: dst_worker_id -> dst_vid -> messages
        msg_buf = dict()

        # msg_buf_size: dst_worker_id -> size
        msg_buf_size = dict()

        while True:
            msg = self._out_messages.get()
            if msg == _EOF:
                # if getting an EOF, send all messages from buffer
                for dst_worker_id in msg_buf:
                    # send each worker only one list
                    msg_list = []
                    for msgs in msg_buf[dst_worker_id].values():
                        msg_list.extend(msgs)

                    comm.send(
                        msg_list,
                        dest=dst_worker_id,
                        tag=_USER_MSG_TAG
                    )

                # end of for
                break
            else:
                # get the destination vertex id
                dst_vid = msg.get_dst_vid()

                # get the destination worker id
                dst_worker_id = self._vertex_to_worker_id(dst_vid)

                # create data structures if needed
                if dst_worker_id not in msg_buf:
                    msg_buf[dst_worker_id] = dict()
                    msg_buf_size[dst_worker_id] = 0

                if dst_vid not in msg_buf[dst_worker_id]:
                    msg_buf[dst_worker_id][dst_vid] = []

                # append the new message
                msg_buf[dst_worker_id][dst_vid].append(msg)

                # increase the corresponding buf size
                msg_buf_size[dst_worker_id] += 1

                if self._combiner:
                    # if there is a combiner, then combine two messages
                    while len(msg_buf[dst_worker_id][dst_vid]) >= 2:
                        # get the last two messages
                        msg_x = msg_buf[dst_worker_id][dst_vid].pop()
                        msg_y = msg_buf[dst_worker_id][dst_vid].pop()

                        # invoke the user-defined combiner
                        msg_src, msg_value = self._combiner.combine(
                            (msg_x.get_src_vid(), msg_x.get_value()),
                            (msg_y.get_src_vid(), msg_y.get_value())
                        )

                        # append the combined message back
                        msg_buf[dst_worker_id][dst_vid].append(
                            _Message(
                                self._local_superstep,
                                msg_src,
                                dst_vid,
                                msg_value
                            )
                        )

                        # decrease the buf size by one
                        msg_buf_size[dst_worker_id] -= 1

                    # end of while

                # end of if

                if msg_buf_size[dst_worker_id] >= _BUFFER_CAPACITY:
                    # if the buffer is full, then send the messages of the destination worker
                    msg_list = []
                    for msgs in msg_buf[dst_worker_id].values():
                        msg_list.extend(msgs)

                    comm.send(
                        msg_list,
                        dest=dst_worker_id,
                        tag=_USER_MSG_TAG
                    )

                    # reset the msg buf and msg buf size
                    msg_buf[dst_worker_id] = dict()
                    msg_buf_size[dst_worker_id] = 0

                # end of if

            # end of if-else

        # end of while

    # end of _send_worker

    def _recv_worker(self):
        """
        the target function of the receiving thread
        """

        comm = self._comm

        while True:
            # wait for messages from any source
            msg_list = comm.recv(source=MPI.ANY_SOURCE, tag=_USER_MSG_TAG)

            if msg_list != _EOF:
                # if receiving a message list
                for msg in msg_list:
                    # append each message belonging to this superstep
                    # to the message buf queue
                    if msg.get_msg_superstep() != self._local_superstep:
                        continue

                    self._in_messages.put(msg)
            else:
                # if receiving EOF, the break
                break
