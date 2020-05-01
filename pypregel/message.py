class _Message:
    """
    _Message is a private class
    """
    def __init__(self, superstep, src_vid, dst_vid, value):
        self._superstep = superstep
        self._src_vid = src_vid
        self._dst_vid = dst_vid
        self._value = value

    def get_msg_superstep(self):
        """
        get the superstep that this message belongs to
        :return: int
        """

        return self._superstep

    def get_src_vid(self):
        """
        get the source vertex id of this message
        :return: int
        """

        return self._src_vid

    def get_dst_vid(self):
        """
        get the source vertex id of this message
        :return: int
        """

        return self._dst_vid

    def get_value(self):
        """
        get the value of this message
        :return: a value object; user-defined type
        """

        return self._value
