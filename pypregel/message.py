class _Message:
    def __init__(self, src_vid, dst_vid, value):
        self._src_vid = src_vid
        self._dst_vid = dst_vid
        self._value = value

    def get_src_vid(self):
        return self._src_vid

    def get_dst_vid(self):
        return self._dst_vid

    def get_value(self):
        return self._value
