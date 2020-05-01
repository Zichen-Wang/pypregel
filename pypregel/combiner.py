class Combiner:
    """
    Combiner is a public class that user has to extend
    """
    def __init__(self):
        pass

    def combine(self, msg_x, msg_y):
        """
        user needs to overwrite this method;
        we don't want users to change the destination of a combined message
        :param msg_x: a tuple (message source vertex id, message value)
        :param msg_y: a tuple (message source vertex id, message value)
        :return: a tuple (message source vertex id, message value)
        """

        raise NotImplementedError("Combiner aggregate not implemented")
