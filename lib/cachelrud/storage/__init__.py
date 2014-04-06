class Base(object):
    @classmethod
    def get_instance(cls, log, params):
        """
        :type log: logging.Logger
        :type params: dict
        :rtype: Base
        """
        raise NotImplementedError()

    def touch_keys(self, keys):
        """
        :type keys: list[str]
        :rtype: None
        """
        raise NotImplementedError()

    def get_stat(self):
        """
        :rtype: (int, int)
        """
        raise NotImplementedError()

    def clean_oldest(self, count):
        """
        :type count: int
        :rtype: (int, int)
        """
        raise NotImplementedError()
