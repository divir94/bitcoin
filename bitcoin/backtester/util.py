class Balance(dict):
    def __setitem__(self, key, value):
        if value < 0:
            raise LessThanZeroException('x is less than zero')
        super(Balance, self).__setitem__(key, value)


class LessThanZeroException(Exception):
    pass
