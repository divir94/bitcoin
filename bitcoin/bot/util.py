import pprint


class JsonObject(object):
    def __getitem__(self, attr):
        return self.__dict__[attr]

    def __repr__(self):
        return '\n' + pprint.pformat(self.__dict__)
