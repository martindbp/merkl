import pickle

class PickleSerializer:
    def serialize(self, value):
        return pickle.dumps(value)

    def deserializer(self, value):
        return pickle.reads(value)
