import pickle


class PickleSerializer:
    @classmethod
    def serialize(cls, value):
        return pickle.dumps(value)

    @classmethod
    def deserialize(cls, value):
        return pickle.loads(value)
