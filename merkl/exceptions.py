class FutureAccessError(BaseException):
    pass


class SerializationError(BaseException):
    pass


class FileNotTrackedError(BaseException):
    pass


class TrackedFileNotUpToDateError(BaseException):
    pass


class BatchTaskError(BaseException):
    pass


class TaskOutsError(BaseException):
    pass
