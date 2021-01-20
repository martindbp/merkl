class FutureAccessError(BaseException):
    pass


class NonSerializableArgError(BaseException):
    pass


class NonSerializableFunctionDepError(BaseException):
    pass


class NonMatchingSignaturesError(BaseException):
    pass


class NonPositiveOutsError(BaseException):
    pass


class BadOutsValueError(BaseException):
    pass


class WrongNumberOfOutsError(BaseException):
    pass


class FileNotTrackedError(BaseException):
    pass


class ReturnTypeMismatchError(BaseException):
    pass


class NumReturnValuesMismatchError(BaseException):
    pass


class TrackedFileNotUpToDateError(BaseException):
    pass


class NoSerializerError(BaseException):
    pass
