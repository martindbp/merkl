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


class ImplicitSingleOutMismatchError(BaseException):
    pass


class FileNotTrackedError(BaseException):
    pass
