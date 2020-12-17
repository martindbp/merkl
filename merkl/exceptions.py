class FutureAccessException(BaseException):
    pass


class NonSerializableArgException(BaseException):
    pass


class NonSerializableFunctionDepException(BaseException):
    pass


class NonMatchingSignaturesException(BaseException):
    pass


class NonPositiveOutsException(BaseException):
    pass


class BadOutsValueException(BaseException):
    pass


class WrongNumberOfOutsException(BaseException):
    pass


class ImplicitSingleOutMismatchException(BaseException):
    pass
