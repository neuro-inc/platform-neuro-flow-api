import logging
from abc import ABC


logger = logging.getLogger(__name__)


class JobsStorageException(Exception):
    pass


class JobStorageTransactionError(JobsStorageException):
    pass


class Storage(ABC):
    pass
