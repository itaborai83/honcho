class HonchoException(Exception): pass

class WorkerError(HonchoException): pass
class TransientWorkerError(WorkerError): pass
class WorkerNotTransientError(WorkerError): pass
class WorkerBusyError(WorkerError): pass
class WorkerIdleError(WorkerError): pass
class WrongWorkItemError(WorkerError): pass
class InvalidWorkerIdError(WorkerError): pass

class WorkItemException(HonchoException): pass
class TransientWorkItemError(WorkItemException): pass
class WorkItemNotTransientError(WorkItemException): pass
class WorkItemNotReadyError(WorkItemException): pass
class WorkItemNotCheckedOutError(WorkItemException): pass
class WorkItemShouldNotBeReadyError(WorkItemException): pass
class WorkItemShouldNotBeFinishedError(WorkItemException): pass
class InvalidWorkItemIdError(WorkItemException): pass
class WorkItemNotFoundError(WorkItemException): pass

class StorageException(HonchoException): pass
class SequenceException(StorageException): pass
class SequenceAlreadyExistsError(SequenceException): pass
class SequenceDoesNotExistError(SequenceException): pass