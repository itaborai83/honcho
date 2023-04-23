class HonchoException(Exception): pass

class WorkerError(HonchoException): pass
class WorkerNotFoundError(WorkerError): pass
class WorkerIsBusyError(WorkerError): pass
class TransientWorkerError(WorkerError): pass

#class WorkerNotTransientError(WorkerError): pass
#class WorkerBusyError(WorkerError): pass
#class WorkerIdleError(WorkerError): pass
#class WrongWorkItemError(WorkerError): pass
#class InvalidWorkerIdError(WorkerError): pass

class WorkItemError(HonchoException): pass
class WorkItemNotFoundError(WorkItemError): pass
class TransientWorkItemError(WorkItemError): pass
class WorkItemIsCheckedOutError(WorkItemError): pass
class WorkItemNotReadyError(WorkItemError): pass
class WorkItemShouldNotBeReadyError(WorkItemError): pass
class WorkItemShouldNotBeFinishedError(WorkItemError): pass
class NoWorkItemsLeftError(WorkItemError): pass
#class WorkItemNotCheckedOutError(WorkItemError): pass
#class WorkItemNotTransientError(WorkItemError): pass
#class InvalidWorkItemIdError(WorkItemError): pass
#class WorkItemNotFoundError(WorkItemError): pass

#class StorageException(HonchoException): pass
#class SequenceException(StorageException): pass
#class SequenceAlreadyExistsError(SequenceException): pass
#class SequenceDoesNotExistError(SequenceException): pass