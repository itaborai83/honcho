class HonchoException(Exception): pass

class WorkerError(HonchoException): pass
class TransientWorkerError(WorkerError): pass
class WorkerBusyError(WorkerError): pass
class WorkerIdleError(WorkerError): pass
class WrongWorkItemError(WorkerError): pass


class WorkItemException(HonchoException): pass
class TransientWorkItemError(WorkItemException): pass
class WorkItemNotReadyError(WorkItemException): pass
class WorkItemNotCheckedOutError(WorkItemException): pass
class WorkItemShouldNotBeReadyError(WorkItemException): pass
class WorkItemShouldNotBeFinishedError(WorkItemException): pass