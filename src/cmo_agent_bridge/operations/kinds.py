from enum import StrEnum


class OperationClass(StrEnum):
    STATUS = "status"
    READ = "read"
    MUTATION = "mutation"
    DESTRUCTIVE = "destructive"
    RECONCILE = "reconcile"
    DYNAMIC = "dynamic"


class ExecutionTarget(StrEnum):
    LOCAL = "local"
    CMO = "cmo"
