"""Shared policy for Host evidence that blocks later CMO mutations."""

from __future__ import annotations

from typing import Final

from cmo_agent_bridge.operations.kinds import OperationClass
from cmo_agent_bridge.state.models import HostRequestState
from cmo_agent_bridge.state.operation_queue import OperationQueueState
from cmo_agent_bridge.state.request_ledger import RequestRecord


EFFECTFUL_HOST_OPERATION_CLASSES: Final[frozenset[OperationClass]] = frozenset(
    {
        OperationClass.MUTATION,
        OperationClass.DESTRUCTIVE,
        OperationClass.RECONCILE,
    }
)

PROVEN_NON_EFFECTFUL_HOST_OPERATION_CLASSES: Final[frozenset[OperationClass]] = frozenset(
    {
        OperationClass.STATUS,
        OperationClass.READ,
    }
)

TERMINAL_HOST_REQUEST_STATES: Final[frozenset[HostRequestState]] = frozenset(
    {
        HostRequestState.COMPLETED,
        HostRequestState.REJECTED,
        HostRequestState.CANCELLED,
        HostRequestState.RESOLVED,
    }
)

NONTERMINAL_HOST_REQUEST_STATES: Final[frozenset[HostRequestState]] = (
    frozenset(HostRequestState) - TERMINAL_HOST_REQUEST_STATES
)

OUTCOME_IRRELEVANT_NON_EFFECTFUL_STATES: Final[frozenset[HostRequestState]] = frozenset(
    {
        HostRequestState.PREPARED,
        HostRequestState.PUBLISHED,
        HostRequestState.RESPONSE_ACCEPTED,
        HostRequestState.IDLE_PUBLISHED,
        HostRequestState.QUARANTINED,
    }
)


def is_effectful_host_operation(operation_class: OperationClass) -> bool:
    """Return whether the manifest explicitly classifies an operation as effectful."""

    return operation_class in EFFECTFUL_HOST_OPERATION_CLASSES


def host_request_blocks_mutations(
    record: RequestRecord,
    *,
    queue_state: OperationQueueState | None,
) -> bool:
    """Apply one barrier rule to Host-only and queue-backed evidence.

    A host-only status/read exchange in one of its real reachable states cannot
    have changed CMO, so its outcome does not block later mutations. A queue row
    for such an exchange, or an impossible state such as CANCEL_PUBLISHED, is a
    structural inconsistency and fails closed. Effectful nonterminal Host
    evidence blocks unless it is a concrete mutation paired with the one
    legitimate recovery owner: an ACTIVE queue row.
    """

    if record.state in TERMINAL_HOST_REQUEST_STATES:
        return False
    # DYNAMIC is not proven safe: valid exchanges resolve it before persistence,
    # so encountering it in durable evidence is itself an unexpected condition.
    # STATUS/READ are safe only on their host-only transport state path.
    if record.operation_class in PROVEN_NON_EFFECTFUL_HOST_OPERATION_CLASSES:
        return queue_state is not None or record.state not in (
            OUTCOME_IRRELEVANT_NON_EFFECTFUL_STATES
        )
    if record.state is HostRequestState.QUARANTINED:
        return True
    return record.state in NONTERMINAL_HOST_REQUEST_STATES and not (
        record.operation_class is OperationClass.MUTATION
        and queue_state is OperationQueueState.ACTIVE
    )
