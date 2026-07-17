import pytest

from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY, OperationRegistry


@pytest.fixture
def registry() -> OperationRegistry:
    return OPERATION_REGISTRY
