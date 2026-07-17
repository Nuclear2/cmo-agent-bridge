from cmo_agent_bridge.errors import BridgeError, ErrorCode


def test_bridge_error_payload() -> None:
    error = BridgeError(ErrorCode.REQUEST_TIMEOUT, "no response", {"seconds": 30})
    assert error.to_payload() == {
        "code": "REQUEST_TIMEOUT",
        "message": "no response",
        "details": {"seconds": 30},
    }
