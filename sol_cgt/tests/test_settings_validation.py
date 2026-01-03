import re

import pytest

from sol_cgt import utils


def test_helius_rpc_url_rejects_enhanced_host() -> None:
    message = (
        "HELIUS_RPC_URL must be mainnet.helius-rpc.com (JSON-RPC). "
        "api-mainnet.helius-rpc.com is Enhanced REST."
    )
    with pytest.raises(ValueError, match=re.escape(message)):
        utils.validate_helius_rpc_url("https://api-mainnet.helius-rpc.com/?api-key=key")


def test_helius_rpc_url_accepts_rpc_host() -> None:
    assert utils.validate_helius_rpc_url("https://mainnet.helius-rpc.com/?api-key=key")
