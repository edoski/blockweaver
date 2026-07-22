"""Strict values shared by transport, storage, and builders."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from uuid import UUID

_QUANTITY = re.compile(r"0x(?:0|[1-9a-f][0-9a-f]*)\Z")
_HASH = re.compile(r"0x[0-9a-f]{64}\Z")


@dataclass(frozen=True, slots=True)
class Request:
    corpus_id: UUID
    chain_id: int
    first_block: int
    last_block: int

    def __post_init__(self) -> None:
        if self.corpus_id.version != 4 or str(self.corpus_id) != str(self.corpus_id).lower():
            raise ValueError("corpus_id must be a canonical UUID4")
        if min(self.chain_id, self.first_block, self.last_block) < 0:
            raise ValueError("chain_id and block numbers must be nonnegative")
        if self.first_block > self.last_block:
            raise ValueError("first_block must not exceed last_block")

    def document(self) -> dict[str, object]:
        definition = {"chain_id": self.chain_id, "first_block": self.first_block, "last_block": self.last_block}
        return {"corpus_id": str(self.corpus_id), "definition": definition}


@dataclass(frozen=True, slots=True)
class Anchor:
    block_number: int
    block_hash: str

    def document(self) -> dict[str, object]:
        return {"block_number": self.block_number, "block_hash": self.block_hash}


@dataclass(frozen=True, slots=True)
class Block:
    block_number: int
    block_hash: str
    parent_hash: str
    timestamp: int
    chain_id: int
    base_fee_per_gas: int
    gas_used: int
    gas_limit: int
    tx_count: int

    def durable_row(self) -> dict[str, int]:
        return {
            "block_number": self.block_number,
            "timestamp": self.timestamp,
            "chain_id": self.chain_id,
            "base_fee_per_gas": self.base_fee_per_gas,
            "gas_used": self.gas_used,
            "gas_limit": self.gas_limit,
            "tx_count": self.tx_count,
        }

    def corpus_row(self, priority_fee_p50: int) -> dict[str, int]:
        return {**self.durable_row(), "effective_priority_fee_per_gas_p50": priority_fee_p50}

    def checkpoint_row(self, priority_fee_p50: int) -> dict[str, int | str]:
        return {
            **self.corpus_row(priority_fee_p50),
            "block_hash": self.block_hash,
            "parent_hash": self.parent_hash,
        }


def quantity(value: Any, label: str) -> int:
    if not isinstance(value, str) or _QUANTITY.fullmatch(value) is None:
        raise ValueError(f"Invalid {label} quantity")
    parsed = int(value, 16)
    if parsed > 2**63 - 1:
        raise ValueError(f"{label} quantity exceeds signed Int64")
    return parsed


def block_hash(value: Any, label: str) -> str:
    if not isinstance(value, str) or _HASH.fullmatch(value) is None:
        raise ValueError(f"Invalid {label}")
    return value[2:]


def parse_block(value: Any, *, expected: int, chain_id: int) -> Block:
    if not isinstance(value, dict):
        raise ValueError("Invalid block response shape")
    required = {
        "number",
        "hash",
        "parentHash",
        "timestamp",
        "baseFeePerGas",
        "gasUsed",
        "gasLimit",
        "transactions",
    }
    if not required <= value.keys():
        raise ValueError("Block response is missing required fields")
    number = quantity(value["number"], "block number")
    if number != expected:
        raise ValueError(f"RPC returned block {number} when {expected} was requested")
    transactions = value["transactions"]
    if not isinstance(transactions, list) or any(not isinstance(item, str) or _HASH.fullmatch(item) is None for item in transactions):
        raise ValueError("Invalid transactions field")
    timestamp = quantity(value["timestamp"], "timestamp")
    base_fee = quantity(value["baseFeePerGas"], "base fee")
    gas_used = quantity(value["gasUsed"], "gas used")
    gas_limit = quantity(value["gasLimit"], "gas limit")
    if base_fee <= 0 or gas_limit <= 0 or gas_used > gas_limit:
        raise ValueError("Invalid fee or gas domain")
    return Block(
        block_number=number,
        block_hash=block_hash(value["hash"], "block hash"),
        parent_hash=block_hash(value["parentHash"], "parent hash"),
        timestamp=timestamp,
        chain_id=chain_id,
        base_fee_per_gas=base_fee,
        gas_used=gas_used,
        gas_limit=gas_limit,
        tx_count=len(transactions),
    )


def validate_links(blocks: list[Block], previous: Block | None = None) -> None:
    for block in blocks:
        if previous is not None:
            if block.block_number != previous.block_number + 1:
                raise ValueError("Blocks are not contiguous")
            if block.parent_hash != previous.block_hash:
                raise ValueError(f"Parent link mismatch at block {block.block_number}")
            if block.timestamp < previous.timestamp:
                raise ValueError(f"Timestamp decreases at block {block.block_number}")
        previous = block
