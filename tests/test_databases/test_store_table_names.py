"""Tests for the shared connector store table names."""

from __future__ import annotations


def test_shared_rule_reproduces_the_spiraldb_name() -> None:
    from orcapod.databases.store_table_names import store_table_name

    path = ("función", "数据", "rdb_v1")
    assert store_table_name(path, hint_length=96, digest_length=64) == (
        "o1_funci_n____rdb_v1_"
        "4c0b0f48110dc57f910431847088e2e11d05b6bf2b26c3e8dfb8dd9dabf2c43b"
    )


def test_shorter_limits_keep_the_same_hint_and_digest_prefixes() -> None:
    from orcapod.databases.store_table_names import store_table_name

    path = ("función", "数据", "rdb_v1")
    assert store_table_name(path, hint_length=7, digest_length=40) == (
        "o1_funci_n_4c0b0f48110dc57f910431847088e2e11d05b6bf"
    )
