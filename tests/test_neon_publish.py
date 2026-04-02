import os

import pytest

from mercor_apex_finance_eval.neon_publish import resolve_database_url


@pytest.mark.parametrize(
    ("env", "expected"),
    [
        ({"DATABASE_URL_UNPOOLED": "postgres://direct"}, "postgres://direct"),
        ({"POSTGRES_URL_NON_POOLING": "postgres://nonpool"}, "postgres://nonpool"),
        ({"DATABASE_URL": "postgres://db"}, "postgres://db"),
        ({"POSTGRES_URL": "postgres://pg"}, "postgres://pg"),
        ({"POSTGRES_PRISMA_URL": "postgres://prisma"}, "postgres://prisma"),
    ],
)
def test_resolve_database_url_prefers_expected_env_vars(monkeypatch: pytest.MonkeyPatch, env, expected):
    for key in [
        "DATABASE_URL_UNPOOLED",
        "POSTGRES_URL_NON_POOLING",
        "DATABASE_URL",
        "POSTGRES_URL",
        "POSTGRES_PRISMA_URL",
    ]:
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    assert resolve_database_url() == expected


def test_resolve_database_url_explicit_value_wins(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DATABASE_URL", "postgres://env")
    assert resolve_database_url("postgres://explicit") == "postgres://explicit"


def test_resolve_database_url_errors_when_missing(monkeypatch: pytest.MonkeyPatch):
    for key in [
        "DATABASE_URL_UNPOOLED",
        "POSTGRES_URL_NON_POOLING",
        "DATABASE_URL",
        "POSTGRES_URL",
        "POSTGRES_PRISMA_URL",
    ]:
        monkeypatch.delenv(key, raising=False)

    with pytest.raises(EnvironmentError):
        resolve_database_url()
