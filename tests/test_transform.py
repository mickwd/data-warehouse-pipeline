from __future__ import annotations

import pandas as pd
import pytest

from batch import transform


@pytest.fixture
def raw_customers() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "customer_id": [1, 1, 2, "x"],
            "first_name": ["  lucía ", "Lucía", "MARTÍN", "a"],
            "last_name": ["gonzález", "González", "  ", "b"],
            "email": [" Lucia@Example.COM ", "lucia@example.com", "martin@example.com", "c"],
            "city": ["buenos aires", "Rosario", "córdoba", "d"],
            "country": ["Argentina", "ar", "Uruguay", "zz-invalid"],
            "segment": ["Retail ", "vip", None, "x"],
            "created_at": ["2024-01-01T10:00:00Z"] * 4,
            "updated_at": [
                "2024-06-01T10:00:00Z",
                "2024-06-01T12:00:00Z",
                "2024-06-01T11:00:00Z",
                "2024-06-01T11:00:00Z",
            ],
        }
    )


class TestCleanCustomers:
    def test_keeps_latest_version_per_customer(self, raw_customers: pd.DataFrame) -> None:
        result = transform.clean_customers(raw_customers)
        assert result["customer_id"].tolist() == [1, 2]
        lucia = result.set_index("customer_id").loc[1]
        assert lucia["city"] == "Rosario"
        assert lucia["segment"] == "vip"

    def test_normalizes_text_fields(self, raw_customers: pd.DataFrame) -> None:
        result = transform.clean_customers(raw_customers).set_index("customer_id")
        assert result.loc[2, "first_name"] == "Martín"
        assert pd.isna(result.loc[2, "last_name"])  # string en blanco pasa a nulo
        assert result.loc[2, "country_code"] == "UY"
        assert result.loc[2, "segment"] == "unknown"
        assert result.loc[1, "email"] == "lucia@example.com"

    def test_invalid_ids_are_dropped(self, raw_customers: pd.DataFrame) -> None:
        assert "x" not in transform.clean_customers(raw_customers)["customer_id"].astype(str).tolist()

    def test_row_hash_changes_only_with_business_attributes(self, raw_customers: pd.DataFrame) -> None:
        base = transform.clean_customers(raw_customers.iloc[[2]])
        touched = raw_customers.iloc[[2]].copy()
        touched["updated_at"] = "2024-07-01T00:00:00Z"  # solo cambia la marca de tiempo
        moved = raw_customers.iloc[[2]].copy()
        moved["city"] = "Montevideo"

        assert transform.clean_customers(touched)["row_hash"].iloc[0] == base["row_hash"].iloc[0]
        assert transform.clean_customers(moved)["row_hash"].iloc[0] != base["row_hash"].iloc[0]


class TestRowHash:
    def test_is_deterministic_and_md5_hex(self) -> None:
        df = pd.DataFrame({"a": ["x", "y"], "b": [1, 2]})
        first = transform.add_row_hash(df, ["a", "b"])["row_hash"]
        second = transform.add_row_hash(df, ["a", "b"])["row_hash"]
        assert first.tolist() == second.tolist()
        assert all(len(h) == 32 for h in first)

    def test_null_and_empty_string_do_not_collide(self) -> None:
        df = pd.DataFrame({"a": [None, ""], "b": ["z", "z"]})
        hashes = transform.add_row_hash(df, ["a", "b"])["row_hash"]
        assert hashes.iloc[0] != hashes.iloc[1]

    def test_empty_frame(self) -> None:
        out = transform.add_row_hash(pd.DataFrame({"a": []}), ["a"])
        assert "row_hash" in out.columns and out.empty


class TestCleanProducts:
    def test_normalizes_and_flags_invalid_prices(self) -> None:
        raw = pd.DataFrame(
            {
                "product_id": [" p00001", "p00002"],
                "name": ["Mate", "Termo"],
                "category": [" Hogar ", None],
                "brand": ["Pampa", ""],
                "list_price": ["1500.456", -10],
                "currency": ["ars", None],
                "is_active": ["true", 0],
                "updated_at": ["2024-06-01T10:00:00Z", "2024-06-01T10:00:00Z"],
            }
        )
        result = transform.clean_products(raw).set_index("product_id")
        assert list(result.index) == ["P00001", "P00002"]
        assert result.loc["P00001", "list_price"] == pytest.approx(1500.46)
        assert pd.isna(result.loc["P00002", "list_price"])
        assert result.loc["P00002", "category"] == "uncategorized"
        assert result.loc["P00002", "currency"] == "ARS"
        assert bool(result.loc["P00001", "is_active"]) is True
        assert bool(result.loc["P00002", "is_active"]) is False
        assert pd.isna(result.loc["P00002", "brand"])


class TestCleanOrderLines:
    @pytest.fixture
    def raw_lines(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "order_id": [10, 10, 11, 11],
                "line_number": [1, 2, 1, 1],
                "customer_id": [1, 1, 2, 2],
                "product_id": ["p00001", "p00002", "p00003", "p00003"],
                "order_ts": ["2024-06-01T09:00:00Z"] * 4,
                "status": ["Pagado", "paid", "canceled", "algo-raro"],
                "quantity": [2, -1, 3, 3],
                "unit_price": [100.0, 50.0, 10.0, 10.0],
                "discount_pct": [0.1, 1.5, 0, 0],
                "updated_at": [
                    "2024-06-01T09:00:00Z",
                    "2024-06-01T09:00:00Z",
                    "2024-06-01T09:00:00Z",
                    "2024-06-01T09:30:00Z",
                ],
            }
        )

    def test_computes_amounts_with_clipping(self, raw_lines: pd.DataFrame) -> None:
        result = transform.clean_order_lines(raw_lines).set_index(["order_id", "line_number"])
        first = result.loc[(10, 1)]
        assert first["gross_amount"] == 200.0
        assert first["discount_amount"] == 20.0
        assert first["net_amount"] == 180.0

        # cantidad negativa y descuento > 100% se acotan
        second = result.loc[(10, 2)]
        assert second["quantity"] == 0
        assert second["discount_pct"] == 1.0
        assert second["net_amount"] == 0.0

    def test_normalizes_status_and_deduplicates(self, raw_lines: pd.DataFrame) -> None:
        result = transform.clean_order_lines(raw_lines).set_index(["order_id", "line_number"])
        assert result.loc[(10, 1), "status"] == "paid"
        assert len(result) == 3
        # para (11, 1) gana la versión más nueva, con un estado inválido
        assert result.loc[(11, 1), "status"] == "unknown"

    def test_net_equals_gross_minus_discount(self, raw_lines: pd.DataFrame) -> None:
        result = transform.clean_order_lines(raw_lines)
        diff = (result["gross_amount"] - result["discount_amount"] - result["net_amount"]).abs()
        assert (diff < 0.01).all()


def test_deduplicate_latest_handles_empty_frames() -> None:
    empty = pd.DataFrame(columns=["id", "updated_at"])
    assert transform.deduplicate_latest(empty, ["id"]).empty


@pytest.mark.parametrize(
    ("value", "expected"),
    [("Argentina", "AR"), ("arg", "AR"), ("br", "BR"), ("México", "MX"), ("Narnia", None), (None, None)],
)
def test_normalize_country(value: str | None, expected: str | None) -> None:
    result = transform.normalize_country(pd.Series([value])).iloc[0]
    assert (pd.isna(result) and expected is None) or result == expected
