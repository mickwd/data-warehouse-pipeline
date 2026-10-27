"""Genera actividad en la base OLTP de origen (altas, pedidos, cambios de clientes).

Uso:
    python -m mock_sources.oltp_simulator --seed-customers 500 --orders-per-minute 120
"""

from __future__ import annotations

import argparse
import logging
import random
import time

from sqlalchemy import Engine, create_engine, text

from common.config import get_settings
from common.logs import configure_logging

logger = logging.getLogger("oltp_simulator")

FIRST_NAMES = ["lucía", "Martín", "SOFÍA", "juan", "Valentina", "Mateo", "Camila", "Santiago", "Julieta", "Tomás"]
LAST_NAMES = ["González", "rodríguez", "Gómez", "Fernández", "López", "DÍAZ", "Martínez", "Pérez", "Romero"]
CITIES = [
    ("Buenos Aires", "Argentina"),
    ("Córdoba", "AR"),
    ("Rosario", "argentina"),
    ("Montevideo", "Uruguay"),
    ("Santiago", "Chile"),
    ("São Paulo", "Brasil"),
    ("Ciudad de México", "México"),
]
SEGMENTS = ["retail", "Retail ", "wholesale", "vip", None]
STATUSES = ["pending", "paid", "shipped", "delivered", "cancelado"]


def seed_customers(engine: Engine, n: int) -> None:
    """Completa hasta `n` clientes, así reiniciar el contenedor no duplica la carga inicial."""
    with engine.begin() as conn:
        existing = conn.execute(text("SELECT count(*) FROM public.customers")).scalar_one()
        for i in range(existing, n):
            city, country = random.choice(CITIES)
            conn.execute(
                text(
                    "INSERT INTO public.customers (first_name, last_name, email, city, country, segment) "
                    "VALUES (:f, :l, :e, :c, :co, :s)"
                ),
                {
                    "f": f"  {random.choice(FIRST_NAMES)} ",
                    "l": random.choice(LAST_NAMES),
                    "e": f"Cliente{i}@Example.com",
                    "c": city,
                    "co": country,
                    "s": random.choice(SEGMENTS),
                },
            )
    logger.info("customers_seeded total=%s new=%s", max(n, existing), max(n - existing, 0))


def place_order(engine: Engine) -> None:
    with engine.begin() as conn:
        customer_id = conn.execute(
            text("SELECT customer_id FROM public.customers ORDER BY random() LIMIT 1")
        ).scalar_one()
        order_id = conn.execute(
            text("INSERT INTO public.orders (customer_id, status) VALUES (:c, 'pending') RETURNING order_id"),
            {"c": customer_id},
        ).scalar_one()
        for line in range(1, random.randint(1, 4) + 1):
            conn.execute(
                text(
                    "INSERT INTO public.order_items "
                    "(order_id, line_number, product_id, quantity, unit_price, discount_pct) "
                    "VALUES (:o, :l, :p, :q, :u, :d)"
                ),
                {
                    "o": order_id,
                    "l": line,
                    "p": f"p{random.randint(1, 500):05d}",
                    "q": random.randint(1, 5),
                    "u": round(random.uniform(500, 80_000), 2),
                    "d": random.choice([0, 0, 0, 0.05, 0.1, 0.15]),
                },
            )


def mutate(engine: Engine) -> None:
    with engine.begin() as conn:
        # Avanza el estado de pedidos recientes
        conn.execute(
            text(
                "UPDATE public.orders SET status = :s WHERE order_id IN "
                "(SELECT order_id FROM public.orders ORDER BY random() LIMIT 3)"
            ),
            {"s": random.choice(STATUSES)},
        )
        # Cambio de atributos de un cliente: genera una nueva versión SCD2 en dim_customer
        city, country = random.choice(CITIES)
        conn.execute(
            text(
                "UPDATE public.customers SET city = :c, country = :co, segment = :s "
                "WHERE customer_id = (SELECT customer_id FROM public.customers ORDER BY random() LIMIT 1)"
            ),
            {"c": city, "co": country, "s": random.choice(SEGMENTS)},
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed-customers", type=int, default=0)
    parser.add_argument("--orders-per-minute", type=float, default=60)
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level)
    engine = create_engine(settings.source_db.url, pool_pre_ping=True)

    if args.seed_customers:
        seed_customers(engine, args.seed_customers)

    interval = 60 / args.orders_per_minute
    n = 0
    while True:
        place_order(engine)
        n += 1
        if n % 10 == 0:
            mutate(engine)
            logger.info("oltp_activity orders=%s", n)
        time.sleep(interval)


if __name__ == "__main__":
    main()
