from datetime import datetime, timedelta
from typing import Any, Iterable, Mapping

from models.booking import Booking
from models.tables import Table
from models.user import User
from postgres_driver import PostgresDriver


def create_tables() -> None:
    with PostgresDriver() as db:
        db.create_table_from_model(User)
        db.create_table_from_model(Table)
        db.create_table_from_model(Booking)


# Users CRUD
def create_user(data: Mapping[str, Any], returning: Iterable[str] | None = None) -> dict[str, Any] | None:
    with PostgresDriver() as db:
        return db.insert("users", data, returning=returning)


def read_users(
    filters: Mapping[str, Any] | None = None,
    columns: Iterable[str] | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> list[dict[str, Any]]:
    with PostgresDriver() as db:
        return db.read_many("users", filters=filters, columns=columns, limit=limit, offset=offset)


def update_user(
    data: Mapping[str, Any],
    filters: Mapping[str, Any],
    returning: Iterable[str] | None = None,
) -> dict[str, Any] | None:
    with PostgresDriver() as db:
        return db.update("users", data=data, filters=filters, returning=returning)


def delete_user(
    filters: Mapping[str, Any],
    returning: Iterable[str] | None = None,
) -> dict[str, Any] | None:
    with PostgresDriver() as db:
        return db.delete("users", filters=filters, returning=returning)


# Tables CRUD
def create_table_record(
    data: Mapping[str, Any], returning: Iterable[str] | None = None
) -> dict[str, Any] | None:
    with PostgresDriver() as db:
        return db.insert("tables", data, returning=returning)


def read_tables(
    filters: Mapping[str, Any] | None = None,
    columns: Iterable[str] | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> list[dict[str, Any]]:
    with PostgresDriver() as db:
        return db.read_many("tables", filters=filters, columns=columns, limit=limit, offset=offset)


def update_table_record(
    data: Mapping[str, Any],
    filters: Mapping[str, Any],
    returning: Iterable[str] | None = None,
) -> dict[str, Any] | None:
    with PostgresDriver() as db:
        return db.update("tables", data=data, filters=filters, returning=returning)


def delete_table_record(
    filters: Mapping[str, Any],
    returning: Iterable[str] | None = None,
) -> dict[str, Any] | None:
    with PostgresDriver() as db:
        return db.delete("tables", filters=filters, returning=returning)


# Bookings CRUD
def create_booking(
    data: Mapping[str, Any], returning: Iterable[str] | None = None
) -> dict[str, Any] | None:
    table_id = data.get("table_id")
    booking_time = data.get("booking_time")
    duration_minutes = int(data.get("duration_minutes", 120))

    if table_id is None:
        raise ValueError("`table_id` is required for booking.")
    if booking_time is None:
        raise ValueError("`booking_time` is required for booking.")

    if isinstance(booking_time, str):
        booking_time = datetime.fromisoformat(booking_time)
    if not isinstance(booking_time, datetime):
        raise ValueError("`booking_time` must be datetime or ISO datetime string.")

    if not check_table_availability(
        table_id=int(table_id),
        booking_time=booking_time,
        duration_minutes=duration_minutes,
    ):
        raise ValueError("Selected table is not available for the requested time.")

    with PostgresDriver() as db:
        return db.insert("bookings", data, returning=returning)


def read_bookings(
    filters: Mapping[str, Any] | None = None,
    columns: Iterable[str] | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> list[dict[str, Any]]:
    with PostgresDriver() as db:
        return db.read_many("bookings", filters=filters, columns=columns, limit=limit, offset=offset)


def update_booking(
    data: Mapping[str, Any],
    filters: Mapping[str, Any],
    returning: Iterable[str] | None = None,
) -> dict[str, Any] | None:
    with PostgresDriver() as db:
        current_booking = db.read_one(
            "bookings",
            filters=filters,
            columns=["id", "table_id", "booking_time"],
        )

    if current_booking is None:
        return None

    table_id = data.get("table_id", current_booking.get("table_id"))
    booking_time = data.get("booking_time", current_booking.get("booking_time"))
    duration_minutes = int(data.get("duration_minutes", 120))

    if table_id is None:
        raise ValueError("`table_id` is required for booking update.")
    if booking_time is None:
        raise ValueError("`booking_time` is required for booking update.")

    if isinstance(booking_time, str):
        booking_time = datetime.fromisoformat(booking_time)
    if not isinstance(booking_time, datetime):
        raise ValueError("`booking_time` must be datetime or ISO datetime string.")

    if not check_table_availability(
        table_id=int(table_id),
        booking_time=booking_time,
        duration_minutes=duration_minutes,
        exclude_booking_id=int(current_booking["id"]),
    ):
        raise ValueError("Selected table is not available for the requested time.")

    with PostgresDriver() as db:
        return db.update("bookings", data=data, filters=filters, returning=returning)


def delete_booking(
    filters: Mapping[str, Any],
    returning: Iterable[str] | None = None,
) -> dict[str, Any] | None:
    with PostgresDriver() as db:
        return db.delete("bookings", filters=filters, returning=returning)


def check_table_availability(
    table_id: int,
    booking_time: datetime,
    duration_minutes: int = 120,
    exclude_booking_id: int | None = None,
) -> bool:
    """
    Проверяет, свободен ли столик в выбранное время.
    Для упрощения считаем, что длительность всех бронирований одинаковая.
    """
    if duration_minutes <= 0:
        raise ValueError("`duration_minutes` must be greater than 0.")

    requested_end = booking_time + timedelta(minutes=duration_minutes)

    with PostgresDriver() as db:
        table = db.read_one("tables", filters={"id": table_id, "is_active": 1}, columns=["id"])
        if table is None:
            return False

        bookings = db.read_many(
            "bookings",
            filters={"table_id": table_id},
            columns=["id", "booking_time"],
        )

        for booking in bookings:
            if exclude_booking_id is not None and booking.get("id") == exclude_booking_id:
                continue

            existing_start = booking.get("booking_time")
            if isinstance(existing_start, str):
                existing_start = datetime.fromisoformat(existing_start)
            if not isinstance(existing_start, datetime):
                continue

            existing_end = existing_start + timedelta(minutes=duration_minutes)

            # Пересечение интервалов: [start1, end1) и [start2, end2)
            if existing_start < requested_end and existing_end > booking_time:
                return False

    return True


if __name__ == "__main__":
    create_tables()