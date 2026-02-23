from __future__ import annotations

import json
import tkinter as tk
from datetime import datetime
from tkinter import messagebox, ttk
from tkinter.scrolledtext import ScrolledText
from typing import Any

from backend import (
    check_table_availability,
    create_booking,
    create_table_record,
    create_tables,
    create_user,
    delete_booking,
    delete_table_record,
    delete_user,
    read_bookings,
    read_tables,
    read_users,
    update_booking,
    update_table_record,
    update_user,
)


def _strip(value: str) -> str:
    return value.strip()


def _to_int(value: str, field_name: str) -> int:
    value = _strip(value)
    if not value:
        raise ValueError(f"`{field_name}` is required.")
    return int(value)


def _to_optional_int(value: str) -> int | None:
    value = _strip(value)
    return int(value) if value else None


def _to_datetime(value: str, field_name: str = "booking_time") -> datetime:
    value = _strip(value)
    if not value:
        raise ValueError(f"`{field_name}` is required.")
    return datetime.fromisoformat(value)


def _pretty(result: Any) -> str:
    return json.dumps(result, ensure_ascii=False, indent=2, default=str)


class BookingApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Booking System (Tkinter)")
        self.geometry("1100x760")
        self.minsize(980, 680)

        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True, padx=10, pady=10)

        self._build_setup_tab()
        self._build_users_tab()
        self._build_tables_tab()
        self._build_bookings_tab()
        self._build_availability_tab()

    def _execute(self, output: ScrolledText, action) -> None:
        try:
            result = action()
            output.delete("1.0", tk.END)
            output.insert(tk.END, _pretty(result))
        except Exception as exc:
            messagebox.showerror("Operation failed", str(exc))

    @staticmethod
    def _add_labeled_entry(parent: ttk.Frame, label: str, row: int) -> ttk.Entry:
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", padx=6, pady=4)
        entry = ttk.Entry(parent, width=34)
        entry.grid(row=row, column=1, sticky="ew", padx=6, pady=4)
        return entry

    @staticmethod
    def _add_output(parent: ttk.Frame) -> ScrolledText:
        output = ScrolledText(parent, height=16, wrap="word")
        output.pack(fill="both", expand=True, padx=8, pady=8)
        return output

    def _build_setup_tab(self) -> None:
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Setup")

        header = ttk.Label(
            tab,
            text="Service actions",
            font=("Segoe UI", 11, "bold"),
        )
        header.pack(anchor="w", padx=8, pady=(8, 4))

        controls = ttk.Frame(tab)
        controls.pack(fill="x", padx=8, pady=6)

        output = self._add_output(tab)

        ttk.Button(
            controls,
            text="Create DB tables",
            command=lambda: self._execute(output, lambda: (create_tables(), {"status": "ok"})),
        ).pack(side="left")

    def _build_users_tab(self) -> None:
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Users")

        forms = ttk.Frame(tab)
        forms.pack(fill="x", padx=8, pady=8)

        # ПРОВЕРКА ПРОБЛЕМЫ: ОШИБКА "NULL в столбце id" возникает, когда автогенерация id работает неправильно.
        # Причина почти всегда — не тот SQL тип столбца или некорректный SQL для создания таблицы.
        # Для PostgreSQL id должен быть либо SERIAL, либо "GENERATED ALWAYS AS IDENTITY".
        # Проверим какой SQL генерирует create_table_from_model(User).

        # Создадим пользовательские формы без изменения их логики,   
        # но добавим диагностический вывод для проверки, что id не передается в create_user.
        create_frame = ttk.LabelFrame(forms, text="Create user")
        create_frame.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)
        read_frame = ttk.LabelFrame(forms, text="Read users")
        read_frame.grid(row=0, column=1, sticky="nsew", padx=6, pady=6)
        update_frame = ttk.LabelFrame(forms, text="Update user")
        update_frame.grid(row=1, column=0, sticky="nsew", padx=6, pady=6)
        delete_frame = ttk.LabelFrame(forms, text="Delete user")
        delete_frame.grid(row=1, column=1, sticky="nsew", padx=6, pady=6)

        # Ключевое: В форме создания пользователя данные передаются как:
        # {
        #   "username": ...,
        #   "email": ...,
        #   "hashed_password": ...,
        #   "full_name": ...,
        #   "is_active": ...
        # }
        # Здесь НЕТ ключа "id"! create_user вызывает db.insert("users", data, ...).

        # Если возникает ошибка "NULL в столбце id", это значит что:
        #   1. База ОЖИДАЕТ, что id будет указан явно (например, если id INTEGER NOT NULL, но не SERIAL или identity).
        #   2. Исполняемый SQL для users неверно формирует id (например, не прописан autoincrement/identity).

        # Для PostgreSQL корректный способ:
        #   id SERIAL PRIMARY KEY         -- для PostgreSQL до 10
        #   или
        #   id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY -- для PostgreSQL 10+

        # Проверьте сгенерированный SQL (выведите его временно перед созданием):
        # print(PostgresDriver._generate_create_table_sql(User))

        # Пример такого вывода:
        # CREATE TABLE IF NOT EXISTS "users" (
        #   "id" INTEGER PRIMARY KEY NOT NULL,    -- неверно! (нет SERIAL/identity)
        #   ...
        # )
        #
        # Такую строчку нужно заменить на:
        #   "id" SERIAL PRIMARY KEY
        # или
        #   "id" INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY

        # Итог: сама логика создания форм НЕ добавляет id.
        # Нужно исправить генерацию SQL в PostgresDriver._generate_create_table_sql,
        # чтобы для id (Integer, primary_key, autoincrement) делался SERIAL или GENERATED AS IDENTITY.


        forms.columnconfigure(0, weight=1)
        forms.columnconfigure(1, weight=1)

        out = self._add_output(tab)

        c_username = self._add_labeled_entry(create_frame, "username", 0)
        c_email = self._add_labeled_entry(create_frame, "email", 1)
        c_password = self._add_labeled_entry(create_frame, "hashed_password", 2)
        c_full_name = self._add_labeled_entry(create_frame, "full_name", 3)
        c_active = self._add_labeled_entry(create_frame, "is_active (0/1)", 4)
        c_active.insert(0, "1")
        ttk.Button(
            create_frame,
            text="Create",
            command=lambda: self._execute(
                out,
                lambda: create_user(
                    {
                        "username": _strip(c_username.get()),
                        "email": _strip(c_email.get()),
                        "hashed_password": _strip(c_password.get()),
                        "full_name": _strip(c_full_name.get()) or None,
                        "is_active": _to_int(c_active.get(), "is_active"),
                    },
                    returning=["id", "username", "email", "is_active"],
                ),
            ),
        ).grid(row=5, column=0, columnspan=2, sticky="ew", padx=6, pady=8)

        r_id = self._add_labeled_entry(read_frame, "id (optional)", 0)
        r_username = self._add_labeled_entry(read_frame, "username (optional)", 1)
        r_email = self._add_labeled_entry(read_frame, "email (optional)", 2)
        r_active = self._add_labeled_entry(read_frame, "is_active (optional)", 3)
        r_limit = self._add_labeled_entry(read_frame, "limit (optional)", 4)
        r_offset = self._add_labeled_entry(read_frame, "offset (optional)", 5)
        ttk.Button(
            read_frame,
            text="Read",
            command=lambda: self._execute(
                out,
                lambda: read_users(
                    filters={
                        k: v
                        for k, v in {
                            "id": _to_optional_int(r_id.get()),
                            "username": _strip(r_username.get()) or None,
                            "email": _strip(r_email.get()) or None,
                            "is_active": _to_optional_int(r_active.get()),
                        }.items()
                        if v is not None
                    },
                    limit=_to_optional_int(r_limit.get()),
                    offset=_to_optional_int(r_offset.get()),
                ),
            ),
        ).grid(row=6, column=0, columnspan=2, sticky="ew", padx=6, pady=8)

        u_id = self._add_labeled_entry(update_frame, "id (required filter)", 0)
        u_username = self._add_labeled_entry(update_frame, "new username", 1)
        u_email = self._add_labeled_entry(update_frame, "new email", 2)
        u_password = self._add_labeled_entry(update_frame, "new hashed_password", 3)
        u_full_name = self._add_labeled_entry(update_frame, "new full_name", 4)
        u_active = self._add_labeled_entry(update_frame, "new is_active", 5)
        ttk.Button(
            update_frame,
            text="Update",
            command=lambda: self._execute(
                out,
                lambda: update_user(
                    data={
                        k: v
                        for k, v in {
                            "username": _strip(u_username.get()) or None,
                            "email": _strip(u_email.get()) or None,
                            "hashed_password": _strip(u_password.get()) or None,
                            "full_name": _strip(u_full_name.get()) or None,
                            "is_active": _to_optional_int(u_active.get()),
                        }.items()
                        if v is not None
                    },
                    filters={"id": _to_int(u_id.get(), "id")},
                    returning=["id", "username", "email", "is_active"],
                ),
            ),
        ).grid(row=6, column=0, columnspan=2, sticky="ew", padx=6, pady=8)

        d_id = self._add_labeled_entry(delete_frame, "id (required)", 0)
        ttk.Button(
            delete_frame,
            text="Delete",
            command=lambda: self._execute(
                out,
                lambda: delete_user(
                    filters={"id": _to_int(d_id.get(), "id")},
                    returning=["id", "username", "email"],
                ),
            ),
        ).grid(row=1, column=0, columnspan=2, sticky="ew", padx=6, pady=8)

    def _build_tables_tab(self) -> None:
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Tables")

        forms = ttk.Frame(tab)
        forms.pack(fill="x", padx=8, pady=8)
        forms.columnconfigure(0, weight=1)
        forms.columnconfigure(1, weight=1)

        create_frame = ttk.LabelFrame(forms, text="Create table")
        create_frame.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)
        read_frame = ttk.LabelFrame(forms, text="Read tables")
        read_frame.grid(row=0, column=1, sticky="nsew", padx=6, pady=6)
        update_frame = ttk.LabelFrame(forms, text="Update table")
        update_frame.grid(row=1, column=0, sticky="nsew", padx=6, pady=6)
        delete_frame = ttk.LabelFrame(forms, text="Delete table")
        delete_frame.grid(row=1, column=1, sticky="nsew", padx=6, pady=6)

        out = self._add_output(tab)

        c_num = self._add_labeled_entry(create_frame, "table_number", 0)
        c_seats = self._add_labeled_entry(create_frame, "seats", 1)
        c_location = self._add_labeled_entry(create_frame, "location", 2)
        c_active = self._add_labeled_entry(create_frame, "is_active (0/1)", 3)
        c_active.insert(0, "1")
        ttk.Button(
            create_frame,
            text="Create",
            command=lambda: self._execute(
                out,
                lambda: create_table_record(
                    {
                        "table_number": _to_int(c_num.get(), "table_number"),
                        "seats": _to_int(c_seats.get(), "seats"),
                        "location": _strip(c_location.get()) or None,
                        "is_active": _to_int(c_active.get(), "is_active"),
                    },
                    returning=["id", "table_number", "seats", "location", "is_active"],
                ),
            ),
        ).grid(row=4, column=0, columnspan=2, sticky="ew", padx=6, pady=8)

        r_id = self._add_labeled_entry(read_frame, "id (optional)", 0)
        r_number = self._add_labeled_entry(read_frame, "table_number (optional)", 1)
        r_active = self._add_labeled_entry(read_frame, "is_active (optional)", 2)
        r_limit = self._add_labeled_entry(read_frame, "limit (optional)", 3)
        r_offset = self._add_labeled_entry(read_frame, "offset (optional)", 4)
        ttk.Button(
            read_frame,
            text="Read",
            command=lambda: self._execute(
                out,
                lambda: read_tables(
                    filters={
                        k: v
                        for k, v in {
                            "id": _to_optional_int(r_id.get()),
                            "table_number": _to_optional_int(r_number.get()),
                            "is_active": _to_optional_int(r_active.get()),
                        }.items()
                        if v is not None
                    },
                    limit=_to_optional_int(r_limit.get()),
                    offset=_to_optional_int(r_offset.get()),
                ),
            ),
        ).grid(row=5, column=0, columnspan=2, sticky="ew", padx=6, pady=8)

        u_id = self._add_labeled_entry(update_frame, "id (required filter)", 0)
        u_number = self._add_labeled_entry(update_frame, "new table_number", 1)
        u_seats = self._add_labeled_entry(update_frame, "new seats", 2)
        u_location = self._add_labeled_entry(update_frame, "new location", 3)
        u_active = self._add_labeled_entry(update_frame, "new is_active", 4)
        ttk.Button(
            update_frame,
            text="Update",
            command=lambda: self._execute(
                out,
                lambda: update_table_record(
                    data={
                        k: v
                        for k, v in {
                            "table_number": _to_optional_int(u_number.get()),
                            "seats": _to_optional_int(u_seats.get()),
                            "location": _strip(u_location.get()) or None,
                            "is_active": _to_optional_int(u_active.get()),
                        }.items()
                        if v is not None
                    },
                    filters={"id": _to_int(u_id.get(), "id")},
                    returning=["id", "table_number", "seats", "location", "is_active"],
                ),
            ),
        ).grid(row=5, column=0, columnspan=2, sticky="ew", padx=6, pady=8)

        d_id = self._add_labeled_entry(delete_frame, "id (required)", 0)
        ttk.Button(
            delete_frame,
            text="Delete",
            command=lambda: self._execute(
                out,
                lambda: delete_table_record(
                    filters={"id": _to_int(d_id.get(), "id")},
                    returning=["id", "table_number", "seats", "location"],
                ),
            ),
        ).grid(row=1, column=0, columnspan=2, sticky="ew", padx=6, pady=8)

    def _build_bookings_tab(self) -> None:
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Bookings")

        forms = ttk.Frame(tab)
        forms.pack(fill="x", padx=8, pady=8)
        forms.columnconfigure(0, weight=1)
        forms.columnconfigure(1, weight=1)

        create_frame = ttk.LabelFrame(forms, text="Create booking")
        create_frame.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)
        read_frame = ttk.LabelFrame(forms, text="Read bookings")
        read_frame.grid(row=0, column=1, sticky="nsew", padx=6, pady=6)
        update_frame = ttk.LabelFrame(forms, text="Update booking")
        update_frame.grid(row=1, column=0, sticky="nsew", padx=6, pady=6)
        delete_frame = ttk.LabelFrame(forms, text="Delete booking")
        delete_frame.grid(row=1, column=1, sticky="nsew", padx=6, pady=6)

        out = self._add_output(tab)

        c_user_id = self._add_labeled_entry(create_frame, "user_id", 0)
        c_table_id = self._add_labeled_entry(create_frame, "table_id", 1)
        c_time = self._add_labeled_entry(create_frame, "booking_time (YYYY-MM-DD HH:MM:SS)", 2)
        c_duration = self._add_labeled_entry(create_frame, "duration_minutes (optional)", 3)
        c_duration.insert(0, "120")
        ttk.Button(
            create_frame,
            text="Create",
            command=lambda: self._execute(
                out,
                lambda: create_booking(
                    {
                        "user_id": _to_int(c_user_id.get(), "user_id"),
                        "table_id": _to_int(c_table_id.get(), "table_id"),
                        "booking_time": _to_datetime(c_time.get()),
                        "duration_minutes": _to_int(c_duration.get(), "duration_minutes"),
                    },
                    returning=["id", "user_id", "table_id", "booking_time"],
                ),
            ),
        ).grid(row=4, column=0, columnspan=2, sticky="ew", padx=6, pady=8)

        r_id = self._add_labeled_entry(read_frame, "id (optional)", 0)
        r_user_id = self._add_labeled_entry(read_frame, "user_id (optional)", 1)
        r_table_id = self._add_labeled_entry(read_frame, "table_id (optional)", 2)
        r_limit = self._add_labeled_entry(read_frame, "limit (optional)", 3)
        r_offset = self._add_labeled_entry(read_frame, "offset (optional)", 4)
        ttk.Button(
            read_frame,
            text="Read",
            command=lambda: self._execute(
                out,
                lambda: read_bookings(
                    filters={
                        k: v
                        for k, v in {
                            "id": _to_optional_int(r_id.get()),
                            "user_id": _to_optional_int(r_user_id.get()),
                            "table_id": _to_optional_int(r_table_id.get()),
                        }.items()
                        if v is not None
                    },
                    limit=_to_optional_int(r_limit.get()),
                    offset=_to_optional_int(r_offset.get()),
                ),
            ),
        ).grid(row=5, column=0, columnspan=2, sticky="ew", padx=6, pady=8)

        u_id = self._add_labeled_entry(update_frame, "id (required filter)", 0)
        u_user_id = self._add_labeled_entry(update_frame, "new user_id", 1)
        u_table_id = self._add_labeled_entry(update_frame, "new table_id", 2)
        u_time = self._add_labeled_entry(update_frame, "new booking_time", 3)
        u_duration = self._add_labeled_entry(update_frame, "duration_minutes (optional)", 4)
        u_duration.insert(0, "120")
        ttk.Button(
            update_frame,
            text="Update",
            command=lambda: self._execute(
                out,
                lambda: update_booking(
                    data={
                        k: v
                        for k, v in {
                            "user_id": _to_optional_int(u_user_id.get()),
                            "table_id": _to_optional_int(u_table_id.get()),
                            "booking_time": _to_datetime(u_time.get()) if _strip(u_time.get()) else None,
                            "duration_minutes": _to_optional_int(u_duration.get()),
                        }.items()
                        if v is not None
                    },
                    filters={"id": _to_int(u_id.get(), "id")},
                    returning=["id", "user_id", "table_id", "booking_time"],
                ),
            ),
        ).grid(row=5, column=0, columnspan=2, sticky="ew", padx=6, pady=8)

        d_id = self._add_labeled_entry(delete_frame, "id (required)", 0)
        ttk.Button(
            delete_frame,
            text="Delete",
            command=lambda: self._execute(
                out,
                lambda: delete_booking(
                    filters={"id": _to_int(d_id.get(), "id")},
                    returning=["id", "user_id", "table_id", "booking_time"],
                ),
            ),
        ).grid(row=1, column=0, columnspan=2, sticky="ew", padx=6, pady=8)

    def _build_availability_tab(self) -> None:
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="Availability")

        frame = ttk.LabelFrame(tab, text="Check table availability")
        frame.pack(fill="x", padx=8, pady=8)

        out = self._add_output(tab)

        table_id = self._add_labeled_entry(frame, "table_id", 0)
        booking_time = self._add_labeled_entry(frame, "booking_time (YYYY-MM-DD HH:MM:SS)", 1)
        duration = self._add_labeled_entry(frame, "duration_minutes", 2)
        duration.insert(0, "120")
        exclude_id = self._add_labeled_entry(frame, "exclude_booking_id (optional)", 3)

        ttk.Button(
            frame,
            text="Check",
            command=lambda: self._execute(
                out,
                lambda: {
                    "available": check_table_availability(
                        table_id=_to_int(table_id.get(), "table_id"),
                        booking_time=_to_datetime(booking_time.get()),
                        duration_minutes=_to_int(duration.get(), "duration_minutes"),
                        exclude_booking_id=_to_optional_int(exclude_id.get()),
                    )
                },
            ),
        ).grid(row=4, column=0, columnspan=2, sticky="ew", padx=6, pady=8)


if __name__ == "__main__":
    app = BookingApp()
    app.mainloop()
