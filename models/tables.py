from sqlalchemy import Column, Integer, String

from .base import Base


class Table(Base):
    __tablename__ = "tables"

    id = Column(Integer, primary_key=True, autoincrement=True)
    table_number = Column(Integer, unique=True, nullable=False)
    seats = Column(Integer, nullable=False)
    location = Column(String(100), nullable=True)
    is_active = Column(Integer, default=1, nullable=False)
