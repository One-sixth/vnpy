from abc import ABC, abstractmethod
from datetime import datetime
from types import ModuleType
from typing import List
from dataclasses import dataclass
from importlib import import_module

from .constant import Interval, Exchange, Dividend
from .object import BarData, TickData, DividendData
from .setting import SETTINGS
from .utility import ZoneInfo
from .locale import _


DB_TZ = ZoneInfo(SETTINGS["database.timezone"])


def convert_tz(dt: datetime) -> datetime:
    """
    Convert timezone of datetime object to DB_TZ.
    """
    dt: datetime = dt.astimezone(DB_TZ)
    return dt.replace(tzinfo=None)


@dataclass
class BarOverview:
    """
    Overview of bar data stored in database.
    """

    symbol: str = ""
    exchange: Exchange = None
    interval: Interval = None
    count: int = 0
    start: datetime = None
    end: datetime = None


@dataclass
class TickOverview:
    """
    Overview of tick data stored in database.
    """

    symbol: str = ""
    exchange: Exchange = None
    count: int = 0
    start: datetime = None
    end: datetime = None


@dataclass
class DividendOverview:
    """
    Overview of dividend data stored in database.
    """
    symbol: str = ""
    exchange: Exchange = None
    count: int = 0
    start: datetime = None
    end: datetime = None


class BaseDatabase(ABC):
    """
    Abstract database class for connecting to different database.
    """

    @abstractmethod
    def save_bar_data(self, bars: List[BarData], stream: bool = False) -> bool:
        """
        Save bar data into database.
        """
        pass

    @abstractmethod
    def save_tick_data(self, ticks: List[TickData], stream: bool = False) -> bool:
        """
        Save tick data into database.
        """
        pass

    @abstractmethod
    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime,
        dividend: Dividend=Dividend.NONE,
    ) -> List[BarData]:
        """
        Load bar data from database.
        """
        pass

    @abstractmethod
    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime,
        dividend: Dividend=Dividend.NONE,
    ) -> List[TickData]:
        """
        Load tick data from database.
        """
        pass

    @abstractmethod
    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """
        Delete all bar data with given symbol + exchange + interval.
        """
        pass

    @abstractmethod
    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """
        Delete all tick data with given symbol + exchange.
        """
        pass

    @abstractmethod
    def get_bar_overview(self) -> List[BarOverview]:
        """
        Return bar data avaible in database.
        """
        pass

    @abstractmethod
    def get_tick_overview(self) -> List[TickOverview]:
        """
        Return tick data avaible in database.
        """
        pass

    @abstractmethod
    def save_dividend_data(self, drs: list[DividendData]) -> bool:
        """
        保存除权数据
        """
        pass

    @abstractmethod
    def load_dividend_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> list[DividendData]:
        """
        获得除权数据
        """
        pass

    @abstractmethod
    def get_dividend_overview(
        self,
        symbol: str=None,
        exchange: Exchange=None,
    ) -> list[DividendOverview]:
        """
        获取除权汇总信息
        """
        pass

    @abstractmethod
    def delete_dividend_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """
        删除除权数据
        """
        pass


database: BaseDatabase = None


def get_database() -> BaseDatabase:
    """"""
    # Return database object if already inited
    global database
    if database:
        return database

    # Read database related global setting
    database_name: str = SETTINGS["database.name"]
    module_name: str = f"vnpy_{database_name}"

    # Try to import database module
    try:
        module: ModuleType = import_module(module_name)
    except ModuleNotFoundError:
        print(_("找不到数据库驱动{}，使用默认的SQLite数据库").format(module_name))
        module: ModuleType = import_module("vnpy_sqlite")

    # Create database object from module
    database = module.Database()
    return database
