from abc import ABC, abstractmethod
from datetime import datetime
from types import ModuleType
from dataclasses import dataclass
from importlib import import_module
from tzlocal import get_localzone_name
import traceback
import numpy as np
from .dividend_tool import make_front_back_dr, make_timetags_apply_dr
from .constant import Interval, Exchange, Dividend, ExtraInterval
from .object import BarData, TickData, DividendData, TradeDateData, MainContractData, HistoryRequest
from .setting import SETTINGS
from zoneinfo import ZoneInfo
from .locale import _


DB_TZ = ZoneInfo(SETTINGS["database.timezone"])
LOCAL_TZ = ZoneInfo(get_localzone_name())


def convert_tz(dt: datetime) -> datetime:
    """
    Convert timezone of datetime object to DB_TZ.
    """
    dt = dt.astimezone(DB_TZ)
    return dt.replace(tzinfo=None)


def to_dbtz(t: datetime|None, clear_tzinfo=True) -> datetime|None:
    # 从 本地时区 转换到 数据库时区
    if t is None:
        return t
    if t.tzinfo is None:
        t = t.replace(tzinfo=LOCAL_TZ)
    t = t.astimezone(DB_TZ)
    if clear_tzinfo:
        t = t.replace(tzinfo=None)
    return t


def from_dbtz(t: datetime|None, clear_tzinfo=False) -> datetime|None:
    # 从 数据库时区 转换到 本地时区
    if t is None:
        return t
    if t.tzinfo is None:
        t = t.replace(tzinfo=DB_TZ)
    t = t.astimezone(LOCAL_TZ)
    if clear_tzinfo:
        t = t.replace(tzinfo=None)
    return t


@dataclass
class BarOverview:
    """
    Overview of bar data stored in database.
    """

    symbol: str = ""
    exchange: Exchange | None = None
    interval: Interval | None = None
    count: int = 0
    start: datetime | None = None
    end: datetime | None = None


@dataclass
class TickOverview:
    """
    Overview of tick data stored in database.
    """

    symbol: str = ""
    exchange: Exchange | None = None
    count: int = 0
    start: datetime | None = None
    end: datetime | None = None


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


@dataclass
class TradeDateOverview:
    """
    Overview of trade date stored in database.
    """
    exchange: Exchange = None
    count: int = 0
    start: datetime = None
    end: datetime = None


@dataclass
class MainContractOverview:
    """
    Overview 主力合约
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
    def save_bar_data(self, bars: list[BarData], stream: bool = False) -> bool:
        """
        Save bar data into database.
        """
        pass

    @abstractmethod
    def save_tick_data(self, ticks: list[TickData], stream: bool = False) -> bool:
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
    ) -> list[BarData]:
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
    ) -> list[TickData]:
        """
        Load tick data from database.
        """
        pass

    @abstractmethod
    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime = None,
        end: datetime = None,
    ) -> int:
        """
        Delete all bar data with given symbol + exchange + interval.
        """
        pass

    @abstractmethod
    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime = None,
        end: datetime = None,
    ) -> int:
        """
        Delete all tick data with given symbol + exchange.
        """
        pass

    @abstractmethod
    def get_bar_overview(
        self,
        symbol: str = None,
        exchange: Exchange = None,
        interval: Interval = None,
    ) -> list[BarOverview]:
        """
        Return bar data avaible in database.
        """
        pass

    @abstractmethod
    def get_tick_overview(self,
        symbol: str = None,
        exchange: Exchange = None
    ) -> list[TickOverview]:
        """
        Return tick data avaible in database.
        """
        pass

    # -----------------------------------------------------------------------

    # @abstractmethod
    def save_dividend_data(self, drs: list[DividendData]) -> bool:
        """
        保存除权数据
        """
        pass

    # @abstractmethod
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

    # @abstractmethod
    def get_dividend_overview(
        self,
        symbol: str = None,
        exchange: Exchange = None,
    ) -> list[DividendOverview]:
        """
        获取除权汇总信息
        """
        pass

    # @abstractmethod
    def delete_dividend_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime = None,
        end: datetime = None,
    ) -> int:
        """
        删除除权数据
        """
        pass

    # -----------------------------------------------------------------------

    # @abstractmethod
    def save_trade_date_data(self, drs: list[TradeDateData]) -> bool:
        """
        保存交易日数据
        """
        pass

    # @abstractmethod
    def load_trade_date_data(
        self,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> list[TradeDateData]:
        """
        获得交易日数据
        """
        pass

    # @abstractmethod
    def get_trade_date_overview(
        self,
        exchange: Exchange = None,
    ) -> list[DividendOverview]:
        """
        获取交易日汇总信息
        """
        pass

    # @abstractmethod
    def delete_trade_date_data(
        self,
        exchange: Exchange,
        start: datetime = None,
        end: datetime = None,
    ) -> int:
        """
        删除交易日数据
        """
        pass

    # -----------------------------------------------------------------------

    # @abstractmethod
    def save_main_contract_data(self, data: list[MainContractData]) -> bool:
        """
        保存主连数据
        """
        pass

    # @abstractmethod
    def load_main_contract_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> list[MainContractData]:
        """
        获得主连数据
        """
        pass

    # @abstractmethod
    def get_main_contract_overview(
        self,
        symbol: str,
        exchange: Exchange = None,
    ) -> list[MainContractOverview]:
        """
        获取主连汇总信息
        """
        pass

    # @abstractmethod
    def delete_main_contract_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime = None,
        end: datetime = None,
    ) -> int:
        """
        删除主连数据
        """
        pass

    # -----------------------------------------------------------------------
    def query_history_uni(self, req: HistoryRequest):
        if req.interval == Interval.TICK:
            return self.load_tick_data(req.symbol, req.exchange, req.start, req.end, req.dividend)
        elif req.interval in [Interval.MINUTE, Interval.MINUTE_5, Interval.HOUR, Interval.DAILY, Interval.WEEKLY]:
            return self.load_bar_data(req.symbol, req.exchange, req.interval, req.start, req.end, req.dividend)
        elif req.interval == ExtraInterval.Dividend:
            return self.load_dividend_data(req.symbol, req.exchange, req.start, req.end)
        elif req.interval == ExtraInterval.TradeDate:
            return self.load_trade_date_data(req.exchange, req.start, req.end)
        else:
            raise AssertionError(f'query_history_uni 不支持请求的类型，{req.interval}')

    def query_overview_uni(self, symbol: str, exchange: Exchange, interval: Interval|ExtraInterval):
        if interval == Interval.TICK:
            return self.get_tick_overview(symbol, exchange)
        elif interval in [Interval.MINUTE, Interval.MINUTE_5, Interval.HOUR, Interval.DAILY, Interval.WEEKLY]:
            return self.get_bar_overview(symbol, exchange, interval)
        elif interval == ExtraInterval.Dividend:
            return self.get_dividend_overview(symbol, exchange)
        elif interval == ExtraInterval.TradeDate:
            return self.get_trade_date_overview(exchange)
        else:
            raise AssertionError(f'query_overview_uni 不支持请求的类型，{interval}')

    # -----------------------------------------------------------------------

    def _apply_dividend(self, type: str, data_list: list[TickData|BarData], dividend_data: list[DividendData], dividend: Dividend):
        '''
        对输入的 data 进行复权
        '''
        # assert dividend in [Dividend.FRONT_RATIO, Dividend.BACK_RATIO, Dividend.FRONT_DIFF, Dividend.BACK_DIFF]
        assert dividend in [Dividend.FRONT_RATIO, Dividend.BACK_RATIO]
        assert type in ['tick', 'bar']

        if len(dividend_data) == 0 or len(data_list) == 0:
            return

        dr_ratio = np.zeros(len(dividend_data), np.float64)
        dr_time = np.zeros(len(dividend_data), np.int64)
        for i, v in enumerate(dividend_data):
            dr_time[i] = v.datetime.timestamp()*1000
            dr_ratio[i] = v.ratio

        front_dr, back_dr = make_front_back_dr(dr_ratio)

        data_time = np.zeros(len(data_list), np.int64)
        for i, v in enumerate(data_list):
            data_time[i] = v.datetime.timestamp()*1000

        if dividend == Dividend.FRONT_RATIO:
            apply_dr = make_timetags_apply_dr(data_time, dr_time, front_dr, 'front', 'ratio')
        elif dividend == Dividend.BACK_RATIO:
            apply_dr = make_timetags_apply_dr(data_time, dr_time, back_dr, 'back', 'ratio')
        else:
            raise AssertionError(f'错误！输入不支持的复权类型{dividend}')

        if type == 'tick':
            col_names = ("last_price", "limit_up", "limit_down",
                "open_price", "high_price", "low_price", "pre_close",
                "bid_price_1", "bid_price_2", "bid_price_3", "bid_price_4", "bid_price_5",
                "ask_price_1", "ask_price_2", "ask_price_3", "ask_price_4", "ask_price_5",
                "bid_volume_1", "bid_volume_2", "bid_volume_3", "bid_volume_4", "bid_volume_5",
                "ask_volume_1", "ask_volume_2", "ask_volume_3", "ask_volume_4", "ask_volume_5"
            )
        elif type == 'bar':
            col_names = ("open_price", "high_price", "low_price", "close_price")
        else:
            raise AssertionError(f'错误！输入不支持的类型{type}')

        for i, d in enumerate(data_list):
            d: TickData|BarData
            r = apply_dr[i]
            for name in col_names:
                setattr(d, name, getattr(d, name) * r)

    def apply_dividend(self, type: str, data_list: list[TickData|BarData], symbol: str, exchange: Exchange, dividend: Dividend):
        '''
        复权数据，注意会修改输入的 data_list 的数据
        '''
        if len(data_list) == 0 or Dividend.NONE == dividend:
            return

        dividend_data = self.load_dividend_data(symbol, exchange, datetime(1970, 1, 1), datetime(2200, 1, 1))
        if len(dividend_data) == 0:
            return

        self._apply_dividend(type, data_list, dividend_data, dividend)

    # -----------------------------------------------------------------------


database: BaseDatabase | None = None


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
    except ModuleNotFoundError as e:
        print(_("无法导入数据库驱动{}，因为{}\n将使用默认的SQLite数据库").format(module_name, str(e)+f"\n{traceback.format_exc()}"))
        module = import_module("vnpy_sqlite")

    # Create database object from module
    try:
        database = module.Database()
    except Exception as e:
        print(_("数据库初始化失败").format(module_name), str(e))
        database = None
    return database     # type: ignore
