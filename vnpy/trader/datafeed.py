from types import ModuleType
from collections.abc import Callable
from importlib import import_module

from .constant import Interval, ExtraInterval
from .object import HistoryRequest, TickData, BarData, DividendData, TradeDateData
from .setting import SETTINGS
from .locale import _


class BaseDatafeed:
    """
    Abstract datafeed class for connecting to different datafeed.
    """

    def init(self, output: Callable = print) -> bool:
        """
        Initialize datafeed service connection.
        """
        return False

    def query_bar_history(self, req: HistoryRequest, output: Callable = print) -> list[BarData]:
        """
        Query history bar data.
        """
        output(_("查询K线数据失败：没有正确配置数据服务"))
        return []

    def query_tick_history(self, req: HistoryRequest, output: Callable = print) -> list[TickData]:
        """
        Query history tick data.
        """
        output(_("查询Tick数据失败：没有正确配置数据服务"))
        return []

    def query_dividend_history(self, req: HistoryRequest, output: Callable = print) -> list[DividendData]:
        """
        Query history dividend data.
        """
        output(_("查询除权数据失败：没有正确配置数据服务"))
        return []

    def query_tradedate_history(self, req: HistoryRequest, output: Callable = print) -> list[TradeDateData]:
        """
        Query history tradedate data.
        """
        output(_("查询交易日数据失败：没有正确配置数据服务"))
        return []

    def query_history_uni(self, req: HistoryRequest, output: Callable = print):
        if req.interval == Interval.TICK:
            return self.query_tick_history(req, output)
        elif req.interval in [Interval.MINUTE, Interval.MINUTE_5, Interval.HOUR, Interval.DAILY, Interval.WEEKLY]:
            return self.query_bar_history(req, output)
        elif req.interval == ExtraInterval.Dividend:
            return self.query_dividend_history(req, output)
        elif req.interval == ExtraInterval.TradeDate:
            return self.query_tradedate_history(req, output)
        else:
            raise AssertionError(f'query_history_uni 不支持请求的类型，{req.interval}')

    def close(self):
        '''
        结束和清理
        :return:
        '''
        pass


datafeed: BaseDatafeed | None = None


def get_datafeed() -> BaseDatafeed:
    """"""
    # Return datafeed object if already inited
    global datafeed
    if datafeed:
        return datafeed

    # Read datafeed related global setting
    datafeed_name: str = SETTINGS["datafeed.name"]

    if not datafeed_name:
        datafeed = BaseDatafeed()

        print(_("没有配置要使用的数据服务，请修改全局配置中的datafeed相关内容"))
    else:
        module_name: str = f"vnpy_{datafeed_name}"

        # Try to import datafeed module
        try:
            module: ModuleType = import_module(module_name)

            # Create datafeed object from module
            datafeed = module.Datafeed()
        # Use base class if failed
        except ModuleNotFoundError:
            datafeed = BaseDatafeed()

            print(_("无法加载数据服务模块，请运行 pip install {} 尝试安装").format(module_name))

    return datafeed     # type: ignore
