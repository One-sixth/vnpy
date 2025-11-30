from types import ModuleType
from collections.abc import Callable
from importlib import import_module

from .constant import Interval, ExtraInterval
from .object import HistoryRequest, TickData, BarData, DividendData, TradeDateData
from .setting import SETTINGS
from .locale import _
from .logger import logger


class BaseDatafeed:
    """
    Abstract datafeed class for connecting to different datafeed.
    """

    def init(self, output: Callable = logger.info, **kwargs) -> bool:
        """
        Initialize datafeed service connection.
        """
        return False

    def query_bar_history(self, req: HistoryRequest, output: Callable = logger.info) -> list[BarData]:
        """
        Query history bar data.
        """
        output(_("查询K线数据失败：没有正确配置数据服务"))
        return []

    def query_tick_history(self, req: HistoryRequest, output: Callable = logger.info) -> list[TickData]:
        """
        Query history tick data.
        """
        output(_("查询Tick数据失败：没有正确配置数据服务"))
        return []

    def query_dividend_history(self, req: HistoryRequest, output: Callable = logger.info) -> list[DividendData]:
        """
        Query history dividend data.
        """
        output(_("查询除权数据失败：没有正确配置数据服务"))
        return []

    def query_tradedate_history(self, req: HistoryRequest, output: Callable = logger.info) -> list[TradeDateData]:
        """
        Query history tradedate data.
        """
        output(_("查询交易日数据失败：没有正确配置数据服务"))
        return []

    def query_history_uni(self, req: HistoryRequest, output: Callable = logger.info):
        '''
        统一接口，可以同时从database和datafeed中获取历史数据
        '''
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


# datafeed: BaseDatafeed | None = None
datafeed_dict: dict[str, BaseDatafeed] = {}


def get_datafeed(name: str='default') -> BaseDatafeed:
    '''
    修改，增加多datafeed的支持，允许使用名字来获取对应的datafeed，并且特别地，datafeed全局共享
    该修改不影响默认行为
    '''
    if name == 'default' and name not in datafeed_dict:
        # 自动初始化 default datafeed
        datafeed_class_name: str = SETTINGS["datafeed.name"]
        if not datafeed_class_name:
            print(_("没有配置要使用的数据服务，请修改全局配置中的datafeed相关内容"))
            return BaseDatafeed()

        add_datafeed(name, datafeed_class_name)

    if name in datafeed_dict:
        return datafeed_dict[name]


def _get_datafeed_class(datafeed_class_name: str=None):
    '''
    获取 datafeed 类，支持三种处理方法
    '''
    assert isinstance(datafeed_class_name, str)

    if '.' in datafeed_class_name:
        # 模块名.类名
        try:
            module_name, class_name = datafeed_class_name.split('.')
            module: ModuleType = import_module(module_name)
            datafeed_class = getattr(module, class_name)
            return datafeed_class
        except ModuleNotFoundError:
            print(_("加载数据服务模块 {} 失败").format(datafeed_class_name))
            return None

    else:
        module_name_1: str = f"vnpy_{datafeed_class_name}"
        module_name_2: str = f"{datafeed_class_name}"

        # 尝试加载方法2
        try:
            module: ModuleType = import_module(module_name_1)
            datafeed_class = getattr(module, 'Datafeed')
            return datafeed_class
        except ModuleNotFoundError:
            pass

        # 尝试加载方法3
        try:
            module: ModuleType = import_module(module_name_2)
            datafeed_class = getattr(module, 'Datafeed')
            return datafeed_class
        except ModuleNotFoundError:
            print(_("加载数据服务模块 {} 失败").format(datafeed_class_name))
        return None


def add_datafeed(name: str=None, datafeed_class_name: str=None, datafeed_kwargs: dict=dict()):
    '''
    增加datafeed，允许用户自定义datafeed
    '''
    assert isinstance(name, str)
    try:
        # 加入新的 datafeed
        datafeed_class = _get_datafeed_class(datafeed_class_name)
        new_datafeed = datafeed_class()
        new_datafeed.init(**datafeed_kwargs)
        remove_datafeed(name)
        datafeed_dict[name] = new_datafeed

    except Exception as e:
        new_datafeed = BaseDatafeed()
        print(_("加载数据服务模块 {} 失败，错误").format(datafeed_class_name, str(e)))

    return new_datafeed


def remove_datafeed(name: str):
    '''
    删除对应的datafeed
    '''
    if name in datafeed_dict:
        datafeed_dict[name].close()
        del datafeed_dict[name]
        return True

    else:
        return False


def get_all_datafeed_names():
    '''
    获取所有datafeed的名字
    '''
    return list(datafeed_dict.keys())
