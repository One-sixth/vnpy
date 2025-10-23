from .constant import Interval, ExtraInterval, Exchange
from .object import HistoryRequest
from datetime import datetime, timedelta
from typing import Literal
from .datafeed import BaseDatafeed, get_datafeed
from .database import BaseDatabase, get_database
from functools import lru_cache


@lru_cache(None, typed=True)
def _auto_cache(symbol: str, exchange: Exchange, interval: Interval|ExtraInterval) -> datetime|None:
    database: BaseDatabase = get_database()
    ows = database.query_overview_uni(symbol, exchange, interval)
    if len(ows) == 0:
        return None
    else:
        assert len(ows) == 1
        ow = ows[0]
        return ow.end - timedelta(days=1)


def query_history_uni(
        # engine,
        req: HistoryRequest,
        cut: datetime | timedelta | Literal['database', 'datafeed', 'auto'] = None
) -> list:
    """
    请求自定义数据，其中部分数据可以从数据库中获取。
    按时点分开获取
    type_: 可以是 datetime 代表以此时点分割，timedelta 代表在 end 前间隔时间作为时点来分割，字符串 database 代表全部使用数据库里的，datafeed 代表全部使用在线的
    注意，分割时点后，都是用 datafeed 的，分割时点前，都是用 database 的，这里会认为 datafeed的数据会比database的更新
    """

    datafeed: BaseDatafeed = get_datafeed()
    database: BaseDatabase = get_database()

    '''
    start_dt    database的起始时间点
    mid_dt      database的结束时间点，datafeed的起始时间点
    end_dt      datafeed的结束时间点
    '''

    if cut == 'auto':
        # 从数据库里面寻找最近的数据，然后再筛选
        r = _auto_cache(req.interval, req.symbol, req.exchange)
        if r is None:
            cut = 'datafeed'
        else:
            assert isinstance(r, datetime)
            cut = r

    if cut == 'database':
        start_dt = req.start
        mid_dt = req.end
        end_dt = None

    elif cut == 'datafeed':
        start_dt = None
        mid_dt = req.start
        end_dt = req.end

    elif isinstance(cut, timedelta):
        start_dt = req.start
        mid_dt = req.end - cut
        end_dt = req.end
        if mid_dt <= start_dt:
            start_dt = None
            mid_dt = start_dt
        elif mid_dt >= end_dt:
            end_dt = None
            mid_dt = end_dt

    elif isinstance(cut, datetime):
        start_dt = req.start
        mid_dt = cut
        end_dt = req.end
        assert start_dt <= mid_dt <= end_dt, 'mid_dt 必须在start_dt和end_dt之间'
        if mid_dt == start_dt:
            start_dt = None
        elif mid_dt == end_dt:
            end_dt = None

    else:
        raise AssertionError('不支持 cut 类型', str(cut))

    out = []
    if start_dt is not None:
        o = database.query_history_uni(
            HistoryRequest(
                req.symbol, req.exchange, start_dt, mid_dt, req.interval, req.dividend
            )
        )
        out.extend(o)

    if end_dt is not None:
        # 需要提供一下时间的错位，避免完全重叠
        mid_dt = mid_dt if start_dt is None else mid_dt + timedelta.resolution

        o = datafeed.query_history_uni(
            HistoryRequest(
                req.symbol, req.exchange, mid_dt, end_dt, req.interval, req.dividend
            )
        )
        out.extend(o)

    return out
