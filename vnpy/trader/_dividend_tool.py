'''
除权工具
'''
import numba
import numpy as np


def make_front_back_dr(dr: list[float] | np.ndarray[np.float32|np.float64]):
    # 从除权因子计算出前复权因子和后复权因子
    assert isinstance(dr, (list, np.ndarray))
    if isinstance(dr, list):
        dr = np.float64(dr)

    if len(dr) == 0:
        back_dr = front_dr = np.zeros([0], np.float64)
    else:
        # 计算后复权因子
        back_dr = np.cumprod(dr)
        # 计算前复权因子
        front_dr = back_dr / back_dr[-1]

    return front_dr, back_dr


def _get_back_dr_list_ori(
        out_dr_list: np.ndarray[np.float32|np.float64],
        timetags: np.ndarray[np.float32|np.float64|np.int64|np.uint64],
        dr_timestamp: np.ndarray[np.float32|np.float64|np.int64|np.uint64],
        dr_back_dr: np.ndarray[np.float32|np.float64]
):
    '''
    :param out_dr_list:     对应K线数据的除权列表
    :param timetags:        K线或tick数据的时间戳
    :param dr_timestamp:    复权因子对应的时间
    :param dr_back_dr:      后复权因子列表
    :return:
    '''
    # 使用 numba 加速生成基于时间的除权列表，100w大约耗时 0.005秒
    # 未加速时 100w大约耗时 0.5秒
    for i in numba.prange(len(out_dr_list)):
        dr = 1.
        t = timetags[i]
        for ti, t2 in enumerate(dr_timestamp):
            if t >= t2:
                dr = dr_back_dr[ti]
            else:
                break

        out_dr_list[i] = dr

# 使用 numba 加速
_get_back_dr_list_nb = numba.njit(parallel=True)(_get_back_dr_list_ori)


def make_timetags_back_dr(timetags, timestamp_dr, back_dr):
    '''
    获得对应K线时间序列的后复权因子
    timetags        是K线或tick的时间戳
    timestamp_dr    是除权日的时间戳
    back_dr         是除权日对应的后复权因子
    '''
    assert isinstance(timetags, np.ndarray)
    assert isinstance(timestamp_dr, np.ndarray)
    assert isinstance(back_dr, np.ndarray)
    # 填充除权因子到时间序列上
    dr_list = np.empty([len(timetags)], np.float32)
    if len(timestamp_dr) > 0:
        # 必须要有复权数据才进行以下计算
        _get_back_dr_list_nb(dr_list, timetags, timestamp_dr, back_dr)
    else:
        # 无复权表时全部填1.
        dr_list.fill(1)

    return dr_list
