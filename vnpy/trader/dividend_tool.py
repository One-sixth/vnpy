'''
复权计算工具
目前只支持等比复权法，暂未支持等差复权法
'''
import numba
import numpy as np
from typing import Literal


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
        dr_inv = 1 / dr
        front_dr = np.cumprod(dr_inv[::-1])[::-1]

    return front_dr, back_dr


dr_type_map = {
    'front': 1,
    'back': 2,
}


def _get_apply_dr_list_ori(
        out_dr_list: np.ndarray[np.float32|np.float64],
        bar_time: np.ndarray[np.float32 | np.float64 | np.int64 | np.uint64],
        dr_time: np.ndarray[np.float32 | np.float64 | np.int64 | np.uint64],
        dr_ratio: np.ndarray[np.float32 | np.float64],
        dr_type: int,
):
    '''
    获取K线的每个数据点的复权值
    :param out_dr_list:     对应K线数据的除权列表
    :param bar_time:        K线或tick数据的时间戳
    :param dr_time:         复权因子对应的时间
    :param dr_ratio:        复权因子列表，可以时前复权因子或后复权因子
    :param dr_type:         复权类型，1是前复权，2是后复权
    :return:
    '''
    # 使用 numba 加速生成基于时间的除权列表，100w大约耗时 0.005秒
    # 未加速时 100w大约耗时 0.5秒
    for i in numba.prange(len(out_dr_list)):
        dr = 1.
        t = bar_time[i]
        if dr_type == 1:
            # front_ratio
            for ti, t2 in enumerate(dr_time[::-1]):
                if t < t2:
                    dr = dr_ratio[::-1][ti]
                else:
                    break
        elif dr_type == 2:
            # back_ratio
            for ti, t2 in enumerate(dr_time):
                if t >= t2:
                    dr = dr_ratio[ti]
                else:
                    break

        out_dr_list[i] = dr

# 使用 numba 加速
_get_apply_dr_list_nb = numba.njit(parallel=True, fastmath=True, cache=True)(_get_apply_dr_list_ori)


def make_timetags_apply_dr(
    bar_timestamp, dr_timestamp, dr_arr, dr_type: Literal['front', 'back'], mode: Literal['ratio', 'diff']= 'ratio'
):
    '''
    获得对应K线时间序列的复权因子
    timetags        是K线或tick的时间戳
    timestamp_dr    是除权日的时间戳
    dr_arr          是除权日对应的前复权或后复权因子
    dr_type         是前复权还是后复权，可选 front 和 back
    mode            是等比复权还是等差复权，可选 'ratio' 和 'diff'，注意diff暂未支持
    '''
    assert isinstance(bar_timestamp, np.ndarray)
    assert isinstance(dr_timestamp, np.ndarray)
    assert isinstance(dr_arr, np.ndarray)
    assert mode == 'ratio', '暂不支持等差复权方法'
    # 填充除权因子到时间序列上
    dr_list = np.empty([len(bar_timestamp)], np.float32)
    if len(dr_timestamp) > 0:
        # 必须要有复权数据才进行以下计算
        _get_apply_dr_list_nb(dr_list, bar_timestamp, dr_timestamp, dr_arr, dr_type_map[dr_type])
    else:
        # 无复权表时全部填1.
        dr_list.fill(1)

    return dr_list
