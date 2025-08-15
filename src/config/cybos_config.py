import win32com.client
from win32com.client.gencache import EnsureDispatch

obj_cp_code_mgr = None
obj_market_eye = None
obj_stock_chart = None
obj_cp_index = None
obj_cp_series = None


def get_obj_cp_code_mgr():
    global obj_cp_code_mgr
    if obj_cp_code_mgr is None:
        obj_cp_code_mgr = win32com.client.Dispatch("CpUtil.CpCodeMgr")
    return obj_cp_code_mgr


def get_obj_market_eye():
    global obj_market_eye
    if obj_market_eye is None:
        obj_market_eye = win32com.client.Dispatch("CpSysDib.MarketEye")
    return obj_market_eye


def get_obj_stock_chart():
    global obj_stock_chart
    if obj_stock_chart is None:
        obj_stock_chart = win32com.client.Dispatch("CpSysDib.StockChart")
    return obj_stock_chart


def get_obj_cp_index():
    global obj_cp_index
    if obj_cp_index is None:
        obj_cp_index = EnsureDispatch("CpIndexes.CpIndex")
    return obj_cp_index

def create_obj_cp_index():
    # return EnsureDispatch("CpIndexes.CpIndex")
    return win32com.client.Dispatch("CpIndexes.CpIndex")

def create_obj_cp_series():
    # return EnsureDispatch("CpIndexes.CpSeries")
    return win32com.client.Dispatch("CpIndexes.CpSeries")

def get_obj_cp_series():
    global obj_cp_series
    if obj_cp_series is None:
        obj_cp_series = win32com.client.Dispatch("CpIndexes.CpSeries")
    return obj_cp_series