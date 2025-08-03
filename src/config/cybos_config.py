import win32com.client

obj_cp_code_mgr = None
obj_market_eye = None


# objStockChart = win32com.client.Dispatch("CpSysDib.StockChart")


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
