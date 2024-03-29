import time
import subprocess
import win32com.client as win32
import json
import os
import logging
from datetime import date, timedelta
from sapgui_flow.secrets.get_token import get_secret
from sapgui_flow.resources.config import SAP_SECRET
from retry import retry

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("my_logger")

@retry(tries=6, delay=61, backoff=2)
def open_sap():
        #########REPLACE WITH YOUR ACTUAL saplogon.exe FILEPATH
        path = r"C:\Users\Administrator\Downloads\BD_NW_7.0_Presentation_7.70_Comp._1_-20231107T174331Z-001\SAPgui\saplogon.exe"
        subprocess.Popen(path)
        logger.info('Opened SAPGUI')
        time.sleep(3)

        SapGuiAuto  = win32.GetObject("SAPGUI")
        application = SapGuiAuto.GetScriptingEngine
        #REPLACE WITH YOUR COMPANY ENV IN SAPGUI
        connection = application.OpenConnection('YOUR COMPANY ENV', True)
        time.sleep(3)
        session = connection.Children(0)

        try:
            #client
            session.findById("wnd[0]/usr/txtRSYST-MANDT").text = '100'
            # #user
            session.findById("wnd[0]/usr/txtRSYST-BNAME").text = json.loads(get_secret(SAP_SECRET))['user']
            # #password
            session.findById("wnd[0]/usr/pwdRSYST-BCODE").text = json.loads(get_secret(SAP_SECRET))['password']
            #ENTER
            session.findById("wnd[0]").sendVKey(0)
            logger.info('Logged-in SAPGUI')
        except Exception as e:
            logger.info(e)
            logger.info("Maybe VPN is disabled, or SAP GUI Scripting was disabled for some reason!")

        session.findById("wnd[0]").maximize
        session.findById("wnd[0]/tbar[0]/okcd").text = "SE16N"
        logger.info('Navigated to SE16N')
        session.findById("wnd[0]").sendVKey(0)

        # time.sleep(3)

        return session

def export_data(tablename:str, open_session):

    current_path = os.getcwd()

    session = open_session
    logger.info(f'{tablename} - Started export')
    session.findById("wnd[0]/usr/ctxtGD-TAB").text = tablename
    session.findById("wnd[0]/usr/txtGD-MAX_LINES").text = ""
    session.findById("wnd[0]/usr/ctxtGD-TAB").caretPosition = 4
    session.findById("wnd[0]/tbar[1]/btn[8]").press()
    session.findById("wnd[0]").maximize()
    session.findById("wnd[0]/usr/cntlRESULT_LIST/shellcont/shell").pressToolbarContextButton("&MB_EXPORT")
    session.findById("wnd[0]/usr/cntlRESULT_LIST/shellcont/shell").selectContextMenuItem("&PC")
    session.findById("wnd[1]/tbar[0]/btn[0]").press()
    session.findById("wnd[1]/usr/ctxtDY_PATH").text = current_path
    session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = "data.txt"
    session.findById("wnd[1]/usr/ctxtDY_FILENAME").caretPosition = 8
    session.findById("wnd[1]/tbar[0]/btn[0]").press()
    session.findById("wnd[0]/tbar[0]/btn[3]").press()
    logger.info(f'{tablename} - Finished export')
    data = "data.txt"

    return data
# def export_data_BSEG(tablename:str, columnfilter:str, open_session, days_range, start_date, end_date):
def export_data_BSEG(tablename:str, columnfilter:str, open_session, start_date, end_date):

    current_path = os.getcwd()

    session = open_session
    logger.info(f'{tablename} - Started export')
    session.findById("wnd[0]/usr/ctxtGD-TAB").text = tablename

    session.findById("wnd[0]").maximize
    session.findById("wnd[0]/usr/ctxtGD-TAB").text = "BSEG"
    session.findById("wnd[0]/usr/txtGD-MAX_LINES").setFocus()
    session.findById("wnd[0]/usr/txtGD-MAX_LINES").caretPosition = 0
    session.findById("wnd[0]/tbar[0]/btn[0]").press()
    session.findById("wnd[0]/usr/txtGD-MAX_LINES").text = ""
    session.findById("wnd[0]/usr/ctxtGD_ADD_COLUMN").text = columnfilter #"H_BUDAT"
    session.findById("wnd[0]/usr/ctxtGD_ADD_COLUMN").setFocus()
    session.findById("wnd[0]/usr/ctxtGD_ADD_COLUMN").caretPosition = 7
    session.findById("wnd[0]/tbar[0]/btn[0]").press()
    session.findById("wnd[0]/usr/tblSAPLSE16NSELFIELDS_TC/ctxtGS_SELFIELDS-LOW[2,1]").text = start_date
    session.findById("wnd[0]/usr/tblSAPLSE16NSELFIELDS_TC/ctxtGS_SELFIELDS-HIGH[3,1]").text = end_date
    session.findById("wnd[0]/usr/tblSAPLSE16NSELFIELDS_TC/ctxtGS_SELFIELDS-HIGH[3,1]").setFocus()
    session.findById("wnd[0]/usr/tblSAPLSE16NSELFIELDS_TC/ctxtGS_SELFIELDS-HIGH[3,1]").caretPosition = 10
    session.findById("wnd[0]/tbar[1]/btn[8]").press()
    session.findById("wnd[0]/usr/cntlRESULT_LIST/shellcont/shell").pressToolbarContextButton("&MB_EXPORT")
    session.findById("wnd[0]/usr/cntlRESULT_LIST/shellcont/shell").selectContextMenuItem("&PC")
    session.findById("wnd[1]/tbar[0]/btn[0]").press()
    session.findById("wnd[1]/usr/ctxtDY_PATH").text = current_path
    session.findById("wnd[1]/usr/ctxtDY_FILENAME").text = "data.txt"
    session.findById("wnd[1]/usr/ctxtDY_FILENAME").caretPosition = 8
    session.findById("wnd[1]/tbar[0]/btn[0]").press()
    session.findById("wnd[0]/tbar[0]/btn[3]").press()
    logger.info(f'{tablename} - Finished export')
    data = "data.txt"

    return data

def ping_active(open_session):

    current_path = os.getcwd()
    session = open_session
   