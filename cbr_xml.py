import argparse
import calendar
import datetime
import os
import re
import schedule
import sys
import time
# for parsing part:
from bs4 import BeautifulSoup
from urllib.request import urlopen
# for database part:
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
# for telegram part:
import telebot


request_url = 'https://www.cbr.ru/scripts/XML_daily'
script_path = os.path.abspath(os.path.dirname(__file__))  # path to the cbr_xml.py
path_to_database = os.path.join(script_path, 'rates_db', 'usd_eur_xml.db')  # path to the database
db_engine = create_engine(f'sqlite:///{path_to_database}')
Session = sessionmaker(bind=db_engine)
session = Session()
Base = declarative_base()


class MainInfo(Base):
    """
    The table with main info about rates: source of information, currencies name, datetime of script running.
    "scraping_site" is website for getting exchange rates, default 'www.cbr.ru/scripts/XML_daily'
    "scraping_datetime" is date&time of script running
    "out_usd" for class USD
    "out_eur" for class EUR
    """
    __tablename__ = 'Parsing XML'
    id = Column(Integer, primary_key=True, autoincrement=True)
    scraping_site = Column(String, default=request_url)
    request_date = Column(String, default=None)
    scraping_datetime = Column(String, default=datetime.datetime.now())
    currency_1 = Column(String, default='USD')
    currency_2 = Column(String, default='EUR')
    out_usd = relationship('USD', backref='main_table_data')
    out_eur = relationship('EUR', backref='main_table_data')


class Currency(Base):
    """
    The table for each currency includes rate on a given by user date, date from the source,
    the difference and the dynamics of change is given relatively to the previous rate on the date given by the source.
    "request_date" is requesting date of exchange rates given by user
    "currency_rate" is rate at requesting date
    "currency_dynamics" displays that rate increased, decreased or has no change in order to its previous value
    """
    __abstract__ = True
    id = Column(Integer, primary_key=True)
    currency_rate = Column(Float, default=None)
    currency_dynamics = Column(String, default=None)


class USD(Currency):
    __tablename__ = 'USD rates'
    request_date = Column(String, ForeignKey('Parsing XML.request_date'))


class EUR(Currency):
    __tablename__ = 'EUR rates'
    request_date = Column(String, ForeignKey('Parsing XML.request_date'))


def get_rates_and_add_to_db(request_date: str) -> 'sqlite':
    """Adds exchange rates, difference and dynamics of changing to the appropriate tables in the database."""
    usd_rate = get_rate_xml(request_date, 'USD')
    eur_rate = get_rate_xml(request_date, 'EUR')
    add_data_to_db(request_date, usd_rate, eur_rate)


def add_data_to_db(request_date: str, usd_rate: float, eur_rate: float) -> 'sqlite':
    """Adds exchange rates, difference and dynamics of changing to the appropriate tables in the database."""
    py_run_datetime = datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S')
    usd_prev = get_previous_rate(USD, request_date)
    eur_prev = get_previous_rate(EUR, request_date)

    Base.metadata.create_all(db_engine)
    usd_dyn = edit_currency_dynamics(usd_rate, usd_prev)
    eur_dyn = edit_currency_dynamics(eur_rate, eur_prev)
    data_inf = MainInfo(scraping_datetime=py_run_datetime, request_date=request_date)
    data_usd = USD(main_table_data=data_inf, currency_rate=usd_rate, currency_dynamics=usd_dyn)
    data_eur = EUR(main_table_data=data_inf, currency_rate=eur_rate, currency_dynamics=eur_dyn)
    session.add_all([data_inf, data_usd, data_eur])
    session.commit()


def get_previous_rate(currency_name: type, req_date: str) -> float:
    """Returns from the appropriate table previous rate of given currency for the last date of rating from the source,
     without taking into account the requested date of the rate."""
    prev_rate = None
    try:
        prev_rate_info = session.query(currency_name).filter(currency_name.request_date != req_date).order_by(
            currency_name.id.desc()).first()  # getting last rate with not {req_date} date
        prev_rate = prev_rate_info.currency_rate
    except Exception as err:
        sys.exit(f'Error in getting previous rate:\n{err}')
    finally:
        return prev_rate


def get_rate_on_date(currency_name: type, input_date: str) -> float:
    """Returns from the appropriate table last inputted rate of given currency and date of rating from the source."""
    rate_on_date = None
    try:
        query_data = session.query(currency_name).filter(currency_name.request_date == input_date).order_by(
            currency_name.id.desc()).first()
        rate_on_date = query_data.currency_rate
    except Exception as err:
        sys.exit(f'Error in get_rate_on_date:\n{err}')
    finally:
        return rate_on_date


def get_rate_xml(requesting_date: str, currency_name: str) -> float:
    """Scrapes URL for getting rate of the given currency and date of rating."""
    scraping_url = f'https://cbr.ru/scripts/XML_daily.asp?date_req={requesting_date}'
    try:
        xml_cbr = urlopen(scraping_url)
        bs_obj = BeautifulSoup(xml_cbr, 'lxml')
        currency_rate = (bs_obj.find('charcode',
                                     text={currency_name}).parent.find('value').get_text()).replace(',', '.')
        return float(format(float(currency_rate), '.2f'))
    except Exception as err:
        sys.exit(f'Scrapy failed:\n{err}')


def edit_currency_dynamics(current_rate: float, previous_rate: float = 0) -> str or None:
    """Returns detailed description of the given currency rate change:
    difference between rates and dynamics of the change."""
    if previous_rate is None:
        return None
    else:
        if current_rate > previous_rate:
            return f"+{format((current_rate-previous_rate), '.2f')}"
        elif current_rate < previous_rate:
            return f"{format((current_rate-previous_rate), '.2f')}"
        else:
            return '0'


def scrapy_period(from_date: str, to_date: str) -> 'sqlite':
    """Scrapes URL for getting rate of the given currency and period of rating.
    Adds exchange rates, difference and dynamics of changing
    to the appropriate tables in the database for inputted period."""
    # https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={DD/MM/YYYY}&date_req2={DD/MM/YYYY}&VAL_NM_RQ={currency_id}
    usd_url = f'https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={from_date}&date_req2={to_date}&VAL_NM_RQ=R01235'
    eur_url = f'https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={from_date}&date_req2={to_date}&VAL_NM_RQ=R01239'
    try:
        usd_xml = urlopen(usd_url)
        eur_xml = urlopen(eur_url)
        usd_bs_obj = BeautifulSoup(usd_xml, 'lxml')
        eur_bs_obj = BeautifulSoup(eur_xml, 'lxml')
        usd_full_data = usd_bs_obj.find_all('record')
        eur_full_data = eur_bs_obj.find_all('record')
        rate_dates = list()
        usd_rates = list()
        eur_rates = list()
        for usd_info in usd_full_data:
            rate_dates.append(usd_info.get('date'))
            usd_rates.append(usd_info.find('value').get_text().replace(',', '.'))
        for eur_info in eur_full_data:
            eur_rates.append(eur_info.find('value').get_text().replace(',', '.'))
        for cnt in range(len(rate_dates)):
            date_cbr = rate_dates[cnt]
            usd_cbr = float(format(float(usd_rates[cnt]), '.2f'))
            eur_cbr = float(format(float(eur_rates[cnt]), '.2f'))
            add_data_to_db(date_cbr, usd_cbr, eur_cbr)

    except Exception as err:
        sys.exit(f'Scrapy failed:\n{err}')


def last_rate_for_tlg(currency_name: type) -> float and str and str:
    """Returns from the appropriate table last inputted data:
    rate, date of rating, difference from the previous value, dynamics of rate changing."""
    last_rate = None
    last_rate_date = None
    last_rate_dyn = None
    try:
        last_row = session.query(currency_name).count()  # number of the last row in table
        last_data = session.query(currency_name).get(last_row)  # full info from the last row in table
        last_rate = last_data.currency_rate  # last given currency_rate in the table
        last_rate_date = last_data.request_date  # last given request_date in the table
        last_rate_dyn = last_data.currency_dynamics
    except Exception as err:
        sys.exit(f'Error in getting data for telegram bot:\n{err}')
    finally:
        return last_rate, last_rate_date, last_rate_dyn


def telegram_bot():
    """Starts telegram bot. Help, display rates and test function are realised."""
    telegram_settings = os.path.join('/'.join(script_path.split('/')[:-1]), 'telegram_settings', 'name_token.txt')
    # gets previous folder for 'cbr_usd_eur.py' and opens file 'name_token.txt' in the folder 'telegram_settings'
    with open(telegram_settings) as t_bot:
        bot_name, bot_token = t_bot
    bot_token = bot_token.rstrip()
    tlg_bot = telebot.TeleBot(bot_token)

    @tlg_bot.message_handler(commands=['start'])
    def start_command(message):
        tlg_bot.send_message(
            message.chat.id,
            'Greetings!\nEnter /rates to get last exchange rates from database\n'
            ' or /rates_on DD/MM/YYYY to get rates on given date.\n' +
            'To get help enter /help.'
        )

    @tlg_bot.message_handler(commands=['help'])
    def help_command(message):
        """Help mode."""
        tlg_bot.send_message(message.chat.id, '1) To get last rates input /rates.\n'
                                              '2) To get rates for a specific date input /rates_on DD/MM/YYYY\n'
                                              'for example, /rates_on 01/02/2022')

    @tlg_bot.message_handler(commands=['rates'])
    def rates_command(message):
        """Displays last inputted to the database information about exchange rates of USD and EUR"""
        usd_rate, usd_date, usd_dyn = last_rate_for_tlg(USD)
        eur_rate, eur_date, eur_dyn = last_rate_for_tlg(EUR)
        if usd_rate is None and usd_date is None:
            tlg_bot.send_message(message.chat.id, f'Error! There is no data for USD rates')
        else:
            tlg_bot.send_message(message.chat.id, f'USD on {usd_date} is {usd_rate} ({usd_dyn})')
        if eur_rate is None and eur_date is None:
            tlg_bot.send_message(message.chat.id, f'Error! There is no data for EUR rates')
        else:
            tlg_bot.send_message(message.chat.id, f'EUR on {eur_date} is {eur_rate} ({eur_dyn})')

    def get_user_date(input_date):
        return input_date.split()[-1]

    @tlg_bot.message_handler(commands=['rates_on'])
    def rates_on_date_command(message):
        """Displays exchange rates of USD and EUR on inputted date"""
        user_date = str(get_user_date(message.text))
        usd_rate_on_date = get_rate_on_date(USD, user_date)
        eur_rate_on_date = get_rate_on_date(EUR, user_date)
        if usd_rate_on_date is None:
            tlg_bot.send_message(message.chat.id, f'Error! There is no USD rates on {user_date}')
        else:
            tlg_bot.send_message(message.chat.id, f'USD on {user_date} was {usd_rate_on_date}')
        if eur_rate_on_date is None:
            tlg_bot.send_message(message.chat.id, f'Error! There is no EUR rates on {user_date}')
        else:
            tlg_bot.send_message(message.chat.id, f'EUR on {user_date} was {eur_rate_on_date}')

    # tlg_bot.infinity_polling()
    tlg_bot.polling(none_stop=True)


def main():
    """This is the main function of the script.
        Firstly, are given few constant arguments:
            full path to the "cbr_xml.py"
            full path to the database in nested folder
            database functions
            class definitions
        Secondly, main arguments are defined from the command line by using argparse module:
            mode ('schedule', 'period', 'schedule_bot', 'telegrambot')
            request period (optional)
        For 'schedule' is executed:
            get_rates_and_add_to_db
        For 'period' is executed:
            scrapy_month
        For 'schedule_bot' are executed next functions:
            get_rates_and_add_to_db
            telegram_bot
        For 'telegrambot' is executed:
            telegram_bot

        * some functions are same for a few modes,so "if-else" structure was used to prevent code repetition:

        if 'schedule' or 'schedule_bot':
            if 'schedule_bot':
                telegram_bot()
            get_rates_and_add_to_db()
        elif 'period':
            scrapy_period()
        else:  # 'telegrambot'
            telegram_bot()

        Extra functions are also used:
            add_data_to_db()
            get_previous_rate
            get_last_rate
            get_rate_xml
            edit_currency_dynamics
            get_info_for_tlg_bot
            check_date
            get_date_for_scrapy
            """
    parser = argparse.ArgumentParser(prog='ScrapyCBR',
                                     usage='scrapy_cbr.py [-h] [mode, query_period(optional)]',
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description='''
            %(prog)s requests exchanged rates of USD and EUR from www.cbr.ru.
            Reference information about script:
              schedule = gets exchange rates according to the schedule, every day at 12:00, and
              entering the data into a table in the database
              period "DD/MM/YYYY-DD/MM/YYYY" = gets exchange rates for the given period
              from "DD/MM/YYYY" to "DD/MM/YYYY" and enters the data into a table in the database
              schedule_bot = runs "schedule" mode and telegram bot launcher
              telegrambot = runs telegram bot launcher only
              ''')
    parser.add_argument('mode', type=str, help='Choose the mode',
                        choices=['schedule', 'period', 'schedule_bot', 'telegrambot'])
    parser.add_argument('query_period', type=str,
                        help='Input the period in format "DD/MM/YYYY-DD/MM/YYYY"', nargs='?', default=None)
    input_args = parser.parse_args()
    query_range = str(input_args.query_period)
    mode = input_args.mode

    if mode in ['schedule', 'schedule_bot']:
        if mode == 'schedule_bot':
            telegram_bot()

        schedule.every().day.at("12:00").do(get_rates_and_add_to_db)
        while True:
            schedule.run_pending()
            time.sleep(1)

    elif mode == 'period':  # range of parsing
        start_parsing, end_parsing = query_range.split('-')
        scrapy_period(start_parsing, end_parsing)

    else:  # mode == 'telegrambot'
        telegram_bot()


if __name__ == '__main__':
    main()
