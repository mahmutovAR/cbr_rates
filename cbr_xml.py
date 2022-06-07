from bs4 import BeautifulSoup
from pathlib import Path
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from urllib.request import urlopen
import argparse
import datetime
import os
import schedule
import sys
import telebot
import time


request_url = 'https://www.cbr.ru/scripts/XML_daily'
script_dir = Path(__file__).resolve().parent  # path to the folder with 'cbr_xml.py'
path_to_database = os.path.join(script_dir, 'rates_db', 'cbr_ru.db')  # path to the database
db_engine = create_engine(f'sqlite:///{path_to_database}')
Session = sessionmaker(bind=db_engine)
session = Session()
Base = declarative_base()


class Currency(Base):
    """
    The table for each currency includes rate on a given by user date, date from the source,
    the difference and the dynamics of change is given relatively to the previous rate on the date given by the source.
    "scraping_site" is website for getting exchange rates, default 'www.cbr.ru'
    "scraping_datetime" is date&time of script running
    "request_date" is requesting date of exchange rates given by user
    "currency_rate" is rate at requesting date
    "currency_dynamics" displays that rate increased, decreased or has no change in order to its previous value
    """
    __abstract__ = True
    id = Column(Integer, primary_key=True, autoincrement=True)
    scraping_site = Column(String, default='www.cbr.ru')
    scraping_datetime = Column(String, default=datetime.datetime.now())
    request_date = Column(String, default=None)
    currency_rate = Column(Float, default=None)
    currency_dynamics = Column(String, default=None)


class USD(Currency):
    """
    The table with USD rates.
    """
    __tablename__ = 'USD rates'


class EUR(Currency):
    """
    The table with EUR rates.
    """
    __tablename__ = 'EUR rates'


def get_rates_and_add_to_db(request_date: str) -> 'database':
    """Adds exchange rates, difference and dynamics of changing to the appropriate tables in the database."""
    usd_rate = get_rate_xml(request_date, 'USD')
    add_data_to_db(USD, request_date, usd_rate)

    eur_rate = get_rate_xml(request_date, 'EUR')
    add_data_to_db(EUR, request_date, eur_rate)


def get_rate_xml(requesting_date: str, currency_name: str) -> float:
    """Scrapes URL for getting rate of the given currency and date of rating."""
    scraping_url = f'https://cbr.ru/scripts/XML_daily.asp?date_req={requesting_date}'
    try:
        xml_cbr = urlopen(scraping_url)
        bs_obj = BeautifulSoup(xml_cbr, 'lxml')
        currency_rate = (bs_obj.find('charcode',
                                     text={currency_name}).parent.find('value').get_text()).replace(',', '.')
        currency_rate = float(currency_rate)
    except Exception as err:
        sys.exit(f'Error! Scrapy failed:\n{err}')
    else:
        return float(f'{currency_rate:.2f}')


def add_data_to_db(currency_name: type, request_date: str, cur_rate: float) -> 'database':
    """Adds exchange rates, difference and dynamics of changing to the appropriate tables in the database."""
    try:
        script_run_datetime = datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S')
        cur_previous = get_previous_rate(currency_name, request_date)
        cur_dyn = edit_currency_dynamics(cur_rate, cur_previous)

        Base.metadata.create_all(db_engine)
        data_cur_rate = currency_name(scraping_datetime=script_run_datetime, request_date=request_date,
                                      currency_rate=cur_rate, currency_dynamics=cur_dyn)
        session.add(data_cur_rate)
        session.commit()
    except Exception as err:
        sys.exit(f'Error! Adding data to database failed:\n{err}')


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


def edit_currency_dynamics(current_rate: float, previous_rate: float) -> str or None:
    """Returns detailed description of the given currency rate change:
    difference between rates and dynamics of the change."""
    if previous_rate is None:
        return None
    else:
        if current_rate > previous_rate:
            return f'+{(current_rate-previous_rate):.2f}'
        else:
            return f'{(current_rate-previous_rate):.2f}'


def scrapy_period(currency_name: type, cur_id: str, from_date: str, to_date: str) -> 'database':
    """Scrapes URL for getting rate of the given currency and period of rating.
    Adds exchange rates, difference and dynamics of changing
    to the appropriate tables in the database for inputted period."""
    # https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={DD/MM/YYYY}&date_req2={DD/MM/YYYY}&VAL_NM_RQ={currency_id}
    req_url = f'https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={from_date}&date_req2={to_date}&VAL_NM_RQ={cur_id}'
    try:
        xml_cbr = urlopen(req_url)
        bs_obj = BeautifulSoup(xml_cbr, 'lxml')
        full_data = bs_obj.find_all('record')

        for rate_info in full_data:
            currency_date = rate_info.get('date')
            currency_rate = float(f"{float(rate_info.find('value').get_text().replace(',', '.')):.2f}")
            add_data_to_db(currency_name, currency_date, currency_rate)

    except Exception as err:
        sys.exit(f'Error! Scrapy failed:\n{err}')


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
        last_rate_dyn = last_data.currency_dynamics  # last given currency_dynamics in the table
    except Exception as err:
        sys.exit(f'Error in getting data for telegram bot:\n{err}')
    finally:
        return last_rate, last_rate_date, last_rate_dyn


def get_rate_on_date(currency_name: type, input_date: str) -> float:
    """Returns from the appropriate table rate of given currency on inputted date."""
    rate_on_date = None
    try:
        query_data = session.query(currency_name).filter(currency_name.request_date == input_date).order_by(
            currency_name.id.desc()).first()
        rate_on_date = query_data.currency_rate
    except Exception as err:
        sys.exit(f'Error in getting last rate:\n{err}')
    finally:
        return rate_on_date


def process_mode_telegrambot():
    """Starts telegram bot. Help, display rates and test function are realised."""
    base_dir = Path(__file__).resolve().parent.parent
    telegram_settings = os.path.join(base_dir, 'telegram_settings', 'name_token.txt')
    with open(telegram_settings) as t_bot:
        bot_name, bot_token = t_bot
    bot_token = bot_token.rstrip()
    tlg_bot = telebot.TeleBot(bot_token)

    @tlg_bot.message_handler(commands=['start'])
    def start_command(message):
        tlg_bot.send_message(
            message.chat.id,
            'Greetings!\nPress "/rates" to get last exchange rates from database\n'
            ' or input "/rates_on DD/MM/YYYY" to get exchange rates on given date.\n' +
            'Press "/help" to get more information.'
        )

    @tlg_bot.message_handler(commands=['help'])
    def help_command(message):
        """Help mode."""
        tlg_bot.send_message(message.chat.id, '1) To get last rates input /rates.\n'
                                              '2) To get rates for a specific date input /rates_on DD.MM.YYYY\n'
                                              'for example, /rates_on 01.02.2022')

    @tlg_bot.message_handler(commands=['rates'])
    def rates_command(message):
        """Displays last inputted to the database information about exchange rates of USD and EUR"""
        usd_rate, usd_date, usd_dyn = last_rate_for_tlg(USD)
        eur_rate, eur_date, eur_dyn = last_rate_for_tlg(EUR)
        if usd_rate is None or usd_date is None:
            tlg_bot.send_message(message.chat.id, f'Error! There is no data for USD rates')
        else:
            tlg_bot.send_message(message.chat.id, f'USD on {usd_date} is {usd_rate} ({usd_dyn})')
        if eur_rate is None or eur_date is None:
            tlg_bot.send_message(message.chat.id, f'Error! There is no data for EUR rates')
        else:
            tlg_bot.send_message(message.chat.id, f'EUR on {eur_date} is {eur_rate} ({eur_dyn})')

    def get_user_date(input_date):
        """
        Returns date from inputted command '/rates_on DD.MM.YYYY'
        """
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
            scrapy_period
        For 'telegrambot' is executed:
            process_mode_telegrambot

        Extra functions are also used:
            add_data_to_db
            edit_currency_dynamics
            get_previous_rate
            get_rate_on_date
            get_rate_xml
            last_rate_for_tlg
            process_mode_telegrambot
            scrapy_period
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
                                               from "DD/MM/YYYY" to "DD/MM/YYYY"
                                               and enters the data into a table in the database
              telegrambot = runs telegram bot launcher
              ''')
    parser.add_argument('mode', type=str, help='Choose the mode',
                        choices=['schedule', 'period', 'telegram'])
    parser.add_argument('query_period', type=str,
                        help='Input the period in format "DD/MM/YYYY-DD/MM/YYYY"', nargs='?', default=None)
    input_args = parser.parse_args()
    query_range = str(input_args.query_period)
    mode = input_args.mode

    if mode == 'schedule':
        process_mode_schedule()
    elif mode == 'period':
        process_mode_period(query_range)
    else:  # elif mode == 'telegram':
        process_mode_telegrambot()


# mode function definitions:
def process_mode_schedule():
    schedule.every().day.at("12:00").do(get_rates_and_add_to_db)
    while True:
        schedule.run_pending()
        time.sleep(1)


def process_mode_period(query_range):
    start_parsing, end_parsing = query_range.split('-')
    scrapy_period(USD, 'R01235', start_parsing, end_parsing)
    scrapy_period(EUR, 'R01239', start_parsing, end_parsing)


if __name__ == '__main__':
    main()
