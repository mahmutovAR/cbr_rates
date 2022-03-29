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
from telebot import types


# https://www.cbr.ru/currency_base/daily/?UniDbQuery.Posted=True&UniDbQuery.To={date in format:DD.MM.YYYY}
request_url = f'https://www.cbr.ru/currency_base/daily/?UniDbQuery.Posted=True&UniDbQuery.To='
# URL with exchange rates, without the query date
script_path = os.path.abspath(os.path.dirname(__file__))  # path to the cbr_usd_eur.py
path_to_database = os.path.join(script_path, 'rates_db', 'usd_eur.db')
# path to the database, placed in the nested folder
db_engine = create_engine(f'sqlite:///{path_to_database}')
Session = sessionmaker(bind=db_engine)
session = Session()
Base = declarative_base()


class MainInfo(Base):
    """
    The table with main info about rates: source of information, currencies name, datetime of script running.
    "scraping_site" is website for getting exchange rates, default 'www.cbr.ru'
    "scraping_datetime" is date&time of script running
    "out_usd" for class USD
    "out_eur" for class EUR
    """
    __tablename__ = 'Scraping Info'
    id = Column(Integer, primary_key=True, autoincrement=True)
    scraping_site = Column(String, default=request_url[:19])
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
    "date_rate_site" is date of exchange rates given by source (website)
    "currency_dynamics" displays that rate increased, decreased or has no change in order to its previous value
    """
    __abstract__ = True
    id = Column(Integer, primary_key=True)
    request_date = Column(String, default=None)
    currency_rate = Column(Float, default=None)
    date_rate_site = Column(String, default=None)
    currency_dynamics = Column(String, default=None)
    currency_difference = Column(Float, default=None)


class USD(Currency):
    __tablename__ = 'USD rates'
    usd_scraping_datetime = Column(String, ForeignKey('Scraping Info.scraping_datetime'))


class EUR(Currency):
    __tablename__ = 'EUR rates'
    eur_scraping_datetime = Column(String, ForeignKey('Scraping Info.scraping_datetime'))


def add_rates_to_db(request_date: str = datetime.datetime.now().strftime('%d.%m.%Y')) -> 'sqlite':
    """Adds exchange rates, difference and dynamics of changing to the appropriate tables in the database."""
    py_run_datetime = datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S')
    usd_rate, site_date = get_rate(request_date, 'USD')
    eur_rate, site_date = get_rate(request_date, 'EUR')

    usd_prev = get_previous_rate(USD, request_date)
    eur_prev = get_previous_rate(EUR, request_date)

    Base.metadata.create_all(db_engine)
    usd_dyn, usd_diff = edit_currency_dynamics(usd_rate, usd_prev)
    eur_dyn, eur_diff = edit_currency_dynamics(eur_rate, eur_prev)
    data_inf = MainInfo(scraping_datetime=py_run_datetime)
    data_usd = USD(main_table_data=data_inf, currency_rate=usd_rate, currency_dynamics=usd_dyn,
                   currency_difference=usd_diff, request_date=request_date, date_rate_site=site_date)
    data_eur = EUR(main_table_data=data_inf, currency_rate=eur_rate, currency_dynamics=eur_dyn,
                   currency_difference=eur_diff, request_date=request_date, date_rate_site=site_date)
    session.add_all([data_inf, data_usd, data_eur])
    session.commit()


def get_previous_rate(currency_name: type, req_date: str) -> float:
    """Returns from the appropriate table previous rate of given currency for the last date of rating from the source,
     without taking into account the requested date of the rate."""
    prev_rate = None
    try:
        prev_rate_info = session.query(currency_name).filter(currency_name.date_rate_site != req_date).order_by(
            currency_name.id.desc()).first()  # getting last rate with date != req_date
        prev_rate = prev_rate_info.currency_rate
    except Exception as err:
        sys.exit(f'Error in getting previous rate:\n{err}')
    finally:
        return prev_rate


def get_last_rate(currency_name: type) -> float and str:
    """Returns from the appropriate table last inputted rate of given currency and date of rating from the source."""
    last_rate = None
    last_rate_date = None
    try:
        last_row = session.query(currency_name).count()  # number of the last row in table
        last_data = session.query(currency_name).get(last_row)  # full info from the last row in table
        last_rate = last_data.currency_rate  # last given currency_rate in the table
        last_rate_date = last_data.date_rate_site  # last given date_rate_site in the table
    except Exception as err:
        sys.exit(f'Error in getting last rates:\n{err}')
    finally:
        return last_rate, last_rate_date


def get_rate(requesting_date: str, currency_name: str) -> float and str:  # stable
    """Scrapes URL for getting rate of the given currency and date of rating."""
    scraping_url = request_url + requesting_date
    try:
        html = urlopen(scraping_url)
        bs_obj = BeautifulSoup(html, 'lxml')
        currency_rate = (bs_obj.find('td',
                                     string={currency_name}).parent.find_all('td')[4].get_text()).replace(',', '.')
        currency_rate = float(format(float(currency_rate), '.2f'))
        date_of_rating_full_text = bs_obj.find('h2', class_="h3").get_text()
        regexp = re.compile(r'(?P<only_date>\d{2}.\d{2}.\d{4})')
        result = regexp.search(date_of_rating_full_text)
        site_date = result.group('only_date')
        return currency_rate, site_date
    except Exception as err:
        sys.exit(f'Scrapy failed:\n{err}')


def edit_currency_dynamics(current_rate: float, previous_rate: float = 0) -> str and float:
    """Returns detailed description of the given currency rate change:
    difference between rates and dynamics of the change."""
    if previous_rate is None:
        return None, None
    else:
        if current_rate > previous_rate:
            return 'increased', format((current_rate-previous_rate), '.2f')
        elif current_rate < previous_rate:
            return 'decreased', format((current_rate-previous_rate), '.2f')
        else:
            return 'no change', 0


def get_info_for_tlg_bot(currency_name: type) -> float and str and float and str:
    """Returns from the appropriate table last inputted data:
    rate, date of rating, difference from the previous value, dynamics of rate changing."""
    last_rate = None
    last_rate_date = None
    last_rate_diff = None
    last_rate_dyn = None
    try:
        last_row = session.query(currency_name).count()  # number of the last row in table
        last_data = session.query(currency_name).get(last_row)  # full info from the last row in table
        last_rate = last_data.currency_rate  # last given currency_rate in the table
        last_rate_date = last_data.date_rate_site  # last given date_rate_site in the table
        last_rate_diff = last_data.currency_difference
        last_rate_dyn = last_data.currency_dynamics
    except Exception as err:
        sys.exit(f'Error in getting data for telegram bot:\n{err}')
    finally:
        return last_rate, last_rate_date, last_rate_diff, last_rate_dyn


def telegram_bot():
    """Starts telegram bot. Help, display rates and test function are realised."""
    telegram_settings = os.path.join(('/').join(script_path.split('/')[:-1]), 'telegram_settings', 'name_token.txt')
    # gets previous folder for 'cbr_usd_eur.py' and opens file 'name_token.txt' in the folder 'telegram_settings'
    with open(telegram_settings) as t_bot:
        bot_name, bot_token = t_bot
    bot_name = bot_name.rstrip()
    bot_token = bot_token.rstrip()
    print(f'Telegram bot name is {bot_name}')
    tlg_bot = telebot.TeleBot(bot_token)

    @tlg_bot.message_handler(commands=['start'])
    def start_command(message):
        tlg_bot.send_message(
            message.chat.id,
            'Greetings!\nEnter /rates to get exchange rates or /F1 to run the function F1.\n' +
            'To get help press /help.'
        )

    @tlg_bot.message_handler(commands=['rates'])
    def rates_command(message):
        """Displays last inputted information about two currencies USD and EUR"""
        usd_rate, usd_date, usd_diff, usd_dyn = get_info_for_tlg_bot(USD)
        eur_rate, eur_date, eur_diff, eur_dyn = get_info_for_tlg_bot(EUR)
        tlg_bot.send_message(message.chat.id, f'USD on {usd_date} is {usd_rate},'
                                              f' {usd_dyn} by {abs(usd_diff)}\n'
                                              f'EUR on {eur_date} is {eur_rate},'
                                              f' {eur_dyn} by {abs(eur_diff)}')

    @tlg_bot.message_handler(commands=['F1'])
    def query_command(message):
        """Test function."""
        tlg_bot.send_message(message.chat.id, "The function F1 is running..")

    @tlg_bot.message_handler(commands=['help'])
    def help_command(message):
        """Help mode."""
        tlg_bot.send_message(message.chat.id, '1) To run function_1 press /F1.\n'
                                              '2) To run function_2 press /F2.\n...\n')

    tlg_bot.polling(none_stop=True)


def scrapy_month(month: int, year: int) -> 'sqlite':
    """Adds exchange rates, difference and dynamics of changing
    to the appropriate tables in the database for inputted month."""

    req_period = list()
    for full_date in calendar.Calendar().itermonthdays3(year, month):
        y, m, d = full_date
        if m == month:
            check_date(req_period, d, m, y)
    for req_date in req_period:
        add_rates_to_db(req_date)


def check_date(input_data: list, checking_day: int, checking_month: int, checking_year: int) -> None:
    """Checks if the specified period is ahead of the current date."""
    current_day = datetime.date.today().day
    current_month = datetime.date.today().month
    current_year = datetime.date.today().year
    if checking_year < current_year:
        input_data.append(get_date_for_scrapy(checking_day, checking_month, checking_year))
    elif checking_year == current_year:
        if checking_month < current_month:
            input_data.append(get_date_for_scrapy(checking_day, checking_month, checking_year))
        elif checking_month == current_month:
            if checking_day <= current_day:
                input_data.append(get_date_for_scrapy(checking_day, checking_month, checking_year))


def get_date_for_scrapy(day: int, month: int, year: int) -> str:
    """Returns date in the needed format for scraping {DD.MM.YYYY}."""
    if day < 10:
        day = '0' + str(day)
    if month < 10:
        month = '0' + str(month)
    return f'{day}.{month}.{year}'


def main():
    """This is the main function of the script.
    Firstly, are given few constant arguments:
        full path to the "cbr_usd_eur.py"
        full path to the database in nested folder
        database functions
        class definitions
    Secondly, main arguments are defined from the command line by using argparse module:
        mode ('schedule', 'period', 'schedule_bot', 'telegrambot')
        request period (optional)
    For 'schedule' is executed:
        add_rates_to_db
    For 'period' is executed:
        scrapy_month
    For 'schedule_bot' are executed next functions:
        add_rates_to_db
        telegram_bot
    For 'telegrambot' is executed:
        telegram_bot

    * some functions are same for a few modes,so "if-else" structure was used to prevent code repetition:

    if 'schedule' or 'schedule_bot':
        if 'schedule_bot':
            telegram_bot()
        add_rates_to_db()
    elif 'period':
        scrapy_month()
    else:  # 'telegrambot'
        telegram_bot()

    Extra functions are also used:
        get_previous_rate
        get_last_rate
        get_rate
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
          period "MM.YYYY" = gets exchange rates for the given month "MM.YYYY" and
          entering the data into a table in the database
          schedule_bot = runs "schedule" mode and telegram bot launcher
          telegrambot = runs telegram bot launcher only
          ''')
    parser.add_argument('mode', type=str, help='Choose the mode',
                        choices=['schedule', 'period', 'schedule_bot', 'telegrambot'])
    parser.add_argument('query_period', type=str,
                        help='Input the period in format "MM.YYYY"', nargs='?', default=None)
    input_args = parser.parse_args()
    query_month = str(input_args.query_period)
    mode = input_args.mode

    if mode in ['schedule', 'schedule_bot']:
        if mode == 'schedule_bot':
            telegram_bot()

        schedule.every().day.at("12:00").do(add_rates_to_db)
        while True:
            schedule.run_pending()
            time.sleep(1)

    elif mode == 'period':
        if len(query_month) == 7 and query_month[2] == '.':
            month = int(query_month[:2])
            year = int(query_month[-4:])
        else:
            sys.exit(f'Invalid format of the inputted period: {query_month}')
        scrapy_month(month, year)

    else:  # mode == 'telegrambot'
        telegram_bot()


if __name__ == '__main__':
    main()
