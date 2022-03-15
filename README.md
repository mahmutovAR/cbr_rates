# CBR_rates

This script receives exchange rates of USD and EUR from "https://www.cbr.ru/currency_base/daily/"
and enters information about rates into the appropriate tables in the database.

Exchange rate data can be obtained using a telegram bot.


## To run script:
`\..\cbr_usd_eur.py "mode" "query_period(optional)"`

### "mode" = schedule, period, schedule_bot, telegrambot
* schedule = receiving exchange rates according to the schedule, every day at 12:00, and
		entering the data into a table in the database
* period = receiving exchange rates for the given month {MM.YYYY} and
          	entering the data into a table in the database
* schedule_bot = running of "schedule" mode and telegram bot launcher
* telegrambot = running of telegram bot launcher


## Script runs on Python 3.8, with next modules:
* `calendar`, `datetime`, `os`, `re`, `sys`, `time` (standard library)
* `argparse`, `bs4`, `urllib`, `schedule`, `sqlalchemy`, `pytelegrambotapi`  (3rd party libraries)
