# CBR_rates

this project presents two scripts for getting exchange rates of USD and EUR from "https://www.cbr.ru"
and entering information about rates into the appropriate tables in the database.

*`cbr_usd_eur.py` gets data from "https://www.cbr.ru/currency_base/daily/?UniDbQuery.Posted=True&UniDbQuery.To=..."

*`cbr_xml.py` gets data from "https://cbr.ru/scripts/XML_daily.asp?date_req=..." or
"https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1=...&date_req2=...&VAL_NM_RQ=..."

Exchange rates can be requested in telegram via telegram bot.


## To run script `cbr_usd_eur.py`:
`\..\cbr_usd_eur.py` `mode` `query_period(optional)`

### `mode` schedule, period, schedule_bot, telegrambot
* `schedule` gets exchange rates according to the schedule, every day at 12:00, and
		entering the data into a table in the database
* `period MM.YYYY` gets exchange rates for the given month "MM.YYYY" and
          	entering the data into a table in the database
* `schedule_bot` runs "schedule" mode and telegram bot launcher
* `telegrambot` runs telegram bot launcher only

## To run script `cbr_xml.py`:
`\..\cbr_xml.py` `mode` `query_period(optional)`

### `mode` schedule, period, schedule_bot, telegrambot
* `schedule` gets exchange rates according to the schedule, every day at 12:00, and
        enters the data into a table in the database
* `period DD/MM/YYYY-DD/MM/YYYY` gets exchange rates for the given period 
from "DD/MM/YYYY" to "DD/MM/YYYY" and enters the data into a table in the database
* `schedule_bot` runs "schedule" mode and telegram bot launcher
* `telegrambot` runs telegram bot launcher only

## Both scripts run on Python 3.8 with next modules:
* `calendar`, `datetime`, `os`, `re`, `sys`, `time` (standard library)
* `argparse`, `beautifulsoup4`, `schedule`, `sqlalchemy`, `pytelegrambotapi`, `urllib3` (3rd party libraries)