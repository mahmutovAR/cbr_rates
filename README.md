# CBR_rates

This script gets exchange rates of USD and EUR from "https://www.cbr.ru" and
enters information about rates into the appropriate tables in the database.
Exchange rates can be requested in telegram via telegram bot.

* data is obtained from "https://cbr.ru/scripts/XML_daily.asp?date_req=..." or
"https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1=...&date_req2=...&VAL_NM_RQ=..."
depending on the request for a specific date or period

## To run script:
`\..\cbr_xml.py` `mode` `query_period(optional)`

### `mode` schedule, period, schedule_bot, telegrambot
* `schedule` gets exchange rates according to the schedule, every day at 12:00, and
        enters the data into a table in the database
* `period DD/MM/YYYY-DD/MM/YYYY` gets exchange rates for the given period
from "DATE_1" to "DATE_2" and enters the data into the tables in the database
* `schedule_bot` runs "schedule" mode and telegram bot launcher
* `telegrambot` runs telegram bot launcher only

## Script runs on Python 3.8 with next modules:
* `calendar`, `datetime`, `os`, `re`, `sys`, `time` (standard libraries)
* `argparse`, `beautifulsoup4`, `schedule`, `sqlalchemy`, `pytelegrambotapi`, `urllib3` (3rd party libraries)