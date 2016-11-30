import time
from datetime import datetime, date, timedelta
from dateutil.parser import parse

import withdrawals

#date1 = str(date.today() - timedelta(days=30))+' 00:00:00'
#date2 = str(date.today() - timedelta(days=1))+' 23:59:59'
#date1 = '2016-02-01 00:00:00'
#date2 = '2016-03-09 23:59:59'

start_date = str(date.today() - timedelta(days=7))+' 00:00:00'

start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")

for num in range(0,14):
	date1 = str(start_date + timedelta(hours=num*12))
	date2 = str(start_date + timedelta(hours=(num*12)+12))
	
	dt1 = parse(str(date1))	
	date3 = str(dt1 - timedelta(days=10))
	date4 = str(dt1 + timedelta(days=10))
		
	withdrawals.start_fetch(date1,date2,date3,date4)
