import time
from datetime import datetime, date, timedelta
from dateutil.parser import parse

import withdrawals

#date1 = str(date.today() - timedelta(days=30))+' 00:00:00'
#date2 = str(date.today() - timedelta(days=1))+' 23:59:59'
#date1 = '2016-02-01 00:00:00'
#date2 = '2016-03-09 23:59:59'

date1 = datetime.today() - timedelta(hours=6)
date2 = datetime.today()

date1 = date1.strftime('%Y-%m-%d %H:%M:%S')
date2 = date2.strftime('%Y-%m-%d')
date2 = date2+' 23:59:59'

dt1 = parse(str(date1))	
date3 = str(dt1 - timedelta(days=10))
date4 = str(dt1 + timedelta(days=10))
	
withdrawals.start_fetch(date1,date2,date3,date4)
