import time
from datetime import datetime, date, timedelta
from dateutil.parser import parse

import consumer_info_persona
import consumer_master

#date1 = str(date.today() - timedelta(days=1))+' 00:00:00'
#date2 = str(date.today() - timedelta(days=1))+' 23:59:59'
#date1 = '2016-03-23 00:00:00'
#date2 = '2016-03-23 23:59:59'

dt2 = parse(str(datetime.now()))
dt2 = dt2 + timedelta(minutes=30)
date2 = datetime.strftime(dt2, "%Y-%m-%d %H:%M:%S")

dt1 = dt2 - timedelta(minutes=75)
date1 = datetime.strftime(dt1, "%Y-%m-%d %H:%M:%S")

consumer_info_persona.start_fetch(date1,date2)

fp = open('log/cron.log', 'a')
fp.write(time.strftime("%Y-%m-%d %H:%M:%S")+', consumer_load \n')
fp.close()
