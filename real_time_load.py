import time
from datetime import datetime, date, timedelta
from dateutil.parser import parse

import card_txn
import gifting
import payment_merchants
import recharge
import billpay
import send_money
import update_keepalive
import add_fund

#date1 = str(date.today() - timedelta(days=1))+' 00:00:00'
#date2 = str(date.today() - timedelta(days=1))+' 23:59:59'
#date1 = '2016-02-01 00:00:00'
#date2 = '2016-03-09 23:59:59'
dt2 = parse(str(datetime.now()))
date2 = datetime.strftime(dt2, "%Y-%m-%d %H:%M:%S")

dt1 = dt2 - timedelta(minutes=15)
date1 = datetime.strftime(dt1, "%Y-%m-%d %H:%M:%S")

#card_txn.start_fetch(date1,date2)
#gifting.start_fetch(date1,date2)
#payment_merchants.start_fetch(date1,date2)
#recharge.start_fetch(date1,date2)
#billpay.start_fetch(date1,date2)
send_money.start_fetch(date1,date2)
#add_fund.start_fetch(date1,date2)

fp = open('log/cron.log', 'a');
fp.write(time.strftime("%Y-%m-%d %H:%M:%S")+', mongo_load \n')
fp.close()
