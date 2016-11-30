import time
from datetime import datetime, date, timedelta

import card_txn
import gifting
import payment_merchants
import recharge
import billpay
import send_money
import consumer_info_persona
import add_fund
import withdrawals

#date1 = str(date.today() - timedelta(days=30))+' 00:00:00'
#date2 = str(date.today() - timedelta(days=1))+' 23:59:59'
#date1 = '2016-02-01 00:00:00'
#date2 = '2016-03-09 23:59:59'

start_date = datetime.strptime("2016-01-29 00:00:00", "%Y-%m-%d %H:%M:%S")
 
for num in range(0,1700):
	date1 = str(start_date + timedelta(hours=num*3))
	date2 = str(start_date + timedelta(hours=(num*3)+3))
	# date1 = str(start_date + timedelta(days=num))
	# date2 = str(start_date + timedelta(days=num+1))
		
	#card_txn.start_fetch(date1,date2)
	#gifting.start_fetch(date1,date2)
	#payment_merchants.start_fetch(date1,date2)
	#recharge.start_fetch(date1,date2)
	billpay.start_fetch(date1,date2)
	#send_money.start_fetch(date1,date2)
	#add_fund.start_fetch(date1,date2)
	#withdrawals.start_fetch(date1,date2,date3,date4)
