import base
import collections
from base import get_mongo_int,get_mongo_string,get_mongo_float,get_mongo_date

def mongo_write(value,add_cms_cron_collection=True):
	try:
		result = base.db.payment_merchants.insert({
				"primary_id"			: get_mongo_int(value['primary_id']),
				"transaction_mode"		: get_mongo_string(value['transaction_mode']),
				"shmart_refID"			: get_mongo_string(value['shmart_refID']),
				"status"				: get_mongo_string(value['status']),
				"email_transaction"		: get_mongo_string(value['email_transaction']),
				"mobile_transaction"	: get_mongo_string(value['mobile_transaction']),
				"total_amount"			: get_mongo_float(value['total_amount']),
				"trans_amount_pg"		: get_mongo_float(value['trans_amount_pg']),
				"trans_amount_wallet"	: get_mongo_float(value['trans_amount_wallet']),
				"app_used"				: get_mongo_string(value['app_used']),
				"app_id"				: get_mongo_string(value['app_id']),
				"transaction_id"		: get_mongo_string(value['transaction_id']),
				"merchant_id"			: get_mongo_int(value['merchant_id']),
				"consumer_id"			: get_mongo_int(value['consumer_id']),
				"consumer_type"			: get_mongo_string(value['consumer_type']),
				"channel"				: get_mongo_string(value['channel']),
				"created_date"			: get_mongo_date(value['created_date']),
				"wallet_trans_type"		: get_mongo_string(value['wallet_trans_type']),
				"general_wallet_ackNo"	: get_mongo_string(value['general_wallet_ackNo']),
				"voucher_wallet_ackNo"	: get_mongo_string(value['voucher_wallet_ackNo']),
				"meal_wallet_ackNo"		: get_mongo_string(value['meal_wallet_ackNo']),
				"medical_wallet_ackNo"	: get_mongo_string(value['medical_wallet_ackNo']),
				"gift_wallet_ackNo"		: get_mongo_string(value['gift_wallet_ackNo']),
				"recharge_wallet_ackNo"	: get_mongo_string(value['recharge_wallet_ackNo']),
				"wallet_trans_status"	: get_mongo_string(value['wallet_trans_status']),
				"is_saved_card_or_not"	: get_mongo_int(value['is_saved_card_or_not']),
				"status_pg"				: get_mongo_string(value['status_pg']),
				"card_type"				: get_mongo_string(value['card_type']),
				"card_provider"			: get_mongo_string(value['card_provider']),
				"name_on_card"			: get_mongo_string(value['name_on_card']),
				"email_pg"				: get_mongo_string(value['email_pg']),
				"mobile_pg"				: get_mongo_string(value['mobile_pg']),
				"pg_amount"				: get_mongo_float(value['pg_amount']),
				"processor"				: get_mongo_string(value['processor']),
				"merchant_name"			: get_mongo_string(value['merchant_name']),
				"merchant_email"		: get_mongo_string(value['merchant_email']),
				"merchant_activated"	: get_mongo_int(value['merchant_activated']),
				"merchant_banned"		: get_mongo_int(value['merchant_banned']),
				"merchant_created"		: get_mongo_date(value['merchant_created']),
				"mobile"				: get_mongo_string(value['mobile']),
				"name_of_customer"		: get_mongo_string(value['name_of_customer']),
				"consumer_email"		: get_mongo_string(value['consumer_email']),
				"registration_mode"		: get_mongo_string(value['registration_mode']),
				"login_type"			: get_mongo_string(value['login_type']),
				"consumer_created"		: get_mongo_date(value['consumer_created']),
				'channel_type'			: 'APP' if get_mongo_int(value['merchant_id']) == 171 else 'PG',
				'channel_value'			: '',
				'mvisa_merchant'		: get_mongo_string(value['mvisa_merchant']) if get_mongo_string(value['app_used']) == 'mvisa' else '',
				'mvisa_card_type'		: get_mongo_string(value['mvisa_card_type']) if get_mongo_string(value['app_used']) == 'mvisa' else '',
				'mvisa_mcc_code'		: get_mongo_string(value['mvisa_mcc_code']) if get_mongo_string(value['app_used']) == 'mvisa' else '',
				'mvisa_city'			: get_mongo_string(value['mvisa_city']) if get_mongo_string(value['app_used']) == 'mvisa' else '',
				'mvisa_mcc_category'	: get_mongo_string(value['mvisa_mcc_category']) if get_mongo_string(value['app_used']) == 'mvisa' else '',
				'mvisa_mcc_sub_category': get_mongo_string(value['mvisa_mcc_sub_category']) if get_mongo_string(value['app_used']) == 'mvisa' else ''
				}
			)
		if add_cms_cron_collection:
			base.write_cms_cron_collection(value['transaction_id'],value['created_date'],'payment_merchant')
	except Exception as e:
		base.log_it(str(e),'payment_merchants_error')
		return False

	return True

def start_fetch(date1,date2):	
	try:
		cursor = base.mysql.cursor()
		cursor.execute("SELECT \
a.id AS primary_id, \
a.transaction_mode, \
a.shmart_refID, \
a.status, \
a.email AS email_transaction, \
a.mobileNo AS mobile_transaction, \
a.total_amount, \
a.trans_amount_pg, \
a.trans_amount_wallet, \
a.app_used, \
a.app_id, \
a.merchant_refID AS transaction_id, \
a.ip_address, \
a.user_id AS merchant_id, \
a.consumer_id, \
CASE WHEN a.consumer_id >0 THEN 'registered' \
ELSE 'guest' END AS consumer_type, \
a.channel, \
a.trans_startTime AS created_date, \
b.wallet_trans_type, \
b.general_wallet_ackNo, \
b.voucher_wallet_ackNo, \
b.meal_wallet_ackNo, \
b.medical_wallet_ackNo, \
b.gift_wallet_ackNo, \
b.recharge_wallet_ackNo, \
b.wallet_trans_status, \
c.is_saved_card_or_not, \
c.status AS status_pg, \
c.cardType AS card_type, \
c.cardProvider AS card_provider, \
c.name_on_card, \
c.email as email_pg, \
c.mobileNo as mobile_pg, \
c.pg_amount, \
c.processor, \
d.username AS merchant_name, \
d.email AS merchant_email, \
d.activated AS merchant_activated, \
d.banned AS merchant_banned, \
d.created AS merchant_created, \
e.username AS mobile, \
e.name_of_customer, \
e.email AS consumer_email, \
e.registration_mode, \
e.login_type, \
e.created AS consumer_created, \
mv.merchant_name as mvisa_merchant, \
mv.card_type as mvisa_card_type, \
mv.mcc_code as mvisa_mcc_code, \
mm.category as mvisa_mcc_category, \
mm.sub_category as mvisa_mcc_sub_category, \
mv.city as mvisa_city \
FROM shmart_merchant.transactions a  \
LEFT JOIN shmart_merchant.transactions_wallet b ON a.shmart_refID=b.shmart_refID \
and b.trans_startTime BETWEEN %(date1)s AND %(date2)s \
LEFT JOIN shmart_merchant.transactions_pg c ON a.shmart_refID=c.shmart_refID \
and c.trans_startTime BETWEEN %(date1)s AND %(date2)s \
LEFT JOIN shmart_merchant.users d ON a.user_id=d.id \
LEFT JOIN shmart_consumer.users e ON a.consumer_id=e.id \
LEFT JOIN shmart_consumer.transactions_mvisa mv ON mv.shmart_refID=a.shmart_refID \
Left JOIN shmart.mcc_master mm on mm.mcc_code=mv.mcc_code \
WHERE a.trans_startTime BETWEEN %(date1)s AND %(date2)s",{'date1':date1,'date2':date2})
						
		result = cursor.fetchall()
		row_count = cursor.rowcount
		
	except Exception as e:
		base.log_it('Could not fetch data from mysql','payment_merchants')
		return False

	i = 0
	j = 0
	if row_count > 0:	
		for value in result:
				res = mongo_write(value)
				if res == True:
					i += 1
				if res == False:
					base.db.payment_merchants.remove({'primary_id':int(value['primary_id'])})
					try:
						res = mongo_write(value,False)
					except Exception as e:
						base.log_it(str(e),'payment_merchants_error')
					j += 1
				try:
					base.db.consumer_info_persona.update({'consumer_id':int(value['consumer_id'])},{'$set':{'last_activity_date':get_mongo_date(value['created_date'])}})	
				except Exception as e:
					base.log_it('Exception: '+str(e),'payment_merchants_error')
	else:
		base.log_it('Null retrieved from MySQL','payment_merchants')
		return False

	base.log_it('Script ran successfully,'+str(date1)+'-'+str(date2)+',total_records_fetched('+str(row_count)+'),total_recotrds_inserted('+str(i)+'),total_records_upserted('+str(j)+')','payment_merchants')
