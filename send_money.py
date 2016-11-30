import base
import collections
from base import get_mongo_int,get_mongo_string,get_mongo_float,get_mongo_date

def mongo_write(value,add_cms_cron_collection=True):
	try:
		result = base.db.send_money.insert({
				'primary_id': 				get_mongo_int(value['primary_id']),
				'consumer_id': 				get_mongo_int(value['consumer_id']),
				'mobile': 					get_mongo_string(value['mobile']),
				'status': 					get_mongo_string(value['status']),
				'amount': 					get_mongo_float(value['amount']),
				'transaction_id': 			get_mongo_string(value['transaction_id']),
				'txn_type': 				get_mongo_string(value['txn_type']),
				'friend_mobile_no': 		get_mongo_string(value['friend_mobile_no']),
				'friend_email_id': 			get_mongo_string(value['friend_email_id']),
				'created_date': 			get_mongo_date(value['created_date']),
				'merchant_id': 				'',
				'channel_type': 			'APP',
				'channel_value': 			'',
				"extra" : {
					'txn_code': 				get_mongo_int(value['txn_code']),
					'customer_master_id': 		get_mongo_int(value['customer_master_id']),
					'txn_customer_master_id': 	get_mongo_int(value['txn_customer_master_id']),
					'txn_agent_id': 			get_mongo_int(value['txn_agent_id']),
					'is_virtual': 				get_mongo_string(value['is_virtual']),
					'txn_corporate_id': 		get_mongo_int(value['txn_corporate_id']),
					'txn_ops_id': 				get_mongo_int(value['txn_ops_id']),
					'purse_master_id': 			get_mongo_int(value['purse_master_id']),
					'customer_purse_id': 		get_mongo_int(value['customer_purse_id']),
					'ip_address': 				get_mongo_string(value['ip_address']),
					'remarks': 					get_mongo_string(value['remarks']),
					'status_ct': 				get_mongo_string(value['status_ct']),
					'TransactionRefNo': 		get_mongo_string(value['TransactionRefNo']),
					'message': 					get_mongo_string(value['message']),
					'is_request_money': 		get_mongo_int(value['is_request_money']),
					'transfer_type': 			get_mongo_string(value['transfer_type']),
					'name_of_customer': 		get_mongo_string(value['name_of_customer']),
					'consumer_email': 			get_mongo_string(value['consumer_email']),
					'registration_mode': 		get_mongo_string(value['registration_mode']),
					'login_type': 				get_mongo_string(value['login_type']),
					'consumer_created': 		get_mongo_date(value['consumer_created'])
				}}
			)
		if add_cms_cron_collection:
			base.write_cms_cron_collection(value['txn_code'],value['created_date'],'send_money')
	except Exception as e:
		base.log_it(str(e),'send_money_error')
		return False

	return True

def start_fetch(date1,date2):	
	try:
		cursor = base.mysql.cursor()
		cursor.execute("SELECT \
a.id as primary_id, \
d.id AS consumer_id, \
d.username AS mobile, \
a.txn_status AS 'status', \
a.amount, \
b.shmart_internal_refID AS transaction_id, \
CASE WHEN b.is_request_money=1 THEN 'request send' \
WHEN b.transfer_type='split_bill_pay' THEN 'split send' ELSE 'send' \
END AS txn_type, \
b.friendMobileNo as friend_mobile_no, \
b.friendEmail AS friend_email_id, \
a.date_created AS created_date, \
a.txn_code, \
a.customer_master_id, \
a.txn_customer_master_id, \
a.txn_agent_id, \
a.is_virtual, \
a.txn_corporate_id, \
a.txn_ops_id, \
a.purse_master_id, \
a.customer_master_id, \
a.ip AS ip_address, \
a.remarks, \
b.status AS status_ct, \
b.TransactionRefNo, \
b.message, \
b.is_request_money, \
b.transfer_type, \
d.name_of_customer, \
d.email AS consumer_email, \
d.registration_mode, \
d.login_type, \
d.created AS consumer_created, \
a.customer_purse_id \
FROM shmart.rat_txn_customer a \
LEFT JOIN shmart_consumer.transactions_wallet_transfers b ON a.txn_code=b.AckNo AND b.creationTime BETWEEN %(date1)s AND %(date2)s \
LEFT JOIN shmart_consumer.users d ON b.consumer_id=d.id \
LEFT JOIN shmart_consumer.users e ON b.mobileNo=e.username \
WHERE a.mode='dr' AND a.txn_type='WWFT' AND a.product_id=24 \
AND a.date_created BETWEEN %(date1)s AND %(date2)s",{'date1':date1,'date2':date2})
						
		result = cursor.fetchall()
		row_count = cursor.rowcount
		
	except Exception as e:
		base.log_it('Could not fetch data from mysql','send_money')
		return False

	i = 0
	j = 0
	if row_count > 0:	
		for value in result:
				res = mongo_write(value)
				if res == True:
					i += 1
				if res == False:
					try:
						base.db.send_money.remove({'primary_id':int(value['primary_id'])})
					except Exception as e:
						base.log_it('Exception: '+str(e),'send_money_error')
					try:
						res = mongo_write(value,False)
					except Exception as e:
						base.log_it('Exception: '+str(e),'send_money_error')
					j += 1
				try:
					base.db.consumer_info_persona.update({'consumer_id':int(value['consumer_id'])},{'$set':{'last_activity_date':get_mongo_date(value['created_date'])}})	
				except Exception as e:
					base.log_it('Exception: '+str(e),'send_money_error')
	else:
		base.log_it('Null retrieved from MySQL','send_money')
		return False

	base.log_it('Script ran successfully,'+str(date1)+'-'+str(date2)+',total_records_fetched('+str(row_count)+'),total_recotrds_inserted('+str(i)+'),total_records_upserted('+str(j)+')','send_money')
