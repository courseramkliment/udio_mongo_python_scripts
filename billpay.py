import base
import collections
from base import get_mongo_int,get_mongo_string,get_mongo_float,get_mongo_date

def mongo_write(value,add_cms_cron_collection=True):
	try:
		result = base.db.recharge_billpay.insert({
				'primary_id': 				get_mongo_string(value['primary_id'])+'_billpay',
				'consumer_id': 				get_mongo_int(value['consumer_id']),
				'mobile': 					get_mongo_string(value['mobile']),
				'transaction_id': 			get_mongo_string(value['transaction_id']),
				'amount': 					get_mongo_float(value['amount']),
				'type': 					get_mongo_string(value['type']),
				'txn_type': 				get_mongo_string(value['txn_type']),
				'operator': 				get_mongo_string(value['operator']),
				'merchant_id': 				get_mongo_int(value['merchant_id']),
				'status': 					get_mongo_string(value['status']),
				'created_date': 			get_mongo_date(value['created_date']),
				'channel_type': 			'APP',
				'channel_value': 			'',
				"extra" : {
					'cyberplat_session_id': 		get_mongo_string(value['cyberplat_session_id']),
					'verification_response': 		get_mongo_int(value['verification_response']),
					'cyberplat_billing_status': 	get_mongo_int(value['cyberplat_billing_status']),
					'telephone_number': 			get_mongo_string(value['telephone_number']),
					'bill_number': 					get_mongo_string(value['bill_number']),
					'cycle_number': 				get_mongo_string(value['cycle_number']),
					'cyberplat_transaction_id': 	get_mongo_string(value['cyberplat_transaction_id']),
					'cyberplat_authcode': 			get_mongo_string(value['cyberplat_authcode']),
					'verification_cyberplat_response_billpay': 	get_mongo_string(value['verification_cyberplat_response_billpay']),
					'transaction_mode': 			get_mongo_string(value['transaction_mode']),
					'shmart_refID': 				get_mongo_string(value['shmart_refID']),
					'total_amount': 				get_mongo_string(value['total_amount']),
					'trans_amount_pg': 				get_mongo_string(value['trans_amount_pg']),
					'trans_amount_wallet': 			get_mongo_string(value['trans_amount_wallet']),
					'app_used': 					get_mongo_string(value['app_used']),
					'app_id': 						get_mongo_string(value['app_id']),
					'channel': 						get_mongo_string(value['channel']),
					'wallet_trans_type': 			get_mongo_string(value['wallet_trans_type']),
					'meal_wallet_ackNo': 			get_mongo_string(value['meal_wallet_ackNo']),
					'medical_wallet_ackNo': 		get_mongo_string(value['medical_wallet_ackNo']),
					'gift_wallet_ackNo': 			get_mongo_string(value['gift_wallet_ackNo']),
					'recharge_wallet_ackNo': 		get_mongo_string(value['recharge_wallet_ackNo']),
					'wallet_trans_status': 			get_mongo_string(value['wallet_trans_status']),
					'txn_code': 					get_mongo_int(value['txn_code']),
					'customer_master_id': 			get_mongo_int(value['customer_master_id']),
					'txn_customer_master_id': 		get_mongo_int(value['txn_customer_master_id']),
					'txn_agent_id': 				get_mongo_int(value['txn_agent_id']),
					'is_virtual': 					get_mongo_string(value['is_virtual']),
					'txn_corporate_id': 			get_mongo_int(value['txn_corporate_id']),
					'txn_ops_id': 					get_mongo_int(value['txn_ops_id']),
					'product_id': 					get_mongo_int(value['product_id']),
					'purse_master_id': 				get_mongo_int(value['purse_master_id']),
					'customer_purse_id': 			get_mongo_int(value['customer_purse_id']),
					'ip_address': 					get_mongo_string(value['ip_address']),
					'amount_legacy': 				get_mongo_float(value['amount_legacy']),
					'remarks': 						get_mongo_string(value['remarks']),
					'name_of_customer': 			get_mongo_string(value['name_of_customer']),
					'consumer_email': 				get_mongo_string(value['consumer_email']),
					'registration_mode': 			get_mongo_string(value['registration_mode']),
					'login_type': 					get_mongo_string(value['login_type']),
					'consumer_created': 			get_mongo_date(value['consumer_created'])
				}}
			)
		if add_cms_cron_collection:
			base.write_cms_cron_collection(value['txn_code'],value['created_date'],'recharge_billpay')
	except Exception as e:
		base.log_it(str(e),'billpay_error')
		return False

	return True

def start_fetch(date1,date2):	
	try:
		cursor = base.mysql.cursor()
		cursor.execute("SELECT \
a.consumer_id, \
e.username AS mobile, \
a.merchant_refID AS transaction_id, \
a.amount, \
'billpay' AS 'type', \
a.mode AS txn_type, \
a.operator_name AS operator, \
b.user_id AS merchant_id, \
b.status, \
a.createdDate AS created_date, \
a.id AS primary_id, \
a.cyberplat_session_id, \
a.verification_response, \
a.cyberplat_billing_status, \
a.telephone_number, \
a.bill_number, \
a.cycle_number, \
a.cyberplat_transaction_id, \
a.cyberplat_authcode, \
a.verification_cyberplat_response AS verification_cyberplat_response_billpay, \
b.transaction_mode, \
b.shmart_refID, \
b.total_amount, \
b.trans_amount_pg, \
b.trans_amount_wallet, \
b.app_used, \
b.app_id, \
b.channel, \
c.wallet_trans_type, \
c.meal_wallet_ackNo, \
c.recharge_wallet_ackNo, \
c.gift_wallet_ackNo, \
c.medical_wallet_ackNo, \
c.wallet_trans_status, \
d.txn_code, \
d.customer_master_id, \
d.txn_customer_master_id, \
d.txn_agent_id, \
d.is_virtual, \
d.txn_corporate_id, \
d.txn_ops_id, \
d.product_id, \
d.purse_master_id, \
d.customer_purse_id, \
d.ip AS ip_address, \
d.amount AS amount_legacy, \
d.remarks, \
e.name_of_customer, \
e.email AS consumer_email, \
e.registration_mode, \
e.login_type, \
e.created AS consumer_created \
FROM shmart_consumer.cyberplat_billpayment_transactions a  \
LEFT JOIN shmart_merchant.transactions b ON a.merchant_refID=b.merchant_refID AND b.trans_startTime BETWEEN %(date1)s AND %(date2)s \
LEFT JOIN shmart_merchant.transactions_wallet c ON b.shmart_refID=c.shmart_refID AND c.trans_startTime BETWEEN %(date1)s AND %(date2)s\
LEFT JOIN shmart.rat_txn_customer d ON( \
d.txn_code=c.general_wallet_ackNo OR \
d.txn_code=c.voucher_wallet_ackNo OR  \
d.txn_code=c.meal_wallet_ackNo OR  \
d.txn_code=c.medical_wallet_ackNo OR  \
d.txn_code=c.gift_wallet_ackNo OR  \
d.txn_code=c.recharge_wallet_ackNo) AND d.date_created BETWEEN %(date1)s AND %(date2)s \
LEFT JOIN shmart_consumer.users e ON a.consumer_id=e.id \
WHERE a.createdDate BETWEEN %(date1)s AND %(date2)s",{'date1':date1,'date2':date2})
						
		result = cursor.fetchall()
		row_count = cursor.rowcount
		
	except Exception as e:
		base.log_it('Could not fetch data from mysql','billpay')
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
						base.db.recharge_billpay.remove({'primary_id':str(value['primary_id'])+'_billpay'})
					except Exception as e:
						base.log_it('Exception: '+str(e),'billpay_error')
					try:
						res = mongo_write(value,False)
					except Exception as e:
						base.log_it('Exception: '+str(e),'billpay_error')
					j += 1
				try:
					base.db.consumer_info_persona.update({'consumer_id':int(value['consumer_id'])},{'$set':{'last_activity_date':get_mongo_date(value['created_date'])}})	
				except Exception as e:
					base.log_it('Exception: '+str(e),'billpay_error')
	else:
		base.log_it('Null retrieved from MySQL','billpay')
		return False

	base.log_it('Script ran successfully,'+str(date1)+'-'+str(date2)+',total_records_fetched('+str(row_count)+'),total_recotrds_inserted('+str(i)+'),total_records_upserted('+str(j)+')','billpay')

