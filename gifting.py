import base
import collections
from base import get_mongo_int,get_mongo_string,get_mongo_float,get_mongo_date

def mongo_write(value,add_cms_cron_collection=True):
	try:
		result = base.db.gifting.insert({
				"primary_id" : get_mongo_int(value['primary_id']),
				"consumer_id" : get_mongo_int(value['consumer_id']),
				"transaction_id" : get_mongo_string(value['transaction_id']),
				"amount" : get_mongo_float(value['amount']),
				"status" : get_mongo_string(value['status']),
				"created_date" : get_mongo_date(value['created_date']),
				"merchant_id" : get_mongo_int(value['merchant_id']),
				"mobile" : get_mongo_string(value['mobile']),
				"gift_segment" : 'EGV',
				'channel_type': 'APP',
				'channel_value': '',
				"extra" : {
						"friend_mobile_no" : get_mongo_string(value['friend_mobile_no']),
						"friend_email_id" : get_mongo_string(value['friend_email_id']),
						"friend_social_id" : get_mongo_string(value['friend_social_id']),
						"type_gift" : get_mongo_string(value['type_gift']),
						"product_name" : get_mongo_string(value['product_name']),
						"product_id" : get_mongo_int(value['product_id']),
						"quantity" : get_mongo_int(value['quantity']),
						"voucher_order_id" : get_mongo_string(value['voucher_order_id']),
						"order_key" : get_mongo_string(value['order_key']),
						"shmart_refID" : get_mongo_string(value['shmart_refID']),
						"status_mt" : get_mongo_string(value['status_mt']),
						"channel" : get_mongo_string(value['channel']),
						"wallet_trans_type" : get_mongo_string(value['wallet_trans_type']),
						"general_wallet_ackNo" : get_mongo_string(value['general_wallet_ackNo']),
						"voucher_wallet_ackNo" : get_mongo_string(value['voucher_wallet_ackNo']),
						"meal_wallet_ackNo" : get_mongo_string(value['meal_wallet_ackNo']),
						"medical_wallet_ackNo" : get_mongo_string(value['medical_wallet_ackNo']),
						"gift_wallet_ackNo" : get_mongo_string(value['gift_wallet_ackNo']),
						"recharge_wallet_ackNo" : get_mongo_string(value['recharge_wallet_ackNo']),
						"reversed" : get_mongo_int(value['reversed']),
						"txn_code" : get_mongo_int(value['txn_code']),
						"customer_master_id" : get_mongo_int(value['customer_master_id']),
						"txn_customer_master_id" : get_mongo_int(value['txn_customer_master_id']),
						"txn_agent_id" : get_mongo_int(value['txn_agent_id']),
						"is_virtual" : get_mongo_string(value['is_virtual']),
						"txn_corporate_id" : get_mongo_int(value['txn_corporate_id']),
						"txn_ops_id" : get_mongo_int(value['txn_ops_id']),
						"product_id_legacy" : get_mongo_int(value['product_id_legacy']),
						"purse_master_id" : get_mongo_int(value['purse_master_id']),
						"customer_purse_id" : get_mongo_int(value['customer_purse_id']),
						"ip_address" : get_mongo_string(value['ip_address']),
						"amount_legacy" : get_mongo_float(value['amount_legacy']),
						"name_of_customer" : get_mongo_string(value['name_of_customer']),
						"consumer_email" : get_mongo_string(value['consumer_email']),
						"registration_mode" : get_mongo_string(value['registration_mode']),
						"login_type" : get_mongo_string(value['login_type']),
						"consumer_created" : get_mongo_date(value['consumer_created'])
				}}
			)
		if add_cms_cron_collection:
			base.write_cms_cron_collection(value['txn_code'],value['created_date'],'gifting')
	except Exception as e:
		base.log_it(str(e),'gifting_error')
		return False

	return True

def start_fetch(date1,date2):	
	try:
		cursor = base.mysql.cursor()
		cursor.execute("SELECT  \
	a.id AS primary_id,  \
	a.consumer_id,  \
	a.merchant_refID AS transaction_id,  \
	a.amount,  \
	a.status,  \
	a.created_on AS created_date,  \
	b.user_id AS merchant_id,  \
	e.username AS mobile,  \
	a.friend_mobile_no,  \
	a.friend_email_id,  \
	a.friend_social_id,  \
	a.type AS type_gift,  \
	a.product_name,  \
	a.product_id,  \
	a.quantity,  \
	a.voucher_order_id,  \
	a.order_key,  \
	b.shmart_refID,  \
	b.status AS status_mt,  \
	b.channel,  \
	c.wallet_trans_type,  \
	c.general_wallet_ackNo,  \
	c.gift_wallet_ackNo,  \
	c.meal_wallet_ackNo,  \
	c.medical_wallet_ackNo,  \
	c.recharge_wallet_ackNo,  \
	c.voucher_wallet_ackNo,  \
	c.reversed,  \
	d.txn_code,  \
	d.customer_master_id,  \
	d.txn_customer_master_id,  \
	d.txn_agent_id,  \
	d.is_virtual,  \
	d.txn_corporate_id,  \
	d.txn_ops_id,  \
	d.product_id AS product_id_legacy,  \
	d.purse_master_id,  \
	d.customer_purse_id,  \
	d.ip AS ip_address,  \
	d.amount AS amount_legacy,  \
	e.name_of_customer,  \
	e.email AS consumer_email,  \
	e.registration_mode,  \
	e.login_type,  \
	e.created AS consumer_created  \
FROM shmart_consumer.consumer_gifting_vouchers_order a   \
LEFT JOIN shmart_merchant.transactions b ON a.merchant_refID=b.merchant_refID  \
AND b.trans_startTime BETWEEN %(date1)s AND %(date2)s  \
LEFT JOIN shmart_merchant.transactions_wallet c ON b.shmart_refID=c.shmart_refID  \
AND c.trans_startTime BETWEEN %(date1)s AND %(date2)s  \
LEFT JOIN shmart.rat_txn_customer d ON(  \
d.txn_code=c.general_wallet_ackNo OR  \
d.txn_code=c.voucher_wallet_ackNo OR   \
d.txn_code=c.meal_wallet_ackNo OR   \
d.txn_code=c.medical_wallet_ackNo OR   \
d.txn_code=c.gift_wallet_ackNo OR   \
d.txn_code=c.recharge_wallet_ackNo) AND d.date_created BETWEEN %(date1)s AND %(date2)s \
LEFT JOIN shmart_consumer.users e ON a.consumer_id=e.id  \
WHERE a.created_on BETWEEN %(date1)s AND %(date2)s",{'date1':date1,'date2':date2})
						
		result = cursor.fetchall()
		row_count = cursor.rowcount
		
	except Exception as e:
		base.log_it('Could not fetch data from mysql','gifting')
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
						base.db.gifting.remove({'primary_id':int(value['primary_id'])})
					except Exception as e:
						base.log_it('Exception: '+str(e),'gifting_error')
					try:
						res = mongo_write(value,False)
					except Exception as e:
						base.log_it('Exception: '+str(e),'gifting_error')
					j += 1
				try:
					base.db.consumer_info_persona.update({'consumer_id':int(value['consumer_id'])},{'$set':{'last_activity_date':get_mongo_date(value['created_date'])}})	
				except Exception as e:
					base.log_it('Exception: '+str(e),'gifting_error')
	else:
		base.log_it('Null retrieved from MySQL','gifting')
		return False

	base.log_it('Script ran successfully,'+str(date1)+'-'+str(date2)+',total_records_fetched('+str(row_count)+'),total_recotrds_inserted('+str(i)+'),total_records_upserted('+str(j)+')','gifting')

