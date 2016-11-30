import base
import collections
from base import get_mongo_int,get_mongo_string,get_mongo_float,get_mongo_date

def mongo_write(value,add_cms_cron_collection=True):
	try:
		result = base.db.add_fund.insert({
					'primary_id': 			get_mongo_int(value['primary_id']),
					'customer_master_id': 	get_mongo_int(value['customer_master_id']),
					'cardholder_id': 		get_mongo_int(value['cardholder_id']),
					'purse_master_id': 		get_mongo_int(value['purse_master_id']),
					'customer_purse_id': 	get_mongo_int(value['customer_purse_id']),
					'txn_type': 			get_mongo_string(value['txn_type']),
					'load_channel': 		get_mongo_string(value['load_channel']),
					'txn_identifier_type': 	get_mongo_string(value['txn_identifier_type']),
					'txn_identifier_num': 	get_mongo_string(value['txn_identifier_num']),
					'amount': 				get_mongo_float(value['amount']),
					'amount_available': 	get_mongo_float(value['amount_available']),
					'amount_used': 			get_mongo_float(value['amount_used']),
					'amount_cutoff': 		get_mongo_float(value['amount_cutoff']),
					'fee': 					get_mongo_float(value['fee']),
					'service_tax': 			get_mongo_float(value['service_tax']),
					'currency': 			get_mongo_string(value['currency']),
					'wallet_code': 			get_mongo_string(value['wallet_code']),
					'txn_no': 				get_mongo_string(value['txn_no']),
					'is_reversal': 			get_mongo_string(value['is_reversal']),
					'original_transaction_id': get_mongo_int(value['original_transaction_id']),
					'txn_code': 			get_mongo_int(value['txn_code']),
					'by_agent_id': 			get_mongo_int(value['by_agent_id']),
					'channel': 				get_mongo_string(value['channel']),
					'ip_address': 			get_mongo_string(value['ip_address']),
					'created_date': 		get_mongo_date(value['created_date']),
					'date_load': 			get_mongo_date(value['date_load']),
					'date_failed': 			get_mongo_date(value['date_failed']),
					'txn_load_id': 			get_mongo_int(value['txn_load_id']),
					'failed_reason': 		get_mongo_string(value['failed_reason']),
					'date_updated': 		get_mongo_date(value['date_updated']),
					'status_load_request': 	get_mongo_string(value['status_load_request']),
					'date_expiry': 			get_mongo_date(value['date_expiry']),
					'mode': 				get_mongo_string(value['mode']),
					'rat_txn_type': 		get_mongo_string(value['rat_txn_type']),
					'rat_txn_mode': 		get_mongo_string(value['rat_txn_mode']),
					'is_virtual': 			get_mongo_string(value['is_virtual']),
					'txn_status': 			get_mongo_string(value['txn_status']),
					'status_consumer_txn': 	get_mongo_string(value['status_consumer_txn']),
					'transaction_id': 		get_mongo_string(value['transaction_id']),
					'is_wallet_credited': 	get_mongo_int(value['is_wallet_credited']),
					'merchant_user_id': 	get_mongo_int(value['merchant_user_id']),
					'transaction_mode': 	get_mongo_string(value['transaction_mode']),
					'shmart_refID': 		get_mongo_string(value['shmart_refID']),
					'status_merch_txn': 	get_mongo_string(value['status_merch_txn']),
					'trans_amount_pg': 		get_mongo_float(value['trans_amount_pg']),
					'trans_amount_wallet': 	get_mongo_float(value['trans_amount_wallet']),
					'app_used': 			get_mongo_string(value['app_used']),
					'app_id': 				get_mongo_string(value['app_id']),
					'merchant_id': 			get_mongo_int(value['merchant_id']),
					'channel_merch_txn': 	get_mongo_string(value['channel_merch_txn']),
					'hybrid_or_not': 		get_mongo_int(value['hybrid_or_not']),
					'is_saved_card_or_not': get_mongo_int(value['is_saved_card_or_not']),
					'cardType': 			get_mongo_string(value['cardType']),
					'cardProvider': 		get_mongo_string(value['cardProvider']),
					'name_on_card': 		get_mongo_string(value['name_on_card']),
					'processor': 			get_mongo_string(value['processor']),
					'account_id': 			get_mongo_int(value['account_id']),
					'merchant_voucher_id': 	get_mongo_string(value['merchant_voucher_id']),
					'voucher_id': 			get_mongo_string(value['voucher_id']),
					'voucher_type': 		get_mongo_string(value['voucher_type']),
					'voucher_amount': 		get_mongo_float(value['voucher_amount']),
					'voucher_refID': 		get_mongo_string(value['voucher_refID']),
					'expiry_date': 			get_mongo_date(value['expiry_date']),
					'is_expired': 			get_mongo_int(value['is_expired']),
					'is_used': 				get_mongo_int(value['is_used']),
					'is_deleted': 			get_mongo_int(value['is_deleted']),
					'upload_id': 			get_mongo_int(value['upload_id']),
					'batch_id': 			get_mongo_int(value['batch_id']),
					'process_status': 		get_mongo_string(value['process_status']),
					'shmart_wallet_type': 	get_mongo_string(value['shmart_wallet_type']),
					'consumer_id': 			get_mongo_int(value['consumer_id']),
					'mobile': 				get_mongo_string(value['mobile']),
					'name_of_customer': 	get_mongo_string(value['name_of_customer']),
					'consumer_email': 		get_mongo_string(value['consumer_email']),
					'registration_mode': 	get_mongo_string(value['registration_mode']),
					'login_type': 			get_mongo_string(value['login_type']),
					'consumer_created': 	get_mongo_date(value['consumer_created']),
					'merchant_af_id': 		get_mongo_string(value['merchant_af_id']),
					'af_source': 			get_mongo_string(value['af_source']),
					'reverse': 				get_mongo_string(value['reverse']),
					'AF_merchant': 			get_mongo_string(value['AF_merchant'])
				}
			)
		if add_cms_cron_collection:
			base.write_cms_cron_collection(value['txn_code'],value['created_date'],'add_fund')
	except Exception as e:
		base.log_it(str(e),'add_fund_error')
		return False

	return True

def start_fetch(date1,date2):	
	try:
		cursor = base.mysql.cursor()
		cursor.execute("SELECT sub.*,mu.username AS 'AF_merchant' \
FROM( \
SELECT \
a.id AS primary_id, \
a.customer_master_id, \
a.cardholder_id, \
a.purse_master_id, \
a.customer_purse_id, \
a.txn_type, \
a.load_channel, \
a.txn_identifier_type, \
a.card_number, \
a.txn_identifier_num, \
a.amount, \
a.amount_available, \
a.amount_used, \
a.amount_cutoff, \
a.fee, \
a.service_tax, \
a.currency, \
a.wallet_code, \
a.txn_no, \
a.is_reversal, \
a.original_transaction_id, \
a.txn_code, \
a.by_agent_id, \
a.channel, \
a.ip AS ip_address, \
a.date_created AS created_date, \
a.date_load, \
a.date_failed, \
a.txn_load_id, \
a.failed_reason, \
a.date_updated, \
a.status AS status_load_request, \
a.date_expiry, \
a.mode, \
b.txn_type as rat_txn_type, \
b.mode as rat_txn_mode, \
b.is_virtual, \
b.txn_status, \
c.status AS status_consumer_txn, \
c.merchant_refID AS transaction_id, \
c.is_wallet_credited, \
c.merchant_user_id, \
d.transaction_mode, \
d.shmart_refID, \
d.status AS status_merch_txn, \
d.trans_amount_pg, \
d.trans_amount_wallet, \
d.app_used, \
d.app_id, \
d.user_id AS merchant_id, \
d.channel AS channel_merch_txn, \
e.hybrid_or_not, \
e.is_saved_card_or_not, \
e.cardType, \
e.cardProvider, \
e.name_on_card, \
e.processor, \
e.account_id, \
f.merchant_id AS merchant_voucher_id, \
f.voucher_id, \
f.voucher_type, \
f.voucher_amount, \
f.voucher_refID, \
f.expiry_date, \
f.is_expired, \
f.is_used, \
f.is_deleted, \
g.upload_id, \
g.batch_id, \
g.process_status, \
g.shmart_wallet_type, \
i.id AS consumer_id, \
i.username AS mobile, \
i.name_of_customer, \
i.email AS consumer_email, \
i.registration_mode, \
i.login_type, \
i.created AS consumer_created, \
CASE \
WHEN c.wallet_txnno=g.transaction_no THEN cpb.user_id  \
WHEN c.wallet_txnno=c.wallet_ackno THEN 'Agent_Load'  \
WHEN c.merchant_refid=d.merchant_refid AND c.merchant_user_id IS NOT NULL THEN c.merchant_user_id  \
WHEN c.merchant_refid=d.merchant_refid AND c.merchant_user_id IS NULL THEN d.user_id  \
WHEN a.txn_no=f.txnNo THEN f.merchant_id  \
WHEN c.merchant_refid IS NOT NULL AND c.merchant_user_id is NOT NULL THEN c.merchant_user_id  \
ELSE 'UNKNOWN' \
END AS 'merchant_af_id', \
CASE \
WHEN c.wallet_txnno=g.transaction_no THEN 'CORP' \
WHEN c.wallet_txnno=c.wallet_ackno THEN 'AGENT'  \
WHEN c.merchant_refid=d.merchant_refid AND c.merchant_user_id IS NOT NULL THEN 'MER-APP' \
WHEN c.merchant_refid=d.merchant_refid AND c.merchant_user_id IS NULL THEN 'UDIO-APP'  \
WHEN a.txn_no=f.txnNo THEN 'VOUCHER' \
WHEN c.merchant_refid IS NOT NULL AND c.merchant_user_id is NOT NULL THEN 'API' \
ELSE 'UNKNOWN' \
END AS af_source, \
case when a.original_transaction_id>0 then  'REVERSE' else 'NORMAL'  end as reverse \
FROM shmart.rat_corp_load_request a \
LEFT JOIN shmart.rat_txn_customer b ON a.txn_code=b.txn_code AND b.product_id=24 \
LEFT JOIN shmart_consumer.transactions c ON a.txn_no=c.wallet_TxnNo  \
LEFT JOIN shmart_merchant.transactions d ON c.merchant_refID=d.merchant_refID  \
LEFT JOIN shmart_merchant.transactions_pg e ON d.shmart_refID=e.shmart_refID  \
LEFT JOIN shmart_consumer.vouchers f ON a.txn_no=f.txnNo \
LEFT JOIN shmart_merchant.corporate_payout_processdetail g ON a.txn_no=g.transaction_no  \
LEFT JOIN shmart.rat_corp_cardholders h ON a.customer_master_id=h.customer_master_id AND h.product_id=24 \
LEFT JOIN shmart_consumer.users i ON h.mobile=i.username \
LEFT JOIN shmart_merchant.corporate_payout_batchdetails cpb  ON g.batch_id=cpb.batch_id \
WHERE a.date_created BETWEEN %(date1)s AND %(date2)s \
AND a.product_id=24 \
AND a.txn_type='CDRL' \
) AS sub \
LEFT JOIN shmart_merchant.users mu ON sub.merchant_af_id=mu.id",{'date1':date1,'date2':date2})
						
		result = cursor.fetchall()
		row_count = cursor.rowcount
		
	except Exception as e:
		base.log_it('Could not fetch data from mysql','add_fund')
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
						base.db.add_fund.remove({'primary_id':int(value['primary_id'])})
					except Exception as e:
						base.log_it('Exception: '+str(e),'add_fund_error')
					try:
						res = mongo_write(value,False)
					except Exception as e:
						base.log_it('Exception: '+str(e),'add_fund_error')
					j += 1
	else:
		base.log_it('Null retrieved from MySQL','add_fund')
		return False

	base.log_it('Script ran successfully,'+str(date1)+'-'+str(date2)+',total_records_fetched('+str(row_count)+'),total_recotrds_inserted('+str(i)+'),total_records_upserted('+str(j)+')','add_fund')

