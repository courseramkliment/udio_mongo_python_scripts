import base
import collections
from base import get_mongo_int,get_mongo_string,get_mongo_float,get_mongo_date

def mongo_write(value,add_cms_cron_collection=True):
	try:
		result = base.db.withdrawals.insert({
					'primary_id' : 			get_mongo_int(value['primary_id']),
					'bank_id' : 			get_mongo_int(value['bank_id']),
					'product_id' : 			get_mongo_int(value['product_id']),
					'customer_master_id' : 	get_mongo_int(value['customer_master_id']),
					'is_virtual' : 			get_mongo_string(value['is_virtual']),
					'purse_master_id' : 	get_mongo_int(value['purse_master_id']),
					'customer_purse_id' : 	get_mongo_int(value['customer_purse_id']),
					'ip' : 					get_mongo_string(value['ip']),
					'amount' : 				get_mongo_float(value['amount']),
					'mode' : 				get_mongo_string(value['mode']),
					'txn_type' : 			get_mongo_string(value['txn_type']),
					'txn_status' : 			get_mongo_string(value['txn_status']),
					'txn_code' : 			get_mongo_int(value['txn_code']),
					'transaction_date' : 	get_mongo_date(value['transaction_date']),
					'req_created_date' : 	get_mongo_date(value['req_created_date']),
					'rat_customer_id' : 	get_mongo_int(value['rat_customer_id']),
					'remitter_id' : 		get_mongo_int(value['remitter_id']),
					'beneficiary_id' : 		get_mongo_int(value['beneficiary_id']),
					'agent_id' : 			get_mongo_int(value['agent_id']),
					'ops_id' : 				get_mongo_int(value['ops_id']),
					'req_amount' : 			get_mongo_float(value['req_amount']),
					'req_fee' : 			get_mongo_float(value['req_fee']),
					'req_service_tax' : 	get_mongo_float(value['req_service_tax']),
					'txnrefnum' : 			get_mongo_string(value['txnrefnum']),
					'req_status' : 			get_mongo_string(value['req_status']),
					'req_channel' : 		get_mongo_string(value['req_channel']),
					'remitter_name' : 		get_mongo_string(value['remitter_name']),
					'remitter_mobile' : 	get_mongo_string(value['remitter_mobile']),
					'remitter_email' : 		get_mongo_string(value['remitter_email']),
					'remitter_reg_date' : 	get_mongo_date(value['remitter_reg_date']),
					'remitter_status' : 	get_mongo_string(value['remitter_status']),
					'bene_name' : 			get_mongo_string(value['bene_name']),
					'ifsc_code' : 			get_mongo_string(value['ifsc_code']),
					'bene_account_number' : get_mongo_string(value['bene_account_number']),
					'bene_mobile' : 		get_mongo_string(value['bene_mobile']),
					'bene_bankname' : 		get_mongo_string(value['bene_bankname']),
					'bene_branchname' : 	get_mongo_string(value['bene_branchname']),
					'bene_branchcity' : 	get_mongo_string(value['bene_branchcity']),
					'bene_created' : 		get_mongo_date(value['bene_created']),
					'bene_status' : 		get_mongo_string(value['bene_status']),
					'ct_amount' : 			get_mongo_float(value['ct_amount']),
					'ct_status' : 			get_mongo_string(value['ct_status']),
					'ct_mobile' : 			get_mongo_string(value['ct_mobile']),
					'consumer_id' : 		get_mongo_int(value['consumer_id']),
					'merchant_user_id' : 	get_mongo_int(value['merchant_user_id']),
					'merchant_name' : 		get_mongo_string(value['merchant_name']),
					'trans_startTime' : 	get_mongo_date(value['trans_startTime']),
					'req_updated_date' : 	get_mongo_date(value['req_updated_date']),
					'actual_startTime' : 	get_mongo_date(value['actual_startTime'])
				}
			)
		if add_cms_cron_collection:
			base.write_cms_cron_collection(value['txn_code'],value['transaction_date'],'withdrawals')
	except Exception as e:
		base.log_it(str(e),'withdrawals_error')
		return False

	return True

def start_fetch(date1,date2,date3,date4):	
	try:
		cursor = base.mysql.cursor()
		cursor.execute("SELECT \
txn.id as primary_id, \
txn.bank_id, \
txn.product_id, \
txn.customer_master_id, \
txn.is_virtual, \
txn.purse_master_id, \
txn.customer_purse_id, \
txn.ip, \
txn.amount, \
txn.mode, \
txn.txn_type, \
txn.txn_status, \
txn.txn_code, \
txn.date_created AS 'transaction_date', \
req.date_created AS 'req_created_date', \
req.rat_customer_id, \
req.remitter_id, \
req.beneficiary_id, \
req.agent_id, \
req.ops_id, \
req.amount AS req_amount, \
req.fee AS req_fee, \
req.service_tax AS req_service_tax, \
req.txnrefnum, \
req.status AS req_status, \
req.channel AS req_channel, \
req.date_updated AS 'req_updated_date', \
CONCAT(IFNULL(remit.name,''),' ',IFNULL(remit.middle_name,''),' ',IFNULL(remit.last_name,'')) AS 'remitter_name', \
remit.mobile AS 'remitter_mobile', \
remit.email AS 'remitter_email', \
remit.date_created AS 'remitter_reg_date', \
remit.status AS 'remitter_status', \
CONCAT(IFNULL(bene.name,''),' ',IFNULL(bene.middle_name,''),' ',IFNULL(bene.last_name,'')) AS 'bene_name', \
bene.ifsc_code, \
AES_DECRYPT(bene.bank_account_number,'goprs010058074ea3dc0bc89ge8aprcf') AS bene_account_number,  \
AES_DECRYPT(bene.mobile,'goprs010058074ea3dc0bc89ge8aprcf') AS bene_mobile,  \
bene.bank_name AS bene_bankname, \
bene.branch_name AS bene_branchname, \
bene.branch_city AS bene_branchcity, \
bene.date_created AS 'bene_created', \
bene.status AS 'bene_status', \
ct.amount AS ct_amount, \
ct.status AS ct_status, \
ct.mobileNo AS ct_mobile, \
ct.consumer_id, \
ct.merchant_user_id, \
usr.username as merchant_name, \
ct.trans_startTime, \
ct.actual_startTime \
FROM  \
shmart.rat_txn_customer txn  \
LEFT JOIN shmart.rat_remittance_request req ON txn.txn_code=req.txn_code \
and req.date_created between %(date3)s AND %(date4)s  \
AND req.product_id=24  \
LEFT JOIN shmart.rat_remit_remitters remit ON req.remitter_id=remit.id \
LEFT JOIN shmart.rat_beneficiaries bene ON req.beneficiary_id=bene.id \
LEFT JOIN shmart_consumer.transactions ct ON txn.txn_code=ct.wallet_ackno AND ct.transaction_type='WM' \
and ct.trans_startTime between %(date3)s AND %(date4)s \
LEFT JOIN shmart_merchant.users usr on ct.merchant_user_id = usr.id \
WHERE  \
txn.product_id=24  \
AND txn.txn_type='REMT' \
AND txn.date_created between %(date1)s AND %(date2)s ORDER BY txn.id DESC",{'date1':date1,'date2':date2,'date3':date3,'date4':date4})
						
		result = cursor.fetchall()
		row_count = cursor.rowcount
		
	except Exception as e:
		base.log_it('Could not fetch data from mysql','withdrawals')
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
						base.db.withdrawals.remove({'primary_id':int(value['primary_id'])})
					except Exception as e:
						base.log_it('Exception: '+str(e),'withdrawals_error')
					try:
						res = mongo_write(value,False)
					except Exception as e:
						base.log_it('Exception: '+str(e),'withdrawals_error')
					j += 1
	else:
		base.log_it('Null retrieved from MySQL','withdrawals')
		return False

	base.log_it('Script ran successfully,'+str(date1)+'-'+str(date2)+',total_records_fetched('+str(row_count)+'),total_recotrds_inserted('+str(i)+'),total_records_upserted('+str(j)+')','withdrawals')

