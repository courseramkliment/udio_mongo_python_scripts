import base
import collections
from base import get_mongo_int,get_mongo_string,get_mongo_float,get_mongo_date

def mongo_write(value):
	try:
		result = base.db.consumer_info_persona.insert({
				'consumer_id'			: 		get_mongo_int(value['consumer_id']),
				'mobile'				: 		get_mongo_string(value['mobile']),
				'name_of_customer'		: 		get_mongo_string(value['name_of_customer']),
				'consumer_email'		: 		get_mongo_string(value['consumer_email']),
				'registration_mode'		: 		'MOBILEAPP' if (get_mongo_string(value['Link_IMEI']) != '' or get_mongo_string(value['Link_Merchant_user_id']) == '7175') else get_mongo_string(value['registration_mode']),
				'old_registration_mode'	:		get_mongo_string(value['registration_mode']),	
				'login_type'			: 		get_mongo_string(value['login_type']),
				'consumer_created'		: 		get_mongo_date(value['consumer_created']),
				'last_activity_date'	: 		get_mongo_date(value['consumer_created']),
				'modified_consumer'		: 		get_mongo_date(value['modified_consumer']),
				'status'				: 		get_mongo_int(value['status']),
				'Link_Merchant_user_id'	: 		get_mongo_string(value['Link_Merchant_user_id']),
				'Link_Merchant_username': 		get_mongo_string(value['Link_Merchant_username']),
				'Link_IMEI'				: 		get_mongo_string(value['Link_IMEI']),
				'usr_device'			: 		get_mongo_string(value['usr_device']),
				'Link_version'			: 		get_mongo_string(value['Link_version']),
				'saved_maskedcardno'	: 		get_mongo_string(value['saved_maskedcardno']),
				'saved_cardtypes'		: 		get_mongo_string(value['saved_cardtypes']),
				'count_merchant'		: 		get_mongo_int(value['count_merchant']),
				'count_device'			: 		get_mongo_int(value['count_device']),
				'count_version'			: 		get_mongo_int(value['count_version']),
				'count_cards'			: 		get_mongo_int(value['count_cards']),
				'first_merchant_link_dt': 		get_mongo_string(value['first_merchant_link_dt']),
				'mer_source'			: 		get_mongo_string(value['mer_source']),
				'customer_master_id'	: 		get_mongo_int(value['customer_master_id']),
				'partner_ref_no'		: 		get_mongo_string(value['partner_ref_no']),
				'customer_name'			: 		get_mongo_string(value['customer_name']),
				'gender'				: 		get_mongo_string(value['gender']),
				'date_of_birth'			: 		get_mongo_date(value['date_of_birth']),
				'customer_type'			: 		get_mongo_string(value['customer_type']),
				'card_pack_id'			: 		get_mongo_string(value['card_pack_id']),
				'name_on_card'			: 		get_mongo_string(value['name_on_card']),
				'address'				: 		get_mongo_string(value['address']),
				'is_card_activated'		: 		get_mongo_string(value['is_card_activated']),
				'activation_date'		: 		get_mongo_date(value['activation_date']),
				'is_card_dispatched'	: 		get_mongo_string(value['is_card_dispatched']),
				'card_dispatch_date'	: 		get_mongo_date(value['card_dispatch_date']),
				'date_activation'		: 		get_mongo_date(value['date_activation']),
				'created_legacy'		: 		get_mongo_date(value['created_legacy']),
				'updated_legacy'		: 		get_mongo_date(value['updated_legacy']),
				'card_type'				: 		get_mongo_string(value['card_type']),
				'status_corp_cardholder': 		get_mongo_string(value['status_corp_cardholder']),
				'total_balance'			: 		get_mongo_float(value['total_balance']),
				'distinct_purse_balance': 		get_mongo_string(value['distinct_purse_balance']),
				'non_zero_wallets'		: 		get_mongo_float(value['non_zero_wallets']),
				'general_cash'			: 		get_mongo_float(value['general_cash']),
				'gcm_device_id'			: 		get_mongo_string(value['gcm_device_id']),
				'gcm_platform'			: 		get_mongo_string(value['gcm_platform']),
				'gcm_creationTime'		: 		get_mongo_date(value['gcm_creationTime']),
				'gcm_updateTime'		: 		get_mongo_date(value['gcm_updateTime'])
				}
			)
		
		linked_merchants = str(value['Link_Merchant_user_id']).split(',')

		if '7175' in linked_merchants:
			user_type = 'micromax'
		else:
			user_type = 'app'
			
		base.write_cms_cron_collection_new_registration(value['mobile'],value['consumer_created'],user_type)
	except Exception as e:
		base.log_it(str(e),'consumer_info_persona_error')
		return False

	return True

def mongo_update(value):
	try:
		result = base.db.consumer_info_persona.update({'consumer_id':get_mongo_int(value['consumer_id'])},{'$set':{
				'mobile'				: 		get_mongo_string(value['mobile']),
				'name_of_customer'		: 		get_mongo_string(value['name_of_customer']),
				'consumer_email'		: 		get_mongo_string(value['consumer_email']),
				'registration_mode'		: 		'MOBILEAPP' if get_mongo_string(value['Link_IMEI']) != '' else get_mongo_string(value['registration_mode']),
				'old_registration_mode'	:		get_mongo_string(value['registration_mode']),	
				'login_type'			: 		get_mongo_string(value['login_type']),
				'modified_consumer'		: 		get_mongo_date(value['modified_consumer']),
				'status'				: 		get_mongo_int(value['status']),
				'Link_Merchant_user_id'	: 		get_mongo_string(value['Link_Merchant_user_id']),
				'Link_Merchant_username': 		get_mongo_string(value['Link_Merchant_username']),
				'Link_IMEI'				: 		get_mongo_string(value['Link_IMEI']),
				'usr_device'			: 		get_mongo_string(value['usr_device']),
				'Link_version'			: 		get_mongo_string(value['Link_version']),
				'saved_maskedcardno'	: 		get_mongo_string(value['saved_maskedcardno']),
				'saved_cardtypes'		: 		get_mongo_string(value['saved_cardtypes']),
				'count_merchant'		: 		get_mongo_int(value['count_merchant']),
				'count_device'			: 		get_mongo_int(value['count_device']),
				'count_version'			: 		get_mongo_int(value['count_version']),
				'count_cards'			: 		get_mongo_int(value['count_cards']),
				'first_merchant_link_dt': 		get_mongo_string(value['first_merchant_link_dt']),
				'mer_source'			: 		get_mongo_string(value['mer_source']),
				'customer_master_id'	: 		get_mongo_int(value['customer_master_id']),
				'partner_ref_no'		: 		get_mongo_string(value['partner_ref_no']),
				'customer_name'			: 		get_mongo_string(value['customer_name']),
				'gender'				: 		get_mongo_string(value['gender']),
				'date_of_birth'			: 		get_mongo_date(value['date_of_birth']),
				'customer_type'			: 		get_mongo_string(value['customer_type']),
				'card_pack_id'			: 		get_mongo_string(value['card_pack_id']),
				'name_on_card'			: 		get_mongo_string(value['name_on_card']),
				'address'				: 		get_mongo_string(value['address']),
				'is_card_activated'		: 		get_mongo_string(value['is_card_activated']),
				'activation_date'		: 		get_mongo_date(value['activation_date']),
				'is_card_dispatched'	: 		get_mongo_string(value['is_card_dispatched']),
				'card_dispatch_date'	: 		get_mongo_date(value['card_dispatch_date']),
				'date_activation'		: 		get_mongo_date(value['date_activation']),
				'created_legacy'		: 		get_mongo_date(value['created_legacy']),
				'updated_legacy'		: 		get_mongo_date(value['updated_legacy']),
				'card_type'				: 		get_mongo_string(value['card_type']),
				'status_corp_cardholder': 		get_mongo_string(value['status_corp_cardholder']),
				'total_balance'			: 		get_mongo_float(value['total_balance']),
				'distinct_purse_balance': 		get_mongo_string(value['distinct_purse_balance']),
				'non_zero_wallets'		: 		get_mongo_float(value['non_zero_wallets']),
				'general_cash'			: 		get_mongo_float(value['general_cash']),
				'gcm_device_id'			: 		get_mongo_string(value['gcm_device_id']),
				'gcm_platform'			: 		get_mongo_string(value['gcm_platform']),
				'gcm_creationTime'		: 		get_mongo_date(value['gcm_creationTime']),
				'gcm_updateTime'		: 		get_mongo_date(value['gcm_updateTime'])
				}}
			)
	except Exception as e:
		base.log_it(str(e),'consumer_info_persona_error')
		return False

	return True
	
def start_fetch(date1,date2):	
	try:
		cursor = base.mysql.cursor()
		cursor.execute("select * from analytics.consumer_master where cm_update_time between %(date1)s and %(date2)s",{'date1':date1,'date2':date2})
						
		result = cursor.fetchall()
		row_count = cursor.rowcount
		
	except Exception as e:
		base.log_it('Could not fetch data from mysql','consumer_info_persona')
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
						res = mongo_update(value)
					except Exception as e:
						base.log_it(str(e),'consumer_info_persona_error')
					j += 1
	else:
		base.log_it('Null retrieved from MySQL','consumer_info_persona')
		return False

	base.log_it('Script ran successfully,'+str(date1)+'-'+str(date2)+',total_records_fetched('+str(row_count)+'),total_recotrds_inserted('+str(i)+'),total_records_upserted('+str(j)+')','consumer_info_persona')

