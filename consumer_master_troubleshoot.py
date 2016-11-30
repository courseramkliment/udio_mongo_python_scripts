import base
import collections
from base import get_mongo_int,get_mongo_string,get_mongo_float,get_mongo_date

#select mobile numbers of the concumers who did register or updated their profile in given time interval
from_time = '2016-08-29 00:00:00'
to_time = '2016-08-30 23:59:59'

try:
	cursor = base.mysql.cursor()
	cursor.execute("select distinct a.mobile from \
					(SELECT trim(mobile) as mobile FROM shmart.rat_corp_cardholders  \
					WHERE (date_updated between '"+from_time+"' and '"+to_time+"' \
					OR date_created between '"+from_time+"' and '"+to_time+"') \
					AND product_id=24  \
					UNION( \
					SELECT trim(username) AS mobile FROM shmart_consumer.users \
					WHERE created between '"+from_time+"' and '"+to_time+"' \
					OR modified between '"+from_time+"' and '"+to_time+"') \
					UNION( \
					SELECT trim(b.username) AS mobile FROM shmart_consumer.users_merchant_link a \
					inner JOIN shmart_consumer.users b ON a.consumer_id=b.id \
					WHERE a.creationTime between '"+from_time+"' and '"+to_time+"' \
					OR a.updateTime between '"+from_time+"' and '"+to_time+"') \
					UNION( \
					SELECT trim(b.username) AS mobile FROM shmart_consumer.mobile_device_details a \
					inner JOIN shmart_consumer.users b ON a.user_id=b.id \
					WHERE a.creationTime between '"+from_time+"' and '"+to_time+"' \
					OR a.updateTime between '"+from_time+"' and '"+to_time+"') \
					UNION( \
					SELECT trim(b.username) AS mobile FROM shmart_consumer.my_saved_cards a \
					inner JOIN shmart_consumer.users b ON a.consumer_id=b.id \
					WHERE a.creation_time between '"+from_time+"' and '"+to_time+"' \
					OR a.updation_time between '"+from_time+"' and '"+to_time+"') \
					UNION( \
					SELECT trim(b.mobile) FROM shmart.rat_customer_purse a \
					inner JOIN shmart.rat_corp_cardholders b ON a.customer_master_id=b.customer_master_id \
					WHERE a.date_updated between '"+from_time+"' and '"+to_time+"' \
					AND a.product_id=24 \
					AND b.product_id=24) \
					\
					UNION( \
					SELECT trim(b.username) AS mobile FROM shmart_consumer.user_gcm_link a \
					inner JOIN shmart_consumer.users b ON a.user_id=b.id \
					WHERE a.creationTime between '"+from_time+"' and '"+to_time+"' \
					OR a.updateTime between '"+from_time+"' and '"+to_time+"')) a")
						
	result = cursor.fetchall()
	count_mobile = cursor.rowcount
	
except Exception as e:
	base.log_it('Could not fetch data from mysql','consumer_master_error')
	print('Could not fetch data from mysql')

#create comma seperated mobile number string
mobile = 0
valid_mobiles = 0
if count_mobile > 0:
	for value in result:
		if (get_mongo_int(value['mobile']) > 6999999999 and get_mongo_int(value['mobile']) != None and get_mongo_int(value['mobile']) != ""):
			mobile = str(mobile) + "," + str(value['mobile']).strip()
			valid_mobiles += 1
					
else:
	base.log_it('Null retrieved from MySQL','consumer_master_error')
	print('Null retrieved from MySQL')

cursor = base.mysql.cursor()
cursor.execute("select count(distinct mobile) as count from analytics.consumer_master where mobile in ("+mobile+")")
result = cursor.fetchall()
print(result)
		
#update consumer master table for selected mobile numbers
if (mobile != None and mobile != '00'):
	try:
		cursor = base.mysql.cursor()
		cursor.execute('SET @@group_concat_max_len = 15000')
		
		cursor = base.mysql.cursor()
		cursor.execute("REPLACE INTO analytics.consumer_master SELECT \
						u.*, \
						cc.customer_master_id, \
						cc.partner_ref_no, \
						CONCAT(cc.first_name,' ',cc.last_name) AS 'customer_name', \
						cc.gender, \
						cc.date_of_birth, \
						cc.customer_type, \
						cc.card_pack_id, \
						cc.name_on_card, \
						concat(cc.city,'-',cc.state,'-',cc.pincode) as address, \
						cc.is_card_activated, \
						cc.activation_date, \
						cc.is_card_dispatched, \
						cc.card_dispatch_date, \
						cc.date_activation, \
						cc.date_created as created_legacy, \
						cc.date_updated AS updated_legacy, \
						cc.card_type, \
						cc.status AS status_corp_cardholder, \
						SUM(cm.amount) AS total_balance, \
						GROUP_CONCAT(distinct CONCAT(cm.purse_master_id,'-',cm.amount)) AS 'distinct_purse_balance', \
						sum(case when cm.amount>0 then 1 else 0 end) as non_zero_wallets, \
						sum(case when cm.purse_master_id in (select id from shmart.purse_master where is_virtual='no') \
						then cm.amount else 0 end) as general_cash, \
						0 as campaign_count, \
						'' as campaign_code, \
						gcm.gcm_device_id, \
						gcm.platform as gcm_platform, \
						gcm.creationTime as gcm_creationTime, \
						gcm.updateTime as gcm_updateTime, \
						NOW() as cm_update_time \
						FROM \
						shmart.rat_corp_cardholders cc \
						LEFT JOIN shmart.rat_customer_purse cm ON cc.customer_master_id=cm.customer_master_id \
						left join  \
						( \
						select \
						u.id AS consumer_id, \
						trim(u.username) AS mobile, \
						u.name_of_customer, \
						u.email AS consumer_email, \
						u.registration_mode, \
						u.login_type, \
						u.created AS consumer_created, \
						u.modified AS last_activity_date, \
						u.modified AS modified_consumer, \
						CASE WHEN u.banned=1 THEN 0 \
						ELSE 1 END AS 'status', \
						GROUP_CONCAT(DISTINCT ml.merchant_user_id) AS 'Link_Merchant_user_id', \
						GROUP_CONCAT(DISTINCT mu.username) AS 'Link_Merchant_username', \
						GROUP_CONCAT(DISTINCT mob.imei) AS 'Link_IMEI', \
						group_concat(distinct concat(mob.imei,'-',mob.version,'-',mob.platform,'-',mob.model)) as usr_device, \
						GROUP_CONCAT(DISTINCT mob.version) AS 'Link_version', \
						GROUP_CONCAT(DISTINCT concat(msc.masked_card_number,'-',msc.card_type,'-',msc.card_provider,'-',msc.expiry_month,'-',msc.expiry_year)) AS 'saved_maskedcardno', \
						GROUP_CONCAT(DISTINCT concat(msc.card_type,'-',msc.card_provider)) AS 'saved_cardtypes', \
						count(distinct ml.merchant_user_id) as count_merchant, \
						count(distinct mob.imei) as count_device, \
						count(distinct mob.version) as count_version, \
						count(distinct msc.masked_card_number) as count_cards, \
						min(ml.creationTime) as first_merchant_link_dt, \
						case when u.created between date_sub(ml.creationTime,interval 30 minute) and \
						date_add(ml.creationTime,interval 30 minute) \
						then mu.username else 'non-merchant' end as mer_source \
						from shmart_consumer.users u  \
						LEFT JOIN shmart_consumer.users_merchant_link ml ON u.id=ml.consumer_id \
						LEFT JOIN shmart_merchant.users mu ON ml.merchant_user_id=mu.id \
						LEFT JOIN shmart_consumer.mobile_device_details mob ON u.id=mob.user_id \
						LEFT JOIN shmart_consumer.my_saved_cards msc ON u.id=msc.consumer_id \
						where trim(u.username) in ("+mobile+") \
						group by trim(u.username) \
						) u on u.mobile=trim(cc.mobile) \
						left join shmart_consumer.user_gcm_link as gcm on u.consumer_id=gcm.user_id \
						WHERE \
						cc.product_id=24 and trim(cc.mobile) in ("+mobile+") \
						GROUP BY trim(cc.mobile)")
		count_mobile_processed = cursor.rowcount
		
		
		#update consumer master table set campaign_count and campaign_code for recently updated records
		cursor = base.mysql.cursor()
		cursor.execute("update analytics.consumer_master a inner join \
						( \
						SELECT \
						c.mobile, \
						COUNT(DISTINCT ct.campaign_id) AS campaign_count, \
						GROUP_CONCAT(DISTINCT ct.campaign_id) AS campaign_code \
						FROM \
						analytics.consumer_master c \
						inner JOIN analytics.campaign_targetted_mobile ct ON c.mobile=ct.mobile \
						GROUP BY \
						c.mobile) b on a.mobile=b.mobile \
						set a.campaign_count=b.campaign_count,a.campaign_code=b.campaign_code  \
						where a.mobile in ("+mobile+")")
		
		#update consumer master table set gcm_device_id and gcm_platform for recently updated records
		cursor = base.mysql.cursor()
		cursor.execute("update analytics.consumer_master a inner join shmart_consumer.user_gcm_link b \
						on a.consumer_id=b.user_id \
						set a.gcm_device_id=b.gcm_device_id,a.gcm_platform=b.platform, \
						a.gcm_creationTime=b.creationTime,a.gcm_updateTime=b.updateTime \
						where a.mobile in ("+mobile+")")
		
		cursor = base.mysql.cursor()
		cursor.execute("select count(distinct mobile) as count from analytics.consumer_master where mobile in ("+mobile+")")
		result = cursor.fetchall()
		print(result)
		base.mysql.commit()
					
	except Exception as e:
		base.mysql.rollback()
		base.log_it('Could not update consumer master - Error:'+str(e),'consumer_master_error')
		print(str(e))
		
base.log_it('Consumer master script completed. Mobiles Fetched: '+str(count_mobile)+',Valid Mobiles: '+str(valid_mobiles)+',Mobiles Processed: '+str(count_mobile_processed/2),'consumer_master')
			
print('Consumer master script completed. Mobiles Fetched: '+str(count_mobile)+',Valid Mobiles: '+str(valid_mobiles)+',Mobiles Processed: '+str(count_mobile_processed/2))
