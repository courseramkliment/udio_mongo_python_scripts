import base
import collections
from base import get_mongo_int,get_mongo_string,get_mongo_float,get_mongo_date

def mongo_write(value,add_cms_cron_collection=True):
    try:
        result = base.db.card_txn.insert({
                "primary_id" : get_mongo_int(value['primary_id']),
                "consumer_id" : get_mongo_int(value['consumer_id']),
                "transaction_id" : get_mongo_string(value['transaction_id']),
                "mobile" : get_mongo_string(value['mobile']),
                "amount" : get_mongo_float(value['amount']),
                "txn_type" : get_mongo_string(value['txn_type']),
                "narration" : get_mongo_string(value['narration']),
                "card_type" : get_mongo_string(value['card_type']),
                "mcc_code" : get_mongo_string(value['mcc_code']),
                "status" : get_mongo_string(value['status']),
                "created_date" : get_mongo_date(value['created_date']),
                "card_number" : get_mongo_string(value['card_number']),
                "merchant_id" : get_mongo_int(value['merchant_id']),
                'channel_type': 			'POS',
                'channel_value': 			'UDIO_CARD',
                "extra" : {
                        "product_id" : get_mongo_int(value['product_id']),
                        "customer_master_id" : get_mongo_int(value['customer_master_id']),
                        "cardholder_id" : get_mongo_int(value['cardholder_id']),
                        "purse_master_id" : get_mongo_int(value['purse_master_id']),
                        "customer_purse_id" : get_mongo_int(value['customer_purse_id']),
                        "amount_billed" : get_mongo_float(value['amount_billed']),
                        "currency_iso" : get_mongo_string(value['currency_iso']),
                        "txn_code" : get_mongo_int(value['txn_code']),
                        "fee" : get_mongo_float(value['fee']),
                        "service_tax" : get_mongo_float(value['service_tax']),
                        "surcharge" : get_mongo_float(value['surcharge']),
                        "wallet_code" : get_mongo_string(value['wallet_code']),
                        "channel_crd_txn" : get_mongo_string(value['channel_crd_txn']),
                        "date_reversal" : get_mongo_date(value['date_reversal']),
                        "failed_reason" : get_mongo_string(value['failed_reason']),
                        "status_ack" : get_mongo_string(value['status_ack']),
                        "txn_customer_master_id" : get_mongo_int(value['txn_customer_master_id']),
                        "txn_agent_id" : get_mongo_int(value['txn_agent_id']),
                        "is_virtual" : get_mongo_string(value['is_virtual']),
                        "txn_corporate_id" : get_mongo_int(value['txn_corporate_id']),
                        "txn_ops_id" : get_mongo_int(value['txn_ops_id']),
                        "ip_address" : get_mongo_string(value['ip_address']),
                        "remarks" : get_mongo_string(value['remarks']),
                        "crn" : get_mongo_string(value['crn']),
                        "card_pack_id" : get_mongo_string(value['card_pack_id']),
                        "afn" : get_mongo_string(value['afn']),
                        "medi_assist_id" : get_mongo_string(value['medi_assist_id']),
                        "employee_id" : get_mongo_string(value['employee_id']),
                        "partner_ref_no" : get_mongo_string(value['partner_ref_no']),
                        "txnrefnum" : get_mongo_string(value['txnrefnum']),
                        "name_on_card" : get_mongo_string(value['name_on_card']),
                        "gender" : get_mongo_string(value['gender']),
                        "date_of_birth" : get_mongo_date(value['date_of_birth']),
                        "city" : get_mongo_string(value['city']),
                        "state" : get_mongo_string(value['state']),
                        "pincode" : get_mongo_int(value['pincode']),
                        "is_card_activated" : get_mongo_string(value['is_card_activated']),
                        "activation_date" : get_mongo_date(value['activation_date']),
                        "is_card_dispatched" : get_mongo_string(value['is_card_dispatched']),
                        "card_dispatch_date" : get_mongo_date(value['card_dispatch_date']),
                        "channel_corp_crdholder" : get_mongo_string(value['channel_corp_crdholder']),
                        "date_blocked" : get_mongo_string(value['date_blocked']),
                        "processor" : get_mongo_string(value['processor']),
                        "block_type" : get_mongo_string(value['block_type']),
                        "failed_reason_corp_crdholder" : get_mongo_string(value['failed_reason_corp_crdholder']),
                        "date_failed" : get_mongo_date(value['date_failed']),
                        "status_ecs" : get_mongo_string(value['status_ecs']),
                        "status_ops" : get_mongo_string(value['status_ops']),
                        "status_corp_crdholder" : get_mongo_string(value['status_corp_crdholder']),
                        "aml_status" : get_mongo_int(value['aml_status']),
                        "mcc_category" : get_mongo_string(value['mcc_category']),
                        "mcc_sub_category" : get_mongo_string(value['mcc_sub_category']),
                        "name_of_customer" : get_mongo_string(value['name_of_customer']),
                        "consumer_email" : get_mongo_string(value['consumer_email']),
                        "registration_mode" : get_mongo_string(value['registration_mode']),
                        "login_type" : get_mongo_string(value['login_type']),
                        "general_cash_amount" : get_mongo_float(value['general_cash_amount']),
                        "consumer_created" : get_mongo_date(value['consumer_created'])
                }}
            )
        if add_cms_cron_collection:
            if value['status'] != 'RATP':
                base.write_cms_cron_collection(value['txn_code'],value['created_date'],'card_txn')
    except Exception as e:
        base.log_it(str(e),'card_txn_error')
        return False

    return True

def start_fetch(date1,date2):
    try:
        cursor = base.mysql.cursor()
        cursor.execute("SELECT \
    d.id AS consumer_id, \
    CONCAT('POS',a.txn_no) AS transaction_id, \
    c.mobile, \
    a.amount_txn AS amount, \
    a.txn_type, \
    a.narration, \
    c.card_type, \
    a.mcc_code, \
    a.status, \
    a.date_created AS created_date, \
    a.card_number, \
    '' AS merchant_id, \
    a.id AS primary_id, \
    a.product_id, \
    a.customer_master_id,a.cardholder_id, \
    a.purse_master_id, \
    a.customer_purse_id, \
    a.amount_billed, \
    a.currency_iso, \
    a.txn_code, \
    a.fee, \
    a.service_tax, \
    a.surcharge, \
    a.wallet_code, \
    a.channel AS channel_crd_txn, \
    a.date_reversal, \
    a.failed_reason, \
    a.status_ack, \
    b.txn_customer_master_id, \
    b.txn_agent_id, \
    b.txn_corporate_id, \
    b.is_virtual, \
    b.txn_ops_id, \
    b.ip AS ip_address, \
    b.amount AS amount_legay, \
    b.remarks, \
    c.crn, \
    c.card_pack_id, \
    c.afn, \
    c.medi_assist_id,c.employee_id, \
    c.partner_ref_no, \
    c.txnrefnum, \
    c.name_on_card, \
    c.gender, \
    c.date_of_birth, \
    c.city, \
    c.state, \
    c.pincode, \
    c.is_card_activated, \
    c.activation_date, \
    c.is_card_dispatched, \
    c.card_dispatch_date, \
    c.channel AS channel_corp_crdholder, \
    c.date_blocked, \
    c.block_type, \
    c.processor, \
    c.failed_reason AS failed_reason_corp_crdholder, \
    c.date_failed, \
    c.status_ecs, \
    c.status_ops, \
    c.status AS status_corp_crdholder, \
    c.aml_status, \
    mcc.category AS mcc_category, \
    mcc.sub_category AS mcc_sub_category, \
    d.name_of_customer, \
    d.email AS consumer_email, \
    d.registration_mode, \
    d.login_type, \
    d.created AS consumer_created, \
    CASE WHEN b.purse_master_id=17 THEN b.amount ELSE 0 END AS general_cash_amount \
    FROM  shmart.card_txn_processing a \
    LEFT JOIN shmart.rat_txn_customer b ON a.txn_code=b.txn_code AND a.purse_master_id=b.purse_master_id AND \
    b.customer_master_id<>5 AND b.date_created BETWEEN %(date1)s AND %(date2)s \
    LEFT JOIN shmart.rat_corp_cardholders c ON a.customer_master_id=c.customer_master_id \
    LEFT JOIN shmart.mcc_master mcc ON a.mcc_code=mcc.mcc_code \
    LEFT JOIN shmart_consumer.users d ON c.mobile=d.username \
    WHERE a.txn_type IN ('CATP','RATP') \
    AND a.date_created BETWEEN %(date1)s AND %(date2)s",{'date1':date1,'date2':date2})
                        
        result = cursor.fetchall()
        row_count = cursor.rowcount
        
    except Exception as e:
        base.log_it('Could not fetch data from mysql','card_txn')
        return False

    i = 0
    j = 0
    if row_count > 0:	
        for value in result:
                res = mongo_write(value)
                if res == True:
                    i += 1
                if res == False:
                    base.db.card_txn.remove({'primary_id':int(value['primary_id'])})
                    try:
                        res = mongo_write(value,False)
                    except Exception as e:
                        base.log_it(str(e),'card_txn_error')
                    j += 1
                try:
                    base.db.consumer_info_persona.update({'consumer_id':int(value['consumer_id'])},{'$set':{'last_activity_date':get_mongo_date(value['created_date'])}})	
                except Exception as e:
                    base.log_it('Exception: '+str(e),'card_txn_error')
    else:
        base.log_it('Null retrieved from MySQL','card_txn')
        return False

    base.log_it('Script ran successfully,'+str(date1)+'-'+str(date2)+',total_records_fetched('+str(row_count)+'),total_recotrds_inserted('+str(i)+'),total_records_upserted('+str(j)+')','card_txn')

