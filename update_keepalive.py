import base
import collections
from base import get_mongo_int,get_mongo_string,get_mongo_float,get_mongo_date

cursor = base.mysql.cursor()
cursor.execute("SELECT * FROM shmart_merchant.wallet_keepalive_sessions ORDER BY id DESC LIMIT 1")

result = cursor.fetchall()
row_count = cursor.rowcount

if row_count > 0:	
		for value in result:
			base.db.wallet_keepalive_session.remove({})
			base.db.wallet_keepalive_session.insert({'session_id':value['SessionID']})
