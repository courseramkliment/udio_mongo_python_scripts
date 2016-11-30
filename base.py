import MySQLdb
import pymongo
from pymongo import MongoClient
import time
from datetime import datetime, date, timedelta
import MySQLdb.cursors
from dateutil.parser import parse

def dd(msg):
    print(msg)
    exit()

def log_it(message='',file=''):
    if message != '' and file != '':
        fp = open('log1/'+file+'.log', 'a');
        fp.write(time.strftime("%Y-%m-%d %H:%M:%S")+','+message+'\n')
        fp.close()

def is_clear(value):
    if value == '0000-00-00 00:00:00':
        return False
    if value == '0000-00-00':
        return False
    if value == None:
        return False
    if value == 'None':
        return False
    if value == '':
        return False
    return True

def get_mongo_date(date):
    if is_clear(date) == False:
        return ''
    dt = parse(str(date))
    return datetime.strptime(str(dt),"%Y-%m-%d %H:%M:%S")

def get_mongo_int(value):
    if is_clear(value) == False:
        return ''
    try:
        return_value = int(str(value).encode('utf-8'))
    except Exception as e:
        try:
            return_value = str(value).encode('utf-8')
        except:
            try:
                return_value = str(value).decode('utf-8')
            except:
                return_value = ''
    return return_value
    
def get_mongo_float(value):
    if is_clear(value) == False:
        return ''
    try:
        return_value = float(str(value).encode('utf-8'))
    except Exception as e:
        try:
            return_value = str(value).encode('utf-8')
        except:
            try:
                return_value = str(value).decode('utf-8')
            except:
                return_value = ''
        
    return return_value

def get_mongo_string(value):
    if is_clear(value) == False:
        return ''
    try:
        return_value = str(value).encode('utf-8')
    except:
        try:
            return_value = str(value).decode('utf-8')
        except:
            return_value = ''
        
    return return_value.strip()
    

def write_cms_cron_collection(txn_code,created_date,txn_type):
    db.scm_cron_txn.insert({
        'cron_status':'pending',
        'transaction_type':txn_type,
        'transaction_id':txn_code,
        'created_date':get_mongo_date(created_date),
        'updated_at':''
    })

def write_cms_cron_collection_new_registration(mobile,created_date,user_type):
    db.campaign_transactions.insert({
        'status':'pending',
        'type':'new_registration',
        'transaction_id':mobile,
        'created_date':get_mongo_date(created_date),
        'updated_at':'',
        'user_type': user_type,
        'campaign_id':0
    })

#Open MySQL database connection
try:
    mysql = MySQLdb.connect("192.168.100.114","mongo_analytics","MjA1m%^&*)(#837","shmart", cursorclass=MySQLdb.cursors.DictCursor)
except Exception as e:
    log_it('Could not connect to MySQL','base')
    exit()

#make connection with Mongo
try:
    mongo = MongoClient("mongodb://root:ikdhil20@localhost:27017/admin")
    db = mongo.analytics_db

except Exception as e:
    log_it('Could not connect to MongoDB','base')
    exit()
