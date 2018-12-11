import pymysql.cursors
import csv
from sys import argv
#import sys

#script, source, campaigns, date_from, date_to = argv

#resources
local_cdr = { 'host':'localhost',
    'user':'mgonzalez',
    'password':'asd123',
    'db':'asterisk'
    }

local_rates = { 'host':'localhost',
    'user':'mgonzalez',
    'password':'asd123',
    'db':'cdr'
    }

servers = { 'cdr':local_cdr,
    'rates':local_rates 
    }

def get_cdr():
    cdrdb = make_cdr(argv['source'],argv['campaigns'],argv['date_from'],argv['date_to'])

def make_cdr(source, campaigns, date_from, date_to):
    server_data = servers[source]
    connection = pymysql.connect(host=server_data['host'],
        user=server_data['user'],
        password=server_data['password'],
        db=server_data['db'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
        )
    
    with connection.cursor() as cursor:
        sql = """
            SELECT vlog.uniqueid as ID,
            vlog.campaign_id as Campaign,
            vlog.call_date as Date,
            vlog.user as Agent,
            CONCAT(vlog.phone_code ,vlog.phone_number) as Phone,
            vlog.length_in_sec as Length_Sec,
            CEILING(vlog.length_in_sec/60) as Length_Min,
            clog.channel as Channel
            FROM vicidial_log vlog
            LEFT JOIN call_log clog ON clog.uniqueid=vlog.uniqueid
            WHERE vlog.call_date >= '{date_from}' AND vlog.call_date < '{date_to}' AND ({campaigns});
            """
        cursor.execute(sql)
        cdrdb = cursor.fetchall()
        print(result)
        connection.close()
        return cdrdb

def get_destination(cursor, number):
    try:
        sql = "SELECT destination,prefix,replace(rate,',','.') AS rate FROM rates WHERE '"+str(int(number))+"' LIKE CONCAT(prefix,'%') ORDER BY prefix DESC LIMIT 1;"
        cursor.execute(sql)
        dest = cursor.fetchone()
        dest = dest['destination'] if not dest == None else 'Destino desconocido'
        cursor.fetchall()
    except ValueError:
        dest = 'Numero invalido'
    finally:
        return dest

def make_csv(cdrdb):
    fields = ['ID','Campaign','Date','Agent','Phone','Length_Sec','Length_Min','Destination','Channel']
    dest = servers['cdr']
    conn = pymysql.connect(host=dest['host'],
        user=dest['user'],
        password=dest['password'],
        db=dest['db'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    #csvf = StringIO()
    #cdrs = csv.DictWriter(csvf, delimiter=',',
    cdrs = csv.DictWriter(delimiter=',',

            quotechar='"', quoting=csv.QUOTE_MINIMAL, fieldnames=fields)
    cdrs.writeheader()
    with conn.cursor() as cursor:
        for i in cdrdb:
            i['Destination'] = get_destination(cursor,i['Phone'])
            cdrs.writerow(i)
    conn.close()
    #return (jsonify(result = cdrdb), csvf)
    return cdrdb
