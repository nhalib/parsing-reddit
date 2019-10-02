from req_py_libraries import *

def set_conn(db_name):
    db = pymysql.connect(host='localhost',user='root',password='Vasily_1992',db=db_name,charset='utf8mb4')
    cursor = db.cursor()
    return(db,cursor)

def define_reddit_heat_bl(dbname,suid,db,cursor,conn):
    db_ara = conn[dbname]
    # for all stocks
    aql = "FOR u in time_series_reddit FILTER u._key == '{0}' RETURN u".format('suid_'+str(suid))
    resps = db_ara.AQLQuery(aql, rawResults=True, batchSize=1)

    heat_df = []
    if len(resps) > 0:
        tkeys = list(resps[0].keys())
        for tkey in tkeys:
            ddict = {}
            try:
                tdate = datetime.strptime(tkey,'%Y-%m-%d').date()
                ddict['date'] = tdate
                ddict['heat'] = float(resps[0][tkey])
                heat_df.append(ddict)
            except:
                pass

    heat_df = pd.DataFrame(heat_df)
    heat_df = heat_df.sort_values(by='date', ascending=False)
    if(len(heat_df)) < 60: #not enough data to make sense
        cursor.execute('INSERT INTO heats.social_heat (stock_uid,heat) VALUES ({0},"{1}")'.format(suid,str(0)))
    else:
        cursor.execute('SELECT heat from heats.social_heat WHERE stock_uid = {0}'.format(suid))
        rslt = cursor.fetchall()

        t = datetime.now().date()
        t_15 = t - timedelta(days=15)
        t_60 = t - timedelta(days=60)

        tdf = heat_df[(heat_df.date < t_15) & (heat_df.date > t_60)]
        bl = tdf['heat'].mean()

        if len(rslt) > 0:
            cursor.execute('UPDATE heats.social_heat SET heat = "{0}" WHERE stock_uid = {1}'.format(str(bl), suid))
        else:
            cursor.execute('INSERT INTO heats.social_heat (stock_uid,heat) VALUES ({0},"{1}")'.format(suid, str(bl)))

    db.commit()


#run every quarter to update baseline heat from reddit chatter for each stock
def main_reddit_bl():
    conn = Connection(username='root', password='Vasily_1992')
    db, cursor = set_conn('stocks_usa')
    db1, cursor1 = set_conn('stocks_usa')
    cursor1.execute('SELECT UID FROM stocks_usa.exch_tick_cik')
    for val in cursor1.fetchall():
        define_reddit_heat_bl(dbname='social_reddit',suid=val[0],db=db,cursor=cursor,conn=conn)