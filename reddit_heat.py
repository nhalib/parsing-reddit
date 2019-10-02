#frequency yet to be set

from req_py_libraries import *

def set_conn(db_name):
    db = pymysql.connect(host='localhost',user='root',password='Vasily_1992',db=db_name,charset='utf8mb4')
    cursor = db.cursor()
    return(db,cursor)


def core_extract(key_word):
    time.sleep(2)
    link = "https://www.reddit.com/r/finance+stocks+options/search.json?q="+key_word+"&sort=new&restrict_sr=1&limit=100"
    user_agent = {'User-Agent':'Mozilla/5.0'}
    response=requests.get(link,headers=user_agent).text
    return json.loads(response)


def reddit_mention(tmax,ticker='XLNX'):
    data = core_extract(ticker)
    data = data['data']['children']
    count = 0
    for post in data:
        post_id=post['data']['name']
        #post_title = post['data']['title']
        #post_text = post['data']['selftext']
        post_utc = post['data']['created']
        if float(post_utc) > tmax:
            tmax = float(post_utc)
            count = count + 1


    post_count = count

    return [post_count,tmax]

def process_base(ticker,suid):
    date_today = datetime.now().date()
    conn = Connection(username='root', password='Vasily_1992')
    db = conn['social_reddit']

    try:
        coll0 = db['time_series_reddit']
    except:
        coll0 = db.createCollection(name='time_series_reddit')

    try:
        coll1 = db['date_key_reddit']
    except:
        coll1 = db.createCollection(name='date_key_reddit')

    try:
        doc0 = coll0['suid_'+str(suid)]
    except:
        doc0 = coll0.createDocument()
        doc0._key = 'suid_'+str(suid)

    try:
        doc1 = coll1['suid_'+str(suid)]
        doc1['latest_date'] = str(date_today)
    except:
        doc1 = coll1.createDocument()
        doc1._key = 'suid_'+str(suid)


    tmax = doc1['tmax']
    if tmax == None:
        tmax = 0
    latest_date = doc1['latest_date']
    if latest_date == None:
        latest_date = str(date_today)

    [post_count,tmax]=reddit_mention(tmax = tmax,ticker=ticker)
    doc0[latest_date] = post_count
    doc0.save()
    doc1['tmax'] = tmax
    doc1['latest_date'] = str(date_today)
    doc1.save()

def define_reddit_heat(dbname,suid):
    db,cursor = set_conn('stocks_usa')
    conn = Connection(username='root',password='Vasily_1992')
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




def temp_operation():
    db, cursor = set_conn('stocks_usa')
    cursor.execute('SELECT UID,ticker FROM exch_tick_cik')
    stocks = cursor.fetchall()
    db.close()
    count = 1
    for val in stocks:
        print(val)
        count = count + 1
        define_reddit_heat(dbname='social_reddit', suid=val[0])


def daily_reddit_heat_routine():
    f = open('reddit_flag.txt')
    for line in f:
        start_pt = int(line.rstrip())
    f.close()

    db,cursor=set_conn('stocks_usa')
    cursor.execute('SELECT UID,ticker FROM exch_tick_cik WHERE UID >= {0} LIMIT 100'.format(str(start_pt)))
    stocks = cursor.fetchall()
    db.close()
    count = 1
    for val in stocks:
        process_base(ticker=val[1],suid=val[0])
        count = count + 1

        end_stat = str(val[0]+1)

    if len(stocks) == 0:
        end_stat = 9999

    f = open('flags/reddit_flag.txt','w',newline='\n')
    f.write(str(end_stat))
    f.write('\n')
    f.close()
