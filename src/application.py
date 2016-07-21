import os, sys,time,csv
import boto
import MySQLdb
import urllib
import memcache
from flask import Flask,request,render_template,redirect,url_for

application = Flask(__name__)

def movetoS3(file):
    starttime = time.time()
    inputfile = file.filename
    x1 = open(inputfile, 'wb')
    readfile = file.read()
    x1.write(readfile)
    x1.close()
    s3 = boto.connect_s3('UserName', 'password')
    bucket = s3.create_bucket('bucketname')  # bucket names must be unique
    key = bucket.new_key(inputfile)
    key.set_contents_from_filename(inputfile)
    key.set_acl('public-read')
    endtime = time.time() - starttime
    return endtime

def createtable():
    createmytable = """create table if not exists ebdb.earthquake(
    rownum int not null primary key AUTO_INCREMENT,
    time char(24) not null,
    latitude DOUBLE not null,
    longitude double not null,
    depth double not null,
    mag double,
    magType	varchar(5),
    nst	int,
    gap	double,
    dmin double,
    rms	double,
    net	varchar(10) not null,
    id varchar(18) not null,
    updated char(24) not null,
    place varchar(90) not null,
    type varchar(15) not null,
    horizontalError	double,
    depthError double,
    magError double,
    magNst int,
    status varchar(10) not null,
    locationSource varchar(5) not null,
    magSource varchar(5) not null);"""

    db1 = MySQLdb.connect(host="amazonaws url",
                          user="UserName",  # your username
                          passwd="password",  # your password
                          db="dbname",
                          port=3306,
                          local_infile=1)

    cur1 = db1.cursor()
    starttime = time.time()
    cur1.execute(createmytable)
    endtime = time.time() - starttime
    db1.commit()
    return endtime


def loadtable():

    my_url_load = 'https://s3.amazonaws.com/bucketname/all_month.csv'
    opener = urllib.URLopener()
    x1 = open('file2.csv', 'wb')
    writer = csv.writer(x1, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    first = 1
    f = opener.open(my_url_load)
    reader = csv.reader(f)
    for row in reader:
        if first == 1:
            first = 0
            pass
        else:
            writer.writerow(row)

    f.close()
    x1.close()

    loaddata = """LOAD DATA LOCAL INFILE 'file2.csv' INTO TABLE earthquake FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'  LINES TERMINATED BY '\n' IGNORE 1 LINES
        (TIME, LATITUDE, LONGITUDE, DEPTH, @MAG, @MAGTYPE, @NST, @GAP, @DMIN, @RMS, NET, ID, UPDATED, PLACE, TYPE, @HORIZONALERROR, @DEPTHERROR, @MAGERROR, @MAGNST, STATUS, LOCATIONSOURCE, MAGSOURCE)
        SET MAG = NULLIF(@MAG,''), MAGTYPE = NULLIF(@MAGTYPE,''), NST = NULLIF(@NST,''), GAP = NULLIF(@GAP,''), DMIN = NULLIF(@DMIN,''), RMS = NULLIF(@RMS,''), HORIZONTALERROR = NULLIF(@HORIZONTALERROR,''),
        DEPTHERROR = NULLIF(@DEPTHERROR,''), MAGERROR = NULLIF(@MAGERROR,''), MAGNST = NULLIF(@MAGNST,''), ROWNUM = NULL;"""


    db1 = MySQLdb.connect(host="amazonaws url",
                          user="Username",  # your username
                          passwd="password",  # your password
                          db="ebdb",
                          port=3306,
                          local_infile=1)


    cur1 = db1.cursor()
    starttime = time.time()
    cur1.execute(loaddata)
    endtime = time.time() - starttime
    db1.commit()
    return endtime

def xThousandTimes(times, query):
    #endRange = get_id()
    starttime = time.time()  # record start time
    db1 = MySQLdb.connect(host="amazonaws url",
                          user="Username",  # your username
                          passwd="password",  # your password
                          db="ebdb",
                          port=3306,
                          local_infile=1)

    cur1 = db1.cursor()
    for num in range(int(times)):
        #randomNumber = random.randint(1, endRange)
        #dynamicQuery = """SELECT * FROM earthquake where rownum = """ + str(randomNumber) + """;"""
        try:
            cur1.execute(query)
            rows = cur1.fetchall()
        except:
            print "Error-p1"
            continue
    endtime = time.time() - starttime  # record end time
    return endtime

def xThousandTimescache(times,query):
    memc = memcache.Client(['amazonawsurl:portnumber'], debug=1);
    key = query
    db1 = MySQLdb.connect(host="amazonaws url",
                          user="Username",  # your username
                          passwd="password",  # your password
                          db="ebdb",
                          port=3306,
                          local_infile=1)

    cur1 = db1.cursor()
    starttime = time.time()  # record start time

    for num in range(int(times)):  # numbers from 1 - xThousand
        query = """SELECT * FROM earthquake where id = 'uw61139602';"""
        cacheval = memc.get(key)
        try:
            print 'try'
            cacheval = memc.get(key)
            print 'harsha'
            print cacheval
            if not cacheval:
                #cursor = conn.cursor()
                cur1.execute(query)
                rows = cur1.fetchall()
                #memc.set_multi(key,rows)
                #df = DataFrame(cur1.fetchall())

                for row in rows:
                    #val = row[0]
                    memc.set(key, row)
                    memc.get(key)
                    sys.exit(0)
                    #print "Updated memcached with MySQL data"

            else:
                return
                '''
                #print "Loaded data from memcached"
                for row in cacheval:
                    #print "%s" % (row[0])
                '''
		    #continue
        except:
            print "Error-p3"
            continue
    endtime = time.time() - starttime  # record end time
    return endtime

@application.route('/userinput', methods = ['POST','GET'])
def userinput():
    if request.method == 'POST':
        if request.form['submit'] == 'Execute with RDB':
            times = request.form['times']
            query = request.form['query']
            endtime = xThousandTimes(times,query)
            result = 'Time taken to execute the SQL in RDB:'
            return render_template('userinput.html',time=endtime,result=result)
        elif request.form['submit'] == 'Execute with Cache':
            times = request.form['times']
            query = request.form['query']
            endtime = xThousandTimescache(times, query)
            result = 'Time taken to execute the SQL in RDB:'
            return render_template('userinput.html', time=endtime,result=result)
    return render_template('userinput.html')


@application.route('/', methods = ['POST','GET'])
def index():
    if request.method == 'POST':
        if request.form['submit'] == 'Move File to S3':
            file = request.files['fileToUpload']
            #print file.filename
            endtime = movetoS3(file)
            result = 'Time taken to move the file to S3:'
            return render_template('index.html', result=result,time=endtime)
        elif request.form['submit'] == 'Create Table':
            endtime = createtable()
            result = 'Time taken to Create table:'
            return render_template('index.html', result=result, time=endtime)
        elif request.form['submit'] == 'Move S3 to RDS':
            endtime = loadtable()
            result = 'Time taken to Load table:'
            return render_template('index.html', result=result, time=endtime)
        elif request.form['submit'] == 'Take user input':
            return redirect(url_for('userinput'))

    return render_template('index.html')
    #return 'hello world'


if __name__ == '__main__':
    application.run()
