
#Simple Extract and Load python script for Postgress DB
import petl as etl, psycog2 as pg, sys 
from sqlalchemy import * 

reload(sys)
sys.setdefaultencoding('utf8')

#Database Connections

db_conns = { 'test_db':"dbname=test_db user=db_user host=127.0.0.1",
			'python':"dbname=python user=db_user host=127.0.0.1"}

#Set Connections and Cursor Objects

sourceConn = pg.connect(db_conns['test_db'])
targetConn = pg.connect(db_conns['python'])
sourceCursor = sourceConn.cursor()
targetCursor = targetConn.cursor()

#Query of tables to export 

sourceCursor.execute("""SELECT table_name
			FROM info_schema
			WHERE table_name = 'public'
			AND table_name in ('returns', 'salesperson')
			GROUP BY 1""")

#Create variable to store result of cursor, which we get by fetchall() method

sourceTables = sourceCursor.fetchall()

#Now itterate through sourceTables

for t in  sourceTables:
	#Drop table if it already exists in our destination, we drop it first.
	targetCursor.execute("drop table if exists %s" % (t[0]))
	sourceDS = etl.fromdb(sourceConn, "SELECT * FROM %s" % (t[0]))
	etl.todb = (sourceDS, targetConn, t[0], create=True, sample=1000)
#Close Cursor connection
sourceCursor.close()
targetCursor.close()

#Close DataBase connections

db_conns.close()














