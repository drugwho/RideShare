import sqlite3

conn = sqlite3.connect('database.db')
print("Opened database successfully")


#to create the table
conn.execute('CREATE TABLE rides (ride_id integer not null primary key,\
                                    created_by varchar(50) not null,\
                                    timestamp varchar(20) not null,\
                                    source integer not null,\
                                    destination integer not null,\
                                    users varchar(300) not null )')


# #to read the contents
# cur = conn.cursor()
# print(cur.execute('SELECT * FROM rides'))

# rows = cur.fetchall()
# for row in rows:

#     print(row)


#to drop thetable
# cur = conn.cursor()
# cur.execute('DROP TABLE rides')


print("Table created successfully")