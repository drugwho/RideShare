import sqlite3

conn = sqlite3.connect('database.db')
print("Opened database successfully")


#to create the table
conn.execute('CREATE TABLE users (username varchar(50) not null primary key,\
                                     password char(40) not null)')



# #to read the contents
# cur = conn.cursor()
# print(cur.execute('SELECT * FROM users'))
# rows = cur.fetchall()
# for row in rows:
#     print(row)


#to drop table
# cur = conn.cursor()
# cur.execute('DROP TABLE users')



print("Table created successfully")