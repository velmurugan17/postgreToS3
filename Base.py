#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
import sys
"""{
  "2018-09-26":
  {
    "orders counts":"20",
    "gross":"20171616.00",
    "net":"2017100.00"
  },
  "2018-09-25":
  {
    "orders counts":"17",
    "gross":"87653431.00",
    "net":"8765300.00"
  },
  "2018-09-24":
  {
    "orders counts":"8",
    "gross":"53756566.00",
    "net":"53756500.00"
  }
}
"""

orders = (
    ("2018-09-26", '20', "20171616.00", "2017100.00"),
    ("2018-09-25","17","87653431.00","8765300.00"),
    ("2018-09-24","8","53756566.00","53756500.00")
)

try:

    con = psycopg2.connect(database='testdb2',user='postgres',password='velu123',host='127.0.0.1')
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS Orders")
    cur.execute("CREATE TABLE Orders(Date VARCHAR PRIMARY KEY, Count VARCHAR, Gross VARCHAR,Net VARCHAR)")
    query = "INSERT INTO Orders (Date, Count, Gross, Net) VALUES (%s, %s, %s, %s)"
    cur.executemany(query, orders)

    con.commit()
    cur.execute('SELECT * FROM Orders;')
    row = cur.fetchall()
    for r in row:
        print(r)

except psycopg2.DatabaseError as e:
    print('Error %s' % e)
    sys.exit(1)


finally:

    if con:
        con.close()
