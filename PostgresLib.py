import psycopg2
import json

def get_cred():
    """

    :return:
    """
    with open('user.config') as f:
        data = json.load(f)
    return data


class PostgreDB():
    def __init__(self):
        cred = get_cred()
        self.db, self.usr, self.pwd, self.host = cred['database'], cred['user'], cred['password'], cred['host']
        self.con = psycopg2.connect(database=self.db, user=self.usr, password=self.pwd, host=self.host)
        self.cur = self.con.cursor()

    def create_table(self):
        """

        :return:
        """
        self.cur.execute("DROP TABLE IF EXISTS Orders")
        self.cur.execute("CREATE TABLE Orders(Date VARCHAR PRIMARY KEY, Count VARCHAR, Gross VARCHAR,Net VARCHAR)")
        return self.cur

    def add_data_to_table(self, row):
        """
        Currently implemented with hard coded value.will update it later
        :param query:
        :return:
        """
        rows = (
            ("2018-09-26", '20', "20171616.00", "2017100.00"),
            ("2018-09-25", "17", "87653431.00", "8765300.00"),
            ("2018-09-24", "8", "53756566.00", "53756500.00")
        )
        query = "INSERT INTO Orders (Date, Count, Gross, Net) VALUES (%s, %s, %s, %s)"

        self.cur.executemany(query, rows)

    def execute_query(self, query):
        """

        :param query:
        :return:
        """
        query = 'SELECT * FROM Orders;'
        self.cur.execute(query)
        rows = self.cur.fetchall()
        return rows

    def close_connection(self):
        """

        :return:
        """
        self.con.close()
