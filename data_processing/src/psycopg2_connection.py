from psycopg2 import connect, sql


class PostgreConnection:

    connection = None

    def __init__(self, host: str, port: str, dbName: str, userName: str, pwd: str) -> None:
        self.host = host
        self.port = port
        self.dbName = dbName
        self.userName = userName
        self.pwd = pwd

        self.connection = connect(dbname=dbName, user=userName,
                                  password=pwd, host=host, port=port)

        if self.connection.status == 1:
            print('Connection to the database established')
        else:
            print('There was an error connecting to the database')

    def checkConnectionStatus(self):
        print(self.connection.status)

    def cur(self):
        return self.connection.cursor()

    def executeQuery(self, query: str, result: bool):
        q = sql.SQL(query)
        self.cur().execute(q)
        if result:
            return self.cur().fetchall()


# QUERIES EXAMPLE
# query = sql.SQL('SELECT* FROM {}').format(sql.Identifier(schema, table))
# query = sql.SQL('SELECT * FROM public.versions;')
# query = sql.SQL(f'SELECT * FROM {schema}.{table};')
