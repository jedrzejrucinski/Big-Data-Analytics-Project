from mysql import connector


class MySQLClient:
    def __init__(self, host, user, password, database, port=3306, debug=False):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None
        self.debug = debug

    def connect(self):
        try:
            self.connection = connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port,
            )
            if self.debug:
                print(f"Connected to {self.database} on {self.host}:{self.port}")
        except connector.Error as err:
            raise RuntimeError(f"Failed to connect to MySQL: {err}")

    def disconnect(self):
        if self.connection:
            self.connection.close()
            if self.debug:
                print("Database connection closed.")

    def insert(self, query, values):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, values)
            self.connection.commit()
            if self.debug:
                print(f"Query executed: {query} with values {values}")
        except connector.Error as err:
            raise RuntimeError(f"Failed to execute insert: {err}")
        finally:
            cursor.close()

    def read(self, query):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            if self.debug:
                print(f"Query executed: {query}")
            return result
        except connector.Error as err:
            raise RuntimeError(f"Failed to execute read: {err}")
        finally:
            cursor.close()

    def fetch_one(self, query):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            if self.debug:
                print(f"Query executed: {query}")
            return result
        except connector.Error as err:
            raise RuntimeError(f"Failed to execute fetch_one: {err}")
        finally:
            cursor.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.disconnect()
