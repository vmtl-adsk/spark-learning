import sqlalchemy as db
from sqlalchemy.orm import Session


class DB_alchemy:

    db_engine = None
    db_con = None
    db_session = None
    metadata_obj = None

    def __init__(self, host: str, port: str, dbName: str, userName: str, pwd: str) -> None:

        # This will connect with a static DB
        #DB_URL = f'postgresql://{userName}:{pwd}@{host}:{port}/{dbName}'
        # This will connect with a postgres container DB (for containers,
        # which are connected to the same network)
        DB_URL = f'postgresql+psycopg2://{userName}:{pwd}@postgres:{port}/{dbName}'
        self.host = host
        self.port = port
        self.dbName = dbName
        self.userName = userName
        self.pwd = pwd

        self.db_engine = db.create_engine(DB_URL, echo=True)
        self.db_con = self.db_engine.connect()
        self.db_session = Session(self.db_engine)  # , future=True)

    def create_schema(self, schema_name):
        # Creating a schema 'public' if not exists
        if not self.db_engine.dialect.has_schema(self.db_engine, schema_name):
            self.db_engine.execute(db.schema.CreateSchema(self.db_engine))

    def create_metadata_obj(self, schema_name):
        self.metadata_obj = db.MetaData(schema=schema_name)

    def create_items_table(self):
        return db.Table(
            'item', self.metadata_obj,
            db.Column('item_id', db.Integer, primary_key=True),
            db.Column('item_name', db.String(50), nullable=False),
            db.Column('description', db.String(250), nullable=True),
            db.Column('price', db.Float(2), nullable=False),
            db.Column('tax', db.Float(2), nullable=True),
            db.Column('tax_with_price', db.Float(2))
        )

    def create_all(self):
        self.metadata_obj.create_all(self.db_engine, checkfirst=True)
