from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

db_config = {
    'host': '127.0.0.1',
    'database': 'MY_EXCHANGE',
    'user': 'root',
    'password': 'nextage2020!'
}

DATABASE_URL = f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()