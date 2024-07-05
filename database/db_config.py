from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

# .env 파일 로드 (경로를 정확히 지정)
load_dotenv(dotenv_path='../../.env')

db_config = {
    'host': os.getenv('DB_HOST', '127.0.0.1'),  # 기본값 설정
    'database': os.getenv('DB_NAME', 'MY_EXCHANGE'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', 'nextage2020!')
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