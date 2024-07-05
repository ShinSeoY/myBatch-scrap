from sqlalchemy.orm import Session
from database.db_config import get_db, engine
from database.model.exchange import Base, Exchange

# 데이터베이스 테이블 생성
Base.metadata.create_all(bind=engine)

def insert_exchange(db: Session, currency_pair: str, rate: float):
    new_exchange = Exchange(currency_pair=currency_pair, rate=rate)
    db.add(new_exchange)
    db.commit()
    db.refresh(new_exchange)
    return new_exchange

def get_exchange(db: Session, currency_pair: str):
    return db.query(Exchange).filter(Exchange.currency_pair == currency_pair).first()

# 사용 예시
if __name__ == "__main__":
    db = next(get_db())
    
    # 데이터 삽입
    insert_exchange(db, "USD/KRW", 1200.50)
    
    # 데이터 조회
    exchange = get_exchange(db, "USD/KRW")
    if exchange:
        print(f"Exchange rate for {exchange.currency_pair}: {exchange.rate}")
    else:
        print("Exchange rate not found")