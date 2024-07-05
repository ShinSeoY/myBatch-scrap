from sqlalchemy.orm import Session
from database.db_config import get_db, engine
from database.model.exchange import Base, Exchange

# 데이터베이스 테이블 생성
Base.metadata.create_all(bind=engine)

def insert_exchange(db: Session, exchange: Exchange):
    db.add(exchange)
    db.commit()
    db.refresh(exchange)
    return exchange

def get_exchange(db: Session, unit: str):
    return db.query(Exchange).filter(Exchange.unit == unit).first()

# 사용 예시
if __name__ == "__main__":
    db = next(get_db())
    
    exchange = Exchange(
        unit='USDDDDD-01',
        name='US Dollar',
        kr_unit='달러',
        deal_bas_r=1200.50, 
        exchange_rate=1200.50,
        ttb=1199.50,
        tts=1201.50
    )
    
    # 데이터 삽입
    insert_exchange(db, exchange)
    
    # 데이터 조회
    exchange = get_exchange(db, "USDDDDD")
    if exchange:
        print(f"Exchange rate for {exchange.unit}: {exchange.exchange_rate}")
    else:
        print("Exchange rate not found")