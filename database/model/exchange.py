from sqlalchemy import Column, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class Exchange(Base):
    __tablename__ = "exchange"

    unit = Column(String(50), primary_key=True, index=True)
    name = Column(String(100))
    kr_unit = Column(String(50))
    deal_basr = Column(Float)
    exchange_rate = Column(Float)
    ttb = Column(Float)
    tts = Column(Float)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), default=func.now())
    
    def __repr__(self):
        return f"<ExchangeRate(id={self.id}, unit={self.unit}, name={self.name})>"
    
    