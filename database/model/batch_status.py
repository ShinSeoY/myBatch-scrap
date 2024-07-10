from sqlalchemy import Column, String, Float, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class BatchStatus(Base):
    __tablename__ = "batch_status2"

    run_id = Column(String(50), primary_key=True, index=True)
    dag_name = Column(String(100))
    status = Column(String(50))
    duration = Column(Float)
    failed_step_name = Column(String(50))
    err_msg = Column(Text)
    start_time = Column(DateTime(timezone=True), server_default=func.now(), default=func.now())
    end_time = Column(DateTime(timezone=True), server_default=func.now(), default=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now(), default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), default=func.now())
    
    def __repr__(self):
        return f"<BatchStatus(run_id={self.run_id}, dag_name={self.dag_name}, status={self.status})>"
