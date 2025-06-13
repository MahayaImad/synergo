# backend/app/models/sync_models.py
from sqlalchemy import Column, Integer, String, BigInteger, DateTime, Boolean, Text, DECIMAL
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from ..core.database import Base


class SyncTable(Base):
    """Configuration des tables à synchroniser"""
    __tablename__ = "sync_tables"
    __table_args__ = {'schema': 'synergo_sync', 'extend_existing': True}

    id = Column(Integer, primary_key=True)
    table_name = Column(String(100), unique=True, nullable=False)
    hfsql_table = Column(String(100), nullable=False)
    sync_strategy = Column(String(20), default='ID_BASED')
    is_active = Column(Boolean, default=True)
    sync_interval_minutes = Column(Integer, default=30)
    batch_size = Column(Integer, default=1000)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class SyncState(Base):
    """État de synchronisation des tables"""
    __tablename__ = "sync_state"
    __table_args__ = {'schema': 'synergo_sync', 'extend_existing': True}

    id = Column(Integer, primary_key=True)
    table_name = Column(String(100), unique=True, nullable=False)
    last_sync_id = Column(BigInteger, default=0)
    last_sync_timestamp = Column(DateTime(timezone=True))
    last_sync_date = Column(String(8))  # Format HFSQL YYYYMMDD
    last_sync_time = Column(String(6))  # Format HFSQL HHMMSS
    total_records = Column(BigInteger, default=0)
    last_sync_duration = Column(Integer)  # secondes
    last_sync_status = Column(String(20), default='PENDING')
    error_message = Column(Text)
    records_processed_last_sync = Column(Integer, default=0)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class SyncLog(Base):
    """Logs des opérations de synchronisation"""
    __tablename__ = "sync_log"
    __table_args__ = {'schema': 'synergo_sync', 'extend_existing': True}

    id = Column(Integer, primary_key=True)
    table_name = Column(String(100))
    operation = Column(String(20))  # 'SYNC_START', 'SYNC_END', 'INSERT', 'UPDATE', 'ERROR'
    hfsql_id = Column(BigInteger)
    postgres_id = Column(BigInteger)
    records_processed = Column(Integer, default=0)
    processing_time_ms = Column(Integer)
    error_details = Column(JSONB)
    created_at = Column(DateTime(timezone=True), server_default=func.now())