# backend/app/core/database.py
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine
from .config import settings
import asyncio
from contextlib import asynccontextmanager

# Base pour les modèles SQLAlchemy - SINGLETON pour éviter les duplications
_Base = None

def get_base():
    """Retourne une instance unique de Base"""
    global _Base
    if _Base is None:
        _Base = declarative_base()
    return _Base

# Utiliser la base singleton
Base = get_base()

# Moteur async pour les opérations principales
async_engine = create_async_engine(
    settings.ASYNC_DATABASE_URL,
    echo=False,  # True pour debug SQL
    pool_size=10,
    max_overflow=20
)

# Session factory async
AsyncSessionLocal = async_sessionmaker(
    async_engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# Dependency pour FastAPI - VERSION CORRIGÉE
async def get_async_session():
    """Générateur de session async pour FastAPI"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

# Context manager pour utilisation directe - NOUVELLE FONCTION
@asynccontextmanager
async def get_async_session_context():
    """Context manager pour session async"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

# Fonction synchrone pour tests simples
async def test_async_connection():
    """Test simple de connexion async"""
    async with AsyncSessionLocal() as session:
        try:
            from sqlalchemy import text
            result = await session.execute(text("SELECT 1 as test"))
            row = result.fetchone()
            return row[0] == 1
        except Exception as e:
            print(f"Erreur test connexion: {e}")
            return False
        finally:
            await session.close()