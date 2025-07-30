from sqlalchemy import create_engine

from src.config.config import DB_URL

engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
    future=True
)

if __name__ == "__main__":
    print(DB_URL)
