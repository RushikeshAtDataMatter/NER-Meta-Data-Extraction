import sqlalchemy as bd
from sqlalchemy.orm import sessionmaker

# Database configuration
host = 'localhost'
port = '5432'
database = 'DMT'
password = 'rushikeshwayal'

# Create the SQLAlchemy engine
DATABASE_URL = f'postgresql://postgres:{password}@{host}:{port}/{database}'
engine = bd.create_engine(DATABASE_URL)

# Create a session maker
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

print("Database connection successful!")
