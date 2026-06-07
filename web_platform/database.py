import os
import json
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine, Column, String, Integer, Float, Boolean, Text, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

# Resolve DATA_DIR locally to avoid circular imports with app.py
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "web_data"

DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL:
    # Render PostgreSQL provides url starting with postgres://, but SQLAlchemy requires postgresql://
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
else:
    # Fallback to local SQLite database in web_data
    sqlite_path = DATA_DIR / "db.sqlite3"
    # Ensure parent folder exists
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f"sqlite:///{sqlite_path}", connect_args={"check_same_thread": False})

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Job(Base):
    __tablename__ = "jobs"
    
    job_id = Column(String(32), primary_key=True, index=True)
    status = Column(String(20), default="queued") # queued, running, done, failed
    message = Column(String(255), nullable=True)
    error = Column(Text, nullable=True)
    video_name = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    config_json = Column(Text, nullable=True)
    low_options_json = Column(Text, nullable=True)
    high_options_json = Column(Text, nullable=True)
    summary_json = Column(Text, nullable=True)
    
    pairs = relationship("PairCandidate", back_populates="job", cascade="all, delete-orphan")

class PairCandidate(Base):
    __tablename__ = "pairs"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    job_id = Column(String(32), ForeignKey("jobs.job_id"))
    pair_id = Column(Integer)
    segment_id = Column(String(50))
    low_idx = Column(Integer)
    high_idx = Column(Integer)
    selected_high_idx = Column(Integer)
    low_path = Column(String(512))
    high_path = Column(String(512))
    low_brightness = Column(Float)
    high_brightness = Column(Float)
    brightness_gap = Column(Float)
    score = Column(Float, nullable=True)
    good_matches = Column(Integer, nullable=True)
    inlier_ratio = Column(Float, nullable=True)
    hog_hits = Column(Integer, nullable=True)
    accepted = Column(Boolean, default=True)
    alternatives_json = Column(Text, nullable=True)

    job = relationship("Job", back_populates="pairs")

class ReviewedPair(Base):
    __tablename__ = "reviewed_pairs"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    dataset_pair_id = Column(Integer, index=True) # Unique ID in the final dataset
    job_id = Column(String(32), index=True)
    source_pair_id = Column(Integer)
    low_path = Column(String(512))
    high_path = Column(String(512))
    saved_low = Column(String(512))
    saved_high = Column(String(512))
    score = Column(Float, nullable=True)
    human_decision = Column(String(20), default="accepted") # accepted, rejected
    created_at = Column(DateTime, default=datetime.utcnow)

def init_db():
    Base.metadata.create_all(bind=engine)
