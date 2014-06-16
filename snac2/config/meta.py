from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base

__all__ = ['engine', 'metadata', 'Session', 'Base']

# SQLAlchemy database engine.  Updated by model.init_model().
engine = None

session_maker = None
# SQLAlchemy session manager.
Session = None

# Global metadata. If you have multiple databases with overlapping table
# names, you'll need a metadata for each database.
metadata = MetaData()
Base = declarative_base()
