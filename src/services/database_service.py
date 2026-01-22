import logging
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from models import Base, postgresql_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DatabaseService")


class DatabaseSessionService:
    """
    Service to manage database sessions.
    """

    def __init__(self, connection_url: str | None = None):
        self._connection_url = connection_url
        # Connection pooling configuration for better resource management
        self.engine = create_engine(
            self.connection_url,
            echo=False,  # Disable echo to reduce logging overhead
            pool_size=10,  # Maintain 10 persistent connections
            max_overflow=20,  # Allow up to 20 additional connections
            pool_pre_ping=True,  # Verify connections before use
        )
        self.session_factory = sessionmaker(
            autocommit=False, autoflush=True, bind=self.engine
        )

    @property
    def connection_url(self) -> str:
        """Get the database connection URL."""
        if self._connection_url is None:
            self._connection_url = (
                f"postgresql://{postgresql_settings.POSTGRES_USER}:{postgresql_settings.POSTGRES_PASSWORD}"
                f"@{postgresql_settings.POSTGRES_HOST}:{postgresql_settings.POSTGRES_PORT}/{postgresql_settings.POSTGRES_DB}"
            )
        return self._connection_url

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Get a new database session."""
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _init_database(self, database_name: str | None = None) -> None:
        """
        Create the database if it doesn't exist.

        This method connects to the default 'postgres' database to create
        the application database. This is necessary because RDS clusters
        created without a default_database_name only have the 'postgres' system database.

        Args:
            database_name: Name of the database to create. If not provided,
                uses POSTGRES_DB_NAME from settings.

        This operation is idempotent - it won't fail if the database already exists.
        """
        if database_name is None:
            database_name = postgresql_settings.POSTGRES_DB

        target_db_url = self.connection_url

        if "?" in target_db_url:
            base_url, params = target_db_url.rsplit("?", 1)
            postgres_url = base_url.rsplit("/", 1)[0] + "/postgres?" + params
        else:
            postgres_url = target_db_url.rsplit("/", 1)[0] + "/postgres"

        target_db_name = database_name

        logger.info(f"Creating database '{target_db_name}' if it doesn't exist...")

        # Create an engine connected to the 'postgres' database
        postgres_engine = create_engine(postgres_url, isolation_level="AUTOCOMMIT")

        try:
            with postgres_engine.connect() as conn:
                # Check if database exists
                result = conn.execute(
                    text("SELECT 1 FROM pg_database WHERE datname = :dbname"),
                    {"dbname": target_db_name},
                )

                if result.fetchone() is None:
                    # Database doesn't exist, create it
                    logger.info(
                        f"Database '{target_db_name}' does not exist. Creating..."
                    )
                    conn.execute(text(f'CREATE DATABASE "{target_db_name}"'))
                    logger.info(f"Database '{target_db_name}' created successfully.")
                else:
                    logger.info(f"Database '{target_db_name}' already exists.")
        finally:
            postgres_engine.dispose()
        try:
            # Create tables
            Base.metadata.create_all(bind=self.engine)

            logger.info(
                f"Connected to PostgreSQL at {postgresql_settings.POSTGRES_HOST}:{postgresql_settings.POSTGRES_PORT}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            self.engine = None
            self.Session = None

    def close(self):
        """Dispose the engine."""
        if self.engine:
            self.engine.dispose()
            logger.info("Closed PostgreSQL connection")


if __name__ == "__main__":
    # Initialize the DatabaseSessionService to create the database and tables
    db_service = DatabaseSessionService(connection_url=None)
    db_service._init_database()
    db_service.close()
