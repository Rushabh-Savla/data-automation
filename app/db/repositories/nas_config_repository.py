from sqlalchemy.orm import Session

from app.db.models.nas_config import NasConfig


class NasConfigRepository:
    def __init__(self, db: Session):
        self.db = db

    def get(self) -> NasConfig | None:
        return self.db.get(NasConfig, 1)

    def upsert(self, **fields) -> NasConfig:
        config = self.get()
        if config is None:
            config = NasConfig(id=1, **fields)
            self.db.add(config)
        else:
            for key, value in fields.items():
                setattr(config, key, value)
        self.db.flush()
        return config

    def delete(self) -> None:
        config = self.get()
        if config is not None:
            self.db.delete(config)
            self.db.flush()
