from sqlalchemy.orm import Mapped, mapped_column

from models import db


class Category(db.Model):
    __tablename__ = 'categories'

    id: Mapped[int] = mapped_column(
        db.Integer,
        primary_key=True,  # index=True, unique=True
        autoincrement=True
    )
    name: Mapped[str] = mapped_column(
        db.String(40)
    )

    questions: Mapped[list['Question']] = db.relationship(
        'Question',
        back_populates='category'
    )

    def __repr__(self):
        return "Category(id={}, name={})".format(
            self.id,
            self.name
        )
