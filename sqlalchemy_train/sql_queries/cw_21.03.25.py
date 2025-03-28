
from typing import Type, Any
import json

from sqlalchemy import func, and_, or_, not_
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.exc import IntegrityError, DataError

from sqlalchemy_train.sql_queries.models import User, Role, Comment, News
from sqlalchemy_train.sql_queries.db_connection import DBConnection
from sqlalchemy_train.sql_queries import engine
import json
from datetime import datetime


with DBConnection(engine) as session:
    # list_autors = session.query(User.role_id, User.last_name, User.rating).filter(
    #     User.role_id == 3,
    #     User.rating > 5
    # ).all()

    users_22 = session.query(User.last_name, User.created_at).filter(
        User.created_at.between(datetime(2022,1, 1), datetime(2022, 12, 31))

    ).all()

    response_data = [
        {
            "last_name":user.last_name,
            "created_at": datetime.strftime(user.created_at, '%Y-%m-%d %H:%M:%S')
        }
        for user in users_22
    ]

    print(json.dumps(response_data, indent=4))

    with DBConnection(engine) as session:
        all_users_with_role = session.query(
            User.last_name,
            User.email,
            Role.name.label("role_name")
        ).join(User.role).all()

        for user in all_users_with_role:
            print(user.last_name, user.email, user.role_name)

    author = session.query(User.last_name,
                           User.email,
                           Role.name.label('name_role'),
                           User.rating,
                           func.count(News.id).label('cn')).join(
        User.role).join(User.news).group_by(User.last_name,
                                            User.email,
                                            Role.name,
                                            User.rating,
                                            News.id).order_by(desc(func.count(News.id))).first()

    print(author)