from expenses_entities import Expense
from expenses_entities import ExpenseCategory
from expenses_entities import User
from expenses_entities import ChatHistory
from expenses_entities import ChatHistoryRepository
from expenses_entities import ExpenseRepository
from expenses_entities import ExpenseCategoriesRepository
from expenses_entities import UserRepository
from pymysql.connections import Connection
from typing import Any

import pymysql


class ExpenseRepositoryImplementation(ExpenseRepository):
    def __init__(self,
                 host: str,
                 user: str,
                 password: str,
                 db_name: str,
                 db_port: int):
        super().__init__(host=host, user=user, password=password, db_name=db_name, db_port=db_port)
        self._connection: Connection = pymysql.connect(
            host=self._host,
            user=self._user,
            password=self._password,
            database=self._db_name,
            port=self._db_port
        )

    def get(self, id) -> Expense:
        query = '''
        SELECT
            e.`expense_id` AS `expense_id`,
            e.`expense_name` AS `expense_name`,
            e.`expense_amount` AS `expense_amount`,
            e.`month_year` AS `month_year`,
            e.`exp_category_id` AS `exp_category_id`,
            ec.`category_name` AS `category_name`,
            e.`status` AS `status`,
            e.`created_at` AS `created_at`,
            e.`updated_at` AS `updated_at`
        FROM `expenses` AS e
        JOIN `expense_categories` AS ec
        ON e.exp_category_id = ec.exp_category_id
        WHERE e.`expense_id` = %s
        '''
        params = (id,)
        with self._connection.cursor() as cursor:
            cursor.execute(query=query, args=params)
            result = cursor.fetchone()
            column_names = [column[0] for column in cursor.description]
        self._connection.commit()

        if result is None:
            return None

        if len(result) == 0:
            return None

        expense = Expense(**dict(zip(column_names, result)))
        return expense

    def get_by(self, **kwargs) -> list[Expense]:
        query = '''
        SELECT
            e.`expense_id` AS `expense_id`,
            e.`expense_name` AS `expense_name`,
            e.`expense_amount` AS `expense_amount`,
            e.`month_year` AS `month_year`,
            e.`user_id` AS `user_id`,
            e.`exp_category_id` AS `exp_category_id`,
            ec.`category_name` AS `category_name`,
            e.`status` AS `status`,
            e.`created_at` AS `created_at`,
            e.`updated_at` AS `updated_at`
        FROM `expenses` AS e
        JOIN `expense_categories` AS ec
        ON e.exp_category_id = ec.exp_category_id
        '''

        if kwargs:
            query += ' WHERE ' + ' AND '.join([f'{key} = %s' for key in kwargs.keys()])

        with self._connection.cursor() as cursor:
            cursor.execute(query, tuple(kwargs.values()))
            result = cursor.fetchall()
            column_names = [column[0] for column in cursor.description]
        self._connection.commit()

        if result is None:
            return None

        if len(result) == 0:
            return None

        expense = [Expense(**dict(zip(column_names, row))) for row in result]
        return expense

    def get_all(self) -> list[Expense]:
        query = '''
        SELECT
            e.`expense_id` AS `expense_id`,
            e.`expense_name` AS `expense_name`,
            e.`expense_amount` AS `expense_amount`,
            e.`month_year` AS `month_year`,
            e.`exp_category_id` AS `exp_category_id`,
            ec.`category_name` AS `category_name`,
            e.`status` AS `status`,
            e.`created_at` AS `created_at`,
            e.`updated_at` AS `updated_at`,
        FROM `expenses` AS e
        JOIN `expense_categories` AS ec
        ON e.exp_category_id = ec.exp_category_id
        '''
        with self._connection.cursor() as cursor:
            cursor.execute(query=query)
            result = cursor.fetchall()
            column_names = [column[0] for column in cursor.description]
        self._connection.commit()

        if result is None:
            return None

        if len(result) == 0:
            return None

        expenses = [Expense(**dict(zip(column_names, row))) for row in result]
        return expenses

    def add(self, entity: Expense) -> Any:
        query = '''
        INSERT INTO `expenses`
            (`expense_name`, `expense_amount`, `month_year`, `exp_category_id`, `user_id`)
        VALUES
            (%s, %s, %s, %s, %s)
        '''
        params = (entity.expense_name, entity.expense_amount,
                  entity.month_year, entity.exp_category_id, entity.user_id)

        with self._connection.cursor() as cursor:
            cursor.execute(query=query, args=params)
            id = cursor.lastrowid
        self._connection.commit()
        return id

    def update(self, id, entity: Expense) -> bool:
        query = '''
        UPDATE `expenses`
        SET `expense_name` = %s,
            `expense_amount` = %s,
            `month_year` = %s,
            `exp_category_id` = %s
        WHERE `expense_id` = %s
        '''
        params = (entity.expense_name, entity.expense_amount, entity.month_year, entity.exp_category_id, id)
        with self._connection.cursor() as cursor:
            cursor.execute(query=query, args=params)
            rows_affected = cursor.rowcount
        self._connection.commit()
        return rows_affected > 0

    def delete(self, id) -> bool:
        query = '''
        DELETE FROM `expenses`
        WHERE `expense_id` = %s
        '''
        params = (id,)
        with self._connection.cursor() as cursor:
            cursor.execute(query=query, args=params)
            rows_affected = cursor.rowcount
        self._connection.commit()
        return rows_affected > 0

    def __del__(self):
        self._connection.close()


class ExpenseCategoriesRepositoryImplementation(ExpenseCategoriesRepository):
    def __init__(self,
                 host: str,
                 user: str,
                 password: str,
                 db_name: str,
                 db_port: int):
        super().__init__(host=host, user=user, password=password, db_name=db_name, db_port=db_port)
        self._connection: Connection = pymysql.connect(
            host=self._host,
            user=self._user,
            password=self._password,
            database=self._db_name,
            port=self._db_port
        )

    def get(self, id) -> ExpenseCategory:
        query = '''
        SELECT
            `exp_category_id`,
            `category_name`,
            `status`,
            `created_at`,
            `updated_at`
        FROM `expense_categories`
        WHERE `exp_category_id` = %s
        '''
        params = (id,)
        with self._connection.cursor() as cursor:
            cursor.execute(query=query, args=params)
            result = cursor.fetchone()
            column_names = [column[0] for column in cursor.description]

        if result is None:
            return None

        if len(result) is None:
            return None

        category = ExpenseCategory(**dict(zip(column_names, result)))
        self._connection.commit()

        return category

    def get_by(self, **kwargs) -> list[ExpenseCategory]:
        query = '''
        SELECT
            `exp_category_id`,
            `category_name`,
            `status`,
            `created_at`,
            `updated_at`
        FROM `expense_categories`
        '''

        if kwargs:
            query += ' WHERE ' + ' AND '.join([f'{key} = %s' for key in kwargs.keys()])

        with self._connection.cursor() as cursor:
            cursor.execute(query, tuple(kwargs.values()))
            result = cursor.fetchall()
            column_names = [column[0] for column in cursor.description]
        self._connection.commit()

        if result is None:
            return None

        if len(result) is None:
            return None

        categories = [ExpenseCategory(**dict(zip(column_names, row))) for row in result]

        return categories

    def get_all(self) -> list[ExpenseCategory]:
        with self._connection.cursor() as cursor:
            cursor.execute(
                '''
                SELECT
                    `exp_category_id`,
                    `category_name`,
                    `status`,
                    `created_at`,
                    `updated_at`
                FROM `expense_categories`
                '''
            )
            result = cursor.fetchall()
            column_names = [column[0] for column in cursor.description]
        self._connection.commit()

        if result is None:
            return None

        if len(result) is None:
            return None

        categories = [ExpenseCategory(**dict(zip(column_names, row))) for row in result]

        return categories

    def add(self, entity: ExpenseCategory) -> Any:
        with self._connection.cursor() as cursor:
            cursor.execute(
                '''
                INSERT INTO `expense_categories`
                    (`exp_category_id`, `exp_category_name`)
                VALUES
                    (%s, %s)
                ''',
                (entity.exp_category_id, entity.category_name)
            )
            id = cursor.lastrowid
        self._connection.commit()
        return id

    def update(self, id, entity: ExpenseCategory) -> bool:
        with self._connection.cursor() as cursor:
            cursor.execute(
                '''
                UPDATE `expense_categories`
                SET `exp_category_name` = %s
                WHERE `exp_category_id` = %s
                ''',
                (entity.category_name, id)
            )
            rows_affected = cursor.rowcount
        self._connection.commit()
        return rows_affected > 0

    def delete(self, id) -> bool:
        with self._connection.cursor() as cursor:
            cursor.execute(
                '''
                DELETE FROM `expense_categories`
                WHERE `exp_category_id` = %s
                ''',
                (id,)
            )
            rows_affected = cursor.rowcount
        self._connection.commit()
        return rows_affected > 0

    def __del__(self):
        self._connection.close()


class UserRepositoryImplementation(UserRepository):
    def __init__(self,
                 host: str,
                 user: str,
                 password: str,
                 db_name: str,
                 db_port: int):
        super().__init__(host=host, user=user, password=password, db_name=db_name, db_port=db_port)
        self._connection: Connection = pymysql.connect(
            host=self._host,
            user=self._user,
            password=self._password,
            database=self._db_name,
            port=self._db_port
        )

    def get(self, id) -> User:
        query = '''
        SELECT
            `user_id`,
            `username`,
            `password`,
            `status`,
            `created_at`,
            `updated_at`
        FROM `users`
        WHERE `user_id` = %s
        '''
        params = (id,)

        with self._connection.cursor() as cursor:
            cursor.execute(query=query, args=params)
            result = cursor.fetchone()
            column_names = [column[0] for column in cursor.description]
        self._connection.commit()

        if result is None:
            return None

        if len(result) == 0:
            return None

        user = User(**dict(zip(column_names, result)))
        return user

    def get_by(self, **kwargs) -> list[User]:
        query = '''
        SELECT
            `user_id`,
            `username`,
            `password`,
            `status`,
            `created_at`,
            `updated_at`
        FROM `users`
        '''

        if kwargs:
            query += ' WHERE ' + ' AND '.join([f'{key} = %s' for key in kwargs.keys()])

        with self._connection.cursor() as cursor:
            cursor.execute(query, tuple(kwargs.values()))
            result = cursor.fetchall()
            column_names = [column[0] for column in cursor.description]
        self._connection.commit()

        if result is None:
            return None

        if len(result) == 0:
            return None

        user = [User(**dict(zip(column_names, row))) for row in result]
        return user

    def get_all(self) -> list[User]:
        query = '''
        SELECT
            `user_id`,
            `username`,
            `password`,
            `status`,
            `created_at`,
            `updated_at`
        FROM `users`
        '''
        with self._connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            column_names = [column[0] for column in cursor.description]
        self._connection.commit()

        if result is None:
            return None

        if len(result) == 0:
            return None

        users = [User(**dict(zip(column_names, row))) for row in result]
        return users

    def add(self, entity: User) -> Any:
        query = '''
        INSERT INTO `users`
            (`username`, `password`)
        VALUES
            (%s, %s)
        '''
        params = (entity.username, entity.password)

        with self._connection.cursor() as cursor:
            cursor.execute(query=query, args=params)
            id = cursor.lastrowid
        self._connection.commit()
        return id

    def update(self, id, entity: User) -> bool:
        query = '''
        UPDATE `users`
        SET `username` = %s,
            `password` = %s
        WHERE `user_id` = %s
        '''

        params = (entity.username, entity.password, id)

        with self._connection.cursor() as cursor:
            cursor.execute(query=query, args=params)
            rows_affected = cursor.rowcount()
        self._connection.commit()
        return rows_affected > 0

    def delete(self, id) -> bool:
        query = '''
        DELETE FROM `users`
        WHERE `user_id` = %s
        '''
        params = (id,)

        with self._connection.cursor() as cursor:
            cursor.execute(query=query, args=params)
            rows_affected = cursor.rowcount()
        self._connection.commit()
        return rows_affected > 0


class ChatHistoryRepositoryImplementation(ChatHistoryRepository):
    def __init__(self,
                 host: str,
                 user: str,
                 password: str,
                 db_name: str,
                 db_port: int):
        super().__init__(host=host, user=user, password=password, db_name=db_name, db_port=db_port)
        self._connection: Connection = pymysql.connect(
            host=self._host,
            user=self._user,
            password=self._password,
            database=self._db_name,
            port=self._db_port
        )

    def get(self, id) -> ChatHistory:
        query = '''
        SELECT
            `chat_id`,
            `user_id`,
            `content`,
            `role_id`,
            `status`,
            `created_at`,
            `updated_at`
        FROM `chats`
        WHERE `history_id` = %s
        '''
        params = (id,)

        with self._connection.cursor() as cursor:
            cursor.execute(query=query, args=params)
            result = cursor.fetchone()
            column_names = [column[0] for column in cursor.description]
        self._connection.commit()

        if result is None:
            return None

        if len(result) == 0:
            return None

        chat = ChatHistory(**dict(zip(column_names, result)))
        return chat

    def get_by(self, **kwargs) -> list[ChatHistory]:
        query = '''
        SELECT
            `chat_id`,
            `user_id`,
            `content`,
            `role_id`,
            `status`,
            `created_at`,
            `updated_at`
        FROM `chats`
        '''

        if kwargs:
            query += ' WHERE ' + ' AND '.join([f'{key} = %s' for key in kwargs.keys()])

        with self._connection.cursor() as cursor:
            cursor.execute(query=query, args=tuple(kwargs.values()))
            result = cursor.fetchall()
            column_names = [column[0] for column in cursor.description]
        self._connection.commit()

        if result is None:
            return None

        if len(result) == 0:
            return None

        chat = [ChatHistory(**dict(zip(column_names, row))) for row in result]
        return chat

    def get_all(self) -> list[ChatHistory]:
        query = '''
        SELECT
            `chat_id`,
            `user_id`,
            `content`,
            `role_id`,
            `status`,
            `created_at`,
            `updated_at`
        FROM `chats`
        '''
        with self._connection.cursor() as cursor:
            cursor.execute(query=query)
            result = cursor.fetchall()
            column_names = [column[0] for column in cursor.description]
        self._connection.commit()

        if result is None:
            return None

        if len(result) == 0:
            return None

        chats = [ChatHistory(**dict(zip(column_names, row))) for row in result]
        return chats

    def add(self, entity: ChatHistory) -> Any:
        query = '''
        INSERT INTO `chats`
            (`user_id`, `role_id`, `content`)
        VALUES
            (%s, %s, %s)
        '''
        params = (entity.user_id, entity.role_id, entity.content)

        with self._connection.cursor() as cursor:
            cursor.execute(query=query, args=params)
            id = cursor.lastrowid
        self._connection.commit()
        return id

    def add_batch(self, entities: list[ChatHistory]) -> Any:
        query = '''
        INSERT INTO `chats`
            (`user_id`, `role_id`, `content`)
        VALUES
            (%s, %s, %s)
        '''
        params = [(entity.user_id, entity.role_id, entity.content) for entity in entities]

        with self._connection.cursor() as cursor:
            rows = cursor.executemany(query=query, args=params)
        self._connection.commit()
        if rows is None:
            return None
        if rows == 0:
            return None

        return rows

    def update(self, id, entity: ChatHistory) -> bool:
        query = '''
        UPDATE `chats`
        SET `user_id` = %s,
            `role_id` = %s,
            `content` = %s
        WHERE `chat_id` = %s
        '''
        params = (entity.user_id, entity.role_id, entity.content, id)

        with self._connection.cursor() as cursor:
            cursor.execute(query=query, args=params)
            rows_affected = cursor.rowcount
        self._connection.commit()
        return rows_affected > 0

    def delete(self, id) -> bool:
        query = '''
        DELETE FROM `chats`
        WHERE `chat_id` = %s
        '''
        params = (id,)

        with self._connection.cursor() as cursor:
            cursor.execute(query=query, args=params)
            rows_affected = cursor.rowcount
        self._connection.commit()
        return rows_affected > 0

    def delete_batch(self, chat_ids: list[Any]) -> bool:
        query = '''
        DELETE FROM `chats`
        WHERE `chat_id` = %s
        '''
        params = [(chat_id,) for chat_id in chat_ids]

        with self._connection.cursor() as cursor:
            rows = cursor.executemany(query=query, args=params)
        self._connection.commit()
        if rows is None:
            return False
        return rows > 0

    def __del__(self):
        self._connection.close()

