# Домашнее задание 4 к лекции «Работа с PostgreSQL из Python»

import psycopg2

def create_db(conn):
    with conn.cursor() as cur:
        
        # Сброс таблиц, закоментиовать если первый раз создаёте
        cur.execute("""
            DROP TABLE client CASCADE;
            DROP TABLE c_phones CASCADE;
            """)
        
        # Создание таблиц
        cur.execute("""
            CREATE TABLE IF NOT EXISTS client(
                id SERIAL PRIMARY KEY,
                f_name VARCHAR(40) NOT NULL,
                l_name VARCHAR(40) NOT NULL,
                email VARCHAR(80) UNIQUE NOT NULL
            );
            """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS c_phones(
                client_id INTEGER NOT NULL REFERENCES client(id),
                phones INTEGER
            );
            """)
        conn.commit()

def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        # Добавляем клиента в таблицу client
        cur.execute("""
            INSERT INTO client(f_name, l_name, email) VALUES(%s, %s, %s) RETURNING f_name, l_name, email;
            """, (first_name, last_name, email))
        client = cur.fetchone()
        
        # Забираем id клиена по уникальному email(только email уникален, имена могут повторяться)
        cur.execute("""
            SELECT id FROM client WHERE email=%s;
            """, (email,))
        cl_id = cur.fetchone()  # Записываем id клиента нашего UNIQUE email
        
        # Записываем телефон в id UNIQUE email
        cur.execute("""
            INSERT INTO c_phones(client_id, phones) VALUES(%s, %s) RETURNING phones;
            """, (cl_id, phones))
        phone = cur.fetchone()
        print(cl_id+client+phone)

def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO c_phones(client_id, phones) VALUES(%s, %s) RETURNING client_id, phones;
            """, (client_id, phone))
        print(cur.fetchone())
        
def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cur:
        
        if first_name: # Смена Имени
            cur.execute("""
                UPDATE client SET f_name=%s
                WHERE id=%s;
                """, (first_name, client_id))
        
        if last_name: # Смена Фамилии
            cur.execute("""
                UPDATE client SET l_name=%s
                WHERE id=%s;
                """, (last_name, client_id))
        
        if email: # Смена Почты
            cur.execute("""
                UPDATE client SET email=%s
                WHERE id=%s;
                """, (email, client_id))
        
        if phones: # Смена телефона
            # Удаление старых
            cur.execute("""
                DELETE FROM c_phones WHERE client_id=%s;
                """, (client_id,))
            
            # Запись нового
            cur.execute("""
                INSERT INTO c_phones(client_id, phones) VALUES(%s, %s) RETURNING phones;
                """, (client_id, phones))
            phone = cur.fetchone()
        
        # Смотрим на изменения
        cur.execute("""
            SELECT id, f_name, l_name, email, phones FROM client c
            LEFT JOIN c_phones cf ON cf.client_id = c.id
            WHERE id=%s;
            """, (client_id,)) # Выбираем данные которые нужны для вывода, без выборки * полных данных
        client = cur.fetchall()
        print(client)

def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM c_phones WHERE client_id=%s AND phones=%s RETURNING phones;
            """, (client_id, phone))
        print(cur.fetchall()[0])

def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM c_phones WHERE client_id=%s;
            """, (client_id,))
        
        cur.execute("""
            DELETE FROM client CASCADE WHERE id=%s RETURNING id;
            """, (client_id,))
        print(cur.fetchall()[0])

def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
                    
        if email:  # Приоритет поиска UNIQUE email
            cur.execute("""
                SELECT id, f_name, l_name, email, phones FROM client c
                LEFT JOIN c_phones cf ON cf.client_id = c.id
                WHERE f_name=%s OR l_name=%s OR email=%s;
                """, (first_name, last_name, email))
            print(cur.fetchall())
            
        if phone and not email:  # Ищем по телефону
            cur.execute("""
                SELECT * FROM c_phones
                WHERE phones=%s;
                """, (phone,))
            cl_id = cur.fetchall()[0][0]  # Достаём id табл. c_phones
                
            cur.execute("""
                SELECT id, f_name, l_name, email, phones FROM client c
                LEFT JOIN c_phones cf ON cf.client_id = c.id
                WHERE f_name=%s OR l_name=%s OR email=%s OR id=%s;
                """, (first_name, last_name, email, cl_id))  # Подставляем id
            print(cur.fetchall())
            
        if first_name and last_name and not email and not phone:  # Ищем по first_name и last_name
            cur.execute("""
                SELECT id, f_name, l_name, email, phones FROM client c
                LEFT JOIN c_phones cf ON cf.client_id = c.id
                WHERE f_name=%s AND l_name=%s;
                """, (first_name, last_name))
            print(cur.fetchall())
            
        if first_name or last_name and not email and not phone:  # Ищем по first_name или last_name
            cur.execute("""
                SELECT id, f_name, l_name, email, phones FROM client c
                LEFT JOIN c_phones cf ON cf.client_id = c.id
                WHERE f_name=%s OR l_name=%s;
                """, (first_name, last_name))
            print(cur.fetchall())    
        
        
if __name__ == "__main__":
    print(__name__)  
    with psycopg2.connect(database="HW_DB_04", user="postgres", password="postgres") as conn:
        #create_db(conn)
        #add_client(conn, 'андрей', 'андреев', '62466@', 6548)
        #add_client(conn, 'сергей', 'серёгин', 'ere333g@', 321656754)
        #add_phone(conn, 3, 84686549)
        #change_client(conn, 2, 'вася', 'васин', 'dfbxdf@', 54645)
        #add_phone(conn, 2, 846849)
        #delete_phone(conn, 2, 54645)
        #delete_client(conn, 3)
        #find_client(conn, email='666@')
        change_client(conn, 2, last_name = 'васичин', email = 'ds3hgdf@')
        
    conn.close()
