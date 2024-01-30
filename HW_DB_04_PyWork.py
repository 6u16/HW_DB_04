# Домашнее задание к лекции «Работа с PostgreSQL из Python»

import psycopg2

def create_db(conn):
    with conn.cursor() as cur:
        # Сброс таблиц, закоментиовать если первый раз создаёте
        cur.execute("""
            DROP TABLE client CASCADE;
            DROP TABLE c_phones CASCADE;
            """)
        
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
        cur.execute("""
            INSERT INTO client(f_name, l_name, email) VALUES(%s, %s, %s) RETURNING f_name, l_name, email;
            """, (first_name, last_name, email))
        #print(cur.fetchone())
        client = cur.fetchone()
        
        cur.execute("""
            SELECT id FROM client WHERE email=%s;
            """, (email,))  # Забираем id по уникальному email(только email уникален, именамогут повторяться)
        cl_id = cur.fetchone()  # Записываем id нашего UNIQUE email
        
        cur.execute("""
            INSERT INTO c_phones(client_id, phones) VALUES(%s, %s) RETURNING phones;
            """, (cl_id, phones))  # записываем телефон в id UNIQUE email
        #print(cur.fetchone())
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
        cur.execute("""
            UPDATE client SET f_name=%s, l_name=%s, email=%s
            WHERE id=%s;
            """, (first_name, last_name, email, client_id))
        
        cur.execute("""
            DELETE FROM c_phones WHERE client_id=%s;
            """, (client_id,))
        
        cur.execute("""
            INSERT INTO c_phones(client_id, phones) VALUES(%s, %s) RETURNING phones;
            """, (client_id, phones))
        phone = cur.fetchone()
        
        cur.execute("""
            SELECT * FROM client
            WHERE id=%s;
            """, (client_id,))
        #print(cur.fetchone())
        client = cur.fetchone()
        print(client+phone)

def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM c_phones WHERE client_id=%s AND phones=%s RETURNING phones;
            """, (client_id,phone))
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
        if phone:  # Если ищем только(или) по телефону
            cur.execute("""
                SELECT * FROM c_phones
                WHERE phones=%s;
                """, (phone,))
            cl_id = cur.fetchall()[0][0]  # Достаём id табл. c_phones
                
            cur.execute("""
                SELECT * FROM client
                WHERE f_name=%s OR l_name=%s OR email=%s OR id=%s;
                """, (first_name, last_name, email, cl_id))  # Подставляем id
            print(cur.fetchall()[0])
        if not email:  # Если нет email
            cur.execute("""
                SELECT * FROM client
                WHERE f_name=%s AND l_name=%s OR email=%s;
                """, (first_name, last_name, email))
            print(cur.fetchall())
        else:  # Если есть email
            cur.execute("""
                SELECT * FROM client
                WHERE f_name=%s AND l_name=%s AND email=%s;
                """, (first_name, last_name, email))
            print(cur.fetchall())
        
    
with psycopg2.connect(database="HW_DB_04", user="postgres", password="postgres") as conn:
    #create_db(conn)
    #add_client(conn, 'андрей', 'андреев', '62466@', 6548)
    #add_client(conn, 'сергей', 'серёгин', 'ere333g@', 321656754)
    #add_phone(conn, 3, 84686549)
    #change_client(conn, 2, 'вася', 'васин', 'dfbxdf@', 54645)
    #add_phone(conn, 2, 846849)
    #delete_phone(conn, 2, 54645)
    #delete_client(conn, 3)
    find_client(conn, first_name = 'андрей', last_name = 'андреев', email = '62466@')
    
conn.close()