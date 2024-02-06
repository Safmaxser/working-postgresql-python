import psycopg2


class СustomerOperations:

    def __init__(self, database, user, password):
        self.database = database
        self.user = user
        self.password = password

    def connect(self):
        self.conn = psycopg2.connect(database=self.database, user=self.user, 
                                     password=self.password)
    
    def disconnect(self):
        self.conn.close()

    def create_db(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                DROP TABLE IF EXISTS сontact;
                DROP TABLE IF EXISTS client;
                """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS client (
                    PRIMARY KEY (client_id),
                    client_id  SERIAL      NOT NULL,
                    first_name VARCHAR(50) NOT NULL,
                    last_name  VARCHAR(50) NOT NULL,
                    email      VARCHAR(50),
                            CONSTRAINT info_correct
                            CHECK (first_name > '' AND last_name > '')
                );
                """)        
            cur.execute("""
                CREATE TABLE IF NOT EXISTS сontact (
                    PRIMARY KEY (сontact_id),
                    сontact_id SERIAL      NOT NULL,
                    client_id  INTEGER     NOT NULL 
                            REFERENCES client(client_id)
                            ON DELETE CASCADE,             
                    phone      VARCHAR(20) NOT NULL
                );
                """)
            self.conn.commit()

    def add_client(self, first_name, last_name, email, phones=None):
        if first_name == '' or last_name == '':
            print('Имя и Фамилия - обязательные поля для заполнения!')
            return        
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO client (first_name, last_name, email)
                VALUES (%s, %s, %s)
                RETURNING client_id;
                """, (first_name, last_name, email))                
            client_id = cur.fetchone()[0]
            if phones is not None:
                for phone in phones:
                    cur.execute("""
                        INSERT INTO сontact (client_id, phone)
                        VALUES (%s, %s);
                        """, (client_id, phone))             
                self.conn.commit()

    def add_phone(self, client_id, phone):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO сontact (client_id, phone)
                VALUES (%s, %s);
                """, (client_id, phone))
            self.conn.commit()

    def change_client(self, client_id, first_name=None, last_name=None, email=None, phones=None):    
        if first_name == '' or last_name == '':
            print('Имя и Фамилия - обязательные поля для заполнения!')
            return 
        with self.conn.cursor() as cur:
            variable = []
            query_str = "UPDATE client SET "
            if first_name is not None:            
                query_str += "first_name = %s "
                variable.append(first_name)
            if last_name is not None:
                if len(query_str) > 18:
                    query_str += ', '    
                query_str += "last_name = %s "
                variable.append(last_name)
            if email is not None:
                if len(query_str) > 18:
                    query_str += ', '  
                query_str += "email = %s "
                variable.append(email)
            query_str += "WHERE client_id = %s;"
            variable.append(client_id)
            if len(query_str) > 39:
                cur.execute(query_str, variable)  
                self.conn.commit()
            if phones is not None:
                for phone in phones:
                    cur.execute("""
                        INSERT INTO сontact (client_id, phone)
                        VALUES (%s, %s);
                        """, (client_id, phone))             
                self.conn.commit()

    def delete_phone(self, client_id, phone):
        with self.conn.cursor() as cur:
            cur.execute("""
                DELETE FROM сontact
                WHERE client_id = %s
                AND phone = %s;      
                """, (client_id, str(phone)))
            self.conn.commit()

    def delete_client(self, client_id):
        with self.conn.cursor() as cur:
            cur.execute("""
                DELETE FROM client
                WHERE client_id = %s;      
                """, (str(client_id)))
            self.conn.commit()

    def find_client(self, client_id=None, first_name=None, last_name=None, email=None, phone=None):
        with self.conn.cursor() as cur:
            variable = []
            query_str = "SELECT * FROM client "
            if phone is not None:
                query_str += ("AS c LEFT JOIN сontact AS c2 "
                            "ON c.client_id = c2.client_id "
                            "WHERE c2.phone = %s ")
                variable.append(str(phone))
            else:
                query_str += "WHERE "
            if client_id is not None:  
                if len(query_str) > 27:
                    query_str += ' AND '               
                query_str += "client_id = %s "
                variable.append(client_id)
            if first_name is not None:  
                if len(query_str) > 27:
                    query_str += ' AND '               
                query_str += "first_name iLike %s "
                variable.append(first_name)
            if last_name is not None:
                if len(query_str) > 27:
                    query_str += ' AND '    
                query_str += "last_name ILike %s "
                variable.append(last_name)
            if email is not None:
                if len(query_str) > 27:
                    query_str += ' AND '  
                query_str += "email iLike %s "
                variable.append(email)
            query_str += ";"
            if len(query_str) > 28:
                cur.execute(query_str, variable)
                print(cur.fetchall()) 


if __name__ == '__main__':
    client_db = СustomerOperations('netology_db', 'postgres', 'password')
    client_db.connect()
    client_db.create_db()

    client_db.add_client('Иван', 'Орлов', 'ivan_orlov@mail.com', 
                         [89233553333, 89233553334])
    client_db.add_client('Пётр', 'Иров', 'petr_irov@mail.com', [89233554444])
    client_db.add_client('Игорь', 'Николаев', 'in@mail.com', [89999999999])
    client_db.add_client('Антон', 'Антипов', 'aa567@mail.com', [89453689899])
    client_db.add_client('Николай', 'Алексеев', 'na555@mail.com', [89453689555])

    client_db.add_phone(1, 89233553335)
    client_db.add_phone(2, 89233554445)

    client_db.change_client(1, last_name='Орёл')
    client_db.change_client(4, 'Алексей', 'Алексеев')
    client_db.change_client(2, phones=[89233554449, 89233554410])
    client_db.change_client(1)

    client_db.delete_phone(1, 89233553333)
    client_db.delete_client(3)

    client_db.find_client(first_name='Иван')
    client_db.find_client(None, None, 'Алексеев')
    client_db.find_client(phone=89233554444)
    client_db.find_client(4)

    client_db.disconnect()