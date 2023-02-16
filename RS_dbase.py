def create_db(conn):
    with conn.cursor() as cur:

        cur.execute("""
                    
            CREATE TABLE IF NOT EXISTS vk_person (
                id SERIAL PRIMARY KEY,
                vk_person_id INTEGER NOT NULL, 
                search_id INTEGER DEFAULT 0
            );

        """)

        conn.commit()
        print('  БД создана!')  
    

def drop_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE vk_person;
        """)
        conn.commit()
        print('  БД удалена!') 

def clear_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM vk_person;
        """)
        conn.commit()
        print('  БД очищена!') 

def check_person(conn, p_id):
    result = False
    with conn.cursor() as cur:
        sql_query = f"""
        SELECT id, vk_person_id
        FROM vk_person
        WHERE vk_person_id={p_id};
        """
        cur.execute(sql_query)     

        for row in cur.fetchall():
            result = True

    return result

def add_person(conn, p_id, search_id):
    with conn.cursor() as cur:
        sql_query = f"""
        INSERT INTO vk_person(vk_person_id, search_id) 
        VALUES( {p_id}, {search_id} );
        """        
        cur.execute(sql_query)
        conn.commit()    

            
      

            
      