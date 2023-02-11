def create_db(conn):
    with conn.cursor() as cur:

        cur.execute("""
                    
            CREATE TABLE IF NOT EXISTS vk_person (
                id SERIAL PRIMARY KEY,
                vk_person_id INTEGER NOT NULL, 
                last_name VARCHAR(100) NOT NULL,
                first_name VARCHAR(50) NOT null,
                search_id INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS vk_photos (
                id SERIAL PRIMARY KEY,
                vk_person_id INTEGER NOT NULL, 
                ph_owner_id INTEGER DEFAULT 0, 
                ph_id INTEGER DEFAULT 0,
                ph_height INTEGER DEFAULT 0,
                ph_width INTEGER DEFAULT 0,
                ph_type VARCHAR(1) DEFAULT '',
                ph_url VARCHAR(500) DEFAULT '',
                ph_likes INTEGER DEFAULT 0,
                ph_comments INTEGER DEFAULT 0
            );

        """)

        conn.commit()
        print('  БД создана!')  
    

def drop_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE vk_photos;
            DROP TABLE vk_person;
        """)
        conn.commit()
        print('  БД удалена!') 

def clear_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM vk_photos;
            DELETE FROM vk_person;
        """)
        conn.commit()
        print('  БД очищена!') 

def check_person(conn, p_id):
    result = False
    with conn.cursor() as cur:
        sql_query = f"""
        SELECT id, vk_person_id, first_name, last_name
        FROM vk_person
        WHERE vk_person_id={p_id};
        """
        cur.execute(sql_query)     

        for row in cur.fetchall():
            result = True

    return result


def add_person(conn, p_id, first_name, last_name, search_id):
    with conn.cursor() as cur:
        sql_query = f"""
        INSERT INTO vk_person(vk_person_id, first_name, last_name, search_id) 
        VALUES( {p_id}, '{first_name}', '{last_name}', {search_id} );
        """        
        cur.execute(sql_query)
        conn.commit()    

def add_photo(conn, p_id, ph_owner_id, ph_id, ph_height, ph_width, ph_type, ph_url, ph_likes, ph_comments):
    with conn.cursor() as cur:
        sql_query = f"""
        INSERT INTO vk_photos(vk_person_id, ph_owner_id, ph_id, ph_height, ph_width, ph_type, ph_url, ph_likes, ph_comments) 
        VALUES( {p_id}, {ph_owner_id}, {ph_id}, '{ph_height}', '{ph_width}', '{ph_type}', '{ph_url}',  '{ph_likes}', '{ph_comments}' );
        """        
        cur.execute(sql_query)
        conn.commit()    


def person_list(conn, search_id):
    lst_tmp = []
    with conn.cursor() as cur:
        cur.execute(f"""
        with 
        rest1 as ( select * from vk_person where search_id = {search_id}),
        rest2 as ( 
            select 
            vk_person_id, 
            count(*) as ph_cnt, 
            sum(ph_likes) as ph_sum_likes
            from vk_photos
            group by vk_person_id 
        )
        select rest1.id, rest1.vk_person_id, rest1.last_name, rest1.first_name, rest2.ph_cnt, rest2.ph_sum_likes
        from rest1
        inner join rest2
            on rest1.vk_person_id = rest2.vk_person_id
        ;
        """)
        for person in cur.fetchall():
            sid, vkid, lname, fname, vkcnt, vksumlikes = person
            d_tmp = {}
            d_tmp['sid']=sid
            d_tmp['vkid']=vkid
            d_tmp['lname']=lname
            d_tmp['fname']=fname
            d_tmp['vkcnt']=vkcnt
            d_tmp['vksumlikes']=vksumlikes
            lst_tmp.append(d_tmp)
    return lst_tmp
            

def photo_list(conn, p_id):
    lst_tmp = []
    with conn.cursor() as cur:
        cur.execute(f"""
        select 
          vp.vk_person_id, 
          vp.ph_owner_id,
          vp.ph_id, 
          vp.ph_url, 
          vp.ph_type,
          vp.ph_likes,
          vp.ph_comments
        from vk_photos vp 
        where vk_person_id = {p_id};
        """)
        for photo in cur.fetchall():
            ph_vkid, ph_ownid, ph_id, ph_url, ph_type, ph_likes, ph_comments = photo
            d_tmp = {}
            d_tmp['ph_vkid']=ph_vkid
            d_tmp['ph_owner_id']=ph_ownid
            d_tmp['ph_id']=ph_id
            d_tmp['ph_url']=ph_url
            d_tmp['ph_type']=ph_type
            d_tmp['ph_likes']=ph_likes
            d_tmp['ph_comments']=ph_comments
            lst_tmp.append(d_tmp)
    return lst_tmp            

            
      