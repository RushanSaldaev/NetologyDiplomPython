import vk_api
import psycopg2
import datetime

from vk_api.utils import get_random_id
from vk_api.longpoll import VkLongPoll, VkEventType 
from operator import itemgetter

from RS_dbase import create_db, clear_db, check_person, add_person, add_photo, person_list, photo_list
from RS_vk_api import VK


vk_bot_token = ''
vk_user_id = ''
vk_token = ''
dbname = ''
login = ''
password = ''

person_count = 0
hometown = ''
sex = 1
status = 1
age = 0
offset = 0
current_search_id = 0

status_dict = { 1:'не женат (не замужем)',2:'встречается',3:'помолвлен(-а)',4:'женат (замужем)',5:'всё сложно',6:'в активном поиске',7:'влюблен(-а)',8:'в гражданском браке',0:'Не указано'}
sex_dict = {1:'Женский',2:'Мужской'}

person_count_flag = False
hometown_flag = False
sex_flag = False 
status_flag = False
age_flag = False

with open('token_bot.txt','r') as file:
    vk_bot_token = file.readline()

with open('db_data.txt','r') as file:
    lines = [line.rstrip() for line in file]
    if len(lines) > 0:
        dbname = lines[0] 
        login = lines[1]    
        password = lines[2]
        
with open('token_vk.txt','r') as file:
    lines = [line.rstrip() for line in file]
    if len(lines) > 0:
        vk_token = lines[0] 
        vk_user_id = lines[1]          

# описание коннектора к БД
server_conn = f"dbname={dbname} user={login} password={password}"

# создание экземпляра сессии в VK
vk_session = vk_api.VkApi(token=vk_bot_token)
session_api = vk_session.get_api()
longpoll = VkLongPoll(vk_session)

session = VK(vk_token, vk_user_id)

def init_arg():
    global person_count_flag
    person_count_flag = False

def write_msg(user_id, message, attachment=None):
    vk_session.method('messages.send', {'user_id': user_id, 'message': message, 'random_id': get_random_id(), "attachment": attachment })

def start_work():
    global server_conn
    with psycopg2.connect(server_conn) as conn:
        create_db(conn)
        clear_db(conn)
    conn.close()

def finish_work():
    global server_conn
    with psycopg2.connect(server_conn) as conn:
        clear_db(conn)
    conn.close()
    
def start_search():
    global server_conn
    global person_count
    global hometown
    global sex
    global status
    global age
    global offset
    global current_search_id
    
    current_search_id = get_random_id()
    #print(person_count,hometown,sex,status,age,offset)

    dataU = session.users_search(person_count, hometown, sex, status, age, offset)
    respU_dct = dataU['response']
    #respU_cnt = respU_dct['count']
    itemsU_list = respU_dct['items']
    if len(itemsU_list) > 0:
        with psycopg2.connect(server_conn) as conn:
            for itemU in itemsU_list:
                p_id = str(itemU['id'])
                p_fname = itemU['first_name']
                p_lname = itemU['last_name']
                p_bdate = itemU['bdate']
                p_city_name = itemU['city']['title'] if 'city' in itemU  else ''
                p_hometown = itemU['home_town'] if 'home_town' in itemU else ''
                p_relation = itemU['relation'] if 'relation' in itemU else ''
                p_sex = 'женский' if itemU['sex']==1 else 'мужской'
    
                s_conn = f"\nid={p_id}, имя={p_fname}, фамилия={p_lname}, дата рождения={p_bdate}, город={p_city_name}, relation={p_relation}, пол={p_sex}, родной город={p_hometown}"
                print(s_conn)
            
                # проверяем пользователя по БД
                # сохраняем в БД только в случае если пользователя нет в БД
                if not check_person(conn, p_id ):
                    # записываем в БД нового найденного пользователя
                    add_person(conn, p_id, p_fname, p_lname, current_search_id)
                
                    # запрашиваем список фотографий пользователя, чтобы найти самые популярные
                    dataF = session.photos_get(p_id, 50)
                
                    if 'response' not in dataF: 
                        print(dataF)
                        continue
                    respF_dct = dataF['response']
                    #respF_cnt = respF_dct['count']
                    itemsF_list = respF_dct['items']
                    
                    write_msg(event.user_id, "Что-то нашли...")
                    
                    lst_tmp = []
                    if len(itemsF_list) > 0:
                        for itemF in itemsF_list:
                            d_tmp = {}
                            d_tmp['ph_id'] = itemF['id']
                            d_tmp['ph_owner_id'] = itemF['owner_id']
                            d_tmp['ph_likes'] = itemF['likes']['count']
                            d_tmp['ph_comments'] = itemF['comments']['count']
                            p_sizes_list = itemF['sizes']
                            if len(p_sizes_list) > 0:
                                # p_sizes = p_sizes_list[0]
                                d_tmp['ph_height'] = p_sizes_list[0]['height']
                                d_tmp['ph_width'] = p_sizes_list[0]['width']
                                d_tmp['ph_type'] = p_sizes_list[0]['type']
                                d_tmp['ph_url'] = p_sizes_list[0]['url']
                            # добавляем словарь с список    
                            lst_tmp.append(d_tmp)
                        # сортируем список по лайкам
                        lst_srt = sorted(lst_tmp, key=itemgetter('ph_likes'), reverse=True)
                        
                        for el in lst_srt[:3]:
                            # записываем в БД три фото найденного пользователя
                            add_photo(conn, p_id, el['ph_owner_id'], el['ph_id'], el['ph_height'], el['ph_width'], el['ph_type'], el['ph_url'], el['ph_likes'], el['ph_comments'])                        
    conn.close()    

def result_search(search_id):
    global server_conn
    #print(server_conn)
    prs_list = []
    with psycopg2.connect(server_conn) as conn:
        prs_list = person_list(conn, search_id)
    conn.close()      
    return prs_list

def photos_get(p_id):
    global server_conn
    photos_list = []
    with psycopg2.connect(server_conn) as conn:
        photos_list = photo_list(conn, p_id)
    conn.close()      
    return photos_list
    
for event in longpoll.listen():
    if event.type == VkEventType.MESSAGE_NEW:
               
        #print(person_count,hometown,sex,status,age,offset)

        if event.to_me:
            msg = event.text.lower()
            
            if person_count_flag:
                person_count = int(msg)
                if person_count < 0 or person_count > 100:
                    person_count = 100
                    write_msg(event.user_id, "Ограничиваем кол-во просматриваемых профилей = 100 профилей!")
                
                dataP = session.getProfileInfo()
                respP_dct = dataP['response']
                hometown = respP_dct['home_town'] if 'home_town' in respP_dct else ''
                status = int(respP_dct['relation']) if 'relation' in respP_dct else 1
                sex = int(respP_dct['sex']) if 'sex' in respP_dct else 1
                bdate = respP_dct['bdate'] if 'bdate' in respP_dct else ''
                if bdate != '':
                    # текущая дата
                    now = datetime.datetime.now()
                    # возраст
                    age = int(now.year) - int(str(bdate.split('.')[2]))

                write_msg(event.user_id, f"Параметры для поиска:\n...Город = {hometown}\n...Семейное положение = {status_dict[status]}\n...Возраст = {age}\n...Пол = {sex_dict[sex]}\n\nСтарт - для запуска поиска...")
                person_count_flag = False
                continue


            if msg == "привет":
                write_msg(event.user_id, "И тебе не хворать, дорогой друг!")
            elif msg == "пока":
                finish_work()
                write_msg(event.user_id, "До новых встреч!!!")
            elif msg == "начать":
                start_work()
                write_msg(event.user_id, "Создание БД перед началом запросов...")
                write_msg(event.user_id, "Какое кол-во профилей следует просмотреть?")
                person_count_flag = True
            elif msg in ["старт","еще","продолжить" ]:
                if msg in ["еще","продолжить" ]: 
                    offset = offset + person_count
                    write_msg(event.user_id, "Старт поиска...")
                start_search()
                write_msg(event.user_id, "Поиск завершён...")
            elif msg == "результат":
                rest_list = result_search(current_search_id)
                #print(rest_list)
                ixd = 0
                for item in rest_list:
                    ixd += 1
                    write_msg(event.user_id, f"==========================\n{ixd}: {item['fname']} {item['lname']}, профиль: https://vk.com/id{item['vkid']} ")                                                
                    ph_list = photos_get(item['vkid'])
                    #print('...',ph_list)
                    ixp = 0
                    for ph in ph_list:
                        ixp += 1
                        attachments = []
                        attachments.append('photo{}_{}'.format(ph['ph_owner_id'], ph['ph_id']))
                        attachment=','.join(attachments)
                        write_msg(event.user_id, f"фото {ixp}: кол-во лайков = {ph['ph_likes']}, кол-во комментов = {ph['ph_comments']}",attachment)
            else:
                write_msg(event.user_id, "Не понял вашего вопроса...")
