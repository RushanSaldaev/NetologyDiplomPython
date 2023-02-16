import vk_api
import psycopg2
import datetime

from vk_api.utils import get_random_id
from vk_api.longpoll import VkLongPoll, VkEventType 
from operator import itemgetter

from RS_dbase import create_db, clear_db, check_person, add_person
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
search_dict = {0:'Запускаем поиск... ',1:'Продолжаем поиск...'}

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
    global status_flag
    global age_flag
    global hometown_flag
    global sex_flag
    
    person_count_flag = False
    status_flag = False
    age_flag = False
    hometown_flag = False
    sex_flag = False

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
    
def age_calc(bd):
    result = 0
    if bd != '':
        now = datetime.datetime.now()
        result = int(now.year) - int(str(bd.split('.')[2]))   
    return result
    
def start_search(continue_flag=0):
    global server_conn
    global person_count
    global hometown
    global sex
    global status
    global age
    global offset
    global current_search_id
    global person_count_flag
    global status_flag
    global age_flag
    global hometown_flag
    global sex_flag
    
    current_search_id = get_random_id()
    
    if person_count == 0:
        write_msg(event.user_id, "Не задано ограничение по кол-ву просматриваемых профилей!\nКакое кол-во профилей следует просмотреть за один проход?")
        person_count_flag = True
        return
    
    # если какой-то из параметров поиска не был найден в профиле, то запрашиваем его у пользователя
    if status == 0:
        write_msg(event.user_id, "В Вашем профиле не указано семейное положение!\nУкажите семейное положение:\n1 — не женат (не замужем),\n2 — встречается,\n3 — помолвлен(-а),\n4 — женат (замужем),\n5 — всё сложно,\n6 — в активном поиске,\n7 — влюблен(-а),\n8 — в гражданском браке")
        status_flag = True
        return

    if age == 0:
        write_msg(event.user_id, "В Вашем профиле не указан возраст!\nУкажите возраст для поиска:")
        age_flag = True
        return

    if hometown == '':
        write_msg(event.user_id, "В вашем профиле не указан родной город!\nУкажите город для поиска:")
        hometown_flag = True
        return

    if sex not in [1,2]:
        write_msg(event.user_id, "В вашем профиле не указан Ваш пол! Укажите пол для поиска:\n1-Женский\n2-Мужской")
        sex_flag = True
        return
        
    if not continue_flag:
        write_msg(event.user_id, f"Параметры для поиска:\n...Город = {hometown}\n...Семейное положение = {status_dict[status]}\n...Возраст = {age}\n...Пол = {sex_dict[sex]}")
    
    write_msg(event.user_id, f"{search_dict[continue_flag]}")
    dataU = session.users_search(person_count, hometown, sex, status, age, offset)
    respU_dct = dataU['response']
    ixd = 0
    itemsU_list = respU_dct['items']
    if len(itemsU_list) > 0:
        with psycopg2.connect(server_conn) as conn:
            for itemU in itemsU_list:
                ixd += 1
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
                # сохраняем в БД только в случае, если пользователя нет в БД
                if not check_person(conn, p_id ):
                    # записываем в БД нового найденного пользователя
                    add_person(conn, p_id, current_search_id)
                
                    # запрашиваем список фотографий пользователя, чтобы найти самые популярные
                    dataF = session.photos_get(p_id, 50)
                
                    if 'response' not in dataF: 
                        print(dataF)
                        continue
                    respF_dct = dataF['response']
                    itemsF_list = respF_dct['items']

                    lst_tmp = []
                    if len(itemsF_list) > 0:
                        # выводим результат поиска только если есть хотя бы одна фотка
                        write_msg(event.user_id, f"==========================\n{ixd}: {p_fname} {p_lname}, профиль: https://vk.com/id{p_id} ") 
                        
                        for itemF in itemsF_list:
                            d_tmp = {}
                            d_tmp['ph_id'] = itemF['id']
                            d_tmp['ph_owner_id'] = itemF['owner_id']
                            d_tmp['ph_likes'] = itemF['likes']['count']
                            d_tmp['ph_comments'] = itemF['comments']['count']
                            # добавляем словарь с список    
                            lst_tmp.append(d_tmp)
                        # сортируем список по лайкам
                        lst_srt = sorted(lst_tmp, key=itemgetter('ph_likes'), reverse=True)
                        
                        ixp = 0
                        for el in lst_srt[:3]:
                            # не записывая в БД сразу показываем до трех фото найденного пользователя, 
                            # с максимальным количеством лайков
                   
                            ixp += 1
                            attachments = []
                            attachments.append('photo{}_{}'.format(el['ph_owner_id'], el['ph_id']))
                            attachment=','.join(attachments)
                            write_msg(event.user_id, f"фото {ixp}: кол-во лайков = {el['ph_likes']}, кол-во комментов = {el['ph_comments']}",attachment)
                            
    conn.close() 
    write_msg(event.user_id, "Поиск завершён...")

# основной цикл проверки сообщений    
for event in longpoll.listen():
    if event.type == VkEventType.MESSAGE_NEW:
               
        if event.to_me:
            msg = event.text.lower()
            
            if person_count_flag:
                person_count = int(msg)
                if person_count < 0 or person_count > 100:
                    person_count = 100
                    write_msg(event.user_id, "Ограничиваем кол-во просматриваемых профилей = 100 профилей!")
                person_count_flag = False
                start_search(0)
                continue

            if status_flag:
                status = int(msg)
                if (status < 1 or status > 8):
                    status = 1
                    write_msg(event.user_id, "Ограничиваем поиcк семейным положением не женат (не замужем)!")
                status_flag = False
                start_search(0)
                continue    

            if age_flag:
                age = int(msg)
                if age < 0 or age > 100:
                    age = 25
                    write_msg(event.user_id, "Ограничиваем поиcк возрастом в 25 лет!")
                age_flag = False
                continue


            if hometown_flag:
                hometown = msg
                if hometown == '':
                    hometown = 'Москва'
                    write_msg(event.user_id, "Ограничиваем поиск по городу Москва!")
                hometown_flag = False
                continue

            if sex_flag:
                sex = int(msg)
                if sex not in [1,2]:
                    sex = 1
                    write_msg(event.user_id, "Ограничиваем поиск женским полом!")
                sex_flag = False
                continue


            if msg == "привет":
                write_msg(event.user_id, "И тебе не хворать, дорогой друг!")
            elif msg == "пока":
                finish_work()
                write_msg(event.user_id, "До новых встреч!!!")
            elif msg == "начать":
                start_work()
                write_msg(event.user_id, "Создание БД перед началом запросов...")
                
                # получаем всю необходимую информацию из профиля пользователя
                dataP = session.getProfileInfo()
                respP_dct = dataP['response']
                hometown = respP_dct['home_town'] if 'home_town' in respP_dct else ''
                status = int(respP_dct['relation']) if 'relation' in respP_dct else 1
                sex = int(respP_dct['sex']) if 'sex' in respP_dct else 0
                # меняем пол на противоположный (для более адекватного поиска)
                sex = 1 if sex == 2 else 1 
                bdate = respP_dct['bdate'] if 'bdate' in respP_dct else ''
                age = age_calc(bdate)
                
                write_msg(event.user_id, "'старт' - для запуска функции поиска. ")
                write_msg(event.user_id, "'еще' или 'продолжить' - для продолжения поиска по текущим параметрам. ")
            elif msg == "старт":
                start_search(0)
            elif msg in ["еще","продолжить" ]:
                offset = offset + person_count
                start_search(1)
            else:
                write_msg(event.user_id, "Не понял вашего вопроса...")
