import requests

class VK:
    URL_VK_PHOTOS_GET: str = 'https://api.vk.com/method/photos.get'
    URL_VK_USERS_SEARCH: str = 'https://api.vk.com/method/users.search'

    def __init__(self, access_token, user_id, version='5.131'):
        self.token = access_token
        self.id = user_id
        self.version = version
        self.params = {'access_token': self.token, 'v': self.version}

    def photos_get(self, vk_user_id, photos_count):
        params = {
            'owner_id': vk_user_id,
            'album_id': 'wall',
            'feed_type': 'photo',
            'extended': '1',
            'rev': '1',
            'count': photos_count,
            'photo_sizes': '1'
        }
        response = requests.get(self.URL_VK_PHOTOS_GET,
                                params={
                                    **self.params,
                                    **params
                                })
        return response.json()

  
    def users_search(self, person_count, hometown, sex, status, age, offset):
        params = {
            'count': person_count,
            'fields': 'city,education,relation,nickname,photo_max,sex,home_town,bdate,books,status',
            'hometown': hometown, 
            'sex': sex,
            'status': status,
            'age_from': age,
            'age_to': age,
            'offset': offset
        }
        response = requests.get(self.URL_VK_USERS_SEARCH,
                                params={
                                    **self.params,
                                    **params
                                })
        return response.json()
