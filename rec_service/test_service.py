import requests
import pandas as pd
import logging
from rec_service.constants import events_store_url, recommendations_url

# создадим логгер
logger = logging.getLogger("test_service")
# создадим вывод сообщений в консоль и в .log файл
stream_handler = logging.StreamHandler()
file_handler = logging.FileHandler("test_service.log", mode='w')
# укажем уровень сообщений и добавим обработчики
logger.setLevel(logging.INFO)
logger.addHandler(stream_handler)
logger.addHandler(file_handler)

headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

# холодный пользователь               
params_1 = {"user_id": 15, 'k': 5}
# пользователь без онлайн истории
params_2 = {"user_id": 1374582, 'k': 5}

resp_cold = requests.post(recommendations_url + "/recommendations", headers=headers, params=params_1)
resp_personal = requests.post(recommendations_url + "/recommendations", headers=headers, params=params_2)

# добавим взаимодействия
user_id = 1374582
event_item_ids = [99262, 41779891, 30964988, 673041]

for event_item_id in event_item_ids:
    resp = requests.post(events_store_url + "/put", 
                         headers=headers, 
                         params={"user_id": user_id, "item_id": event_item_id})

# пользователь с онлайн историей
params_3 = {"user_id": 1374582, 'k': 5}

resp_online = requests.post(recommendations_url + "/recommendations", headers=headers, params=params_3)

if resp.status_code == 200:

    recs_cold = resp_cold.json()["recs"]
    recs_personal = resp_personal.json()["recs"]
    recs_online = resp_online.json()["recs"]

    print(recs_cold)
    print(recs_personal)
    print(recs_online) 

    # загрузим данные для получения названия треков
    items = pd.read_parquet("/home/mle-user/mle-project-sprint-4-v001/items.parquet")

    def display_items(item_ids):
        """
        Отображает id треков в названия
        """

        item_columns_to_use = ["item_id", "name"]
        
        items_selected = items.query("item_id in @item_ids")[item_columns_to_use]
        items_selected = items_selected.set_index("item_id").reindex(item_ids)
        items_selected = items_selected.reset_index()
        
        return items_selected

    logger.info(f"Холодный пользователь: \n\
    {display_items(recs_cold)}")

    logger.info(f"Персональные Без онлайн истории: \n\
    {display_items(recs_personal)}")

    logger.info(f"Онлайн-события: \n\
    {display_items(event_item_ids)}")

    logger.info(f"Персональные с онлайн историей: \n\
    {display_items(recs_online)}")

else:
    logger.info(f"Internal Server Error, status code: {resp.status_code}")