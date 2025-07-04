import logging
from fastapi import FastAPI

logger = logging.getLogger("uvicorn.error")

class EventStore:

    def __init__(self, max_events_per_user=10):

        self.events = {}
        self.max_events_per_user = max_events_per_user

    def put(self, user_id, item_id):
        """
        Сохраняет событие
        """
        try:
            user_events = self.events[user_id]
            self.events[user_id] = [item_id] + user_events[: self.max_events_per_user]
        except KeyError:
            self.events[user_id] = [item_id]

    def get(self, user_id, k):
        """
        Возвращает события для пользователя
        """
        try:
            user_events = self.events[user_id][: k]
        except KeyError:
            logger.error(f"No recommendations found for user: {user_id}")
            user_events = []

        return user_events

events_store = EventStore()

# создаём приложение FastAPI
app = FastAPI(title="events")

@app.post("/put")
async def put(user_id: int, item_id: int):
    """
    Сохраняет событие для user_id, item_id
    """

    events_store.put(user_id, item_id)

    return {"result": "ok"}

@app.post("/get")
async def get(user_id: int, k: int = 10):
    """
    Возвращает список последних k событий для пользователя user_id
    при наличии истории недавних взаимодействий (k =< len(user_events))
    """

    events = events_store.get(user_id, k)

    return {"events": events}