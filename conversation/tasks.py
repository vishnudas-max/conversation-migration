from .utils import map_conversations
from celery import shared_task

@shared_task()
def start_conversation_migration():
    print("Starting conversation migration...")
    map_conversations()
    print("conversation migration finished..")
