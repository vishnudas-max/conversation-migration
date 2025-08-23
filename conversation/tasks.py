from .utils import map_remaining
from celery import shared_task

@shared_task()
def start_conversation_migration():
    print("Starting remaining migration...")
    map_remaining()
    print("Remaining migration finished..")
