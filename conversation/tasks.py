from .utils import create_notes_for_messages,map_conversations
from celery import shared_task

@shared_task()
def start_conversation_migration():
    print("Creating notes for messages started...")
    create_notes_for_messages()
    print("creating notes for messages finisheed..")
