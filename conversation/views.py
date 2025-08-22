from django.shortcuts import render

# Create your views here.
from rest_framework.views import APIView
from rest_framework.response import Response
from .utils import map_conversations
from .tasks import start_conversation_migration

class ConversationView(APIView):
    def get(self, request):
        start = int(request.query_params.get('start', 0))
        print(start)
        if start == 1:
            print("Starting conversation migration...")
            start_conversation_migration.delay()
        else:
            print("pass query parameter start=1 to start migration")
        return Response({"message": "pass query parameter start=1 to start migration"})