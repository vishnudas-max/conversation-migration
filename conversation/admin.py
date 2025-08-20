from django.contrib import admin

# Register your models here.
from .models import cercuscontact, inkadmincontact, cfieldmapping, conversation, messages

admin.site.register(cercuscontact)
admin.site.register(inkadmincontact)
admin.site.register(cfieldmapping)
admin.site.register(conversation)
admin.site.register(messages)