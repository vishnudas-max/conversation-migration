from django.contrib import admin

# Register your models here.
from .models import cercuscontact, inkadmincontact, cfieldmapping

admin.site.register(cercuscontact)
admin.site.register(inkadmincontact)
admin.site.register(cfieldmapping)