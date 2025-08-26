from django.contrib import admin

# Register your models here.
from .models import cercuscontact, inkadmincontact, cfieldmapping, conversation, i_messages, c_messages, Notes

@admin.register(Notes)
class NotesAdmin(admin.ModelAdmin):
    list_display = ('note_id', 'i_message', 'contact', 'note_type')
    list_filter = ('note_type',)
    search_fields = ('note_id', 'i_message__i_message_id', 'contact__contact_id')

@admin.register(inkadmincontact)
class InkAdminContactAdmin(admin.ModelAdmin):
    list_display = ('contact_id', 'locationId', 'phone', 'email')
    list_filter = ('locationId',)
    search_fields = ('contact_id', 'phone', 'email')


@admin.register(cercuscontact)
class CercusContactAdmin(admin.ModelAdmin):
    list_display = ('contact_id', 'inkadmin_contact', 'locationId', 'phone', 'email', 'is_newly_created')
    list_filter = ('locationId', 'is_newly_created')
    search_fields = ('contact_id', 'phone', 'email')


@admin.register(cfieldmapping)
class CFieldMappingAdmin(admin.ModelAdmin):
    list_display = ('field_name', 'cercus_cfield_id', 'inkadmin_cfield_id')
    search_fields = ('field_name', 'cercus_cfield_id', 'inkadmin_cfield_id')


@admin.register(conversation)
class ConversationAdmin(admin.ModelAdmin):
    list_display = ('i_conversation_id', 'c_conversation_id', 'i_contact', 'c_contact', 'is_migrated')
    list_filter = ('is_migrated',)
    search_fields = ('i_conversation_id', 'c_conversation_id')


@admin.register(i_messages)
class IMessagesAdmin(admin.ModelAdmin):
    list_display = ('i_message_id', 'conversation', 'msg_type')
    list_filter = ('msg_type',)
    search_fields = ('i_message_id',)


@admin.register(c_messages)
class CMessagesAdmin(admin.ModelAdmin):
    list_display = ('c_message_id', 'conversation', 'i_message', 'msg_type', 'is_reply')
    list_filter = ('msg_type', 'is_reply')
    search_fields = ('c_message_id', 'i_message__i_message_id')
