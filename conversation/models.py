from django.db import models

# Create your models here.

class inkadmincontact(models.Model):
    contact_id = models.CharField(max_length=100)
    locationId= models.CharField()
    phone = models.CharField(max_length=15, blank=True, null=True)
    email = models.EmailField(max_length=254, blank=True, null=True)
    def __str__(self):
        return f"({self.contact_id})"

class cercuscontact(models.Model):
    contact_id = models.CharField(max_length=100, unique=True)
    inkadmin_contact = models.OneToOneField(inkadmincontact, on_delete=models.CASCADE, related_name='cercus_contacts', blank=True, null=True)
    locationId= models.CharField()
    is_newly_created = models.BooleanField(default=False)
    phone = models.CharField(max_length=15, blank=True, null=True)
    email = models.EmailField(max_length=254, blank=True, null=True)
    
    def __str__(self):
        return f" ({self.contact_id})"
    

    

class cfieldmapping(models.Model):
    field_name = models.CharField(max_length=100)
    cercus_cfield_id=models.CharField(max_length=100,blank=True,null=True)
    inkadmin_cfield_id=models.CharField(max_length=100,blank=True,null=True)

    def __str__(self):
        return f"{self.field_name}"


class conversation(models.Model):
    c_contact = models.ForeignKey(cercuscontact, on_delete=models.CASCADE, related_name='conversations',blank=True, null=True)
    c_conversation_id=models.CharField(max_length=100, unique=True,blank=True, null=True)
    i_contact = models.ForeignKey(inkadmincontact, on_delete=models.CASCADE, related_name='conversations',blank=True, null=True)
    i_conversation_id=models.CharField(max_length=100, unique=True,blank=True, null=True)
    is_migrated = models.BooleanField(default=False)

    def __str__(self):
        return f"Conversation inka-conv-id {self.i_conversation_id} - cercus-conv-id {self.c_conversation_id}"

class i_messages(models.Model):
    i_message_id = models.CharField(max_length=100)
    conversation = models.ForeignKey(conversation, on_delete=models.CASCADE, related_name='i_messages',blank=True, null=True)
    msg_type = models.CharField(max_length=40)
    emil_msg_ids=models.JSONField(blank=True, null=True)


    def __str__(self):
        return f"Message inka-msg-id {self.i_message_id} - of type {self.msg_type} in conversation {self.conversation}  "
    

class c_messages(models.Model):
    c_message_id = models.CharField(max_length=100)
    conversation = models.ForeignKey(conversation, on_delete=models.CASCADE, related_name='c_messages',blank=True, null=True)
    i_message = models.ForeignKey(i_messages, on_delete=models.CASCADE, related_name='c_messages',blank=True, null=True)
    msg_type = models.CharField(max_length=40)
    is_reply = models.BooleanField(default=False)
    c_email_msg_id = models.CharField(max_length=100, blank=True, null=True)
    i_email_msg_id= models.CharField(max_length=100, blank=True, null=True)
    call_recording_url = models.URLField(blank=True, null=True)
