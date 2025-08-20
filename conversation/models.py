from django.db import models

# Create your models here.

class inkadmincontact(models.Model):
    contact_id = models.CharField(max_length=100)
    locationId= models.CharField()
    def __str__(self):
        return f"({self.contact_id})"

class cercuscontact(models.Model):
    contact_id = models.CharField(max_length=100, unique=True)
    inkadmin_contact = models.OneToOneField(inkadmincontact, on_delete=models.CASCADE, related_name='cercus_contacts', blank=True, null=True)
    locationId= models.CharField()
    is_newly_created = models.BooleanField(default=False)

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

    def __str__(self):
        return f"Conversation inka-conv-id {self.i_conversation_id} - cercus-conv-id {self.c_conversation_id}"

class messages(models.Model):
    i_message_id = models.CharField(max_length=100)
    c_message_id = models.CharField(max_length=100, blank=True, null=True)
    conversation = models.ForeignKey(conversation, on_delete=models.CASCADE, related_name='messages',blank=True, null=True)


    def __str__(self):
        return f"Message inka-msg-id {self.i_message_id} - cercus-msg-id {self.c_message_id}"