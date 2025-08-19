from django.db import models

# Create your models here.


class cercuscontact(models.Model):
    contact_id = models.CharField(max_length=100, unique=True)
    first_name = models.CharField(max_length=100,blank=True, null=True)
    last_name = models.CharField(max_length=100,blank=True, null=True)
    phone = models.CharField(max_length=15,blank=True, null=True)
    email = models.EmailField(blank=True, null=True)
    locationId= models.CharField()
    is_newly_created = models.BooleanField(default=False)


    def __str__(self):
        return f"{self.first_name} {self.last_name} ({self.contact_id})"
    

class inkadmincontact(models.Model):
    contact_id = models.CharField(max_length=100)
    first_name = models.CharField(max_length=100,blank=True, null=True)
    last_name = models.CharField(max_length=100,blank=True, null=True)
    phone = models.CharField(max_length=15,blank=True, null=True)
    email = models.EmailField(blank=True, null=True)
    locationId= models.CharField()
    cercuscontact = models.ForeignKey(cercuscontact, on_delete=models.CASCADE, related_name='ink_admin_contacts',blank=True,null=True)
    def __str__(self):
        return f"{self.first_name} {self.last_name} ({self.contact_id})"
    

class cfieldmapping(models.Model):
    field_name = models.CharField(max_length=100)
    cercus_cfield_id=models.CharField(max_length=100,blank=True,null=True)
    inkadmin_cfield_id=models.CharField(max_length=100,blank=True,null=True)

    def __str__(self):
        return f"{self.field_name}"


