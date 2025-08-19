import requests
from django.conf import settings
from requests.exceptions import RequestException
from .models import cercuscontact,inkadmincontact,cfieldmapping
import phonenumbers
import pycountry
from django.db import transaction
from django.db.models import Q


def country_name_to_code(country_name):
    try:
        country = pycountry.countries.lookup(country_name)
        return country.alpha_2
    except LookupError:
        return None

def normalize_phone(phone):
    try:
        # Try to parse as E.164 (with country code)
        parsed = phonenumbers.parse(phone, None)
        return str(parsed.national_number)
    except Exception:
        # If parsing fails, return as is
        return phone


def add_contacts_to_db(ghlcontacts, locationId,is_cercus):
    # Get the GHLAccessToken instance for the location


    contacts_to_create = []
    contacts_to_update = []
    emails_to_create = []
    phones_to_create = []

    # Get existing contacts in one query for efficiency
    contact_ids = [str(contact.get('id')) for contact in ghlcontacts]


    if is_cercus:
        existing_contacts = {
            contact.contact_id: contact
            for contact in cercuscontact.objects.filter(locationId=locationId, contact_id__in=contact_ids)
        }
    else:
        existing_contacts = {
            contact.contact_id: contact
            for contact in inkadmincontact.objects.filter(locationId=locationId, contact_id__in=contact_ids)
        }

  
    seen_ids=set()
    for contact in ghlcontacts:
        contact_id = str(contact.get('id'))
        if contact_id in seen_ids:
            continue
        seen_ids.add(contact_id)
        first_name = contact.get('firstNameLowerCase')
        last_name = contact.get('lastNameLowerCase')
        email = contact.get('email')
        not_parsed_phone = contact.get('phone')
        if not_parsed_phone:
            phone = normalize_phone(not_parsed_phone)
        else:
            phone = None
        city = contact.get('city')
        state = contact.get('state')
        zip_code = contact.get('postalCode')
        
        country_fullname = contact.get('country')
        country = None
        if country_fullname:
            country = country_name_to_code(country_fullname)

        street = contact.get('address')
        source_modified_on_str = contact.get('dateUpdated')
        

        # Extra emails/phones (excluding the primary)
        additionalEmailobj =contact.get('additionalEmails')
        additionalPhoneobj =contact.get('additionalPhones')
        # extra_emails = extract_email_values(additionalEmailobj,email) #extract the email wethere it's array of object or array of values
        # extra_phones = extract_phone_values(additionalPhoneobj,email)
        
        existing_contact = existing_contacts.get(contact_id)

        if existing_contact:
            # Check if source data has been modified since last sync
                # Update all fields since source data has changed
                existing_contact.first_name = first_name
                existing_contact.last_name = last_name
                existing_contact.email = email
                existing_contact.phone = phone                
                contacts_to_update.append(existing_contact)

                # # Remove old extra emails/phones and add new ones
                # existing_contact.extra_emails.all().delete()
                # existing_contact.extra_phones.all().delete()
                
                # for e in extra_emails:
                #     emails_to_create.append(GHLEmail(ghlcontact=existing_contact, email=e))
                    
                # for p in extra_phones:
                #     phones_to_create.append(GHLPhone(ghlcontact=existing_contact, phone=p))
        else:
            # New contact
            if is_cercus:
                new_contact = cercuscontact(
                    contact_id=contact_id,
                    first_name=first_name,
                    last_name=last_name,
                    email=email,
                    phone=phone,
                    locationId=locationId,
                )
                contacts_to_create.append((new_contact))
            else:
                new_contact = inkadmincontact(
                    contact_id=contact_id,
                    first_name=first_name,
                    last_name=last_name,
                    email=email,
                    phone=phone,
                    locationId=locationId,
                )
                contacts_to_create.append((new_contact))

    try:
        total_processed = 0
        total_created = 0
        total_updated =0
        with transaction.atomic():
            # Create new contacts in bulk
            if contacts_to_create:
                if is_cercus:
                    cercuscontact.objects.bulk_create(contacts_to_create, batch_size=500)
                else:
                    inkadmincontact.objects.bulk_create(contacts_to_create, batch_size=500)

                total_created = len(contacts_to_create)
                total_processed += total_created
    
                
            # Bulk update contacts
            if contacts_to_update:
                if is_cercus:
                    cercuscontact.objects.bulk_update(
                        contacts_to_update,
                        ['first_name', 'last_name', 'email', 'phone'],
                        batch_size=500
                    )
                else:
                    inkadmincontact.objects.bulk_update(
                        contacts_to_update,
                        ['first_name', 'last_name', 'email', 'phone'],
                        batch_size=500
                    )

                total_updated = len(contacts_to_update)
                total_processed += total_updated
           


            # Bulk create emails and phones
            # if emails_to_create:
            #     GHLEmail.objects.bulk_create(emails_to_create, batch_size=500)
            # if phones_to_create:
            #     GHLPhone.objects.bulk_create(phones_to_create, batch_size=500)


            # GHLContact.objects.filter(location=ghlLocation,contact_id__in=contact_ids).update(is_active=True)

        
        return total_processed,total_created,total_updated

    except Exception as e:
        print(f"Failed to save GHL contacts: {e}")
        raise e
    




def fetchcercuscontacts(access_token,location_id,is_cercus):

    
    ghl_access_token = access_token
    location_id = location_id
    
    headers = {
        "Authorization": f"Bearer {ghl_access_token}",
        "Accept": "application/json",
        "Version": settings.GHL_API_VERSION,
    }
    
    searchAfter = None
    base_url = "https://services.leadconnectorhq.com/contacts/search"
    
    # Tracking variables
    total_fetched = 0
    total_processed = 0
    total_created = 0
    total_updated = 0
    has_more = True

    print('Fetching and Processing all contacts from GHL......')
    
    try:
        while has_more:
            payload = {
                "locationId": location_id,
                "pageLimit": 100
            }
            
            # If there is searchAfter add that into body
            if searchAfter:
                payload["searchAfter"] = searchAfter
            
            response = requests.post(
                base_url,
                headers=headers,
                json=payload,
                timeout=30
            )
            
            response.raise_for_status()
            data = response.json()
            
            page_contacts = data.get("contacts", [])
            batch_fetched = len(page_contacts)
            total_fetched += batch_fetched
            
            if not page_contacts:
                has_more = False
                continue
            
            # Process this batch immediately
            try:
                batch_pocessed, batch_created, batch_updated = add_contacts_to_db(page_contacts, location_id,is_cercus)
                total_processed += batch_pocessed
                total_created += batch_created
                total_updated += batch_updated
            except Exception as e:
                print(f"Error processing batch: {e}")

            # Get searchAfter from the last contact in the batch
            searchAfter = page_contacts[-1].get("searchAfter", None)
            has_more = bool(searchAfter)
            
    except (RequestException, ValueError, KeyError) as e:
        print(f"Error fetching contacts: {e}")
        print(response.text)
         # Rollback is_active to previous state
    
    except IndexError:
        # This will catch if page_contacts is empty and we try to access [-1]
        print("No contacts in page, stopping pagination.")
        has_more = False
        
    print(f"Completed Contact Fetch: Total fetched {total_fetched}\n Total processed {total_processed}\n Total created {total_created} \n Total Updated {total_updated}")
    return f"Fetching and adding or updating contacts finished successfully!..."



def fetch_contacts_cercus():
    access_token = settings.CERCUS_GHL_ACCESS_TOKEN
    location_id = settings.CERCUS_LOCATION_ID
    fetchcercuscontacts(access_token, location_id,is_cercus=True)

def fetch_contacts_inkadmin():
    access_token = settings.INKA_GHL_ACCESS_TOKEN
    location_id = settings.INKA_LOCATION_ID
    fetchcercuscontacts(access_token, location_id,is_cercus=False)


def get_contact(location_id,access_token,contact_id,):

    if not contact_id:
        raise ValueError("contact_id is required")

    url = f"https://services.leadconnectorhq.com/contacts/{contact_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Version": settings.GHL_API_VERSION, 
    }

    # location_id is typically implied by the token; not required for this endpoint.
    # You can pass it as a query param if you want to be explicit, most servers ignore it.
    params = {"locationId": location_id} if location_id else None

    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code == 404:
        return None
    r.raise_for_status()

    data = r.json()

    return data.get("contact", data)


def get_cercus_inkadmin_contact_cfieldid() -> str | None:
    # fetch_custom_fields should return {name: id}
    c_fields = fetch_custom_fields(
        settings.CERCUS_LOCATION_ID,
        settings.CERCUS_GHL_ACCESS_TOKEN,
        settings.GHL_API_VERSION,
    )
   
    if isinstance(c_fields, dict):
        return c_fields.get("InkAdmin Contact ID")
    
    for f in c_fields or []:
        if (f.get("InkAdmin Contact ID")):
            return f.get("id")
    return None



def create_contact(contact_data: dict, location_id: str, access_token: str) -> dict:
    url = "https://services.leadconnectorhq.com/contacts"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Version": settings.GHL_API_VERSION,
    }

    email = contact_data.get("email")
    phone = contact_data.get("phone")

    first_name = (
        contact_data.get("firstName")
        or contact_data.get("firstNameLowerCase")
        or None
    )
    last_name = (
        contact_data.get("lastName")
        or contact_data.get("lastNameLowerCase")
        or None
    )
    full_name = (
        contact_data.get("fullName")
        or contact_data.get("fullNameLowerCase")
        or (" ".join([n for n in [first_name, last_name] if n]) or None)
    )

    address = contact_data.get("address") or None

    additionalemails = contact_data.get("additionalEmails") or []
    additionalphones = contact_data.get("additionalPhones") or []

    # Build mapped custom fields for Cercus (id + value)
    pcfields = []
    for field in contact_data.get("customFields") or []:

        ink_id = str(field.get("id") or "")
        value = field.get("value", field.get("field_value"))
        if not ink_id or value in (None, ""):
            continue

        mapping = cfieldmapping.objects.filter(inkadmin_cfield_id=ink_id).first()
        if mapping:
            pcfields.append({"id": mapping.cercus_cfield_id, "field_value": value})

    # Add a custom field that captures the InkAdmin contact id (if mapping exists)
    cfield_id = get_cercus_inkadmin_contact_cfieldid()
    if cfield_id and contact_data.get("id"):
        pcfields.append({"id": cfield_id, "value": contact_data["id"]})

    # Tags: make sure inkadmin is present, avoid duplicates
    tags = contact_data.get("tags") or []
    tags_lower = {t.lower(): t for t in tags if isinstance(t, str) and t.strip()}
    if "inkadmin" not in tags_lower:
        tags.append("inkadmin")

    payload = {
        "locationId": location_id,
        "email": email,
        "phone": phone,
        "firstName": first_name,
        "lastName": last_name,
        "fullName": full_name,
        "additionalEmails": [e for e in additionalemails if e],
        "additionalPhones": [p for p in additionalphones if p],
        "address": address,
        "customFields": pcfields,
        "tags": tags,
    }

    print(f"Creating contact {contact_data.get('id')} firstname: {first_name} lastname: {last_name} email: {email} phone: {phone}\n")
    return True
    # r = requests.post(url, headers=headers, json=payload, timeout=30)
    # r.raise_for_status()
    # data = r.json()

    # return data.get("contact", data)


def map_contacts():
    inkadmin_contacts = inkadmincontact.objects.all()
    created = 0
    for contact in inkadmin_contacts:
        email = (contact.email).strip() if contact.email else None
        phone = (contact.phone).strip() if contact.phone else None
        fname = (contact.first_name).strip() if contact.first_name else None
        lname = (contact.last_name).strip() if contact.last_name else None
        # name = (f"{fname} {lname}").strip()
        ccontact = None
        if email or phone:
            ccontact = cercuscontact.objects.filter(Q(email=email) | Q(phone=phone)).first()
        else:
            ccontact = cercuscontact.objects.filter(first_name=fname,last_name=lname).first()
        
        if ccontact:
            contact.cercuscontact = ccontact 
            contact.save()

        if not ccontact:
            contact_detailes = get_contact(settings.INKA_LOCATION_ID, settings.INKA_GHL_ACCESS_TOKEN, contact.contact_id)
            if not contact_detailes:
                print(f"Contact {contact.contact_id} not found in GHL")
            
            is_created = create_contact(contact_detailes,settings.CERCUS_LOCATION_ID,settings.CERCUS_GHL_ACCESS_TOKEN)
            if is_created:
                created += 1
                print(f"Contact {contact.contact_id} created in GHL")
                
    print(f"Total contacts created in GHL: {created}")

def fetch_custom_fields(location_id: str, token: str, version: str) -> dict[str, str]:
    url = f"https://services.leadconnectorhq.com/locations/{location_id}/customFields"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Version": version,
    }
    params = {"model": "contact"}

    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    fields = data.get("customFields", []) or []
    out: dict[str, str] = {}
    for f in fields:
        name = (f.get("name") or "").strip()
        if not name:
            continue
        out[name] = f.get("id")
    return out


@transaction.atomic
def mapcustomFields():
    try:
        inka_fields = fetch_custom_fields(
            settings.INKA_LOCATION_ID, settings.INKA_GHL_ACCESS_TOKEN, settings.GHL_API_VERSION
        )
        cercus_fields = fetch_custom_fields(
            settings.CERCUS_LOCATION_ID, settings.CERCUS_GHL_ACCESS_TOKEN, settings.GHL_API_VERSION
        )
    except Exception as e:
        print(f"Error fetching custom fields: {e}")
        return

    print(f"inka customfields: {inka_fields}")
    print(f"cercus customfields: {cercus_fields}")

    created = updated = 0
    for field_name, inkadmin_cfield_id in inka_fields.items():
        c_cfield_id = cercus_fields.get(field_name)
        if not c_cfield_id:
            print(f"Skipping field '{field_name}' as it does not exist in cercus custom fields.")
            continue

        # NOTE: don't shadow the model class name; use a different var
        mapping_obj, was_created = cfieldmapping.objects.update_or_create(
            field_name=field_name,
            defaults={
                "inkadmin_cfield_id": inkadmin_cfield_id,
                "cercus_cfield_id": c_cfield_id,
            },
        )
        if was_created:
            created += 1
        else:
            updated += 1

    print(f"Custom field mappings: created={created}, updated={updated}")



