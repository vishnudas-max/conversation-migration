import requests
from django.conf import settings
from requests.exceptions import RequestException
from .models import cercuscontact, conversation,inkadmincontact,cfieldmapping,i_messages,c_messages,Notes
import phonenumbers
import pycountry
from django.db import transaction
from django.db.models import Q
from django.shortcuts import get_object_or_404
import os
from datetime import datetime




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

def get_cf_value(cf_list, wanted_id):
    """Return the value for the custom field with id=wanted_id (supports 'value' or 'field_value')."""
    if not cf_list:
        return None
    wanted_id = str(wanted_id)
    for cf in cf_list:
        if str(cf.get("id")) == wanted_id:
            return cf.get("value", cf.get("field_value"))
    return None


def add_contacts_to_db(ghlcontacts,locationId,is_cercus):
    inkadmin_link_cfield_id=settings.CFILED_ID

    contacts_to_create = []
    contacts_to_update = []

    # Build list of contact ids from API
    contact_ids = [str(c.get("id")) for c in ghlcontacts if c.get("id")]

    # Fetch existing by location + ids
    if is_cercus:
        existing_contacts = {
            c.contact_id: c
            for c in cercuscontact.objects.filter(locationId=locationId, contact_id__in=contact_ids)
        }
    else:
        existing_contacts = {
            c.contact_id: c
            for c in inkadmincontact.objects.filter(locationId=locationId, contact_id__in=contact_ids)
        }

    # If this is Cercus data and we have the link field id, pre-collect all referenced InkAdmin IDs
    inkadmin_id_by_cercus_id = {}  # cercus_contact_id -> inkadmin_contact_id
    inkadmin_ids_needed = set()

    if is_cercus and inkadmin_link_cfield_id:
        for c in ghlcontacts:
            cid = str(c.get("id") or "")

            if not cid:
                continue

            link_val = get_cf_value(c.get("customFields"), inkadmin_link_cfield_id)

            if link_val:
                link_val = str(link_val)
                inkadmin_id_by_cercus_id[cid] = link_val
                inkadmin_ids_needed.add(link_val)

    # Prefetch InkAdmin contacts referenced by custom field
    inkadmin_lookup = {}

    if is_cercus and inkadmin_ids_needed:
        q = Q(contact_id__in=inkadmin_ids_needed)
        inkadmin_lookup = {x.contact_id: x for x in inkadmincontact.objects.filter(q)}

    seen_ids = set()
    for c in ghlcontacts:
        contact_id = str(c.get("id") or "")
        phone = c.get("phone",None)
        email = c.get("email",None)

        if not contact_id or contact_id in seen_ids:
            continue
        seen_ids.add(contact_id)

        existing = existing_contacts.get(contact_id)

        # If this is a Cercus contact, try to find linked InkAdmin contact via custom field
        linked_inkadmin_obj = None

        if is_cercus and inkadmin_link_cfield_id:
            linked_inkadmin_id = inkadmin_id_by_cercus_id.get(contact_id)
            if linked_inkadmin_id:
                linked_inkadmin_obj = inkadmin_lookup.get(linked_inkadmin_id)

        if existing:
            if is_cercus:
                existing.inkadmin_contact = linked_inkadmin_obj
                existing.phone = phone
                existing.email = email
            else:
                existing.phone = phone
                existing.email = email

            contacts_to_update.append(existing)
        else:
            # Create new
            if is_cercus:
                new_obj = cercuscontact(
                    contact_id=contact_id,
                    locationId=locationId,
                    inkadmin_contact=linked_inkadmin_obj,
                    phone=phone,
                    email=email
                )
            else:
                new_obj = inkadmincontact(
                    contact_id=contact_id,
                    locationId=locationId,
                    phone=phone,
                    email=email
                )
            contacts_to_create.append(new_obj)

    try:
        with transaction.atomic():
            total_created = total_updated = 0

            if contacts_to_create:
                if is_cercus:
                    cercuscontact.objects.bulk_create(contacts_to_create, batch_size=500)
                else:
                    inkadmincontact.objects.bulk_create(contacts_to_create, batch_size=500)
                total_created = len(contacts_to_create)

            if contacts_to_update:
                fields = []

                if is_cercus:
                    fields.append("inkadmin_contact","email","phone")
                if is_cercus:
                    cercuscontact.objects.bulk_update(contacts_to_update, fields, batch_size=500)
                else:
                    inkadmincontact.objects.bulk_update(contacts_to_update, fields, batch_size=500)
                total_updated = len(contacts_to_update)

        return (total_created + total_updated), total_created, total_updated

    except Exception as e:
        print(f"Failed to save GHL contacts: {e}")
        raise



def add_inkadmin_contacts_to_db(ghlcontacts, locationId):
    """
    - Simple version for InkAdmin.
    - No link resolution needed.
    - Bulk create/update InkAdmin contacts.
    """
    contacts_to_create = []
    contacts_to_update = []

    # Build list of contact ids from API
    contact_ids = [str(c.get("id")) for c in ghlcontacts if c.get("id")]

    # Existing InkAdmin contacts for this location
    existing_by_id = {
        c.contact_id: c
        for c in inkadmincontact.objects.filter(locationId=locationId, contact_id__in=contact_ids)
    }

    seen_ids = set()
    for c in ghlcontacts:
        contact_id = str(c.get("id") or "")
        if not contact_id or contact_id in seen_ids:
            continue
        seen_ids.add(contact_id)

        phone = c.get("phone")
        email = c.get("email")

        existing = existing_by_id.get(contact_id)
        if existing:
            existing.phone = phone
            existing.email = email
            contacts_to_update.append(existing)
        else:
            new_obj = inkadmincontact(
                contact_id=contact_id,
                locationId=locationId,
                phone=phone,
                email=email,
            )
            contacts_to_create.append(new_obj)

    try:
        with transaction.atomic():
            total_created = total_updated = 0

            if contacts_to_create:
                inkadmincontact.objects.bulk_create(contacts_to_create, batch_size=500)
                total_created = len(contacts_to_create)

            if contacts_to_update:
                fields = ["email", "phone"]
                inkadmincontact.objects.bulk_update(contacts_to_update, fields, batch_size=500)
                total_updated = len(contacts_to_update)

            return (total_created + total_updated), total_created, total_updated

    except Exception as e:
        print(f"Failed to save InkAdmin contacts: {e}")
        raise


def add_cercus_contacts_to_db(ghlcontacts, locationId):
    """
    - Simple version for Cercus.
    - For each contact, fetch linked InkAdmin contact inside the loop (no prefetch).
    - Bulk create/update Cercus contacts.
    """
    inkadmin_link_cfield_id = getattr(settings, "CFILED_ID", None)

    contacts_to_create = []
    contacts_to_update = []

    # Build list of contact ids from API
    contact_ids = [str(c.get("id")) for c in ghlcontacts if c.get("id")]

    # Existing Cercus contacts for this location
    existing_by_id = {
        c.contact_id: c
        for c in cercuscontact.objects.filter(locationId=locationId, contact_id__in=contact_ids)
    }

    seen_ids = set()
    for c in ghlcontacts:
        contact_id = str(c.get("id") or "")
        if not contact_id or contact_id in seen_ids:
            continue
        seen_ids.add(contact_id)

        phone = c.get("phone")
        email = c.get("email")
        if contact_id == "MWfXItjaHeWswv1mlRT4":
            print(c.get("customFields"))
            print(inkadmin_link_cfield_id)

        # Fetch linked InkAdmin contact on each loop (simple, no lookup)
        linked_inkadmin_obj = None
        if inkadmin_link_cfield_id:
            link_val = get_cf_value(c.get("customFields"), inkadmin_link_cfield_id)
            if link_val in ["dowHfn4CLwNwGSdvgfwy","isWHRvVuR2bWvFiLdfpP","zwdLWeitH8mzNqLmXZD1","4fhHPQtoWJ5XR2ifhNsU","XP5vxJeM0cJ6YvKBYzTH","VpowxOprkIEFMiY1gVD0","6yOJ5ysFmjbNooz94yW7","LYFM9Tt6D4wqBNgoydGm"]:
                print(f"cercus contact_id {contact_id} has suspicious link value {link_val}; skipping link")
            if link_val:
                linked_inkadmin_obj = inkadmincontact.objects.filter(
                    contact_id=str(link_val)
                ).first()

        existing = existing_by_id.get(contact_id)
        if existing:
            existing.phone = phone
            existing.email = email
            existing.inkadmin_contact = linked_inkadmin_obj
            contacts_to_update.append(existing)
        else:
            new_obj = cercuscontact(
                contact_id=contact_id,
                locationId=locationId,
                phone=phone,
                email=email,
                inkadmin_contact=linked_inkadmin_obj,
            )
            contacts_to_create.append(new_obj)

    try:
        with transaction.atomic():
            total_created = total_updated = 0

            if contacts_to_create:
                cercuscontact.objects.bulk_create(contacts_to_create, batch_size=500)
                total_created = len(contacts_to_create)

            if contacts_to_update:
                fields = ["inkadmin_contact", "email", "phone"]
                cercuscontact.objects.bulk_update(contacts_to_update, fields, batch_size=500)
                total_updated = len(contacts_to_update)

            return (total_created + total_updated), total_created, total_updated

    except Exception as e:
        print(f"Failed to save Cercus contacts: {e}")
        raise

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
                if is_cercus:
                    batch_pocessed, batch_created, batch_updated = add_cercus_contacts_to_db(page_contacts, location_id)
                else:
                    batch_pocessed, batch_created, batch_updated = add_inkadmin_contacts_to_db(page_contacts, location_id)

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
    url = "https://services.leadconnectorhq.com/contacts/"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Version": settings.GHL_API_VERSION,
    }

    email = contact_data.get("email")
    phone = contact_data.get("phone")

    first_name = contact_data.get("firstName")
    last_name = contact_data.get("lastName")
    full_name = (
        contact_data.get("fullName")
        or contact_data.get("fullNameLowerCase")
        or (" ".join([n for n in [first_name, last_name] if n]) or None)
    )

    address = contact_data.get("address") or None
    source = contact_data.get("source",None)
    type = contact_data.get("type",None)
    website = contact_data.get("website",None)
    dnd = contact_data.get("dnd",None)
    dndSettings = contact_data.get("dndSettings",None)
    state = contact_data.get("state",None)
    bussinessName = contact_data.get("businessName",None)
    companyName = contact_data.get("companyName",None)
    city = contact_data.get("city",None)
    dateOfBirth = contact_data.get("dateOfBirth",None)
    assignedTo = contact_data.get("assignedTo",None)
    followers =contact_data.get("followers",[])
    opportunities =contact_data.get("opportunities",[])
    postalCode = contact_data.get("postalCode",None)
    businessId = contact_data.get("businessId",None)
    additionalemails = contact_data.get("additionalEmails") or []
    additionalphones = contact_data.get("additionalPhones") or []
    country = contact_data.get("country",None)
    
    # Build mapped custom fields for Cercus (id + value)
    pcfields = []
    for field in contact_data.get("customFields") or []:

        ink_id = str(field.get("id") or "")
        value = field.get("value", field.get("field_value"))
        if not ink_id or value in (None, ""):
            continue

        mapping = cfieldmapping.objects.filter(inkadmin_cfield_id=ink_id).first()
        if mapping:
            print(f"inkadmin customefield with id{ink_id} mapped to cercus customefield with id{mapping.cercus_cfield_id} for name {mapping.field_name}")
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
        "additionalEmails": [e for e in additionalemails if e],
        "additionalPhones": [p for p in additionalphones if p],
        "address1": address,
        "customFields": pcfields,
        "tags": tags,
        "city": city,
        "state": state,
        "postalCode": postalCode,
        "country": country,
        "website": website,
        "dnd": dnd,
        "source": source,
        "companyName": companyName
    }
    if dndSettings:
        payload["dndSettings"] = dndSettings

    print(payload)
    print(f"Creating contact {contact_data.get('id')} firstname: {first_name} lastname: {last_name} email: {email} phone: {phone}\n")
    # return True
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        return data.get("contact", data)
    except RequestException as e:
        print(f"Error creating contact: {e}")
        print(r.text if 'r' in locals() else "No response object")
        return {}



# Consider removing the decorator or scoping atomic to just the DB write
# @transaction.atomic
def map_contacts():
    created = updated = 0

    inkadmin_contacts = (
        inkadmincontact.objects
        .filter(cercus_contacts__isnull=True)  
        .exclude(contact_id__in=[None, "","iZlZQjIGInd9Y2y9uuxz"])
        .distinct()
        .iterator()
    )

    for contact in inkadmin_contacts:
      
        details = get_contact(
            settings.INKA_LOCATION_ID,
            settings.INKA_GHL_ACCESS_TOKEN,
            contact.contact_id,
        )
        if not details:
            print(f"Contact {contact.contact_id} not found in InkAdmin; skipping")
            continue

        try:
            resp = create_contact(
                details,
                settings.CERCUS_LOCATION_ID,
                settings.CERCUS_GHL_ACCESS_TOKEN,
            )
        except Exception as e:
            print(f"Create failed for {contact.contact_id}: {e}")
            continue
        
        ccontact = resp
        new_id = str(ccontact.get("id") or "")
        phone = ccontact.get("phone",None)
        email = ccontact.get("email",None)
        if not new_id:
            print(f"Create failed for {contact.contact_id}; bad response: {resp}")
            continue


        with transaction.atomic():
            cc, was_created = cercuscontact.objects.update_or_create(
                contact_id=new_id,
                defaults={
                    "locationId": settings.CERCUS_LOCATION_ID,
                    "is_newly_created": True,
                    "inkadmin_contact": contact,
                    "phone": phone,
                    "email": email
                },
            )
        if was_created:
            created += 1
        else:
            updated += 1

    print(f"created={created}, total_processed={created+updated}")


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





# -------------------------------------------conversation part------------------------------------------------------


def add_conversations_to_db_inka(conversations_batch, location_id):

    from .models import conversation, inkadmincontact  

    to_create = []
    to_update = []

    # Dedup batch by conversation id
    seen = set()
    conv_ids = []
    contact_ids = set()

    for conv in conversations_batch:

        cid = str(conv.get("id") or "")

        if not cid or cid in seen:
            continue
        seen.add(cid)
        conv_ids.append(cid)
        if conv.get("contactId"):
            contact_ids.add(str(conv["contactId"]))

    # Prefetch existing convs and inkadmin contacts
    existing_convs = {
        obj.i_conversation_id: obj
        for obj in conversation.objects.filter(i_conversation_id__in=conv_ids)
    }

    ink_lookup = {
        ic.contact_id: ic
        for ic in inkadmincontact.objects.filter(
            locationId=location_id, contact_id__in=contact_ids
        )
    }

    for conv in conversations_batch:

        conv_id = str(conv.get("id") or "")

        if not conv_id or conv_id not in seen:
            continue  

        ink_contact = ink_lookup.get(str(conv.get("contactId") or ""))
        ink_contact_id = str(conv.get("contactId") or "")

        existing = existing_convs.get(conv_id)
        if not ink_contact:
            print(f"Couln't find contact conversation {conv_id} for contact_id {ink_contact_id}")
            
        if existing:
            if existing.i_contact.contact_id != ink_contact_id:
                existing.i_contact = ink_contact
                to_update.append(existing)
        else:
            to_create.append(
                conversation(
                    i_conversation_id=conv_id,
                    i_contact=ink_contact,
                    # c_* fields left None
                )
            )

    created = updated = 0
    with transaction.atomic():
        if to_create:
            conversation.objects.bulk_create(to_create, batch_size=500)
            created = len(to_create)
        if to_update:
            conversation.objects.bulk_update(to_update, ["i_contact"], batch_size=500)
            updated = len(to_update)

    return created, updated

def update_conversations_with_cercus(conversations_batch, location_id):
    from .models import conversation, cercuscontact  
    print("started")
    to_update = []

    # Step 1: Collect all convo + contact IDs
    seen, conv_ids, contact_ids = set(), [], set()
    for conv in conversations_batch:
        cid = str(conv.get("id") or "")
        if not cid or cid in seen:
            continue
        seen.add(cid)
        conv_ids.append(cid)
        if conv.get("contactId"):
            contact_ids.add(str(conv["contactId"]))

    # Step 2: Prefetch Cercus contacts with mapping
    cercus_contacts = {
        c.contact_id: c
        for c in cercuscontact.objects.select_related("inkadmin_contact").filter(
            contact_id__in=contact_ids
        )
    }

    # Step 3: Prefetch Inka-side conversations for mapped contacts
    inka_contact_ids = [c.inkadmin_contact.contact_id for c in cercus_contacts.values() if c.inkadmin_contact]
    inka_convs = conversation.objects.filter(i_contact__contact_id__in=inka_contact_ids)
    inka_convs_lookup = {conv.i_contact.contact_id: conv for conv in inka_convs}

    # Step 4: Process updates only
    for conv in conversations_batch:
        conv_id = str(conv.get("id") or "")
        if not conv_id or conv_id not in seen:
            continue  

        cercus_contact = cercus_contacts.get(str(conv.get("contactId") or ""))

        if not cercus_contact or not cercus_contact.inkadmin_contact:
            continue  

        existing = inka_convs_lookup.get(cercus_contact.inkadmin_contact.contact_id)

        if existing:
            existing.c_contact = cercus_contact
            existing.c_conversation_id = conv_id
            to_update.append(existing)

    updated = 0
    with transaction.atomic():
        if to_update:
            conversation.objects.bulk_update(to_update, ["c_contact", "c_conversation_id"], batch_size=500)
            updated = len(to_update)

    return updated



def fetch_inkadmin_conversations():
    location_id = settings.INKA_LOCATION_ID
    access_token = settings.INKA_GHL_ACCESS_TOKEN

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Version": settings.GHL_API_VERSION,
    }

    url = "https://services.leadconnectorhq.com/conversations/search"
    limit = 100

    total_fetched = 0
    total_created = 0
    total_updated = 0

    startAfterDate = None
    print("Fetching and processing conversations...")

    try:
        while True:
            params = {
                "locationId": location_id,
                "limit": limit,
                "sortBy":"last_message_date",
                "sort":"asc"
            }
            if startAfterDate:
                params["startAfterDate"] = startAfterDate

            resp = requests.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            batch = data.get("conversations", []) or []
            fetched = len(batch)
            total_fetched += fetched

            if not batch:
                break

            # Process this page
            try:
                batch_created, batch_updated = add_conversations_to_db_inka(batch, location_id)
                total_created += batch_created
                total_updated += batch_updated
            except Exception as e:
                print(f"Error processing batch: {e}")

            # Advance cursor using the largest dateAdded in the batch
            dates = [c.get("lastMessageDate") for c in batch if c.get("lastMessageDate") is not None]
            new_cursor = max(dates) if dates else None

            # Stop if fewer than a full page OR cursor didn't advance
            if fetched < limit or not new_cursor or new_cursor == startAfterDate:
                break
            startAfterDate = new_cursor

    except RequestException as e:
        print(f"Error fetching conversations: {e}")
        # Optional: inspect last response body if available
        try:
            print(resp.text)  # may not exist on first-iteration failures
        except Exception:
            pass

    summary = {
        "fetched": total_fetched,
        "created": total_created,
        "updated": total_updated,
        "processed": total_created + total_updated,
    }
    print(f"Done. {summary}")
    return summary

def fetch_cercus_conversations():
    location_id = settings.CERCUS_LOCATION_ID
    access_token = settings.CERCUS_GHL_ACCESS_TOKEN

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Version": settings.GHL_API_VERSION,
    }

    url = "https://services.leadconnectorhq.com/conversations/search"
    limit = 100

    total_fetched = 0
    total_created = 0
    total_updated = 0

    startAfterDate = None
    print("Fetching and processing conversations...")

    try:
        while True:
            params = {
                "locationId": location_id,
                "limit": limit,
                "sortBy":"last_message_date",
                "sort":"asc"
            }
            if startAfterDate:
                params["startAfterDate"] = startAfterDate

            resp = requests.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            batch = data.get("conversations", []) or []
            fetched = len(batch)
            total_fetched += fetched

            if not batch:
                break

            # Process this page
            try:
                batch_updated = update_conversations_with_cercus(batch, location_id)
                total_updated += batch_updated
            except Exception as e:
                print(f"Error processing batch: {e}")

            # Advance cursor using the largest dateAdded in the batch
            dates = [c.get("lastMessageDate") for c in batch if c.get("lastMessageDate") is not None]
            new_cursor = max(dates) if dates else None

            # Stop if fewer than a full page OR cursor didn't advance
            if fetched < limit or not new_cursor or new_cursor == startAfterDate:
                break
            startAfterDate = new_cursor

    except RequestException as e:
        print(f"Error fetching conversations: {e}")
        # Optional: inspect last response body if available
        try:
            print(resp.text)  # may not exist on first-iteration failures
        except Exception:
            pass

    summary = {
        "fetched": total_fetched,
        "updated": total_updated,
        "processed": total_created + total_updated,
    }
    print(f"Done. {summary}")
    return summary



def fetch_conversation_id(contact_id):
    location_id = settings.CERCUS_LOCATION_ID
    token = settings.CERCUS_GHL_ACCESS_TOKEN

    url = f"https://services.leadconnectorhq.com/conversations/search?locationId={location_id}&contactId={contact_id}"

    headers = {
        "Accept": "application/json",
        "Version": "2021-04-15",
        "Authorization": f"Bearer {token}"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        conversations = data.get("conversations", [])
        if not conversations:
            print("No conversations found.")
            return None

        # Return first conversation's id
        return conversations[0].get("id")

    except requests.RequestException as e:
        print(f"Error fetching conversation: {e}")
        return None


def create_conversation_for_contact(i_contact_id,i_conversation_id):

    # Step 1: Find the inkadmin contact
    i_contact = get_object_or_404(inkadmincontact, contact_id=i_contact_id)

    # Step 2: Find mapped cercus contact
    c_contact = getattr(i_contact, "cercus_contacts", None)
    
    if not c_contact:
        print(f"{i_contact_id} has no mapped cercus contact; cannot create conversation")
        return False,None



    url = "https://services.leadconnectorhq.com/conversations/"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {settings.CERCUS_GHL_ACCESS_TOKEN}",
        "Version": settings.GHL_API_VERSION
    }

    payload = {
        "locationId": settings.CERCUS_LOCATION_ID,  # from settings
        "contactId": c_contact.contact_id,
    }

    # Step 4: Make request
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        c_conversation = data.get("conversation", {})
        c_conversation_id = c_conversation.get("id")
        c_contact_id = c_conversation.get("contactId")

        if not c_conversation_id or not c_contact_id:
            print(f"Failed to create conversation for contact {i_contact_id}: {data}")
            return False,None
        
        conv=conversation.objects.filter(i_contact=i_contact, i_conversation_id=i_conversation_id).first()
        conv.c_contact=c_contact
        conv.c_conversation_id=c_conversation_id
        conv.save()
    
        
    except requests.RequestException as e:
        if response.status_code == 400:
            data= response.json()
            message = data.get("message","")

            if message.lower().strip() == "conversation already exists":
                print(f"Conversation already exists for contact {i_contact_id}; skipping creation.")
                c_conversation_id = fetch_conversation_id(c_contact.contact_id)

                if not c_conversation_id:
                    print(f"Failed to fetch existing conversation for contact {i_contact_id}")
                    return False,None
                
                print(f"Fetched existing conversation {c_conversation_id} for contact {c_contact.contact_id}")

                conv=conversation.objects.filter(i_contact=i_contact, i_conversation_id=i_conversation_id).first()
                conv.c_contact=c_contact
                conv.c_conversation_id=c_conversation_id
                conv.save()

                return True,c_conversation_id
            
        print(f"Error creating conversation for contact {i_contact_id}: {e}")
        print(response.text if 'response' in locals() else "No response object")
        return False,None

    return True,c_conversation_id


def _deep_get(d, path, default=None):
    cur = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


def build_create_message_payload(msg,conv_id):

    conversation_provider ="68a619d8a3f9a7912b382a9a"

    raw_type = msg.get("messageType") or ""

    TYPE_MAP = {
        "TYPE_SMS": "SMS",
        "TYPE_EMAIL": "Email",
        "TYPE_WHATSAPP": "WhatsApp",
        "TYPE_GMB": "GMB",
        "TYPE_INSTAGRAM": "IG",
        "TYPE_FACEBOOK": "FB",
        "TYPE_CALL": "Call",
        "TYPE_LIVE_CHAT": "Live_Chat"
    }


    msg_type = TYPE_MAP.get(raw_type, None)
    if msg_type is None:
        return False,{}

    msg.get()
    payload = {
        "type": msg_type,
        "attachments": msg.get("attachments") or None, 
        "message": msg.get("body") or None,
        "conversationId": conv_id, 
        "conversationProviderId": conversation_provider,
        "altId": msg.get("altId") or None,
        "direction": msg.get("direction") or None,     
        "date": msg.get("dateAdded") or None
    }

    
    meta = msg.get("meta", {})
    emails = meta.get("email", {})
    emailids= emails.messageIds if emails else []
    
    if msg_type == "EMAIL":
        payload.update({
            "html": msg.get("html") or None,
            "subject": msg.get("subject") or None,
            "emailFrom": msg.get("emailFrom") or None,
            "emailTo": msg.get("emailTo") or None,
            "emailCc": msg.get("emailCc") or None,
            "emailBcc": msg.get("emailBcc") or None,
            "emailMessageId": (_deep_get(msg, ["meta", "email", "email", "messageIds"], []) or [None])[0],
        })

    
    if msg_type == "CALL":
        # Different responses show call info either at meta.call or split as callDuration/callStatus
        call_meta = msg.get("meta", {}).get("call") or {}
        call_status = call_meta.get("status") or msg.get("meta", {}).get("callStatus")
        call_block = {
            # 'to' and 'from' are not present in GET samples; omit if unknown
            "status": call_status or None,
        }
        # Only attach "call" when at least something meaningful exists
        if any(v is not None for v in call_block.values()):
            payload["call"] = {k: v for k, v in call_block.items() if v is not None}

    # Remove keys with None or empty lists so we don't send irrelevant fields
    cleaned = {}
    for k, v in payload.items():
        if v is None:
            continue
        if isinstance(v, (list, dict)) and not v:
            continue
        cleaned[k] = v

    return cleaned




def get_email_data(email_msg_id):

    location_id = settings.INKA_LOCATION_ID
    access_token = settings.INKA_GHL_ACCESS_TOKEN


    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Version": settings.GHL_API_VERSION,
    }

    url = f"https://services.leadconnectorhq.com/conversations/messages/email/{email_msg_id}"

    try:
        response = requests.get(url,headers=headers)
        response.raise_for_status()
        data = response.json()
        return data.get("emailMessage", {})

    except requests.RequestException as e:
        print(f"Error fetching email data for message ID {email_msg_id}: {e}")
        return None


def get_call_recording_urls(messageId):

    location_id = settings.INKA_LOCATION_ID

    url = f"https://services.leadconnectorhq.com/conversations/messages/{messageId}/locations/{location_id}/recording"
    file_upload_url = "https://services.leadconnectorhq.com/medias/upload-file"

    payload = {}

    headers = {
    'Version': '2021-04-15',
    'Authorization': f'Bearer {settings.INKA_GHL_ACCESS_TOKEN}'
    }

    response = requests.get(url, headers=headers, data=payload)
    id_downloaded = False
    if response.status_code == 200:

        file_path = os.path.join(settings.MEDIA_ROOT, f"{messageId}.mp3")

        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        id_downloaded = True
        print(f"Success: call recording downloaded for messageId {messageId} {file_path}")
    else:
        print(f"Failed: call recording might not be available {response.status_code}")
        return False,None

    # upload the file to ghl media and get it's url 
    if id_downloaded:

        payload = {

        }

        headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {settings.CERCUS_GHL_ACCESS_TOKEN}',
        'Version': settings.GHL_API_VERSION
         }

        
        with open(file_path, "rb") as f:
                files = {
                    "file": (f"{messageId}.mp3", f, "audio/mpeg")  
                }
                name = f"{messageId}.mp3"
                response = requests.post(file_upload_url, headers=headers, files=files,data={"name":name})
                data = response.json()
                print(response.text)
                file_url = data.get("url", None)
                print(data)

                if response.status_code in [200,201] and file_url:
                    if file_url:
                        return True,file_url
                    
                else:
                    print(f"Failed to upload file to GHL media {response.status_code}")
                    raise Exception("File upload failed")

def add_outbound_call(payload):
    
   

    url = "https://services.leadconnectorhq.com/conversations/messages/outbound"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {settings.CERCUS_GHL_ACCESS_TOKEN}",
        "Version": settings.GHL_API_VERSION
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        response_data = response.json()
        return True, response_data
    except requests.exceptions.RequestException as e:
        print(response.text if 'response' in locals() else "No response object")
        raise Exception(f"Error while sending outbound call: {e}")




def create_message(msg, conv, imsg_obj):

    header ={
        "Content-Type": "application/json",
        "Accept" : "application/json",
        "Version": settings.GHL_API_VERSION,
        "Authorization" : f"Bearer {settings.CERCUS_GHL_ACCESS_TOKEN}"
    }
    

    TYPE_MAP = {
        "TYPE_SMS": "SMS",
        "TYPE_EMAIL": "Email",
        "TYPE_WHATSAPP": "WhatsApp",
        "TYPE_GMB": "GMB",
        "TYPE_INSTAGRAM": "IG",
        "TYPE_FACEBOOK": "FB",
        "TYPE_CALL": "Call",
        "TYPE_LIVE_CHAT": "Live_Chat"
    }

    url = "https://services.leadconnectorhq.com/conversations/messages/inbound"


    msg_type = TYPE_MAP.get(msg.get("messageType"), None)

    if msg_type == "Email":
    
        payload ={}
        payload["type"]="Email"
        payload["conversationId"]=conv.c_conversation_id
        payload["conversationProviderId"]="68a63434417a73ba21439f4a"

        email_msg_ids = msg.get("meta", {}).get("email", {}).get("messageIds", [])

        
        prev = None
        for emsgid in email_msg_ids:
            
            is_reply=False

            # getting the email details
            email_msg_data = get_email_data(emsgid)

            payload["html"]=email_msg_data.get("body", "")
            payload["subject"]=email_msg_data.get("subject", "")
            payload["emailFrom"]=email_msg_data.get("from", "")

            tomail = email_msg_data.get("to", [])

            if isinstance(tomail, list):
                to = tomail.pop(0)
            else:
                to = tomail

            payload["emailTo"]=to
            payload["emailCc"]=email_msg_data.get("cc", [])
            payload["emailBcc"]=email_msg_data.get("bcc", [])
            payload["direction"] =email_msg_data.get("direction")
            payload["altId"] = email_msg_data.get("altId", "")
            payload["date"] = email_msg_data.get("dateAdded", None)

            attachments =email_msg_data.get("attachments", [])
            if attachments:
                payload["attachments"] =attachments

            replyToMessageId = email_msg_data.get("replyToMessageId", None)

            # if  it has replytomessageid finding currespoding emailmsgidfrom cercus
            if replyToMessageId:
                c_reaplyToMessageId = c_messages.objects.filter(i_email_msg_id=replyToMessageId).first()
                payload["emailMessageId"] = c_reaplyToMessageId.c_email_msg_id if c_reaplyToMessageId else None
                is_reply = True

            else:
                if prev:
                    payload["emailMessageId"] = prev
                    is_reply = True
            try:
                print(f"Creating message  email for inkaemail id {imsg_obj.i_message_id} with payload: {payload}")
                response = requests.post(url, headers=header, json=payload)
                response.raise_for_status()
                data = response.json()
                c_message_id= data.get("messageId", None)
                c_email_msg_id = data.get("emailMessageId", None)
                i_email_msg_id = emsgid

                cmsgobj = c_messages.objects.create(
                    c_message_id=c_message_id,
                    conversation=conv,
                    msg_type=msg_type,
                    is_reply=is_reply,
                    c_email_msg_id=c_email_msg_id,
                    i_email_msg_id=i_email_msg_id,
                    i_message=imsg_obj
                )

                prev = c_email_msg_id

                print(f"Email message created in cercus with email msg id {c_email_msg_id} under message id {c_message_id}, created from inkadmin message with email msg id {i_email_msg_id}")
            except requests.RequestException as e:
                print(response.text if 'response' in locals() else "Error creating message for email!")
                print(f"Error creating message for email ID {emsgid}: {e}")


    elif msg_type == "SMS":
        payload={}
        i_message_id = imsg_obj.i_message_id
        payload["type"]="SMS"
        payload["conversationId"]=conv.c_conversation_id
        payload["conversationProviderId"]="68a619d8a3f9a7912b382a9a"
        payload["message"] = msg.get("body", "")
        payload["direction"] = msg.get("direction")
        payload["date"] = msg.get("dateAdded", None)

        attachments = msg.get("attachments", [])
        if attachments:
            payload["attachments"] = attachments

        altid = msg.get("altId", "")
        if altid:
            payload["altId"] = altid

        try:
                print(f"Creating message for SMS ID {i_message_id} with payload: {payload}")
                response = requests.post(url, headers=header, json=payload)
                response.raise_for_status()
                data = response.json()
                c_message_id = data.get("messageId", None)

                smsobj = c_messages.objects.create(
                    c_message_id=c_message_id,
                    conversation=conv,
                    msg_type=msg_type,
                    i_message=imsg_obj
                )

                print(f"SMS message created in cercus with message id {c_message_id} created from inkadmin message with id {smsobj.i_message_id}")
        except requests.RequestException as e:
            print(response.text if 'response' in locals() else "Error creating message for SMS!")
            print(f"Error creating message for SMS: {e}")


    if msg_type == "Call":

        payload ={}
        call ={}

        direction = msg.get("direction", None)

        payload["type"]="Call"
        payload["conversationId"]=conv.c_conversation_id
        payload["conversationProviderId"]="68a6f7bf0a839cd9d8aa89f6"
        payload["date"] = msg.get("dateAdded", None)

        altid = msg.get("altId", None)
        if altid:
            payload["altId"] = altid

        status = msg.get("meta",{}).get("call",{}).get("status", None)
        if status == 'ringing':
            call["status"] = "pending"
        else:
            call["status"] = status 

        payload["direction"] = direction

        try:
            is_success,call_recording_url=get_call_recording_urls(msg.get("id"))
        except Exception as e:
            print(f"Error fetching call recording URL: {e}")
            raise e
        
        account_phone ="+12163251865"
        contactid= msg.get("contactId", None)
        contactobj = inkadmincontact.objects.filter(contact_id=contactid).first()
        contact_phone = contactobj.phone if contactobj else None
        
        if direction == "inbound":
            call["to"] = account_phone
            call["from"] = contact_phone

        if direction == "outbound":
            call["to"] = contact_phone
            call["from"] = account_phone

        payload["call"] = call

        if is_success and call_recording_url:
            payload["attachments"] = [call_recording_url]
        
        try:
            print(f"Creating message for Call ID {imsg_obj.i_message_id} with payload: {payload}")
            data=None
            if direction == 'inbound':
                response = requests.post(url, headers=header, json=payload)
                response.raise_for_status()
                data = response.json()

            elif direction == 'outbound':
                try:
                    is_success,data = add_outbound_call(payload)
                except Exception as e:
                   print(f"Error adding outbound call: {e}")
                   raise e
            if data is None:
                print(f"Failed to create outbound call data {data}")
                raise

            message_id = data.get("messageId", None)
            call_recording_url = call_recording_url

            callmsgobj = c_messages.objects.create(
                c_message_id=message_id,
                conversation=conv,
                msg_type="Call",
                call_recording_url=call_recording_url,
                i_message=imsg_obj
            )

            print(f"Call message created in cercus with message id {message_id} and recording url {call_recording_url} created from inkadmin message with id {imsg_obj.i_message_id}")
        
        except requests.RequestException as e:
            print(response.text if 'response' in locals() else "Error creating message for Call!")
            print(f"Error creating message for Call: {e}")


    else:
        print(f"Message Type Out of scope for creation : {msg_type}")




def fetch_messages_for_conversation(conversation_id,):

    location_id = settings.INKA_LOCATION_ID
    access_token = settings.INKA_GHL_ACCESS_TOKEN

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Version": settings.GHL_API_VERSION,
    }

    url = f"https://services.leadconnectorhq.com/conversations/{conversation_id}/messages"

    all_messages = []
    last_message_id = None

    try:
        while True:
            params = {"locationId": location_id, "limit": 100}
            if last_message_id:
                params["lastMessageId"] = last_message_id

            resp = requests.get(url, headers=headers, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            messages_data = data.get("messages", {})
            batch = messages_data.get("messages", []) or []
            all_messages.extend(batch)

            # pagination handling
            next_page = messages_data.get("nextPage", False)
            last_message_id = messages_data.get("lastMessageId")

            if not next_page or not last_message_id:
                break  # no more pages

    except RequestException as e:
        print(f"Error fetching messages for conversation {conversation_id}: {e}")
        try:
            print(resp.text)
        except Exception:
            pass

    print(f"Fetched {len(all_messages)} messages for conversation {conversation_id}")
    return all_messages


def save_inka_messages(i_conversation_id, inkmessages):
    try:
        conv = conversation.objects.get(i_conversation_id=i_conversation_id)
    except conversation.DoesNotExist:
        print(f"No conversation found for ID {i_conversation_id}")
        return 0

    to_create = []

    for msg in inkmessages:
        msg_id = msg.get("id")
        msg_type = msg.get("messageType")

        # Extract email message IDs safely (list)
        email_msg_ids = (
            msg.get("meta", {})
               .get("email", {})
               .get("messageIds", [])
        )

        # Skip if already exists
        if i_messages.objects.filter(i_message_id=msg_id).exists():
            continue

        # Wrap list in dict for JSONField if available
        email_msg_ids_object = {"messageIds": email_msg_ids} if email_msg_ids else None

        to_create.append(
            i_messages(
                i_message_id=msg_id,
                conversation=conv,
                msg_type=msg_type,
                emil_msg_ids=email_msg_ids_object
            )
        )

    # Bulk insert
    if to_create:
        i_messages.objects.bulk_create(to_create, batch_size=500)

    return len(to_create)



def map_conversations():
    from .models import conversation

    inka_conv_ids = []

    conversations = conversation.objects.filter(c_messages__isnull=True,i_conversation_id__in=inka_conv_ids).iterator()
    
    created = 0
    for conv in conversations:
        print(f"processing conversation {conv.i_conversation_id}")
        i_contact_id = conv.i_contact.contact_id
        i_conversation_id = conv.i_conversation_id
        
        try:
            
            result,conv_id = create_conversation_for_contact(i_contact_id, i_conversation_id)
            conv.refresh_from_db()
            created += 1

            if not result:
                continue
            
        except Exception as e:
            print(f"Error mapping conversation for contact {i_contact_id}: {e}")
            

        inkmessages = fetch_messages_for_conversation(i_conversation_id)
        
        for msg in inkmessages:

            email_msg_ids= msg.get("meta", {}).get("email", {}).get("messageIds", [])
            email_msg_ids_obj = {"messageIds":email_msg_ids} if email_msg_ids else None


            imsg_obj, created = i_messages.objects.update_or_create(
                                    i_message_id=str(msg.get("id")), 
                                    defaults={
                                        "conversation": conv,
                                        "msg_type": msg.get("messageType"),
                                        "emil_msg_ids": email_msg_ids_obj,
                                    }
                                )
            
            try:
                create_message(
                    msg,
                    conv,
                    imsg_obj
                )
            except Exception as e:
                print(f"Error creating message for conversation {conv.i_conversation_id}: {e}")


        print(f"Migration completed for conversation {conv}")
        conv.is_migrated = True
        conv.save()

        

def clean_contacts():
    conv = conversation.objects.all().delete()
    print(conv)
    c = cercuscontact.objects.all().delete()
    print(c)
    i = inkadmincontact.objects.all().delete()
    print(i)




def get_message(message_id: str):

    url = f"https://services.leadconnectorhq.com/conversations/messages/{message_id}"
    print(f"Fetching message for message id {message_id}")
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {settings.INKA_GHL_ACCESS_TOKEN}",
        "Version":settings.GHL_API_VERSION
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  
        print(response.json())
        message = response.json().get("message", {})
        return message
    except requests.exceptions.RequestException as e:
        print(f"Error fetching message {message_id}: {e}")
        return None



def map_remaining():
    msg_types = ["TYPE_CALL","TYPE_EMAIL","TYPE_SMS"]

    allmessages = i_messages.objects.filter(msg_type__in=msg_types,c_messages__isnull=True)

    count = 0
    processed = 0
    for i_msg in allmessages:
        conv = i_msg.conversation
        i_contact_id = conv.i_contact.contact_id
        i_conversation_id = conv.i_conversation_id

        print(f"processing message {i_msg.i_message_id} for conversation {conv.i_conversation_id}")

        if conv.c_conversation_id is None or conv.c_contact is None:
            try:
                result,conv_id = create_conversation_for_contact(i_contact_id, i_conversation_id)
                conv.refresh_from_db()
                created += 1

                if not result:
                    continue
            except Exception as e:
                print(f"Error mapping conversation for contact {i_contact_id}: {e}")
        
        msg_data = get_message(i_msg.i_message_id)

        if not msg_data:
            print(f"Could not retrieve data for message {i_msg.i_message_id}")
            continue

        email_msg_ids= msg_data.get("meta", {}).get("email", {}).get("messageIds", [])
        email_msg_ids_obj = {"messageIds":email_msg_ids} if email_msg_ids else None

        msgtype = msg_data.get("messageType", None)

        
        imsg_obj, created = i_messages.objects.update_or_create(
                                    i_message_id=str(msg_data.get("id")), 
                                    defaults={
                                        "conversation": conv,
                                        "msg_type": msg_data.get("messageType"),
                                        "emil_msg_ids": email_msg_ids_obj,
                                    }
                                )

            
        try:
            create_message(
                msg_data,
                conv,
                imsg_obj
            )
            count += 1
        except Exception as e:
            print(f"Error creating message for conversation {conv.i_conversation_id}: {e}")
            processed +=1


        print(f"message {i_msg.i_message_id} migrated succesfully")
        conv.is_migrated = True
        conv.save()
        print(f"processed {count}")

    print(f"Total messages migrated: {count}")
    print(f"Total messages processed: {processed}")





from django.db.models import Count

def deduplicate_i_messages():
    msg_types = ["TYPE_CALL","TYPE_EMAIL","TYPE_SMS"]

    # Find i_message_id groups with duplicates
    duplicate_groups = (
        i_messages.objects.filter(msg_type__in=msg_types)
        .values("i_message_id")
        .annotate(count=Count("id"))
        .filter(count__gt=1)
    )

    for group in duplicate_groups:
        i_msg_id = group["i_message_id"]
        duplicates = list(i_messages.objects.filter(i_message_id=i_msg_id))

        # Separate connected and unconnected
        connected = [msg for msg in duplicates if msg.c_messages.exists()]
        unconnected = [msg for msg in duplicates if not msg.c_messages.exists()]

        if connected:
            # Keep the first connected, delete all others
            to_keep = connected[0]
            to_delete = [msg for msg in duplicates if msg != to_keep]
        else:
            # No connected → keep one unconnected, delete rest
            to_keep = unconnected[0]
            to_delete = [msg for msg in duplicates if msg != to_keep]

        # Delete unwanted ones
        for msg in to_delete:
            print(f"Deleting duplicate i_message {msg.id} (i_message_id={i_msg_id})")
            msg.delete()

    print("✅ Deduplication completed")







def clear_conversations():
    cids=['ovI54K30dENxubwiLRm0', '3KejntT3eOwe2YD69vxn', 'cJ8DLyfpu5VF1hTkvHmB', 'EXcyOCWh4Ny4MPY4c3sA', 'k3hYcC8reXbkNsCfFtCm', 'olFDmDGFGSRH3Oe4KVt0', 'CE8xBkehubgiocHKB6aq', 'ZVSpY7xY7ZO7dgfPEDM3', 'YfiDrt96QwqzRbPlxfvf', 'rD0YNJttKJohXT96P6I9', '3KOtsYvv6QjV8alLSfri', '2RyK3d7sCyHuS86q5eqI', 'GylmpU1jRcAckcW9Mhzw', 's6Xa3Jv6KITqUuDNch8W', 'zRvojz2GJFnqtCXw18p9', 'vjiwtVMkmtie1QJrOSQx', '7hZYn15VYzAWNqrQfEA7', 'hkeLNaXn0NNvPb1K24s0', 'Jp8H3lKQ7sllQ9oEyQV2', 'GMYQmHKCO9FMGbapexr9', 'nctZixi6qKQo9pV47Bdm', 'rWKBkXDJdrfOWlYJaME2', 'nWQyuwCQ4afRVBQG8pMp', 'xRDvu3AMS98Owj4pQ0sF', 'be1f6AZfZrJLyMEzI0cd', 'H2p9e8j162blO6c7Z4CX', 'uKo9X0jYWzwVUBtIaqLH', 'QIdtmLZD0e39yVcI5rjx', 'djKr2ICwjjt1FDDqcSzn', 'YZgVL6qqIrsczp7Chztv', 'pUnbjJwOCAw3e6koWYQF', 'jPUoew9KSNEo9nWy43Mq', 'hrDfX7FQA2GjiALQTiXC', 'vrTilUGRjXipm4XWds9t', '4fK7G4VGVMrAm8ulGwMx', 'bZvuGbfIte9vi7yYMpsr', 'yxiNEYKqm4mvp9migA2Y', 'yDbGJ8uZ6rYDCE6y2puI', 'PA2tBQbBcdtwuhwreBqg', 'I0UqIoHWpSHqD2sZtqHK', 'ADkhZ2ipCeigggdLQBrB', 'CrM6iXMb9tddqx57RC4G', 'EvSvboQqtcN84Xcrduf6', 'nRUwHWfPmtyyatquw97U', 'jjvkQSVbd5VM2rfWvV5D', 'VYlGWObidvph1ZDEEbg6', 'dO21oquprp4OdvCW9DKw', 'WZkjDGGkcRKEPPirUbET', 'JzFn4Yvc2XvnkX8VkMhH', '6hWeqNLaxMdI2JqzVUCI', 'vs6yy2DpxOc8S2TewRfR', 'rWEtFRazOD0wAirNMaOt', 'qG7gnxWYKC7r7TSRjQra', 'nDKXAKJ3sRYWHxGiSE8k', 'ESd8cZ9q2Ej6G8S9tR72', 'nIBnoT4HzUdGh6ycFd3w', 'OP2BjZBOpdJfCILIwcHN', '5pPhe2dOUyRnuKgQmMB8', 'p7BpdqOgQjaikjUhAhhQ', 'd5PeZpbciT9pcDvHVOVj', '125W0pIU7sFkYLAHnFjp']
    
    conversations = conversation.objects.filter(c_messages__c_message_id__in=cids).distinct()

    header ={
        "Accept" : "application/json",
        "Version": settings.GHL_API_VERSION,
        "Authorization" : f"Bearer {settings.CERCUS_GHL_ACCESS_TOKEN}"
    }

    for conv in conversations:
        print(f"Cleaning conversation {conv}")

        url = f"https://services.leadconnectorhq.com/conversations/{conv.c_conversation_id}"

        try:
            response = requests.delete(url, headers=header)
            response.raise_for_status()
            conv.c_messages.all().delete()
            conv.c_conversation_id = None
            conv.c_contact = None
            conv.save()
            print(f"Cleaned conversation {conv}")

        except requests.RequestException as e:
            print(f"Error cleaning conversation {conv}:{e} \n {response.text if 'response' in locals() else e}")




def parse_date(date_str: str):
    if date_str.endswith("Z"):
        date_str = date_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(date_str)
    return dt.date().strftime("%d-%m-%Y")


def create_note_from_body(body, msgtype, i_contact_id, messages):
    """Helper to send one note for multiple messages of same conversation/type"""
    i_contact_obj = inkadmincontact.objects.filter(contact_id=i_contact_id).first()
    c_contact = getattr(i_contact_obj, "cercus_contacts", None)
    c_contact_id = c_contact.contact_id if c_contact else None

    if not c_contact_id:
        raise Exception(f"{i_contact_id} has no mapped cercus contact")

    payload = {"body": body}
    url = f"https://services.leadconnectorhq.com/contacts/{c_contact_id}/notes"

    header = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Version": settings.GHL_API_VERSION,
        "Authorization": f"Bearer {settings.CERCUS_GHL_ACCESS_TOKEN}"
    }

    response = requests.post(url, json=payload, headers=header)
    response.raise_for_status()
    data = response.json().get("note", {})
    nid = str(data.get("id"))

    # Link this note to all included messages
    for i_msg in messages:
        Notes.objects.create(
            note_id=nid,
            i_message=i_msg,
            contact=c_contact,
            note_type=msgtype
        )



def create_notes_for_messages():
    msg_types = ["TYPE_INSTAGRAM", "TYPE_FACEBOOK"]

    # Step 1: Get distinct conversations that have pending messages of those types
    conversations = (
        conversation.objects.filter(i_messages__msg_type__in=msg_types, i_messages__notes__isnull=True)
        .distinct()[:1]
    )

    notes_created = 0
    processed = 0

    for conv in conversations:

        i_contact_id = conv.i_contact.contact_id if conv.i_contact else None

        if not i_contact_id:
            print(f"Conversation {conv.id} has no i_contact mapped")
            continue

        for msg_type in msg_types:  # process Instagram first, then Facebook
            messages = conv.i_messages.filter(msg_type=msg_type, notes__isnull=True)

            if not messages.exists():
                print(f"not messsages for {conv.i_conversation_id} with type {msg_type}")
                continue

            note_parts = []

            if msg_type == "TYPE_INSTAGRAM":
                note_parts.append(f"Instagram\n")
            if msg_type == "TYPE_FACEBOOK":
                note_parts.append(f"Facebook\n")
            
            for i_msg in messages:
                print(f"Processing message {i_msg.i_message_id} of type {msg_type}")

                msg_data = get_message(i_msg.i_message_id)

                if not msg_data:
                    print(f"Could not retrieve data for message {i_msg.i_message_id}")
                    continue

                # Build message string (reuse same logic as create_note, but not POST yet)
                mtype = msg_data.get("messageType", None)
                direction = msg_data.get("direction")
                attachments = msg_data.get("attachments", [])
                status = msg_data.get("status")
                message = msg_data.get("body")
                date_str = msg_data.get("dateAdded")
                date = parse_date(date_str) if date_str else ""

                pageId, pageName = None, None
                if mtype == "TYPE_INSTAGRAM":
                    pageId = msg_data.get("meta", {}).get("ig", {}).get("pageId")
                    pageName = msg_data.get("meta", {}).get("ig", {}).get("pageName")
                elif mtype == "TYPE_FACEBOOK":
                    pageId = msg_data.get("meta", {}).get("fb", {}).get("pageId")
                    pageName = msg_data.get("meta", {}).get("fb", {}).get("pageName")

                note_parts.append(
                    f"Date: {date}\n"
                    f"Flow: {direction}\n"
                    f"Message: {message or '[No Message]'}\n"
                    f"Status: {status}\n"
                    f"Page ID: {pageId}\n"
                    f"Page Name: {pageName}\n"
                    f"Attachments: {', '.join(attachments) if attachments else 'None'}"
                )

            # Step 2: If we gathered any notes, join and create one single note
            if note_parts:
                combined_body = "\n\n---\n\n".join(note_parts)
                try:
                    create_note_from_body(combined_body, msg_type, i_contact_id, messages)
                    notes_created += 1
                except Exception as e:
                    print(f"Error creating combined note for conv {conv.id}, type {msg_type}: {e}")
                    processed += 1
                    continue

    print(f"Finished. Notes created: {notes_created}, Errors: {processed}")