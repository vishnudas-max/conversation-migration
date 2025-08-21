import requests
from django.conf import settings
from requests.exceptions import RequestException
from .models import cercuscontact, conversation,inkadmincontact,cfieldmapping,i_messages,c_messages
import phonenumbers
import pycountry
from django.db import transaction
from django.db.models import Q
from django.shortcuts import get_object_or_404
import os



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
            if contact_id == "MWfXItjaHeWswv1mlRT4":
                print(link_val)
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



# Consider removing the decorator or scoping atomic to just the DB write
# @transaction.atomic
def map_contacts():
    created = updated = 0

    inkadmin_contacts = (
        inkadmincontact.objects
        .filter(cercus_contacts__isnull=True)   # not linked to any Cercus contact yet
        .exclude(contact_id__in=[None, ""])
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
        if resp:
            created +=1
        # contact = resp
        # new_id = str(contact.get("id") or "")
        # if not new_id:
        #     print(f"Create failed for {contact.contact_id}; bad response: {resp}")
        #     continue


        # with transaction.atomic():
        #     cc, was_created = cercuscontact.objects.update_or_create(
        #         contact_id=new_id,
        #         defaults={
        #             "locationId": settings.CERCUS_LOCATION_ID,
        #             "is_newly_created": True,
        #             "inkadmin_contact": contact,
        #         },
        #     )
        # if was_created:
        #     created += 1
        # else:
        #     updated += 1

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

        file_path = os.path.join(settings.MEDIA_ROOT, f"testing.mp3")

        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        id_downloaded = True
    else:
        print(f"Failed: {response.status_code}")
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

        try:
            with open(file_path, "rb") as f:
                files = {
                    "file": (f"testing.mp3", f, "audio/mpeg")  
                }
                response = requests.post(file_upload_url, headers=headers, files=files,data={"name":"testing.mp3"})
                data = response.json()
                url = data.get("url", None)
                print(data)
                if url:
                    return True,url
                else:
                    return False,None
        except requests.RequestException as e:
            print(response.text if response in locals() else "Error uploading file!")
            return False,None



def create_message(msg, conv, imsg_obj):

    header ={
        "Content-Type": "application/json",
        "Accept" : "application/json",
        "Version": settings.GHL_API_VERSION,
        "Authorization" : f"Bearer {settings.CERCUS_GHL_ACCESS_TOKEN}"
    }
    payload = {}

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

        payload["type"]="Email"
        payload["conversationId"]=conv.c_conversation_id
        payload["conversationProviderId"]="68a63434417a73ba21439f4a"

        email_msg_ids = msg.get("meta", {}).get("email", {}).get("messageIds", [])
        
        for emsgid in email_msg_ids:

            is_reply=False

            # getting the email details
            email_msg_data = get_email_data(emsgid)

            payload["html"]=email_msg_data.get("body", "")
            payload["subject"]=email_msg_data.get("subject", "")
            payload["emailFrom"]=email_msg_data.get("from", "")
            payload["emailTo"]=email_msg_data.get("to", [])
            payload["emailCc"]=email_msg_data.get("cc", [])
            payload["emailBcc"]=email_msg_data.get("bcc", [])
            payload["direction"] =email_msg_data.get("direction")
            payload["altId"] = email_msg_data.get("altId", "")
            payload["date"] = email_msg_data.get("dateAdded", None)

            replyToMessageId = email_msg_data.get("replyToMessageId", None)

            # if  it has replytomessageid finding currespoding emailmsgidfrom cercus
            if replyToMessageId:
                c_reaplyToMessageId = c_messages.objects.filter(i_email_msg_id=replyToMessageId).first()
                payload["emailMessageId"] = c_reaplyToMessageId.c_email_msg_id if c_reaplyToMessageId else None
                is_reply = True

            try:
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

                print(f"Email message created in cercus with email msg id {c_email_msg_id} under message id {c_message_id}, created from inkadmin message with email msg id {i_email_msg_id}")
            except requests.RequestException as e:
                print(f"Error creating message for email ID {emsgid}: {e}")


    elif msg_type == "SMS":
        i_message_id = imsg_obj.i_message_id
        payload["type"]="SMS"
        payload["conversationId"]=conv.c_conversation_id
        payload["conversationProviderId"]="68a619d8a3f9a7912b382a9a"
        payload["message"] = msg.get("body", "")
        payload["direction"] = msg.get("direction")
        payload["date"] = msg.get("dateAdded", None)
        try:
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

                print(f"SMS message created in cercus with message id {c_message_id} created from inkadmin message with id {smsobj.i_sms_msg_id}")
        except requests.RequestException as e:
            print(f"Error creating message for SMS: {e}")

    elif msg_type == "Call":

        payload ={}
        call ={}

        direction = msg.get("direction", None)
        payload["type"]="Call"
        payload["conversationId"]=conv.c_conversation_id
        payload["conversationProviderId"]="68a6f7bf0a839cd9d8aa89f6"

        status = msg.get("meta",{}).get("call",{}).get("status", None)

        call["status"] = status 

        payload["direction"] = direction

        is_success,call_recording_url=get_call_recording_urls(msg.get("id"))

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

        if is_success:
            payload["attachments"] = [call_recording_url]
        
        try:
            response = requests.post(url, headers=header, json=payload)
            response.raise_for_status()
            data = response.json()

            message_id = data.get("messageId", None)
            call_recording_url = call_recording_url

            callmsgobj = c_messages.objects.create(
                c_message_id=message_id,
                conversation=conv,
                msg_type="Call",
                call_recording_url=call_recording_url
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
    conversations = conversation.objects.filter().iterator()
    created = 0
    for conv in conversations:
        i_contact_id = conv.i_contact.contact_id
        i_conversation_id = conv.i_conversation_id
        

        # try:
        #     result,conv_id = create_conversation_for_contact(i_contact_id, i_conversation_id)
        #     conv.refresh_from_db()
        #     created += 1
        # except Exception as e:
        #     print(f"Error mapping conversation for contact {i_contact_id}: {e}")

        inkmessages = fetch_messages_for_conversation(i_conversation_id)
        conv_id=90
        for msg in inkmessages:

            email_msg_ids= msg.get("meta", {}).get("email", {}).get("messageIds", [])
            email_msg_ids_obj = {"messageIds":email_msg_ids} if email_msg_ids else None

            imsg_obj=i_messages.objects.create(
                i_message_id=str(msg.get("id")),
                conversation=conv,
                msg_type=msg.get("messageType"),
                email_msg_ids=email_msg_ids_obj,
            )

            create_message(
                msg,
                conv,
                imsg_obj
            )

            print(f"Migration completed for conversation {conv}")
        
