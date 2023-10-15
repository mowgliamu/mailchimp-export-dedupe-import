"""
Mailchimp API Interface and functions for PartsAvatar

mailchimp3: https://github.com/charlesthk/python-mailchimp
MailChimp API reference: https://developer.mailchimp.com/documentation/mailchimp/reference/overview/

Author: Prateek Goel
E-mail: prateek.goel@partsavatar.ca

"""

import os, sys, time
from tqdm import tqdm
import csv, ujson, hashlib
import requests, io
import zipfile, tarfile
import socket
import pandas as pd
from pathlib import Path
from mailchimp3 import MailChimp
from pprint import pprint

PWD = os.getcwd()
PATH_AUDIENCE = './All_segments_deduplicated'
CREATE_NEW_AUDIENCE = False
WRITE_AUDIENCE_MEMBERS = True

# API key
mc_api = " "

# MailChimp client
headers = requests.utils.default_headers()
client = MailChimp(mc_api=mc_api, timeout=30.0, request_headers=headers)

# List id
TEMPLATE_LIST_ID = '776708c17f'


def get_headers(segment):
    """ Helper function to extract the headers list
    """

    return list(segment.columns.values)


def get_all_lists(f = "lists.name,lists.id"):
    """ Returns all the lists
    """

    lists = client.lists.all(get_all=True, fields=f)
    lists_output = ujson.dumps(lists, indent=4)

    return lists, lists_output


def get_list_by_id(listid):
    """ Returns the list matching provided id
    """

    list_matching = client.lists.get(listid)
    list_matching_output = ujson.dumps(list_matching, indent=4)

    return list_matching, list_matching_output


def delete_list_by_id(listid):
    """ Delete an entire audience matching provided id."""

    client.lists.delete(listid)

    return


def get_merge_fields_list(listid):
    """ Given a list id, get all the merge fields"""

    response = client.lists.merge_fields.all(listid, get_all=True)
    all_merge_fields = response['merge_fields']
    n_merge_fields = len(all_merge_fields)
    merge_fields_info = []
    for i in range(n_merge_fields):
        current_field = all_merge_fields[i]
        if current_field['tag'] == 'FNAME' or current_field['tag'] == 'LNAME':
            pass
        else:
            merge_fields_info.append({'name': current_field['name'], 'type': current_field['type'], 'tag': current_field['tag'], 'public': True})

    #pprint(merge_fields_info)

    return merge_fields_info



def add_merge_fields_to_list(listid, merge_fields_to_create):
    """ Add additional merge fields to an existing list
    """

    n_fields = len(merge_fields_to_create)
    for i in range(n_fields):
        client.lists.merge_fields.create(listid, merge_fields_to_create[i])
        #client.lists.merge_fields.create(listid, {"name": merge_fields_to_create, "type": "text", "tag": merge_fields_to_create[i]})

    return


def get_all_members_subscribed_list(listid):
    """ Returns all members inside list."""

    members = client.lists.members.all(listid, status="subscribed", get_all=True)
    members_output = ujson.dumps(members, indent=4)

    return members, members_output



def get_all_members_list_batch(listid):
    """ Retrieve all members of a segment, identified by id"""

    operations = [{
        'method': 'GET',
        'path': '/lists/' + listid + '/members',
    }]

    response = client.batch_operations.create(data={"operations": operations})
    BATCH_ID = response["id"]

    return BATCH_ID


def create_new_list(list_name, company_contact, company_campaign_defaults, permission_reminder, email_type_option):
    """ Create a blank new audience, with basic settings given

    Required Arguments: name, contact, permission_reminder, campaign_defaults, email_type_option
    Optional Arguments: use_archive_bar, notify_on_subscribe, notify_on_unsubscribe, visibility, double_optin, marketing_permissions
    """

    data = {"name": list_name,
            "contact": company_contact,
            "campaign_defaults": company_campaign_defaults,
            "permission_reminder": permission_reminder,
            "email_type_option": email_type_option
    }

    response = client.lists.create(data)

    return response


def create_members_list_batch(listid, csvfile):
    """ Batch operation for creating multiple members at once from CSV file
    """

    response = get_list_by_id(listid)
    print('Will write new members to list', response[0]['name'])
    text = input('Do you want to continue?: Type Yes\n')

    if text == 'Yes' or text == 'Y' or text == 'y':
        primary_fields = ['email_address','status']
        result = []
        jsonfile = csvfile.split('.')[0]+'.json'

        with open(csvfile, encoding='utf-8-sig') as csv_file:
            reader = csv.DictReader(csv_file, skipinitialspace=True)
            for row in reader:
                d = {k: v for k, v in row.items() if k in primary_fields}

                d['merge_fields'] = {k: v for k, v in row.items() if k not in primary_fields}
                result.append(d)  
            with open(jsonfile, 'w')as fp:
                ujson.dump(result, fp, indent=4)


        with open(jsonfile, "r") as read_file:

            data = ujson.load(read_file)

            operations = [{
                'method': 'POST',
                'path': '/lists/' + listid + '/members',
                'body': ujson.dumps(member)
            } for member in data]

        response = client.batch_operations.create(data={"operations": operations})
        BATCH_ID = response["id"]
        print('Current bath operation id:\n')
        print(BATCH_ID)
    else:
        pass

    return


def check_batch_operation_status(batchid):
    """ Get info on a submitted batch operation """

    # Get response
    response = client.batch_operations.get(batchid)

    # Extract status and other details from response
    status = response['status']
    response_body_url = response['response_body_url']

    return status, response_body_url



# MAIN
if __name__ == "__main__":

    
    # Get template list properties
    template_list, template_list_json = get_list_by_id(TEMPLATE_LIST_ID)
    template_merge_fields = get_merge_fields_list(TEMPLATE_LIST_ID)
    partsavatar_contact = template_list['contact']
    partsavatar_campaign_defaults = template_list['campaign_defaults']
    partsavatar_permission_reminder = template_list['permission_reminder']
    partsavatar_email_type_option = template_list['email_type_option']

    print('Merge fields as extracted from the template audience:\n')
    pprint(template_merge_fields)
    print()

    # Audiences filenames
    audience_names_file = sys.argv[1]

    # Get names from file
    all_audiences_name = []
    all_segments_name = []
    with open(audience_names_file, "r") as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')
        for lines in csv_reader:
            all_segments_name.append(lines['Segment Name'])
            all_audiences_name.append(lines['List Name'])

    all_segments_csv = [xxx + '.csv' for xxx in all_segments_name]

    print('All segments:\n')
    pprint(all_segments_csv)

    # Create new audience and write members
    if CREATE_NEW_AUDIENCE:
        fwrite = open('created_audience_info', 'w+')
        fwrite.write('ID      NAME\n')
        number_of_audiences = len(all_audiences_name)
        for s in range(number_of_audiences):

            # Create new audience, take settings from template file
            current_list_name = all_audiences_name[s]
            print('Creating audience:\n')
            print(current_list_name)
            current_audience = create_new_list(current_list_name, partsavatar_contact, partsavatar_campaign_defaults,\
                                               partsavatar_permission_reminder, partsavatar_email_type_option)

            print('Done creating audience\n')
            # Get list id of the audience just created
            current_list_id = current_audience['id']

            # Write list name and id to file
            fwrite.write('{}  {}\n'.format(current_list_id, current_list_name))

            # Add merge tags
            add_merge_fields_to_list(current_list_id, template_merge_fields)
        fwrite.close()
    else:
        pass


    if WRITE_AUDIENCE_MEMBERS:
        # Read audience IDs from external file
        all_audiences_id = []
        number_of_audiences = len(all_audiences_name)
        fread = open('created_audience_info').readlines()
        for s in range(number_of_audiences):
            current_list_id = fread[s+1].split()[0]
            all_audiences_id.append(current_list_id)

        # Start writing
        os.chdir(PATH_AUDIENCE)
        for s in range(number_of_audiences):

            # Current directory
            print('Current working directory:\n')
            print(os.getcwd())

            # Get list id of the audience just created
            current_list_id = all_audiences_id[s]

            # Get name of audience from id!
            list_info, list_info_json = get_list_by_id(current_list_id)
            current_list_name = list_info['name']

            # Add members to list
            print ('New members to follwing audience will be written:')
            print(current_list_id,  current_list_name)
            print ('From this CSV file:')
            print(all_segments_csv[s])

            # Check if segment csv file is empty
            create_members_list_batch(current_list_id, all_segments_csv[s])
        os.chdir(PWD)
    else:
        pass


