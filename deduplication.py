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
from subprocess import check_output
from mailchimp3 import MailChimp
from pprint import pprint

FITMENT = False
MERGE_FITMENTS = False
DE_DUPLICATION = True

PWD = os.getcwd()
PATH_SEGMENT = './All_segments'
PATH_FITMENT = './All_fitments'
PATH_DEDUP_SEG = './All_segments_deduplicated'

# API key
mc_api = " "

# MailChimp client
headers = requests.utils.default_headers()
client = MailChimp(mc_api=mc_api, timeout=30.0, request_headers=headers)



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


def get_all_members_subscribed_list(listid):
    """ Returns all members inside list."""

    members = client.lists.members.all(listid, status="subscribed", get_all=True)
    members_output = ujson.dumps(members, indent=4)

    return members, members_output


def delete_all_members_list(listid):
    """ Delete all members of al ist
    """

    # Note "delete_permanent". Difference from regular delete?
    client.lists.members.delete_permanent(listid)

    return


def delete_all_members_segment(listid, segmentid):
    """ Delete all members of al ist
    """

    client.lists.segments.members.delete(listid, segmentid, subscriber_hash='')

    return


def add_merge_fields_to_list(listid, merge_fields_to_create):
    """ Add additional merge fields to an existing list
    """

    n_fields = len(merge_fields_to_create)
    for i in range(n_fields):
        client.lists.merge_fields.create(listid, {"name": merge_fields_to_create[i].lower(), "type": "text", "tag": merge_fields_to_create[i]})

    return


def create_members_list_batch(listid, csvfile):
    """ Batch operation for creating multiple members at once from CSV file
    """

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

    return BATCH_ID


def delete_members_list_batch(listid, md5_hashes):
    """ Batch operation for deleting multiple members at once.
    
        Needs MD5 hashes of members, which can be obtained by GET request.
    """

    operations = [{
        'method': 'POST',
        'path': '/lists/' + listid + '/members/' + md5hash + '/actions/delete-permanent',
        'body': md5hash
    } for md5hash in md5_hashes]

    response = client.batch_operations.create(data={"operations": operations})
    BATCH_ID = response["id"]

    return BATCH_ID



def remove_full_duplicates(segment_1, segment_2, newfile):
    """Given two segments, remove duplicates in second and write new file.

    Parameters
    ----------
    segment_1: pandas DataFrame
        First segment
    segment_2: pandas DataFrame
        Second segment
    newfile: str
        Name of new file where new entries will be written

    """

    # Note that the eqaulity check here only sees whether the two segments
    # are a copy of each other with SAME ORDERING!
    if segment_2.equals(segment_1):
        print('Provided segments are exactly the same,\
               no attempt made to find duplicates')
    else:
        # Merge using 'outer'
        seg2_unique = segment_1.merge(segment_2, how='outer', indicator=True).loc[lambda x : x['_merge']=='right_only']

        # Reset index and emove _merge column
        seg2_unique = seg2_unique.reset_index(drop=True)
        del seg2_unique['_merge']

        # Write to file, csv file (use index=False)
        # Can decide here how to write missign data
        seg2_unique['YEAR'] = seg2_unique['YEAR'].fillna(9999)
        seg2_unique['YEAR'] = seg2_unique['YEAR'].astype(int)
        seg2_unique.to_csv(newfile, index=False)

    return


def assign_priority_segments_by_size(list_of_segments):

    """
    list_of_segments has all names of segments (csv files!)
    shreshth will provide segments name, match it to yours
    (the ones obtained by API)
    """

    # Get filesize (Make a list/array, sort)   
    n_segments = len(list_of_segments)
    all_segments_size = []
    for i in range(n_segments):
        current_segment_filename = list_of_segments[i]
        all_segments_size.append(Path(current_segment_filename).stat().st_size)

    # Sort by size (ascending) and return index
    assign_priority_index = sorted(range(n_segments), reverse=True, key=lambda k: all_segments_size[k])
    ordered_segments = [list_of_segments[x] for x in assign_priority_index]

    return ordered_segments


def merge_segments_create_audience(all_segments_csv, filename):
    """ Concatenate given dataframes to create one audience and write to file
    """

    all_dataframes = [pd.read_csv(x, encoding='utf_8_sig') for x in all_segments_csv]
    bigdataframe = pd.concat(all_dataframes)
    bigdataframe.to_csv(filename, index=False)

    return


def get_all_subscribers_hash(listid):
    """
    Batchify, as well! This function is crucial as the 
    hashes are needed for deleting members from an audience. 
    """
    # Get all audience members. MD5 hash key is "id"
    all_members, all_members_json = get_all_members_subscribed_list(listid)
    n_members = len(all_members["members"])
    print('There are', n_members, 'members in this Audience')
    all_subscribers_hash = []
    for i in range(n_members):
        md5_hash_current = all_members["members"][i]["id"]
        all_subscribers_hash.append(md5_hash_current)

    return all_subscribers_hash


def get_all_members_list_batch(listid):
    """ Retrieve all members of a segment, identified by id"""

    operations = [{
        'method': 'GET',
        'path': '/lists/' + listid + '/members',
    }]

    response = client.batch_operations.create(data={"operations": operations})
    BATCH_ID = response["id"]

    return BATCH_ID



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


    # Segments and Fitments filenames
    segment_file = sys.argv[1]
    #fitment_file = sys.argv[2]

    # Read from file
    merge_fields_pass = ['FNAME', 'LNAME', 'PROVINCE', 'CITY', 'ZIP_CODE', 'MAKE', 'MODEL', 'YEAR']

    # Get segment names and ids from file
    all_segments_name = []
    all_segments_id = []
    all_audiences_name = []
    with open(segment_file, "r") as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=',')
        for lines in csv_reader:
            all_segments_name.append(lines['Segment Name'])
            all_audiences_name.append(lines['List Name'])
            all_segments_id.append(lines['Segment id'])


    # CSV name lists for segments and fitments
    all_segments_csv = [xxx + '.csv' for xxx in all_segments_name]

    if FITMENT:
        # Get fitment info
        all_fitments_name = []
        all_fitments_id = []
        with open(fitment_file, "r") as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=',')
            for lines in csv_reader:
                all_fitments_name.append(lines['Fitment Name'])
                all_fitments_id.append(lines['Fitment id'])

        all_fitments_csv = [xxx + '.csv' for xxx in all_fitments_name]
    else:
        pass

    # Merge all fitments to create full fitment file!
    if MERGE_FITMENTS:
    # Concatenate all fitment - create a large file
    # This becomes priority 1 segment before deduplication
        os.chdir(PATH_FITMENT)
        big_fitment = 'all_fitments_merged.csv'
        merge_segments_create_audience(all_fitments_csv, big_fitment)
        # Copy to segment directory
        os.chdir(PWD)
        os.system('cp ' + PATH_FITMENT + '/all_fitments_merged.csv ' + PATH_SEGMENT + '/.')
    else:
        pass

    # Deduplication
    if DE_DUPLICATION:
        # Create deduplication directory
        Path(PATH_DEDUP_SEG).mkdir(parents=True, exist_ok=True)

        # Copy files from segments directory to the deduplication directory
        check_output('cp ' + PATH_SEGMENT + '/*.csv ' +  PATH_DEDUP_SEG + '/.', shell=True)

        # Move to deup directory
        os.chdir(PATH_DEDUP_SEG)

        # Assign priority 
        all_segments_in_order = assign_priority_segments_by_size(all_segments_csv)

        # Add big fitment file to first position
        # FEB 13: Commenting out for now, no need for fitments
        # all_segments_in_order.insert(0, 'all_fitments_merged.csv')

        print('All segments by size in descending order')
        pprint(all_segments_in_order)

        n_segments = len(all_segments_in_order)
        # Remove Duplicates from Segments
        for i in range(n_segments - 1):
            current_i_file = pd.read_csv(all_segments_in_order[i]) 
            for j in range(i, n_segments):
                if i != j:
                    current_j_file = pd.read_csv(all_segments_in_order[j]) 
                    write_to_file = all_segments_in_order[j]
                    print('Checking segment', j+1, 'against', i+1)
                    print('Current segment file', all_segments_in_order[j])
                    remove_full_duplicates(current_i_file, current_j_file, write_to_file)
                else:
                    pass
        os.chdir(PWD)
    else:
        pass

