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
import logging, socket
import pandas as pd
from pathlib import Path
from mailchimp3 import MailChimp

EXPORT_SEGMENTS = False
EXPORT_FITMENTS = False

# API key
mc_api = " "

# MailChimp client
headers = requests.utils.default_headers()
client = MailChimp(mc_api=mc_api, timeout=30.0, request_headers=headers)

# List id
PARTSAVATAR_CUSTOMERS_LIST_ID = "8adfbf295d"     # PartsAvatar Customers

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


def get_all_segments(listid):
    """ Get all segments info for a particular list
    """

    try:
        segments = client.lists.segments.all(listid, get_all=True)
    except socket.timeout:
        raise 
    segments_output = ujson.dumps(segments, indent=4)

    return segments, segments_output


def get_info_segment(listid, segmentid):
    """ Get information on a particular segment by id"""

    response = client.lists.segments.get(listid, segmentid)
    segment_name = response['name']
    segment_member_count = response['member_count']

    return segment_name, segment_member_count


def get_all_segment_members_direct(listid, segmentid, merge_fields):
    """ Get all members in one shot, no count/offset business"""


    # Get the segment name, members etc from segmentid directly
    seg_name, total_members = get_info_segment(listid, segmentid)

    response = client.lists.segments.members.all(listid, segmentid, get_all=True)

    segment_dict = create_segment_to_dict(response, merge_fields, total_members)

    # Convert to csv
    write_dict_to_csv(current_segment_dict, seg_name+'.csv', merge_fields)

    return


def get_segment_members(listid, segmentid, merge_fields, count, offset, counter, parts):
    """ Get all members of a segment in pieces using offset and count"""


    SUCCESS = False

    # Get the segment name, members etc from segmentid directly
    seg_name, total_members = get_info_segment(listid, segmentid)

    print('Retrieving members for Segment - ', seg_name)
    print('There are', total_members, 'members in this segment')
    print()

    all_parts_csv = []

    if total_members != 0:
        while offset < total_members:
            print('Current offset', offset)
            print('Current counter', counter)
            print('Getting members for next set')
            response = client.lists.segments.members.all(listid, segmentid, count=count, offset=offset)
            # Convert response to dictionary
            temp = offset + count
            if temp < total_members:
                current_segment_dict = create_segment_to_dict(response, merge_fields, count)
            else:
                current_segment_dict = create_segment_to_dict(response, merge_fields, total_members-offset)

            # Convert to csv
            temp_name_csv = "".join(seg_name.split()) + '-part-' + str(counter)
            all_parts_csv.append(temp_name_csv + '.csv')
            write_dict_to_csv(current_segment_dict, temp_name_csv, merge_fields)
            offset = offset + count
            counter = counter + 1
            time.sleep(60)

        # Merge all parts to get the final segment csv file!
        if counter == parts+1:
            SUCCEES = True
            print('SUCCESS!')
            print('All parts of current segment', segmentid, 'have been exported, will merge them now')
            merge_segments_create_audience(all_parts_csv, seg_name+'.csv')
            # Remove all parts csv files
            for x in all_parts_csv:
                os.system('rm ' + x)
        else:
            pass

    else:
        print('This segment has no members! Exiting without exporting CSV.')

    return SUCCESS, offset, counter


def create_segment_to_dict(segment, merge_fields, nmem):
    """ Given segment members info, create a csv file
    """

    # Define primary fields
    primary_fields = ["email_address", "status"]

    # Merge primary fields and merge_fields to have all fields together!
    # This also defines the headers of csv file basically
    all_fields = primary_fields + merge_fields

    # Create new dictionary
    newdict_segment = {}

    # Fill dictionary elements and convert to CSV
    for i in range(nmem):
        member_current = segment['members'][i]
        newdict_segment[i] = {}
        for key in all_fields:
            if key in primary_fields:
                newdict_segment[i][key] = member_current[key]
            else:
                newdict_segment[i][key] = member_current['merge_fields'][key]

    return newdict_segment


def write_dict_to_csv(newdict_segment, file_prefix, merge_fields):

    # Define primary fields
    primary_fields = ["email_address", "status"]

    # Merge primary fields and merge_fields to have all fields together!
    # This also defines the headers of csv file basically
    all_fields = primary_fields + merge_fields

    # Write to CSV
    csv_file = file_prefix + ".csv"
    try:
        with open(csv_file,'w',encoding='utf-8-sig',newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=all_fields)
            writer.writeheader()
            for data in newdict_segment:
                writer.writerow(newdict_segment[data])
    except IOError:
        print("I/O error")

    return csv_file



def merge_segments_create_audience(all_segments_csv, filename):
    """ Concatenate given dataframes to create one audience and write to file
    """

    all_dataframes = [pd.read_csv(x, encoding='utf_8_sig') for x in all_segments_csv]
    bigdataframe = pd.concat(all_dataframes)
    bigdataframe.to_csv(filename, index=False)

    return


def get_all_members_segment_batch(listid, segmentid):
    """ Retrieve all members of a segment, identified by id"""

    #'params': {"count":100, "offset":0}
    # 'path': '/lists/' + listid + '/segments/' + segmentid + '/members',
    operations = [{
        'method': 'GET',
        'path': '/lists/' + listid + '/segments/' + segmentid + '/members',
        'params': {"count":100, "offset":0}
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


def get_file_from_response_url(segment_name, response_url):
    """ Download and extract the zip file from a response body URL"""

    # Get the filename (Zip filename)
    filename = response_url.split('?')[0].split('/')[-1]

    # NOTE: filename is actually "batchid-response.tar.gz (or zip)! Interesting.

    # Download and extract zip (Check GET success with r.ok)
    r = requests.get(response_url, stream=True, allow_redirects=True)
    total_size = int(r.headers.get('content-length'))
    initial_pos = 0

    with open(filename,'wb') as f:
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=filename,initial=initial_pos, ascii=True) as pbar:
            for ch in r.iter_content(chunk_size=1024):
                if ch:
                    f.write(ch)
                    pbar.update(len(ch))
                else:
                    pass

    os.mkdir(segment_name)
    if tarfile.is_tarfile(filename):
        z = tarfile.open(filename)
        z.extractall(segment_name)
    elif zipfile.is_zipfile(filename):
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(segment_name)
    else:
        pass

    return


def get_segment_from_json_to_csv(segment_name):
    """ Convert JSON downloaded from batch to CSV """

    os.chdir(segment_name)
    # Sort the files, bigger one has the data
    (_, _, filenames) = next(os.walk('.'))
    ordered_files = assign_priority_segments_by_size(filenames)
    datafile = ordered_files[-1]
    s = open(datafile)
    data = ujson.load(s)
    json_data = ujson.loads(data[0]['response'])
    all_members = json_data['members']
    merge_fields = list(all_members[0]['merge_fields'].keys())
    headers = ['email_address', 'status', 'FNAME', 'LNAME', 'PROVINCE', 'CITY', 'ZIP_CODE', 'MAKE', 'MODEL', 'YEAR']

    os.chdir('..')

    f = csv.writer(open(segment_name+".csv", "w+"))
    f.writerow(headers)
    for member in all_members:
        f.writerow([member['email_address'], 
                    member['status'], 
                    member['merge_fields']['FNAME'],
                    member['merge_fields']['LNAME'],
                    member['merge_fields']['PROVINCE'],
                    member['merge_fields']['CITY'],
                    member['merge_fields']['ZIP_CODE'],
                    member['merge_fields']['MAKE'],
                    member['merge_fields']['MODEL'],
                    member['merge_fields']['YEAR']])

    return


def export_segment_to_csv(listid, segmentid, segmentname):
    """Given a segment id, export members to a CSV file.

    Args:
        listid (str): unique id of the given audience
        segmentid (str): unique id of a segment for a given list
    """

    current_batch_id = get_all_members_segment_batch(listid, segmentid)
    batch_status, batch_response_url = check_batch_operation_status(current_batch_id)
    while not batch_status == 'finished':
        print('Waiting for batch operation to complete')
        time.sleep(5)
        batch_status, batch_response_url = check_batch_operation_status(current_batch_id)
    
    assert batch_status == 'finished'   # quick test

    get_file_from_response_url(segmentname, batch_response_url)
    get_segment_from_json_to_csv(segmentname)

    return


# MAIN
if __name__ == "__main__":

    # Get Audience Info
     all_lists, all_lists_json = get_all_lists()
     print(all_lists_json)
     sys.exit()

