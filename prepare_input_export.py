import csv
import requests
import os, sys, time
import numpy as np
from subprocess import call
from pathlib import Path
from mailchimp3 import MailChimp
from tabulate import tabulate


EXPORT_SEGMENTS = True
EXPORT_FITMENTS = False

# API key
mc_api = " "

# MailChimp client
headers = requests.utils.default_headers()
client = MailChimp(mc_api=mc_api, timeout=30.0, request_headers=headers)

# List id
PARTSAVATAR_CUSTOMERS_LIST_ID = "8adfbf295d"     # PartsAvatar Customers


def get_info_segment(listid, segmentid):
    """ Get information on a particular segment by id"""

    response = client.lists.segments.get(listid, segmentid)
    segment_name = response['name']
    segment_member_count = response['member_count']

    return segment_name, segment_member_count


# Read segments info from segments.csv
segments_info_file = sys.argv[1]
all_segments_id = []
all_segments_name = []
with open(segments_info_file, "r") as csv_file:
    csv_reader = csv.DictReader(csv_file, delimiter=',')
    if EXPORT_SEGMENTS:
        Path('./All_segments').mkdir(parents=True, exist_ok=True)
        for lines in csv_reader:
            all_segments_id.append(lines['Segment id'])
            all_segments_name.append(lines['Segment Name'])
    elif EXPORT_FITMENTS:
        Path('./All_fitments').mkdir(parents=True, exist_ok=True)
        for lines in csv_reader:
            all_segments_id.append(lines['Fitment id'])
            all_segments_name.append(lines['Fitment Name'])
    else:
        pass

# Get name and members, make sure name matches from API call and 
# what has been provided.
n_segments = len(all_segments_id)
all_segments_members = []
all_segments = [['ID', 'NAME', 'MEMBERS']]
for i in range(n_segments):
    sname, smem = get_info_segment(PARTSAVATAR_CUSTOMERS_LIST_ID, all_segments_id[i])
    assert sname == all_segments_name[i]
    all_segments_members.append(smem)


# Sort segments based on numbers of members (ascending)
index_members_sort = np.argsort(all_segments_members)
index_members = index_members_sort.tolist()
sorted_segment_id = [all_segments_id[i] for i in index_members]
sorted_segment_name = [all_segments_name[i] for i in index_members]
sorted_segment_members = [all_segments_members[i] for i in index_members]

for j in range(n_segments):
    all_segments.append([sorted_segment_id[j], sorted_segment_name[j], sorted_segment_members[j]])

print(tabulate(all_segments, headers='firstrow', showindex='always', tablefmt='plain'))

# Create a reasonable value of COUNT based on total number of members in a segment
all_counts = []
all_parts = []
for i in range(n_segments):
    nmem = sorted_segment_members[i]
    if nmem < 200:
        count = 50
    elif nmem > 200 and nmem < 600:
        count = 100
    elif nmem > 600 and nmem < 800:
        count = 150
    elif nmem > 800 and nmem < 1000:
        count = 200
    else:
        count = 500

    div_mod = divmod(nmem, count)
    quotient = div_mod[0]
    remainder = div_mod[1]
    if remainder > 0:
        n_parts = quotient + 1
    else:
        n_parts = quotient

    all_counts.append(count)
    all_parts.append(n_parts)

# Write to file
if EXPORT_SEGMENTS:
    fwrite = open('run_export_segments', 'w')
    fwrite.write('# ID, COUNT, OFFSET, COUNTER, MEMBERS, PARTS \n')
    for i in range(n_segments):
        fwrite.write('python3 export_segments.py {} {} {} {} {} {} \n'.format(sorted_segment_id[i], all_counts[i], 0, 1, sorted_segment_members[i], all_parts[i]))
        fwrite.write('sleep 100\n')
    fwrite.close()
elif EXPORT_FITMENTS:
    fwrite = open('run_export_fitments', 'w')
    fwrite.write('# ID, COUNT, OFFSET, COUNTER, MEMBERS, PARTS \n')
    for i in range(n_segments):
        fwrite.write('python3 export_fitments.py {} {} {} {} {} {} \n'.format(sorted_segment_id[i], all_counts[i], 0, 1, sorted_segment_members[i], all_parts[i]))
        fwrite.write('sleep 30\n')
    fwrite.close()
else:
    pass
