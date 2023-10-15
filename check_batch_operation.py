import sys
import requests
from mailchimp3 import MailChimp

# API key
mc_api = " "

# MailChimp client
headers = requests.utils.default_headers()
client = MailChimp(mc_api=mc_api, timeout=30.0, request_headers=headers)

all_batches = client.batch_operations.all(get_all=True)['batches']
for response in all_batches:
    print('BATCH_ID:', response['id'])
    print('STATUS:', response['status'])
    print('TOTAL:', response['total_operations'])
    print('FINISHED:', response['finished_operations'])
    print('ERROR:', response['errored_operations'])
    print('SUBMITTED:', response['submitted_at'])
    print('COMPLETED:', response['completed_at'])
    print('RESPONSE BODY URL:', response['response_body_url'])
    print()
sys.exit()

batch_id = sys.argv[1]
#response = client.batch_operations.get(batch_id)

print('BATCH_ID:', response['id'])
print('STATUS:', response['status'])
print('TOTAL:', response['total_operations'])
print('FINISHED:', response['finished_operations'])
print('ERROR:', response['errored_operations'])
print('SUBMITTED:', response['submitted_at'])
print('COMPLETED:', response['completed_at'])
print('RESPONSE BODY URL:', response['response_body_url'])
