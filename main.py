import os, re
import tempfile
from flask import Flask, request

from google.cloud import storage
from cloudevents.http import from_http

app = Flask(__name__)
storage_client = storage.Client()
gcs_pattern = 'buckets/(.*)/objects/(.*)'


@app.route('/', methods=['POST'])
def read_gcs():
    # create a CloudEvent
    event = from_http(request.headers, request.get_data())
    print(
        f"=== Found {event['source']} with type "
        f"{event['type']} and subject {event['subject']}"
    )

    # a GCS object is created
    if 'methodname' in event and event['methodname'] == 'storage.objects.create':
        m = re.search(gcs_pattern, event['resourcename'])
        if not m:
            return ('No resourcename attribute', 500)
        if m.group(1) == 'cr-bucket-haiyu-eventflow':
            bucket = storage_client.get_bucket(m.group(1))
            blob = bucket.get_blob(m.group(2))
            temp_file_name = m.group(2)
            blob.download_to_filename(temp_file_name)

            with open (temp_file_name, "r") as myfile:
               temp_str = myfile.read()
               print (temp_str)

    return ('', 204)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
