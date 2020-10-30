import os, re, bs4
from flask import Flask, request

from google.cloud import storage, firestore
from cloudevents.http import from_http

app = Flask(__name__)

storage_client = storage.Client()
gcs_pattern = 'buckets/(.*)/objects/(.*)'
gcs_bucket = 'cr-bucket-haiyu-eventflow'

db = firestore.Client()
highlight_pattern = 'Highlight.*Location ([0-9]+)'
note_pattern = 'Note.*Location ([0-9]+)'

def parse(html_doc):
    soup = bs4.BeautifulSoup(html_doc, 'html.parser')
    book_title = soup.find('div', {'class': 'bookTitle'}).text.strip()
    book_ref = db.collection('books').document(book_title)
    highlights_ref = book_ref.collection('highlights')
    for section in soup.find_all('div', {'class': 'sectionHeading'}):
        section_ref = highlights_ref.document(section.text.strip())

        for div in section.next_siblings:
            if isinstance(div, bs4.element.NavigableString):
                continue
            if div.attrs and div.attrs['class'][0] == 'sectionHeading':
                # next section
                break
            if div.attrs['class'][0] == 'noteHeading':
                note_heading = div.text.strip()
                # next_sibling is empty NavigableString
                note_text = div.next_sibling.next_sibling.text.strip()

                m = re.search(highlight_pattern, note_heading)
                if m:
                    section_ref.set({
                        'Location_' + m.group(1): {'highlight': note_text}
                    }, merge=True)

                else:
                    m = re.search(note_pattern, note_heading)
                    if m:
                        section_ref.set({
                            'Location_' + m.group(1): {'note': note_text}
                        }, merge=True)
    return


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
        if m.group(1) == gcs_bucket:
            bucket = storage_client.get_bucket(m.group(1))
            blob = bucket.get_blob(m.group(2))
            temp_file_name = m.group(2)
            blob.download_to_filename(temp_file_name)

            with open (temp_file_name, 'r') as myfile:
               html_doc = myfile.read()
               parse(html_doc)


    return ('', 204)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
