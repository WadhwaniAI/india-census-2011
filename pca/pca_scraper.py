import mechanize
import lxml.html
import sys
import os
import multiprocessing
import socket
import re
import json

"""
A utility to scrape the government of India census website.

"""

URL = "http://www.censusindia.gov.in/pca/pca.aspx"
HEADERS = [('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')]
RETRIES = 3
DISTRICT_IDS = "district_ids.json"

def set_form_attribute(form, attribute, value, br):
    br.select_form(form)
    br.set_all_readonly(False)
    br[attribute] = [value]
    response = br.submit()
    return response.read()

def mechanize_select_state(state_id, br):
    br.addheaders = HEADERS
    response = br.open(URL)
    rdata = response.read()
    return set_form_attribute("form1", "StateDropDownList1", state_id, br)

def get_district_ids(state_id):
    br = mechanize.Browser()
    rdata = mechanize_select_state(state_id, br)
    root = lxml.html.fromstring(rdata)
    options =  root.xpath('//select[@id="DistDropDownList1"]')[0]
    districts = []
    for district in options:
        value = district.get('value')
        # some of the options are for all of India or other
        if len(value) == 3:
            districts.append(value)

    return districts

def get_district(state_id, district_id, mode):
    br = mechanize.Browser()
    mechanize_select_state(state_id, br)
    rdata = set_form_attribute("form1", "DistDropDownList1", district_id, br)
    root = lxml.html.fromstring(rdata)
    table = root.xpath('//table[@id="GridView1"]')
    lines = []
    if mode == 'header':
        row = table[0].xpath('.//tr')[1]
        column = []
        prefix = 'Gridview1_ctl02_lbl_'
        for span in row.xpath('.//td/span'):
            label = span.attrib['id']
            column.append(label[len(prefix):])
        lines.append(",".join(column))
        return lines
    else:
        tdxpath = './/td'
    for row in table[0].xpath('.//tr'):
        cells = row.xpath(tdxpath)
        if len(cells):
            column = []
            for cell in cells:
                content = cell.text_content().encode("utf8").strip()
                column.append(content)
            lines.append(",".join(column))
    return lines

def write_district_census(state_id, district_id, mode):
    print 'start state %s district %s' % (state_id, district_id)
    fname = state_id + '_' + district_id + '.csv' if mode == 'data' else 'header.csv'
    try:
        if os.path.getsize(fname) > 0:
            print >>sys.stderr, "found %s, skipping" % fname
            return
    except:
        pass
    with open(fname, "w") as f:
        lines = None
        for i in range(RETRIES):
            try:
                lines = get_district(state_id, district_id, mode)
                break
            except socket.error as e:
                # connection reset by peer, probably
                pass

        if lines and len(lines):
            f.write('\n'.join(lines) + '\n')
            print >>sys.stderr, "\twrote %d lines in %s" % (len(lines), fname)

def district_thread_wrapper(item):
    write_district_census(item[0], item[1], 'data')

def state_thread_wrapper(state_id):
    district_ids = get_district_ids(state_id)
    districts = []
    for district_id in district_ids:
        districts.append((state_id, district_id))
    return districts

def progress(districts_with_state):
    districts = {}
    for state_id, district_id in districts_with_state:
        districts[district_id] = True

    for filename in os.listdir('.'):
        if re.match(r'\d\d_\d\d\d.csv', filename):
            district_id = re.search(r'\d\d\d', filename).group(0)
            assert district_id in districts
            del districts[district_id]

    print "%d districts remaining" % len(districts)

def read_in_district_ids_or_query():
    if os.path.isfile(DISTRICT_IDS):
        with open(DISTRICT_IDS, "r") as json_file:
            return json.load(json_file)

    pool = multiprocessing.Pool(processes=4)
    state_ids = []

    for i in range(35):
        state_id = '%02d' % (i + 1)
        state_ids.append(state_id)

    results = pool.map(state_thread_wrapper, state_ids)

    districts_with_state = []
    for result in results:
        districts_with_state += result

    with open(DISTRICT_IDS, "w") as json_file:
        json.dump(districts_with_state, json_file)
        return districts_with_state

def main():
    progress_only = False
    if len(sys.argv) > 1:
        if sys.argv[1] == 'header':
            write_district_census("01", "0", 'header')
            return
        elif sys.argv[1] == 'progress':
            progress_only = True

    mode = 'data'

    districts_with_state = read_in_district_ids_or_query()
    print("done querying for district ids")
    progress(districts_with_state)

    if progress_only:
        return

    pool.map(district_thread_wrapper, districts_with_state)

    pool.close()
    pool.join()

if __name__ == '__main__':
    main()
