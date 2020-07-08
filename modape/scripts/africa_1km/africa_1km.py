#!/usr/bin/env python
"""
  africa_1km.py: Flask Service app for collecting, processing and disseminating filtered NDVI.
                 Production the time series leverages the WFP VAM MODAPE toolkit: https://github.com/WFP-VAM/modape

  Dependencies: arc-modape (0.4), Numpy, ...
  
  Author: Rob Marjot
  
"""

import os
import glob
import re
import json
import argparse
try:
    from types import SimpleNamespace as Namespace
except ImportError:
    from argparse import Namespace
from flask import Flask, jsonify, send_file
from threading import Thread
import h5py
import numpy as np
from modape.utils import fromjulian
from modape.scripts.modis_download import modis_download, curate_downloads
from modape.scripts.modis_collect import modis_collect
from modape.scripts.modis_smooth import modis_smooth
from modape.scripts.modis_window import modis_window
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from modape.timeslicing import Dekad, ModisInterleavedOctad


app = Flask('Africa 1km')
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
this_dir, _ = os.path.split(__file__)
with open(os.path.join(this_dir, 'africa_1km.json')) as f:
    state = json.load(f, object_hook=lambda d: Namespace(**d))


def dateFromRawH5ModisTile(f, idx):
    with h5py.File(f) as h5f:
        return fromjulian(h5f.get('dates')[idx].decode())


def firstDateInRawH5ModisTile(f):
    return dateFromRawH5ModisTile(f, 0)


def lastDateInRawH5ModisTile(f):
    return dateFromRawH5ModisTile(f, -1)


def firstDateInRawH5ModisTiles(folder):
    files = glob.glob('{}/*.h5'.format(folder))
    first_dates = []
    for f in files:
        first_dates.append(firstDateInRawH5ModisTile(f))
    if len(first_dates) > 0:
        return max(first_dates)
    else:
        return None


def lastDateInRawH5ModisTiles(folder):
    files = glob.glob('{}/*.h5'.format(folder))
    last_dates = []
    for f in files:
        last_dates.append(lastDateInRawH5ModisTile(f))
    if len(last_dates) > 0:
        return min(last_dates)
    else:
        return None


def transform(array):
    return np.round(array, -2)


def slicename(region, dte):
    return "NDVI10_{}_MODAPE04_{}".format(str(Dekad(fromjulian(dte))), region)


@app.route('/')
def index():
    if getattr(state, 'fetcherThread', None) is not None:
        return "Fetcher is running (or suspended), try again later\n", 404
    else:
        files = {}
        for f in sorted(glob.glob(os.path.join(state.repository, state.file_pattern))):
            if os.path.isfile(f + '.md5'):
                with open(f + '.md5') as mdf:
                    files[os.path.basename(f)] = re.sub('\s+', '', mdf.readline())
        return jsonify(files)


@app.route('/download/<filename>')
def download(filename):
    if getattr(state, 'fetcherThread', None) is not None:
        return "Fetcher is running (or suspended), try again later\n", 404
    else:
        return send_file(os.path.join(state.repository, filename), as_attachment=True, mimetype=state.mimetype)


@app.route('/fetch')
def fetch():
    if getattr(state, 'fetcherThread', None) is not None:
        return "Fetcher is already running (or suspended), try again later\n", 404
    else:
        state.fetcherThread = Thread(target=do_fetching)
        state.fetcherThread.start()
        return "[{}] Fetcher started\n".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

@app.route('/suspend')
def fetch():
    if getattr(state, 'fetcherThread', None) is not None:
        if getattr(state, 'suspended', False):
            return "Fetcher is already suspended.\n", 404
        else:
            return "Fetcher is running, try again later.\n", 404
    else:
        state.fetcherThread = Thread(target=do_nothing)
        state.fetcherThread.start()
        return "[{}] Fetcher suspended; restart to resume.\n".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

def do_nothing():
    state.suspended = True
    pass

def do_fetching():
    try:
        # Do the hard work: download + ingest + smooth + export

        # download and ingest:
        while True:
            last_date = lastDateInRawH5ModisTiles(os.path.join(state.basedir, 'VIM'))
            next_date = last_date + relativedelta(days=8)
            if(last_date.year < next_date.year):
                # handle turning of the year:
                next_date = datetime(next_date.year, 1, 1).date()

            if next_date > date.today(): # stop after today:
                break

            download_params = {'targetdir': state.basedir, 'username': state.username, 'password': state.password,
                               'begin_date': next_date.strftime("%Y-%m-%d"), 'end_date': next_date.strftime("%Y-%m-%d"),
                               'aria2': True, 'tile_filter': state.tile_filter, 'download': True,
                               'strict_begindate': True, 'product': ['M?D13A2']}

            print('Downloading: {}...'.format(next_date))
            downloads = modis_download(**download_params)
            if len(downloads) < 1 and not state.debug:
                break
            else:
                if len(downloads) > 0:
                    # check download completeness:
                    if not curate_downloads(download_params['targetdir'], download_params['tile_filter'], next_date, next_date):
                        break
                    modis_collect(**{'srcdir': state.basedir, 'interleave': True, 'cleanup_ingested': True})

                if state.debug_redo_smooth or len(downloads) > 0:
                    # smooth, enabling update mode and setting N/n
                    modis_smooth(**{'input': os.path.join(state.basedir, 'VIM'), 'update': True,
                                    'targetdir': os.path.join(state.basedir, 'VIM', 'SMOOTH'),
                                    'nsmooth': 64, 'nupdate': 6,
                                    'tempint': 10, 'constrain': True})

                # export dekads, from back to front (n = 6):
                nexports = 1
                exportOctad = ModisInterleavedOctad(lastDateInRawH5ModisTiles(os.path.join(state.basedir, 'VIM')))
                exportDekad = Dekad(exportOctad.getDateTimeEnd(), True)
                print('')
                print('Octad-end for last ingested date: {}'.format(str(exportOctad.getDateTimeEnd())))
                print(' > Corresponding dekad: {}'.format(str(exportDekad)))
                if state.debug_report_last_octad:
                    print('')
                    break

                while Dekad(exportOctad.prev().getDateTimeEnd(), True).Equals(exportDekad) and nexports < 6:
                    nexports = nexports + 1
                    exportOctad = exportOctad.prev()

                first_date = firstDateInRawH5ModisTiles(os.path.join(state.basedir, 'VIM'))
                while (not exportDekad.startsBeforeDate(first_date)) and nexports <= 6:
                    print('>>Export: {} [Update: {}]'.format(str(exportDekad), str(nexports)))
                    for region, roi in dict(**vars(state.export)).items():
                        modis_window(**{'path': os.path.join(state.basedir, 'VIM', 'SMOOTH'), 'roi': roi,
                                        'targetdir': os.path.join(state.basedir, 'VIM', 'SMOOTH', 'EXPORT'),
                                        'region': region,
                                        'begin_date': exportDekad.getDateTimeMid().strftime('%Y-%m-%d'),
                                        'end_date': exportDekad.getDateTimeMid().strftime('%Y-%m-%d'),
                                        'cb_transform': lambda array: transform(array),
                                        'cb_slicename': lambda region, dte: slicename(region, dte),
                                        'overwrite': True,
                                        'md_list': ['UPDATE_NUMBER={}'.format(nexports),
                                                    'FINAL={}'.format('FALSE' if nexports < 6 else 'TRUE')]
                                        })

                    nexports = nexports + 1
                    exportOctad = exportOctad.prev()
                    exportDekad = Dekad(exportOctad.getDateTimeEnd(), True)
                    while Dekad(exportOctad.prev().getDateTimeEnd(), True).Equals(exportDekad) and nexports < 6:
                        nexports = nexports + 1
                        exportOctad = exportOctad.prev()

            if state.debug:
                break
    finally:
        state.fetcherThread = None
  
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Incremental download+ingest+smooth+export')
    parser.add_argument('--debug', help='Run without Flask: debug fetch procedure', action='store_true')
    parser.add_argument('--debug-redo-smooth', help='When running debug: redo smoothing even when no new downloads have been ingested', action='store_true')
    parser.add_argument('--debug-report-last-octad', help='When running debug: report last ingested octad and quit', action='store_true')
    p = parser.parse_args()
    state.debug = p.debug
    state.debug_redo_smooth = p.debug_redo_smooth
    state.debug_report_last_octad = p.debug_report_last_octad
    if p.debug:
        fetch()
        state.fetcherThread.join()
    else:
        app.run(port=5001, threaded=False) # Configure for single threaded request handling
