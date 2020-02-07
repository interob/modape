#!/usr/bin/env python
"""
  kenia_1km.py: Flask Service app for collecting, processing and disseminating filtered NDVI.
                 Production the time series leverages the WFP VAM MODAPE toolkit: https://github.com/WFP-VAM/modape

  Dependencies: arc-modape (0.4), Numpy, ...
  
  Author: Rob Marjot
  
"""

import os
import glob
import re
import json
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


app = Flask('Kenya 1km')
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
this_dir, _ = os.path.split(__file__)
with open(os.path.join(this_dir, 'kenya_1km.json')) as f:
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


def slicename(dte):
    return "NDVI10_{}_MODAPE04_KENYA".format(str(Dekad(fromjulian(dte))))


@app.route('/')
def index():
    if getattr(state, 'fetcherThread', None) is not None:
        return "Fetcher is running, try again later\n", 404
    else:
        files = {}
        for f in sorted(glob.glob(os.path.join(state.repository, state.file_pattern))):
            if os.path.isfile(f + '.md5'):
                with open(f + '.md5') as mdf:
                    files[os.path.basename(f)] = re.sub('\s+', '', mdf.readline())
        return jsonify(files)


@app.route('/download/<filename>')
def download(filename):
    return send_file(os.path.join(state.repository, filename), as_attachment=True, mimetype=state.mimetype)


@app.route('/fetch')
def fetch():
    if getattr(state, 'fetcherThread', None) is not None:
        return "Fetcher is already running, try again later\n", 404
    else:
        state.fetcherThread = Thread(target=do_fetching)
        state.fetcherThread.start()
        return "Fetcher started\n"


def do_fetching():
    try:
        # Do the hard work: download + ingest + smooth + export
        first_date = firstDateInRawH5ModisTiles(os.path.join(state.basedir, 'VIM'))
        # download and ingest:
        while True:
            last_date = lastDateInRawH5ModisTiles(os.path.join(state.basedir, 'VIM'))
            next_date = last_date + relativedelta(days=8)
            if(last_date.year < next_date.year):
                # handle turning of the year:
                next_date = datetime(next_date.year, 1, 1).date()

            if next_date > date.today(): # stop after today:
                break

            download_params = {}
            download_params['targetdir'] = state.basedir
            download_params['username'] = state.username
            download_params['password'] = state.password
            download_params['begin_date'] = next_date.strftime("%Y-%m-%d")
            download_params['end_date'] = next_date.strftime("%Y-%m-%d")
            download_params['aria2'] = True
            download_params['tile_filter'] = state.tile_filter
            download_params['download'] = True
            download_params['strict_begindate'] = True
            download_params['product'] = ['M?D13A2']

            print('Downloading: {}...'.format(next_date))
            downloads = modis_download(**download_params)
            if len(downloads) < 1:
                break
            else:
                # check download completeness:
                if not curate_downloads(download_params['targetdir'], download_params['tile_filter'], next_date, next_date):
                    break
                modis_collect(**{'srcdir': state.basedir, 'interleave': True, 'cleanup_ingested': True})

                ingested_date = lastDateInRawH5ModisTiles(os.path.join(state.basedir, 'VIM'))
                dekadForIngested = Dekad(ingested_date)
                dekadForPreviousIngested = Dekad(last_date)
                if not dekadForPreviousIngested.Equals(dekadForIngested):

                    # smooth, enabling update mode and setting N/n
                    modis_smooth(**{'input': os.path.join(state.basedir, 'VIM'), 'update': True,
                                    'targetdir': os.path.join(state.basedir, 'VIM', 'SMOOTH'),
                                    'nsmooth': 64, 'nupdate': 6,
                                    'tempint': 10, 'constrain': True})

                    # export dekads, from back to front (n = 6):
                    nexports = 1
                    exportDekad = dekadForIngested
                    while (not exportDekad.startsBeforeDate(first_date)) and nexports <= 6:
                        # also check: do not export before cutoff date
                        print('>>Export: {}'.format(str(exportDekad)))
                        modis_window(**{'path': os.path.join(state.basedir, 'VIM', 'SMOOTH'),
                                        'roi': [33.0, -5.0, 42.0, 5.0],
                                        'targetdir': os.path.join(state.basedir, 'VIM', 'SMOOTH', 'EXPORT'),
                                        'region': 'WFPVAM_NDVI',
                                        'begin_date': exportDekad.getDateTimeMid().strftime('%Y-%m-%d'),
                                        'end_date': exportDekad.getDateTimeMid().strftime('%Y-%m-%d'),
                                        'cb_transform': lambda array: transform(array),
                                        'cb_slicename': lambda dte: slicename(dte),
                                        'overwrite': True,
                                        'md_list': ['UPDATE_NUMBER={}'.format(nexports),
                                                    'FINAL={}'.format('FALSE' if nexports < 6 else 'TRUE')]
                                        })
                        nexports = nexports + 1
                        exportDekad = exportDekad.prev()

            if str(getattr(state, 'debug', 'false')).lower() == 'true':
                break
    finally:
        state.fetcherThread = None
  
if __name__ == '__main__':
    if str(getattr(state, 'debug', 'false')).lower() == 'true':
        fetch()
        state.fetcherThread.join()
    else:
        app.run(port=5001, threaded=False) # Configure for single threaded request handling
