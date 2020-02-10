#This script integrates modis_download and modis_collect to incrementally download and ingest the NDVI archive,
# year by year.
#
# Usage: python3 app/kenya_1km_init.py -b 2002-07-03 -e 2019-12-31

import os, sys; sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import json
try:
    from types import SimpleNamespace as Namespace
except ImportError:
    from argparse import Namespace
import argparse
from modape.scripts.modis_download import modis_download, curate_downloads
from modape.scripts.modis_collect import modis_collect
from modape.scripts.modis_smooth import modis_smooth
from modape.scripts.modis_window import modis_window
from datetime import datetime
from dateutil.relativedelta import relativedelta
from modape.utils import fromjulian
from modape.timeslicing import Dekad
import h5py
import numpy as np
from pathlib import Path


def dateFromRawH5ModisTile(f, idx):
    with h5py.File(f) as h5f:
        dates = h5f.get('dates')
        return fromjulian(dates[idx].decode())


def firstDateInRawH5ModisTile(f):
    return dateFromRawH5ModisTile(f, 0)


def lastDateInRawH5ModisTile(f):
    return dateFromRawH5ModisTile(f, -1)


def firstDateInRawH5ModisTiles(folder):
    files = Path(folder).glob('*.h5')
    first_dates = []
    for f in files:
        first_dates.append(firstDateInRawH5ModisTile(f))
    if len(first_dates) > 0:
        return max(first_dates)
    else:
        return None


def lastDateInRawH5ModisTiles(folder):
    files = Path(folder).glob('*.h5')
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MODIS NDVI bootstrapper')
    parser.add_argument('-b', '--begin-date', help='Start date (YYYY-MM-DD) for initialization',
                        default='2002-07-03', metavar='')
    parser.add_argument('-e', '--end-date', help='End date (YYYY-MM-DD) for initialization',
                        default='2019-12-31', metavar='')
    parser.add_argument('--download-only', help='Only download data', action='store_true')
    parser.add_argument('--export-only', help='Only export data', action='store_true')
    args = parser.parse_args()

    this_dir, _ = os.path.split(__file__)
    with open(os.path.join(this_dir, 'kenya_1km.json')) as f:
        config = json.load(f, object_hook=lambda d: Namespace(**d))

    if not args.export_only:
        end_date = None
        # download and ingest:
        begin_date = lastDateInRawH5ModisTiles(os.path.join(config.basedir, 'VIM'))
        if begin_date is None:
            begin_date = datetime.strptime(args.begin_date, '%Y-%m-%d').date()
        else:
            begin_date = begin_date + relativedelta(days=8)
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        if not args.download_only:
            end_date = min([end_date, begin_date + relativedelta(years=1) - relativedelta(days=1)])

        while begin_date < end_date:
            download_params = {}
            download_params['targetdir'] = config.basedir
            download_params['username'] = config.username
            download_params['password'] = config.password
            download_params['begin_date'] = begin_date.strftime("%Y-%m-%d")
            download_params['end_date'] = end_date.strftime("%Y-%m-%d")
            download_params['tile_filter'] = config.tile_filter
            download_params['download'] = True
            download_params['product'] = ['M?D13A2']

            print('Downloading: {} - {}...'.format(begin_date, end_date))

            # DOWNLOAD:
            urls = modis_download(**download_params)
            if len(urls) == 0:
                break
            # Check download: for all distinct dates: is there a download for EACH selected tile?
            # See if all tiles are on disk:
            for url in urls:
                fname = url[url.rfind('/') + 1:]
                if not os.path.exists(os.path.join(config.basedir, fname)):
                    raise SystemExit('Download missing on disk: {}'.format(fname))

            if not args.download_only:
                # SEE IF DOWNLOAD (HDFs) IS COMPLETE?
                if not curate_downloads(config.basedir, config.tile_filter, begin_date, end_date):
                    exit(1)
                # COLLECT:
                modis_collect(**{'srcdir': config.basedir, 'interleave': True, 'cleanup_ingested': True})
                # move on:
                begin_date = lastDateInRawH5ModisTiles(os.path.join(config.basedir, 'VIM')) + relativedelta(days=8)
                end_date = min([datetime.strptime(args.end_date, '%Y-%m-%d').date(),
                                begin_date + relativedelta(years=1) - relativedelta(days=1)])

        if args.download_only:
            if not curate_downloads(config.basedir, config.tile_filter, begin_date, end_date):
                exit(1)
            exit(0)

        # smooth downloaded archive: setting the 'init_only' to True, this can be done only once per product tile:
        modis_smooth(**{'input': os.path.join(config.basedir, 'VIM'), 'init_only': True,
                        'targetdir': os.path.join(config.basedir, 'VIM', 'SMOOTH'),
                        'tempint': 10, 'constrain': True})

    # export dekads:
    first_date = firstDateInRawH5ModisTiles(os.path.join(config.basedir, 'VIM'))
    last_date = lastDateInRawH5ModisTiles(os.path.join(config.basedir, 'VIM'))
    last_date = last_date + relativedelta(days=8)
    last_date = datetime(last_date.year, last_date.month, last_date.day)

    if first_date < datetime.strptime(args.begin_date, '%Y-%m-%d').date():
        first_date = datetime.strptime(args.begin_date, '%Y-%m-%d').date()

    exportSlice = Dekad(first_date)
    if exportSlice.startsBeforeDate(first_date):
        exportSlice = exportSlice.next()

    toSlice = exportSlice
    cnt = 1
    while True:
        if cnt == 9 or toSlice.next().getDateTimeMid() > last_date:
            print('\nExporting {} to {} ...'.format(str(exportSlice), str(toSlice)))
            modis_window(**{'path': os.path.join(config.basedir, 'VIM', 'SMOOTH'), 'roi': [33.0, -5.0, 42.0, 5.0],
                            'targetdir': os.path.join(config.basedir, 'VIM', 'SMOOTH', 'EXPORT'), 'region': 'WFPVAM_NDVI',
                            'begin_date': exportSlice.getDateTimeMid().strftime('%Y-%m-%d'),
                            'end_date': toSlice.getDateTimeMid().strftime('%Y-%m-%d'),
                            'cb_transform': lambda array: transform(array), 'cb_slicename': lambda dte: slicename(dte),
                            'md5': True, 'md_list': [ 'FINAL=TRUE' ]})
        if toSlice.next().getDateTimeMid() > last_date:
            # every date represents (a) the *mid* of the one composite and the *start* of the other
            break

        if cnt == 9:
            exportSlice = toSlice.next()
            toSlice = exportSlice
            cnt = 1
        else:
            toSlice = toSlice.next()
            cnt = cnt + 1

