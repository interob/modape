#!/usr/bin/env python
"""
  africa_1km_export.py: Export a (new) roi from start date to last available, e.g.: python africa_1km_export.py CHAD
                        **BEWARE**: make sure the Flask app is not accessing the production storage at the same time! Suspend it by calling:
                                    >> wget --content-on-error -O- http://127.0.0.1:5001/suspend
                        Production the time series leverages the WFP VAM MODAPE toolkit: https://github.com/WFP-VAM/modape

  Dependencies: arc-modape (0.4), Numpy, ...
  
  Author: Rob Marjot
  
"""

import os
import glob
import json
import argparse
try:
    from types import SimpleNamespace as Namespace
except ImportError:
    from argparse import Namespace
import h5py
import numpy as np
from modape.utils import fromjulian
from modape.scripts.modis_window import modis_window
from modape.timeslicing import Dekad, ModisInterleavedOctad


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


def do_export(region):
    this_dir, _ = os.path.split(__file__)
    settings = None
    with open(os.path.join(this_dir, 'africa_1km.json')) as f:
        settings = json.load(f, object_hook=lambda d: Namespace(**d))

    roi = dict(**vars(settings.export))[region]
    print('Exporting {}: {}'.format(region, " ".join(str(a) for a in roi)))

    # export dekads, from back to front (n = 6):
    nexports = 1
    exportOctad = ModisInterleavedOctad(lastDateInRawH5ModisTiles(os.path.join(settings.basedir, 'VIM')))
    exportDekad = Dekad(exportOctad.getDateTimeEnd(), True)
    print('')
    print('Octad-end for last ingested date: {}'.format(str(exportOctad.getDateTimeEnd())))
    print(' > Corresponding dekad: {}'.format(str(exportDekad)))

    while Dekad(exportOctad.prev().getDateTimeEnd(), True).Equals(exportDekad) and nexports < 6:
        nexports = nexports + 1
        exportOctad = exportOctad.prev()

    first_date = firstDateInRawH5ModisTiles(os.path.join(settings.basedir, 'VIM'))
    while (not exportDekad.startsBeforeDate(first_date)):
        print('>>Export: {} [Update: {}]'.format(str(exportDekad), str(min(6, nexports))))

        modis_window(**{'path': os.path.join(settings.basedir, 'VIM', 'SMOOTH'), 'roi': roi,
                        'targetdir': os.path.join(settings.basedir, 'VIM', 'SMOOTH', 'EXPORT'),
                        'region': region,
                        'begin_date': exportDekad.getDateTimeMid().strftime('%Y-%m-%d'),
                        'end_date': exportDekad.getDateTimeMid().strftime('%Y-%m-%d'),
                        'cb_transform': lambda array: transform(array),
                        'cb_slicename': lambda region, dte: slicename(region, dte),
                        'overwrite': True,
                        'md_list': ['UPDATE_NUMBER={}'.format(str(min(6, nexports))),
                                    'FINAL={}'.format('FALSE' if nexports < 6 else 'TRUE')]
                        })

        nexports = nexports + 1
        exportOctad = exportOctad.prev()
        exportDekad = Dekad(exportOctad.getDateTimeEnd(), True)
        while Dekad(exportOctad.prev().getDateTimeEnd(), True).Equals(exportDekad) and nexports < 6:
            nexports = nexports + 1
            exportOctad = exportOctad.prev()

  
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Exporter for existing archive')
    parser.add_argument('region', help='Defined region in export entry in africa_1km.json')
    args = parser.parse_args()
    do_export(args.region)