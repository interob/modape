"""
MODIS raw HDF5 class.

This file contains the class representing a raw MODIS HDF5 file.

Author: Valentin Pesendorfer, April 2019
"""
#pylint: disable=E0401
import gc
import logging
from pathlib import Path
import re
import time
import traceback
from typing import List
import warnings

import numpy as np
try:
    import gdal
except ImportError:
    from osgeo import gdal
import h5py

from modape.constants import REGEX_PATTERNS, VAM_PRODUCT_CODES, TEMPORAL_DICT, LST_NAME_LUD
from modape.modis.io import HDF5Base
from modape.utils import dtype_GDNP, FileHandler, fromjulian

log = logging.getLogger(__name__)


class ModisRawH5(HDF5Base):
    """Class for raw MODIS data collected into HDF5 file, ready for smoothing.

    MOD/MYD 13 products can be interleaved into a combined MXD.
    """

    def __init__(self,
                 files: List[str],
                 targetdir: str,
                 vam_product_code: str = None,
                 interleave: bool = False) -> None:
        """Create a ModisRawH5 class

        Args:
            files: A list of absolute paths to MODIS raw hdf files to be processed
            vam_product_code: VAM product code to be processed (default VIM/LTD)
            targetdir: Target directory for raw MODIS HDF5 file
            interleave: Boolean flag if MOD/MYD 13  products should be interleaved
        """

        self.targetdir = Path(targetdir)

        # check if all files provided are either VIM or TDA
        for file in files:
            if not re.match(REGEX_PATTERNS['VIMLST'], file.split('/')[-1]):
                log.error("File %s not NDVI or LST", file)
                raise ValueError("MODIS collect processing only implemented for M{O|Y}D {11,13} products!")

        self.rawdates = [re.findall(REGEX_PATTERNS["date"], x)[0] for x in files]
        self.files = [x for (y, x) in sorted(zip(self.rawdates, files))]
        self.rawdates.sort()

        # extract product patterns
        products = []
        for file_tmp in self.files:
            products.append(
                re.findall(REGEX_PATTERNS["product"], file_tmp.split('/')[-1])[0]
            )

        # Make sure it's the same product
        assert len({x[3:] for x in products}) == 1, "Found different products in input files!"

        # remove duplicates
        if len(set(self.rawdates)) != len(self.files):
            duplicate_warning_msg = f"Possibly duplicate files in {self.targetdir}! Using files with most recent processing date."
            log.warning("Found duplicate files in %s", str(self.targetdir))
            warnings.warn(duplicate_warning_msg, Warning)

            # make sure number of dates is equal to number of files, so no duplicates!
            processing_timestamps = [int(re.sub(REGEX_PATTERNS["processing_timestamp"], '\\1', x)) for x in self.files]

            dups = []
            dt_prev = None
            for dt in self.rawdates:
                if dt == dt_prev and dt not in dups:
                    dups.append(dt)
                dt_prev = dt

            # Loop over duplicates and exclude files based on processing timestamp
            to_pop = []
            for dup in dups:
                # max TS for duplicate
                max_ts = max([processing_timestamps[ix] for ix, x in enumerate(self.rawdates) if x == dup])

                for ix, x in enumerate(self.rawdates):
                    if x == dup and processing_timestamps[ix] != max_ts:
                        to_pop.append(ix)

            # re-set file-based instance variables after removal of duplicates
            for ix in sorted(to_pop, reverse=True):
                del self.files[ix]
                del self.rawdates[ix]
                del processing_timestamps[ix]

            # assert that there are no duplicates now
            assert len(set(self.rawdates)) == len(self.files)

        if vam_product_code is None:

            refname = self.files[0].split('/')[-1]

            if re.match(REGEX_PATTERNS["VIM"], refname):
                self.vam_product_code = "VIM"

            elif re.match(REGEX_PATTERNS["LST"], refname):
                self.vam_product_code = "LTD"
            else:
                pass
        else:
            assert vam_product_code in VAM_PRODUCT_CODES, "Supplied product code not supported."
            self.vam_product_code = vam_product_code

        satset = {x[:3] for x in products}

        if interleave:

            assert self.vam_product_code == "VIM", "Interleaving only possible for M{O|Y}D13 (VIM) products!"

            # assert we have both satellites
            assert len(satset) == 2, "Interleaving needs MOD & MYD products!"
            self.satellite = "MXD"
            self.product = f"MXD{products[0][3:]}"

            # for interleaving, exclude all dates before 1st aqua date
            ix = 0
            min_date = fromjulian("2002185")
            for x in self.rawdates:
                start = ix
                if fromjulian(x) >= min_date:
                    break
                ix += 1

            # update instance variables
            self.files = self.files[start:]
            self.rawdates = self.rawdates[start:]

            # TODO: enforce sequential dates?

        else:
            # if no interleave, only one satellite allowed
            assert len(satset) == 1, "Without interleave, only one satellite allowed!"
            self.satellite = list(satset)[0]
            self.product = products[0]

        tempinfo = TEMPORAL_DICT[self.product[:5]]

        self.temporalresolution = tempinfo["temporalresolution"]
        self.tshift = tempinfo["tshift"]
        self.nfiles = len(self.files)
        self.reference_file = Path(self.files[0])

        ## Build filename

        # LST has specific VAM product codes for the file naming
        if self.vam_product_code in ["LTD", "LTN"]:
            fn_code = LST_NAME_LUD[self.vam_product_code][self.satellite]
            test_vampc = REGEX_PATTERNS["LST"]
        else:
            fn_code = self.vam_product_code
            test_vampc = REGEX_PATTERNS["VIM"]

        assert test_vampc.match(self.product), f"Incomaptible VAM code {self.vam_product_code} for product {self.product}"

        refname = self.reference_file.name

        tile = REGEX_PATTERNS["tile"].findall(refname)
        version = REGEX_PATTERNS["version"].findall(refname)
        tile_version = '.'.join(tile + version)

        filename = f"{self.targetdir}/{fn_code}/{self.product}.{tile_version}.{fn_code}.h5"

        super().__init__(filename=filename)

    def create(self, compression='gzip', chunk=None):
        """Creates the HDF5 file.

        Args:
            compression: Compression method to be used (default = gzip)
            chunk: Number of pixels per chunk (needs to define equal sized chunks!)
        """

        ref = gdal.Open(self.reference_file.as_posix())
        ref_sds = [x[0] for x in ref.GetSubDatasets() if self.vam_product_code_dict[self.vam_product_code] in x[0]][0]

        # reference raster
        rst = gdal.Open(ref_sds)
        ref_sds = None
        ref = None
        nrows = rst.RasterYSize
        ncols = rst.RasterXSize

        # check chunksize
        if not chunk:
            self.chunks = ((nrows*ncols)//25, 10) # default
        else:
            self.chunks = (chunk, 10)

        # Check if chunksize is OK
        if not ((nrows*ncols)/self.chunks[0]).is_integer():
            rst = None # close ref file
            raise ValueError('\n\nChunksize must result in equal number of chunks. Please adjust chunksize!')

        self.nodata_value = int(rst.GetMetadataItem('_FillValue'))

        # Read datatype
        dt = rst.GetRasterBand(1).DataType

        # Parse datatype - on error use default Int16
        try:
            self.datatype = dtype_GDNP(dt)
        except IndexError:
            print('\n\n Couldn\'t read data type from dataset. Using default Int16!\n')
            self.datatype = (3, 'int16')

        trans = rst.GetGeoTransform()
        prj = rst.GetProjection()
        rst = None

        # Create directory if necessary
        self.outname.parent.mkdir(parents=True, exist_ok=True)

        # Create HDF5 file
        try:
            with h5py.File(self.outname.as_posix(), 'x', libver='latest') as h5f:
                dset = h5f.create_dataset('data',
                                          shape=(nrows*ncols, self.nfiles),
                                          dtype=self.datatype[1],
                                          maxshape=(nrows*ncols, None),
                                          chunks=self.chunks,
                                          compression=compression,
                                          fillvalue=self.nodata_value)

                h5f.create_dataset('dates',
                                   shape=(self.nfiles,),
                                   maxshape=(None,),
                                   dtype='S8',
                                   compression=compression)

                dset.attrs['geotransform'] = trans
                dset.attrs['projection'] = prj
                dset.attrs['resolution'] = trans[1] # res ## commented for original resolution
                dset.attrs['nodata'] = self.nodata_value
                dset.attrs['temporalresolution'] = self.temporalresolution
                dset.attrs['tshift'] = self.tshift
                dset.attrs['RasterXSize'] = ncols
                dset.attrs['RasterYSize'] = nrows
            self.exists = True
        except:
            print('\n\nError creating {}! Check input parameters (especially if compression/filter is available) and try again. Corrupt file will be removed now. \n\nError message: \n'.format(self.outname.as_posix()))
            self.outname.unlink()
            raise

    def update(self):
        """Update MODIS raw HDF5 file with raw data.

        When a new HDF5 file is created, update will also handle the first data ingest.
        """

        try:
            with h5py.File(self.outname.as_posix(), 'r+', libver='latest') as h5f:
                dset = h5f.get('data')
                dates = h5f.get('dates')
                self.chunks = dset.chunks
                self.nodata_value = dset.attrs['nodata'].item()
                self.ncols = dset.attrs['RasterXSize'].item()
                self.nrows = dset.attrs['RasterYSize'].item()
                self.datatype = dtype_GDNP(dset.dtype.name)
                dset.attrs['processingtimestamp'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

                # Load any existing dates and combine with new dates
                dates_combined = [x.decode() for x in dates[...] if len(x) > 0 and x.decode() not in self.rawdates]
                _ = [dates_combined.append(x) for x in self.rawdates]

                # New total temporal length
                dates_length = len(dates_combined)

                # if new total temporal length is bigger than dataset, datasets need to be resized for additional data
                if dates_length > dset.shape[1]:
                    dates.resize((dates_length,))
                    dset.resize((dset.shape[0], dates_length))

                # Sorting index to ensure temporal continuity
                sort_ix = np.argsort(dates_combined)

                # Manual garbage collect to prevent out of memory
                _ = [gc.collect() for x in range(3)]

                # preallocate array
                arr = np.zeros((self.chunks[0], dates_length), dtype=self.datatype[1])

                # Open all files and keep reference in handler
                handler = FileHandler(self.files, self.vam_product_code_dict[self.vam_product_code])
                handler.open()
                ysize = self.chunks[0]//self.ncols

                for b in range(0, dset.shape[0], self.chunks[0]):
                    yoff = b//self.ncols

                    for b1 in range(0, dates_length, self.chunks[1]):
                        arr[..., b1:b1+self.chunks[1]] = dset[b:b+self.chunks[0], b1:b1+self.chunks[1]]
                    del b1

                    for fix, f in enumerate(self.files):
                        try:
                            arr[..., dates_combined.index(self.rawdates[fix])] = handler.handles[fix].ReadAsArray(xoff=0,
                                                                                                                  yoff=yoff,
                                                                                                                  xsize=self.ncols,
                                                                                                                  ysize=ysize).flatten()
                        except AttributeError:
                            print('Error reading from {}. Using nodata ({}) value.'.format(f, self.nodata_value))
                            arr[..., dates_combined.index(self.rawdates[fix])] = self.nodata_value
                    arr = arr[..., sort_ix]

                    for b1 in range(0, dates_length, self.chunks[1]):
                        dset[b:b+self.chunks[0], b1:b1+self.chunks[1]] = arr[..., b1:b1+self.chunks[1]]
                handler.close()

                # Write back date list
                dates_combined.sort()
                dates[...] = np.array(dates_combined, dtype='S8')
        except:
            print('Error updating {}! File may be corrupt, consider creating the file from scratch, or closer investigation. \n\nError message: \n'.format(self.outname.as_posix()))
            traceback.print_exc()
            raise

    def __str__(self):
        """String to be displayed when printing an instance of the class object"""
        return 'ModisRawH5 object: {} - {} files - exists on disk: {}'.format(self.outname.as_posix(), self.nfiles, self.exists)
