
import json
import time
import zipfile
import logging
from copy import deepcopy
from datetime import datetime, timedelta

from cdscompute.errors import NoDataException

from cds_common.cams.regional_fc_api import regional_fc_api
from cds_common.url2.downloader import Downloader
from cds_common.system import cds_forms_dir
from cds_common import hcube_tools, tree_tools, date_tools

from .preprocess_requests import preprocess_requests
from .nc_request_groups import nc_request_groups
from .grib2request import grib2request_init
from .which_fields_in_file import which_fields_in_file
from .process_grib_files import process_grib_files
from .convert_grib import convert_grib
from .create_file import create_file
from .formats import Formats
from .cacher import Cacher
from .assert_valid_grib import assert_valid_grib

from .api_retrieve import api_retrieve

# Used to temporarily disable access to archived data in an emergency, e.g.
# when too many archived requests are blocking access to latest data
ARCHIVED_OFF = False

def do_second_mapping(mapping, requests):
    for request in requests:
        for widget in request:
            if widget in mapping:
                request[widget] = [mapping[widget][value] for value in request[widget]]
    return requests

class MockResultFile():
    def __init__(self, path):
        self.path = path

def new_cams_regional_fc(context, config, requests, forms_dir=None):
    context.add_stdout("----------> Entering new_cams_regional_fc...")
    
    # Get an object which will give us information/functionality associated
    # with the Meteo France regional forecast API
    regapi = regional_fc_api(
        integration_server=config.get('integration_server', False),
        logger=context)
    
    context.add_stdout(f"----------> integration_server: {config.get('integration_server', False)}")
    
    second_mapping_that_I_do_not_understand = {
        "model": {
            "chimere": "CHIMERE",
            "dehm": "DEHM",
            "emep": "EMEP",
            "ensemble_median": "ENS",
            "eurad_im": "EURADIM",
            "gem_aq": "GEMAQ",
            "lotos_euros": "LOTOS",
            "match": "MATCH",
            "minni": "MINNI",
            "mocage": "MOCAGE",
            "monarch": "MONARCH",
            "silam": "SILAM"
        },
        "time": {
            "00:00": "0000",
            "01:00": "0100",
            "02:00": "0200",
            "03:00": "0300",
            "04:00": "0400",
            "05:00": "0500",
            "06:00": "0600",
            "07:00": "0700",
            "08:00": "0800",
            "09:00": "0900",
            "10:00": "1000",
            "11:00": "1100",
            "12:00": "1200",
            "13:00": "1300",
            "14:00": "1400",
            "15:00": "1500",
            "16:00": "1600",
            "17:00": "1700",
            "18:00": "1800",
            "19:00": "1900",
            "20:00": "2000",
            "21:00": "2100",
            "22:00": "2200",
            "23:00": "2300"
        },
        "variable": {
            "alder_pollen": "C_POL_ALDER",
            "ammonia": "NH3_USI",
            "birch_pollen": "C_POL_BIRCH",
            "carbon_monoxide": "CO_USI",
            "dust": "DUST_USI",
            "formaldehyde": "HCHO_USI",
            "glyoxal": "CHOCHO_USI",
            "grass_pollen": "C_POL_GRASS",
            "mugwort_pollen": "C_POL_MUGW",
            "nitrogen_dioxide": "NO2_USI",
            "nitrogen_monoxide": "NO_USI",
            "non_methane_vocs": "NMVOC_USI",
            "olive_pollen": "C_POL_OLIVE",
            "ozone": "O3_USI",
            "particulate_matter_10um": "PM10_USI",
            "particulate_matter_2.5um": "PM25_USI",
            "peroxyacyl_nitrates": "PANS_USI",
            "pm10_sea_salt_dry": "DYNSAL_USI",
            "pm10_wildfires": "PM_WF_USI",
            "pm2.5_anthropogenic_fossil_fuel_carbon": "EC_FF_USI",
            "pm2.5_anthropogenic_wood_burning_carbon": "EC_WB_USI",
            "pm2.5_total_organic_matter": "PM25_OM_USI",
            "ragweed_pollen": "C_POL_RAGW",
            "residential_elementary_carbon": "EC_RES_USI",
            "secondary_inorganic_aerosol": "SIA_USI",
            "sulphur_dioxide": "SO2_USI",
            "total_elementary_carbon": "EC_TOT_USI"
        }
    }
    
    requests = do_second_mapping(second_mapping_that_I_do_not_understand, requests)
    
    context.request = {"mapping": {
            "remap": {
                "model": {
                    "ensemble_median": "ensemble",
                    "eurad_im": "euradim",
                    "gem_aq": "gemaq",
                    "lotos_euros": "lotos"
                },
                "time": {
                    "00_00": "00:00",
                    "01_00": "01:00",
                    "02_00": "02:00",
                    "03_00": "03:00",
                    "04_00": "04:00",
                    "05_00": "05:00",
                    "06_00": "06:00",
                    "07_00": "07:00",
                    "08_00": "08:00",
                    "09_00": "09:00",
                    "10_00": "10:00",
                    "11_00": "11:00",
                    "12_00": "12:00",
                    "13_00": "13:00",
                    "14_00": "14:00",
                    "15_00": "15:00",
                    "16_00": "16:00",
                    "17_00": "17:00",
                    "18_00": "18:00",
                    "19_00": "19:00",
                    "20_00": "20:00",
                    "21_00": "21:00",
                    "22_00": "22:00",
                    "23_00": "23:00"
                },
                "variable": {
                    "particulate_matter_10_\u00b5m": "particulate_matter_10um",
                    "particulate_matter_2_5_\u00b5m": "particulate_matter_2.5um",
                    "pm10_sea_salt_only": "pm10_sea_salt_dry",
                    "pm10_wildfires_only": "pm10_wildfires",
                    "pm2_5_anthropogenic_fossil_fuel_carbon_only": "pm2.5_anthropogenic_fossil_fuel_carbon",
                    "pm2_5_anthropogenic_wood_burning_carbon_only": "pm2.5_anthropogenic_wood_burning_carbon",
                    "pm2_5_total_organic_matter_only": "pm2.5_total_organic_matter"
                }
            },
            "rename": {
                "leadtime_hour": "step"
            }
        }
    }
    
    def create_result_file(self, extension):
        request_uid = config["request_uid"]
        result_path = f'/cache/debug/{request_uid}.{extension}'
        self.add_stdout("----------> MOCK RESULT FILE HERE")
        return MockResultFile(result_path)
    
    context.create_result_file = create_result_file.__get__(context)
    
    context.create_result_file(".alabala")
    
    # Pre-process requests
    requests, info = preprocess_requests(context, requests, regapi)
    for i in range(len(requests)):
        leadtime_hour = requests[i]["leadtime_hour"]
        requests[i].pop('leadtime_hour', None)
        requests[i]["step"] = leadtime_hour
        #requests[i]["format"] = 'grib'
    context.add_stdout(f"----------> REQUESTS: {requests}")
    
    # If converting to NetCDF then different groups of grib files may need to be
    # converted separately. Split and group the requests into groups that can be
    # converted together.
    if 'convert' in info['stages']:
        grps = nc_request_groups(context, requests, info)
        req_groups = [{'group_id': k, 'requests': v} for k, v in grps.items()]
    else:
        req_groups = [{'group_id': None, 'requests': requests}]
        
    # Output a zip file if creating >1 NetCDF file. netcdf_cdm format files
    # are always zipped.
    if len(req_groups) > 1 or info['format'] == Formats.netcdf_cdm:
        info['stages'].append('zip')
        
    dataset_dir = "/src/cads-adaptors/cads_adaptors/adaptors/cams_regional_fc/config"

    # Initialisation for function that can understand GRIB file contents
    grib2request_init(dataset_dir)
    
    # Get locally stored fields
    get_local(req_groups, context)
    
    # Divide non-local fields betwen latest and archived
    set_backend(req_groups, regapi, dataset_dir, context)
    
    # Retrieve non-local latest (fast-access) fields
    get_latest(req_groups, regapi, dataset_dir, context)
    
    # Retrieve non-local archived (slow-access) fields
    get_archived(req_groups, regapi, dataset_dir, context)
    
    # Remove groups that had no matching data
    req_groups = [x for x in req_groups if 'retrieved_files' in x]
    if not req_groups:
        raise NoDataException('No data found for this request', '')
    
    context.add_stdout(f"------------------------------> req_groups number is {len(req_groups)}")
    
    return req_groups[0]['retrieved_files'][0]


def cams_regional_fc(context, requests, forms_dir=None):
    """Main work routine for handling a request for CAMS regional forecast
       data"""

    if forms_dir is None:
        forms_dir = cds_forms_dir()

    # Get the content of the adaptor.yaml
    fullconfig = {} if context.fullconfig is None else context.fullconfig

    # Get an object which will give us information/functionality associated
    # with the Meteo France regional forecast API
    regapi = regional_fc_api(
        integration_server=fullconfig.get('integration_server', False),
        logger=context)

    # Pre-process requests
    requests, info = preprocess_requests(context, requests, regapi)

    # If converting to NetCDF then different groups of grib files may need to be
    # converted separately. Split and group the requests into groups that can be
    # converted together.
    if 'convert' in info['stages']:
        grps = nc_request_groups(context, requests, info)
        req_groups = [{'group_id': k, 'requests': v} for k, v in grps.items()]
    else:
        req_groups = [{'group_id': None, 'requests': requests}]

    # Output a zip file if creating >1 NetCDF file. netcdf_cdm format files
    # are always zipped.
    if len(req_groups) > 1 or info['format'] == Formats.netcdf_cdm:
        info['stages'].append('zip')

    dataset_dir = forms_dir + '/' + context.request['metadata']['resource']

    # Initialisation for function that can understand GRIB file contents
    grib2request_init(dataset_dir)

    # Get locally stored fields
    get_local(req_groups, context)

    # Divide non-local fields betwen latest and archived
    set_backend(req_groups, regapi, dataset_dir, context)

    # Retrieve non-local latest (fast-access) fields
    get_latest(req_groups, regapi, dataset_dir, context)

    # Retrieve non-local archived (slow-access) fields
    get_archived(req_groups, regapi, dataset_dir, context)

    # Remove groups that had no matching data
    req_groups = [x for x in req_groups if 'retrieved_files' in x]
    if not req_groups:
        raise NoDataException('No data found for this request', '')

    # Process and merge grib files
    process_grib_files(req_groups, info, context)

    # Convert to netCDF?
    if 'convert' in info['stages']:
        convert_grib(req_groups, info, dataset_dir, context)

    # Zip output files?
    if 'zip' in info['stages']:
        zip_files(req_groups, info, context)

    try:
        return info['result_file']
    except KeyError:
        raise Exception('Bug: result_file not set') from None


def set_backend(req_groups, regapi, dataset_dir, context):
    """Divide requests between "latest" and "archived" and set their "_backend"
       attribute accordingly"""
    for req_group in req_groups:
        online, offline = \
            split_latest_from_archived(req_group['uncached_requests'],
                                       regapi, dataset_dir, context)
        for r in online:
            r['_backend'] = ['latest']
        for r in offline:
            r['_backend'] = ['archived']
        req_group['uncached_latest_requests'] = online
        req_group['uncached_archived_requests'] = offline


def split_latest_from_archived(requests, regapi, dataset_dir, context):
    """Split requests into "latest" and "archived" groups"""

    if requests:

        # Get the catalogue that lists all fields that are currently in the
        # fast "synopsis" part of the backend
        try:
            online_cat = regapi.get_catalogue('latest', retry={'timeout': 10})
        except Exception as e:
            # If there is a problem at Meteo France and the catalogue can't be
            # accessed, try using the one stored in the dataset directory
            # instead. It's probably up-to-date and better than failing the
            # request.
            context.error(f'Failed to download latest catalogue: {e!r}. '
                          f'Reading from {dataset_dir} instead')
            with open(f'{dataset_dir}/catalogue_latest.json') as f:
                online_cat = json.load(f)

        # Split latest from archived fields
        lcat = tree_tools.to_list(online_cat)
        latest, archived, _ = hcube_tools.hcubes_intdiff2(requests, lcat)

        # Holes in the latest catalogue will have been assigned to the archived
        # list. This is not a problem if they result in 404s but the archive
        # backend will reject any requests for dates less than N days old with a
        # 400 HTTP error, so remove any if they exist
        archived, invalid = archive_maxdate_split(archived, regapi)
        if invalid:
            context.info('Not attempting to retrieve ' +
                         str(hcube_tools.count_fields(invalid)) + ' fields '
                         'which are not in latest catatalogue but also too new '
                         'to be in archived')

        context.debug('Latest fields: ' + repr(latest))
        context.debug('Archived fields: ' + repr(archived))

    else:
        latest = []
        archived = []

    return (latest, archived)


def archive_maxdate_split(reqs, regapi):
    """Return a copy of requests with fields that are too recent to be in the
       archive backend removed. Requesting these fields would result in a HTTP
       400 (invalid request) error"""

    valid = []
    invalid = []

    if reqs:
        # The maximum date that the archived backend will allow in a request
        # without raising a HTTP 400 (bad request) error
        date_max = (datetime.utcnow() -
                    timedelta(days=regapi.archive_min_delay)).date()
        fmt = date_tools.guess_date_format(reqs[0]['date'][0])
        for r in reqs:
            rdates = [d.date() for d in
                      date_tools.expand_dates_list(r['date'], as_datetime=True)]
            rv = deepcopy(r)
            ri = deepcopy(r)
            rv['date'] = [d.strftime(fmt) for d in rdates if d <= date_max]
            ri['date'] = [d.strftime(fmt) for d in rdates if d > date_max]
            if rv['date']:
                valid.append(rv)
            if ri['date']:
                invalid.append(ri)

    assert hcube_tools.count_fields(reqs) == \
        hcube_tools.count_fields(valid) + \
        hcube_tools.count_fields(invalid)

    return (valid, invalid)


def get_local(req_groups, context):
    """Retrieve only the fields which are stored locally (in the cache or on
       the datastore) and identify the remaining non-local fields."""

    # Cacher has knowledge of cache locations
    with Cacher(context) as cacher:
        for req_group in req_groups:
            _get_local(req_group, cacher, context)


def _get_local(req_group, cacher, context):
    """Retrieve only the fields which are already stored locally (in the cache
       or on the datastore) and identify non-local fields."""

    # Date lists must not be compressed
    reqs = [r.copy() for r in req_group['requests']]
    for r in reqs:
        r['date'] = date_tools.expand_dates_list(r['date'])

    # Download local fields
    urls = ({'url': cacher.cache_file_url(field),
             'req': field}
            for field in hcube_tools.unfactorise(reqs))
    # downloader = Downloader(context,
    #                         max_rate=50,
    #                         max_simultaneous=15,
    #                         combine_method='cat',
    #                         target_suffix='.grib',
    #                         response_checker=assert_valid_grib,
    #                         response_checker_threadsafe=False,
    #                         combine_in_order=False,
    #                         write_to_temp=True,
    #                         request_timeout=[60, 300],
    #                         max_attempts={404: 1, 'default': 3},
    #                         nonfatal_codes=[404, 'exception'],
    #                         retry_wait=5,
    #                         allow_no_data=True,
    #                         min_log_level=logging.INFO)
    # grib_file = downloader.execute(urls)
    grib_file = None

    # Identify uncached fields - the ones not present in the file
    cached, uncached = which_fields_in_file(reqs, grib_file, context)
    req_group['uncached_requests'] = uncached
    context.info('Retrieved ' + str(hcube_tools.count_fields(cached)) +
                 ' local fields')
    context.info('Number of remaining uncached fields = ' +
                 str(hcube_tools.count_fields(uncached)))

    if grib_file is not None:
        req_group['retrieved_files'] = req_group.get('retrieved_files', []) + \
                                       [grib_file]


def get_latest(req_groups, regapi, dataset_dir, context):
    """Retrieve uncached latest fields"""

    for req_group in req_groups:
        if not req_group['uncached_latest_requests']:
            continue

        # Uncached fields are retrieved in sub-requests which are limited to
        # one-at-a-time by QOS, thus one long request could hog the resource
        # for a long period. Prevent this by retrieving in chunks.
        for reqs in hcube_tools.hcubes_chunk(
                req_group['uncached_latest_requests'], 5000):

            grib_file = new_retrieve_subrequest(reqs, req_group, regapi,
                                            dataset_dir, context)

            # Fields may have expired from the latest backend by the time the
            # request was made. Reassign any missing fields to the archive
            # backend.
            reassign_missing_to_archive(reqs, grib_file, req_group, regapi,
                                        context)


def get_archived(req_groups, regapi, dataset_dir, context):
    """Retrieve uncached slow-access archived fields"""

    for req_group in req_groups:
        if not req_group['uncached_archived_requests']:
            continue

        if ARCHIVED_OFF:
            raise Exception('Access to archived data is temporarily ' +
                            'suspended. Only the latest few days are available')

        # Archived fields are retrieved in sub-requests which are limited to
        # one-at-a-time by QOS, thus one long request could hog the resource
        # for a long period. Prevent this by retrieving in 900 field chunks
        # which, at 1 field/second, limits each sub-request to 15 minutes.
        for reqs in hcube_tools.hcubes_chunk(
                req_group['uncached_archived_requests'], 900):

            new_retrieve_subrequest(reqs, req_group, regapi, dataset_dir, context)


def retrieve_latest(*args):
    """Adaptor only intended to be called as a sub-request from the main
       adaptor to retrieve uncached latest fields only. The separate entry
       point from retrieve_archived allows a different QOS to be applied."""
    return retrieve_xxx(*args)


def retrieve_archived(*args):
    """Adaptor only intended to be called as a sub-request from the main
       adaptor to retrieve uncached archived fields only. The separate entry
       point from retrieve_latest allows a different QOS to be applied."""

    if ARCHIVED_OFF:
        raise Exception('Access to archived data is temporarily ' +
                        'suspended. Only the latest few days are available')

    try:
        return retrieve_xxx(*args)

    except Exception:
        maintenance_end_time = datetime(2021, 3, 3, 18, 0)
        if datetime.utcnow() < maintenance_end_time: 
            #raise Exception(
            #    'Apologies: your request requires data from a remote server '
            #    'which is currently undergoing maintenance. Normal service is '
            #    'expected to be resumed by ' +
            #    maintenance_end_time.strftime('%A %d %B %H:%M UTC')) from None
            raise Exception(
                'Apologies: your request requires data from a remote server '
                'which is undergoing a maintenance session that is taking '
                'longer than expected. Please try again later.')
        else:
            raise


def retrieve_xxx(context, requests, dataset_dir, integration_server):
    def create_result_file(self, extension):
        result_path = f'/cache/debug/alabala_retrieve_xxx.{extension}'
        self.add_stdout("----------> MOCK RESULT FILE HERE (in retrieve_xxx)")
        return MockResultFile(result_path)

    context.create_result_file = create_result_file.__get__(context)

    # Get an object which will give us information/functionality associated
    # with the Meteo France regional forecast API
    regapi = regional_fc_api(integration_server=integration_server,
                             logger=context)

    file = api_retrieve(context, requests, regapi, dataset_dir)

    context.info('Sub-request completed')

    return file


def new_retrieve_subrequest(requests, req_group, regapi, dataset_dir, context):
    from cdsapi import Client
    """Retrieve chunk of uncached fields in a sub-request"""

    backend = requests[0]['_backend'][0]
    assert backend in ['latest', 'archived']

    # Retrieve uncached fields in sub-requests to allow a different QOS to be
    # applied to avoid overloading the Meteo France API.
    context.info('Executing sub-request to retrieve uncached fields: ' +
                 repr(requests))
    t0 = time.time()
    # result = context.call('adaptor.cams_regional_fc.retrieve_' + backend,
    #                       requests, dataset_dir, regapi.integration_server)
    client = Client()
    if backend == "latest":
        #result = retrieve_latest(context, requests, dataset_dir, regapi.integration_server)
        dataset = 'cams-europe-air-quality-forecasts-latest'
    else:
        #result = retrieve_archived(context, requests, dataset_dir, regapi.integration_server)
        dataset = 'cams-europe-air-quality-forecasts-archived'
    target = f'/cache/debug/alabala.portocala'
    client.retrieve(dataset,{'requests': requests, 'dataset_dir': dataset_dir, 'integration_server': regapi.integration_server} ,target)
    result = MockResultFile(target)
    context.info('... sub-request succeeded after ' +
                 str(time.time() - t0) + 's')

    if result is not None:
        # Download result to a local file
        context.info(f"----------------------------------------> {result}")
        grib_file = result #context.get_data(result)
        req_group['retrieved_files'] = req_group.get('retrieved_files', []) + \
                                       [grib_file]
    else:
        context.info('... but found no data')
        grib_file = None

    return grib_file


def retrieve_subrequest(requests, req_group, regapi, dataset_dir, context):
    """Retrieve chunk of uncached fields in a sub-request"""

    backend = requests[0]['_backend'][0]
    assert backend in ['latest', 'archived']

    # Retrieve uncached fields in sub-requests to allow a different QOS to be
    # applied to avoid overloading the Meteo France API.
    context.info('Executing sub-request to retrieve uncached fields: ' +
                 repr(requests))
    t0 = time.time()
    result = context.call('adaptor.cams_regional_fc.retrieve_' + backend,
                          requests, dataset_dir, regapi.integration_server)
    context.info('... sub-request succeeded after ' +
                 str(time.time() - t0) + 's')

    if result is not None:
        # Download result to a local file
        grib_file = context.get_data(result)
        req_group['retrieved_files'] = req_group.get('retrieved_files', []) + \
                                       [grib_file]
    else:
        context.info('... but found no data')
        grib_file = None

    return grib_file


def reassign_missing_to_archive(reqs, grib_file, req_group, regapi, context):
    """Re-assign fields which are in reqs but not in grib_file to the archived
       backend"""

    # Which are in the file and which aren't?
    context.add_stdout(f"----------> {grib_file}")
    context.add_stdout(f"----------> {grib_file.path}")
    present, missing = which_fields_in_file(reqs, grib_file.path, context)

    # The archive backend will reject any requests for dates less than N
    # days old with a 400 HTTP error, so remove any if they exist. There
    # shouldn't be any though.
    missing_valid, missing_invalid = archive_maxdate_split(missing,
                                                           regapi)
    if missing_invalid:
        context.error('Fields missing from latest backend but cannot '
                      'be on archived backend: ' + repr(missing_invalid))
    if missing_valid:
        context.info('Resorting to archived backend for missing fields: ' +
                     repr(missing_valid))

    for r in missing_valid:
        r['_backend'] = ['archived']
    req_group['uncached_archived_requests'].extend(missing_valid)


def zip_files(req_groups, info, context):

    assert info['format'] != Formats.grib

    path = create_file('zip', '.zip', info, context)
    with zipfile.ZipFile(path, 'w') as zf:
        for req_group in req_groups:
            zf.write(req_group['nc_file'],
                     arcname='_'.join(req_group['group_id']) + '.nc')
