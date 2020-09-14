# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 05:10:18 2020
@author: pjoshi
"""

import os

import dask
from dask import delayed

os.environ['NLS_LANG'] = ".AL32UTF8"  # Allow non-ascii characters LIKE "…"
import datetime as dt
import sys
import cx_Oracle as ora
import time
import logging

# import static variables#
from Rally.helper.Rally_config import db_con_str, run_stat, env, stag, target, info, warn, err, \
    flag_status, \
    sql_delete_tag_assignment, tag_entity
# import dynamic variables #
from Rally.helper.Rally_config import sql_fetch_syscon_pi, sql_trunc_pi, sql_trunc_aa_pi, \
    sql_active_artifacts_pi, sql_insert_pi, fields_pi, fields_clob_pi, db_pkg_name_pi, log_file_path_pi, process_id_pi, \
    sql_trunc_gap_assignment
# --import all the functions required for this program--#
from Rally.helper.Rally_utils import get_active_artifacts, rally, prepare_api_query, \
    not_dask_paginate_all_rows, insert_rows, dask_insert_rows

# --IMPORTANT NOTE--#
# -- In order to run this program for another entity, just update one variable i.e. entity_type--#
# -- And import the variables as required --#
# --And you are set up --#
# --YOU'RE WELCOME !!! --#


##############################################################################
########################----Main Function Call ---############################
##############################################################################


start_all = time.perf_counter()

# --Change this variable if you wish to run the program for a different entity--#
entity_type = 'PortfolioItem'

"""
options = [arg for arg in sys.argv[1:] if arg.startswith('--')]
args    = [arg for arg in sys.argv[1:] if arg not in options]
server, user, password, apikey, workspace, project = rallyWorkset(options)
rally = Rally(server, user, password, apikey=apikey, workspace=workspace, project=project)
rally.enableLogging('mypyral.log')
"""

today = dt.datetime.now()
dt_log = today.strftime('%d_%b_%Y_%I_%M_%p')
log_file = log_file_path_pi + str(dt_log) + '.log'
logger = logging.getLogger()
hdlr = logging.FileHandler(log_file)
formatter = logging.Formatter('%(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

try:
    conn = ora.connect(db_con_str)
except Exception:
    stat = 'DataBase connection failed'
    logging.exception(stat)
    hdlr.close()
    sys.exit()

c_log = conn.cursor()
# calling stored proc to make entry in log table
l_run = c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_DTL', (today, process_id_pi, env, run_stat, 0))
l_run_id = l_run[4]

# logging starts here#
stat = 'Rally Extract - Starts'
logging.info(stat)
logging.info('T_PROCESS_RUN.PROCESS_RUN_ID - {}'.format(l_run_id))

c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stat, 'Log File - ' + str(log_file)))
c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag + ' - STARTS', ''))

# get prepared query for API call
cur_sys_config = conn.cursor()
try:
    sys_con, query = prepare_api_query(sql_fetch_syscon_pi, cur_sys_config, entity_type)
    cur_sys_config.close()
    stat = "Sys_config value to be considered  : {}".format(sys_con)
    logging.info(stat)
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag, stat))
except Exception:
    stat = "Error while fetching sys_config value or preparing the API query."
    logging.exception(stat)
    cur_sys_config.close()
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag, stat))
    c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
    c_log.close()
    conn.close()
    hdlr.close()
    sys.exit()

# TRUNCATE Staging Table
c_trunc = conn.cursor()
try:
    c_trunc.execute(sql_trunc_pi)
    c_trunc.execute(sql_trunc_aa_pi)
    if entity_type == 'PortfolioItem':
        c_trunc.execute(sql_trunc_gap_assignment)
    if entity_type in tag_entity:
        c_trunc.execute(sql_delete_tag_assignment, type=entity_type)
    conn.commit()
    c_trunc.close()
    stat = "Staging Data Table and active artifact Table Truncated"
    logging.info(stat)
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag, stat))
except:
    stat = "Staging Table Truncate failed"
    logging.exception(stat)
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag, stat))
    c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
    c_trunc.close()
    c_log.close()
    conn.close()
    hdlr.close()
    sys.exit()

# call active artifacts function--

c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag + ' - Active Artifacts START', ''))
start = time.perf_counter()
c1 = conn.cursor()
try:
    get_active_artifacts(l_run_id, c_log, c1, entity_type, sql_active_artifacts_pi, hdlr)
    c1.close()
    finish = time.perf_counter() - start
    stat = f'Active Artifacts load finished in {round(finish, 2)} second(s)'
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag + ' - Active Artifacts END', stat))
except:
    stat = "Active artifacts fetch failed. Check previous errors/log file"
    logging.exception(stat)
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag + ' - Active Artifacts', stat))
    c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
    c_log.close()
    c1.close()
    conn.close()
    hdlr.close()
    sys.exit()

response = rally.get(entity_type, fetch=True, query=query, projectScopeDown=True, limit=500)

query_errors = response.errors
total = response.content['QueryResult']['TotalResultCount']

stat = 'Total records to be fetched - {}'.format(total)
logging.info(stat)
c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag, stat))

if not query_errors and total > 0:
    stat = 'API response iterator fetch complete for entity - {} and fetch_query - {}'.format(entity_type, query)
    logging.info(stat)
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag, stat))
else:
    if total == 0:
        stat = f'No matches found for the given query - {query} , in the API call. Check the API query in function - ' \
               'prepare_api_query. Try calling the API manually. '
        logging.warning(stat)
        c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, warn, stag, stat))
        # c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'C'))
    else:
        stat = f'API data fetch failed for query - {query} and error - {query_errors[0]}. Check the query and try ' \
               f'calling the API manually. '
        logging.error(stat)
        c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag, stat))
    c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
    c_log.close()
    conn.close()
    hdlr.close()
    sys.exit()

# c_nls = conn.cursor()
# c_nls.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'DD-MM-YYYY HH:MI:SS AM'")
# c_nls.close()

########################################################################################
#################################----Dask logic starts here----#########################
########################################################################################

batches = total // 500
if total % 500 > 0:
    batches += 1

###
dask_result_object = []
###

try:
    for batch in range(1, batches + 1):
        offset = ((batch - 1) * 500) + 1
        result_set = delayed(not_dask_paginate_all_rows)(l_run_id, batch, offset,
                                                         entity_type,
                                                         fields_pi, query, fields_clob_pi)
        ###
        dask_result_object.append(result_set)
        ###

        # if flag_status == 'N':
        #    break

    ###
    dask_result_raw = dask.compute(dask_result_object)
    dask_result = dask_result_raw[0]
    ###
    stat = 'Function call paginate_all_rows completed.'
    logging.info(stat)
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag, stat))
except:
    stat = 'Function call paginate_all_rows failed. Check the traceback for more info'
    logging.exception(stat)
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag, stat))
    c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
    c_log.close()
    conn.close()
    hdlr.close()
    sys.exit()

dask_result_set = []
for row in dask_result:
    dask_result_set += row


if flag_status == 'Y':

    try:
        error_list = dask_insert_rows(l_run_id, sql_insert_pi, dask_result_set)
        if error_list:
            stat = 'Errors found'
            logging.error(stat)
            c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag, stat))
            c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
            c_log.close()
            hdlr.close()
            sys.exit()
    except:
        exc_type, exc_value, exc_tb = sys.exc_info()
        stat = 'Insertion statement failed for batch - {}. Error Type : {}, error_value : {}, error_trace : {}'.format(
            batches, exc_type, exc_value, exc_tb)
        logging.exception(stat)
        c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, warn, stag, stat))
        c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
        c_log.close()
        hdlr.close()
        sys.exit()
    finish = time.perf_counter() - start_all
    stat = f'Staging Process finished in {round(finish, 2)} second(s)'
    logging.info(stat)
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag + ' - END', stat))

    # -Target call starts here-#
    start = time.perf_counter()
    stat = target + ' - STARTS'
    logging.info(stat)
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stat, ''))
    try:
        c_proc_call = conn.cursor()
        c_proc_call.callproc(db_pkg_name_pi)
        c_proc_call.close()

        finish = time.perf_counter() - start
        stat = f'Target Process finished in {round(finish, 2)} second(s)'
        logging.info(stat)
        c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, target + ' - END', stat))

    except:
        stat = 'DB Package call failed.'
        logging.exception(stat)
        c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL',
                       (l_run_id, err, target, stat + 'Check the file log - {} for full traceback'.format(log_file)))
        c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
        c_log.close()
        conn.close()
        hdlr.close()
        sys.exit()

else:

    stat = 'Staging Process failed. Check previous logs'
    logging.info(stat)
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag, stat))
    finish = time.perf_counter() - start_all
    stat = f'Staging Process finished in {round(finish, 2)} second(s)'
    logging.info(stat)
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag + ' - FAILED', stat))
    c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
    c_log.close()
    conn.close()
    hdlr.close()
    sys.exit()

finish = time.perf_counter() - start_all
stat = f'Complete Process finished in {round(finish, 2)} second(s)'
logging.info(stat)
c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, 'Rally Extract - END', stat))
c_log.close()
conn.close()
hdlr.close()
