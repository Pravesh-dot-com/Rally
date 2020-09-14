# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 05:10:18 2020
@author: pjoshi
"""

import os
os.environ['NLS_LANG'] = ".AL32UTF8"  # Allow non-ascii characters LIKE "…"
import datetime as dt
import sys
import cx_Oracle as ora
import time
import logging
import pandas as pd

import Rally
# import static variables#
from Rally.helper.Rally_config import db_con_str, run_stat, env, stag, target, info, warn, err, \
    flag_status, sql_delete_tag_assignment, tag_entity, PROJECTS
# import dynamic variables #
from Rally.helper.Rally_config import sql_fetch_syscon_us, sql_trunc_us, sql_trunc_aa_us, \
    sql_active_artifacts_us, sql_insert_us, fields_us, fields_clob_us, db_pkg_name_us, log_file_path_us, process_id_us
# --import all the functions required for this program--#
from Rally.helper.Rally_utils import get_active_artifacts, rally, prepare_api_query, paginate_all_rows_batch

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
entity_type = 'UserStory'

# Set file logging #
today = dt.datetime.now()
dt_log = today.strftime('%d_%b_%Y_%I_%M_%p')
log_file = log_file_path_us + str(dt_log) + '.log'
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
l_run = c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_DTL', (today, process_id_us, env, run_stat, 0))
l_run_id = l_run[4]

# logging starts here#
stat = 'Rally Extract - {}'.format(entity_type)
logging.info(stat)
logging.info('T_PROCESS_RUN.PROCESS_RUN_ID - {}'.format(l_run_id))

c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stat, 'Log File - ' + str(log_file)))
c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag + ' - STARTS', ''))

# get prepared query for API call
cur_sys_config = conn.cursor()
try:
    sys_con, query = prepare_api_query(entity_type, sql_fetch_syscon_us, cur_sys_config)
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

    c_trunc.execute(sql_trunc_us)
    c_trunc.execute(sql_trunc_aa_us)
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
    get_active_artifacts(l_run_id, c_log, c1, entity_type, sql_active_artifacts_us, hdlr)
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

# Run the process for all the projects
total_records_fetched = 0
for project_name in PROJECTS:
    rally.setProject(project_name)

    stat = 'Staging process started for project - {}'.format(project_name)
    logging.info(stat)
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag + ' - Project : ' + project_name, stat))

    response_total = rally.get(entity_type, fetch='ObjectID', query=query, projectScopeDown=True, limit=500,
                               stream=True)

    query_errors = response_total.errors

    if not query_errors:  # and total > 0:
        try:
            query_result = response_total.content['QueryResult']
            total = query_result['TotalResultCount']
            total_records_fetched += total

            if total == 0:
                continue
            else:
                stat = 'API response iterator fetch complete for entity - {} and fetch_query - {}'.format(entity_type,
                                                                                                          query)
                logging.info(stat)
                c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag, stat))
                stat = 'Total records to be fetched - {}'.format(total)
                logging.info(stat)
                c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag, stat))

        except Exception:
            stat = 'Error while fetching total query results'
            logging.exception(stat)
            c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag, stat))
            c_log.close()
            conn.close()
            hdlr.close()
            sys.exit()
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

    batches = total // 500
    if total % 500 > 0:
        batches += 1

    try:
        for batch in range(1, batches + 1):
            offset = ((batch - 1) * 500) + 1

            response = rally.get(entity_type, fetch=True, query=query, projectScopeDown=True, start=offset,
                                 limit=500, stream=True)
            query_errors = response.errors

            if not query_errors:  # and total > 0:
                try:
                    result = response.content['QueryResult']['Results']
                    df = pd.json_normalize(result)

                    stat = 'DataFrame creation complete for batch - {}'.format(batch)
                    logging.info(stat)
                    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag, stat))

                except Exception:
                    stat = 'DataFrame creation failed for batch - {}'.format(batch)
                    logging.info(stat)
                    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag, stat))
                    c_log.close()
                    conn.close()
                    hdlr.close()
                    sys.exit()
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

            flag_status = paginate_all_rows_batch(l_run_id, c_log, batch, sql_insert_us, df, entity_type,
                                                  fields_us, flag_status, hdlr, fields_clob_us)
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

if total_records_fetched == 0:
    stat = f'No matches found for given query - {query} , in the API call. ' \
           f'Check the API query in function - prepare_api_query. Try calling the API manually. '
    logging.warning(stat)
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, warn, stag, stat))
    c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'C'))
    c_log.close()
    conn.close()
    hdlr.close()
    sys.exit()

if flag_status == 'Y':

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
        c_proc_call.callproc(db_pkg_name_us,[l_run_id])
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