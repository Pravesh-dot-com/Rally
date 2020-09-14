# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 05:10:18 2020
@author: pjoshi
"""

import datetime as dt
import sys
import cx_Oracle as ora
import time
import logging
import pandas as pd
# import static variables#
from Rally.helper.Rally_config import db_con_str, run_stat, env, stag, target, info, warn, err, \
    flag_status, PROJECTS
# import dynamic variables #
from Rally.helper.Rally_config import dimension_group, log_file_path_dim_grp, process_id_dim_grp, dim_grp_str, \
    sql_trunc_dim_grp, fields_dim_grp, fields_clob_dim_grp, sql_insert_dim_grp, db_pkg_name_dim_grp

# --import all the functions required for this program--#
from Rally.helper.Rally_utils import rally, prepare_api_query, paginate_all_rows_batch

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
entity_type = dim_grp_str

today = dt.datetime.now()
dt_log = today.strftime('%d_%b_%Y_%I_%M_%p')
log_file = log_file_path_dim_grp + str(dt_log) + '.log'
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
l_run = c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_DTL', (today, process_id_dim_grp, env, run_stat, 0))
l_run_id = l_run[4]

# logging starts here#
stat = 'Rally Extract - {}'.format(entity_type)
logging.info(stat)
logging.info('T_PROCESS_RUN.PROCESS_RUN_ID - {}'.format(l_run_id))

c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stat, 'Log File - ' + str(log_file)))
c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag + ' - STARTS', ''))

# get prepared query for API call

try:
    query = prepare_api_query(entity_type)
    stat = 'The API query used in the GET call - {} '.format(query)
    logging.info(stat)
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag, stat))

except Exception:
    stat = "Error while preparing the API query."
    logging.exception(stat)
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag, stat))
    c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
    c_log.close()
    conn.close()
    hdlr.close()
    sys.exit()

# TRUNCATE Staging Table
c_trunc = conn.cursor()
try:

    for sql in sql_trunc_dim_grp:
        c_trunc.execute(sql)
    c_trunc.close()
    stat = "Staging Data Tables Truncated"
    logging.info(stat)
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag, stat))
except Exception:
    stat = "One or more Staging Table Truncate failed"
    logging.exception(stat)
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag, stat))
    c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
    c_log.close()
    c_trunc.close()
    conn.close()
    hdlr.close()
    sys.exit()




for entity_type_dim in dimension_group:
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag + ' - ' + entity_type_dim, ''))
    total_records_fetched = 0
    for project_name in PROJECTS:
        rally.setProject(project_name)

        # only few dimension entities hold unique data for other Projects #
        if project_name != 'Portfolio Backlog' and entity_type_dim not in ['Release', 'Iteration']:
            continue

        else:

            stat = 'Staging process started for entity - {} , project - {}'.format(entity_type_dim, project_name)
            logging.info(stat)
            c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL',
                           (l_run_id, info, stag + ' - Project : ' + project_name, stat))

            start_entity = time.perf_counter()
            # if entity_type_dim == 'User_Iter_Cap':
            #     fetch_fields = fields_dim_grp[entity_type_dim]
            # else:
            #     fetch_fields = True

            response_total = rally.get(entity_type_dim, fetch='ObjectID', query=query, projectScopeDown=True, stream=True, limit=500)

            query_errors = response_total.errors

            ######

            if not query_errors:
                try:
                    query_result = response_total.content['QueryResult']
                    total = query_result['TotalResultCount']
                    total_records_fetched += total

                    if total == 0:
                        continue
                    else:
                        stat = 'API response iterator fetch complete for entity - {} and fetch_query - {}'.format(
                            entity_type_dim, query)
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
                stat = f'API data fetch failed for query - {query} and error - {query_errors[0]}. Check the query and' \
                       f' try calling the API manually.'
                logging.error(stat)
                c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag, stat))
                c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
                c_log.close()
                conn.close()
                hdlr.close()
                sys.exit()

            ######

            batches = total // 500
            if total % 500 > 0:
                batches += 1

            try:
                for batch in range(1, batches + 1):
                    offset = ((batch - 1) * 500) + 1

                    fetch_fields = ','.join(fields_dim_grp[entity_type_dim])
                    response = rally.get(entity_type_dim, fetch=fetch_fields, query=query, projectScopeDown=True, start=offset,
                                         limit=500, stream=True)
                    query_errors = response.errors

                    if not query_errors:
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

                    # Call the driver function
                    flag_status = paginate_all_rows_batch(l_run_id, c_log, batch, sql_insert_dim_grp[entity_type_dim],
                                                          df, entity_type_dim, fields_dim_grp[entity_type_dim], flag_status,
                                                          hdlr, fields_clob_dim_grp[entity_type_dim])
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

        finish = time.perf_counter() - start_entity
        stat = f'Staging Process finished in {round(finish, 2)} second(s)'
        logging.info(stat)
        c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL',
                       (l_run_id, info, stag + ' - ' + entity_type_dim + ' - END', stat))

        # -Target call starts here-#
        start_target = time.perf_counter()
        stat = target + ' - ' + entity_type_dim + ' - STARTS'
        logging.info(stat)
        c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stat, ''))
        c_pkg_call = conn.cursor()
        try:

            c_pkg_call.callproc(db_pkg_name_dim_grp[entity_type_dim],[l_run_id])
            c_pkg_call.close()

            finish = time.perf_counter() - start_target
            stat = f'Target Process finished in {round(finish, 2)} second(s)'
            logging.info(stat)
            c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL',
                           (l_run_id, info, target + ' - ' + entity_type_dim + ' - END', stat))

        except Exception:
            stat = 'DB Package call failed. Check the file log for full traceback'
            logging.exception(stat)
            c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL',
                           (l_run_id, err, target + ' - ' + entity_type_dim, stat))
            c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
            c_log.close()
            c_pkg_call.close()
            conn.close()
            hdlr.close()
            sys.exit()

    else:

        stat = 'Staging Process failed. Check previous logs'
        logging.info(stat)
        c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag, stat))
        finish = time.perf_counter() - start_entity
        stat = f'Staging Process finished in {round(finish, 2)} second(s)'
        logging.info(stat)
        c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL',
                       (l_run_id, err, stag + ' - ' + entity_type_dim + ' - FAILED', stat))
        c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
        c_log.close()
        conn.close()
        hdlr.close()
        sys.exit()

    finish = time.perf_counter() - start_entity
    stat = f'Processing for entity {entity_type_dim} finished in {round(finish, 2)} second(s)'
    logging.info(stat)
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, 'Rally Extract - END', stat))

finish = time.perf_counter() - start_all
stat = f'Complete Process finished in {round(finish, 2)} second(s)'
logging.info(stat)
c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, 'Rally Extract - END', stat))
c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'C'))
c_log.close()
conn.close()
hdlr.close()
