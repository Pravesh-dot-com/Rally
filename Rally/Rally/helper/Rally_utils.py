# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 05:10:18 2020

@author: pjoshi
"""
import os
os.environ['NLS_LANG'] = ".AL32UTF8"  # Allow non-ascii characters LIKE "…"
import datetime as dt
import sys
from pyral import Rally
import cx_Oracle as ora
import logging
import logging.handlers
# import static variables#
from Rally.helper.Rally_config import db_con_str, stag, info, warn, err, \
    server, user, password, workspace, project, dim_grp_str, dim_grp_start_date, dimension_group, PROJECTS, \
    creation_dt_query_grp

from Rally.helper.Rally_config import sql_insert_gap, tag_entity, sql_insert_tag_assign

from bs4 import BeautifulSoup
import unicodedata
import pandas as pd

# --IMPORTANT NOTE--#
# -- In order to run this program for another entity, just update one variable i.e. entity_type--#
# --And you are set up--#
# --YOU'RE WELCOME !!! --#


# today = dt.datetime.now()
conn = ora.connect(db_con_str)

rally = Rally(server, user, password, workspace=workspace, project=project, isolated_workspace=True)

def prepare_api_query(entity_type, sql_fetch_syscon=None, cursor=None):
    """
    :param sql_fetch_syscon: str
        sql to fetch last processed date , by default None
    :param cursor: cx_Oracle cursor object
    :param entity_type: str
    :return: sys_con : last processed date
             query   : prepared query for API call

    """

    if entity_type == dim_grp_str:
        sys_con = dim_grp_start_date  # in case of dimension group, use static date - Jan,1, 2010
        date_string = sys_con + str('T00:00:00.000000Z')
        query = '(CreationDate > "{}")'.format(date_string)
        return query

    else:
        result_raw = cursor.execute(sql_fetch_syscon)
        result = result_raw.fetchone()
        sys_con = result[0]

    date_string = sys_con + str('T00:00:00.000000Z')
    if entity_type in creation_dt_query_grp:
        query = '(CreationDate > "{}")'.format(date_string)

        # This is part of full load. Remove this block when migrating.
        if entity_type in ['Revision']:
            from_date = "2010-01-01T00:00:00.000000Z"
            to_date = "2011-01-01T00:00:00.000000Z"
            query = '((CreationDate >= "%s") and (CreationDate <= "%s"))' % (from_date, to_date)

    else:
        query = '(LastUpdateDate > "{}")'.format(date_string)

    return sys_con, query


def insert_rows(l_run_id, c_log, sql_insert, record_set, cursor):
    """
    :param l_run_id:    process_run_id for DB logging
    :param c_log:       cursor object for DB logging
    :param sql_insert:  sql to insert rows into Staging table
    :param record_set:  Final set of records to be inserted
    :param cursor:      cx_Oracle cursor object to execute the insert sql
    :return:            list of errors, if found; else empty list object
    """

    cursor.executemany(sql_insert, record_set, batcherrors=True)
    conn.commit()

    error_list = []
    for errorObj in cursor.getbatcherrors():
        error_item = [errorObj.offset, errorObj.message]
        # During pagination last record could move to the next batch as well. In this case check the record with warning
        if error_item[1][:28] == 'ORA-00001: unique constraint':
            stat = 'DATA ERROR - Error_msg - {}, ObjectID - {}'.format(errorObj.message,
                                                                       record_set[errorObj.offset][0])

            logging.warning(stat)
            c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, warn, stag, stat))
            continue
        stat = 'DATA ERROR - Error_msg - {}, ObjectID - {}'.format(errorObj.message,
                                                                   record_set[errorObj.offset][0])

        logging.error(stat)
        c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag, stat))
        error_list.append(error_item)

        return error_list


def stage_gap_data(df, batches, l_run_id, c_log, c1, hdlr):
    # only to be used by Portfolio items

    df = df.astype('str')
    df_gap = df['PortfolioItemTypeName'] + ',' + df['ObjectID'] + ',' + df['c_GapID']
    record_set = []
    for raw_value in df_gap:
        value = raw_value.split(',')
        obj_type = value[0].strip()
        obj = value[1].strip()
        gap_id_exists = value[2].strip()

        if gap_id_exists == 'None':
            pass
        else:
            for gap in value[2:]:
                gap_id = gap.strip()
                if gap_id:
                    record = [obj_type, obj, gap_id]
                    record_tup = tuple(record)
                    record_set.append(record_tup)

    if not record_set:
        stat = f'Gap data not found for batch - {batches}'
        logging.warning(stat)
        c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, warn, stag + ' - Gap Assignment', stat))
    else:

        error_list = insert_rows(l_run_id, c_log, sql_insert_gap, record_set, c1)
        if error_list:
            stat = 'Error found while inserting Gap assignment records'
            logging.error(stat)
            logging.error(error_list)
            stat += ' Check file log for full list of errors. error sample - {}'.format(error_list[0])
            c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag + ' - Gap Assignment', stat))
            c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
            c_log.close()
            c1.close()
            hdlr.close()
            sys.exit()

        else:
            stat = f'Gap Assignment load for batch - {batches} complete'
            logging.info(stat)
            c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag + ' - Gap Assignment', stat))
    c1.close()


def stage_tag_assignment_data(df, entity_type, batches, l_run_id, c_log, c1, hdlr):
    # only to be used by Tag entities

    df = df.astype('str')

    if entity_type == 'PortfolioItem':
        df_tag_raw = 'PortfolioItem' + '|' + df['ObjectID'] + '|' + df['Tags._tagsNameArray']
    elif entity_type == 'UserStory':
        df_tag_raw = 'UserStory' + '|' + df['ObjectID'] + '|' + df['Tags._tagsNameArray']
    else:
        df_tag_raw = df['_type'] + '|' + df['ObjectID'] + '|' + df['Tags._tagsNameArray']

    record_set = []
    for raw_value in df_tag_raw:
        value = raw_value.split('|')
        obj_type = value[0].strip()
        obj = value[1].strip()
        tag_id_exists = value[2]

        if tag_id_exists == '[]':
            continue
        else:
            values_lst = eval(value[2])  # convert to list
            for tag in values_lst:
                # The format is something like this - {'Name': 'MC - UK Private Banking', '_ref': '/tag/617055'}
                tag_name = tag['Name']
                tag_id_raw = tag['_ref']
                tag_id_lst = tag_id_raw.split('/')
                tag_id = tag_id_lst[-1].strip()
                row = [obj_type, obj, tag_id, tag_name]
                row_tup = tuple(row)
                record_set.append(row_tup)

    if not record_set:
        stat = f'TAG data not found for batch - {batches}'
        logging.warning(stat)
        c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, warn, stag + ' - Tag Assignment', stat))
    else:
        error_list = insert_rows(l_run_id, c_log, sql_insert_tag_assign, record_set, c1)
        if error_list:
            stat = 'Error found while inserting TAG assignment records'
            logging.error(stat)
            logging.error(error_list)
            stat += ' Check file log for full list of errors. error sample - {}'.format(error_list[0])
            c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag + ' - TAG Assignment', stat))
            c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
            c_log.close()
            c1.close()
            hdlr.close()
            sys.exit()

        else:
            stat = f'TAG Assignment load for batch - {batches} complete'
            logging.info(stat)
            c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag + ' - TAG Assignment', stat))
    c1.close()


def get_active_artifacts(l_run_id, c_log, c1, entity, sql_insert, hdlr):
    """
    Parameters
    ----------
    l_run_id : int
        process_run_id for DB logging
    c_log  : TYPE cursor object for DB logging
    c1      : Cursor object to execute sql statement.
    entity : TYPE str
        DESCRIPTION. Rally Entity Name. In case of Portfolio Items, use the individual entity like Feature, Epic or Theme
    sql_insert : TYPE str
        DESCRIPTION. DB Insert statement
    hdlr : file logging handler
    Returns
    -------
    None.

    """
    records = []
    for project_name in PROJECTS:
        rally.setProject(project_name)

        if entity == 'PortfolioItem':
            for ent in ['Theme', 'Epic', 'Feature']:
                response = rally.get(ent, fetch='ObjectID,_type', projectScopeDown=True)

                for item in response:
                    type_attr = ent  # use entity name as entity type
                    tup = (item.ObjectID, type_attr)
                    records.append(tup)

        else:
            response = rally.get(entity, fetch='ObjectID,_type', projectScopeDown=True)

            for item in response:
                type_attr = item._type
                tup = (item.ObjectID, type_attr)
                records.append(tup)

    total_active = len(records)

    stat = 'Total Active records - {}'.format(total_active)
    logging.info(stat)
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag + ' - Active Artifacts', stat))

    error_list = insert_rows(l_run_id, c_log, sql_insert, records, c1)
    conn.commit()
    if error_list:
        stat = 'Error found while inserting active artifacts'
        logging.error(stat)
        logging.error(error_list)
        stat += ' Check file log for full list of errors. error sample - {}'.format(error_list[0])
        c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag + ' - Active Artifacts', stat))
        c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
        c_log.close()
        hdlr.close()
        sys.exit()

    else:
        stat = 'Active Artifacts load complete'
        logging.info(stat)
        c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag + ' - Active Artifacts', stat))


def parse_text_field(raw_value, field, field_type, field_clob=None):
    """
    Parameters
    ----------
    raw_value : TYPE Multiple
        DESCRIPTION. takes a raw_value to be parsed
    field : TYPE List
        DESCRIPTION.
    field_type : TYPE Dict
        DESCRIPTION.
    field_clob : TYPE, optional
        DESCRIPTION. Its for all the CLOB fields in DB. Required only for TEXT and STRING type fields. The default is None.

    Returns
    -------
    value : TYPE According to the raw_value, changes to appropriate type
        DESCRIPTION. Returns the parsed value

    """

    if raw_value in [None, '', 'None', [], 'NA', 'N/A', '<NA>', 'nan', 'NaN', 'NAT']:
        value = None
    else:
        if field_type in ['TEXT', 'STRING']:
            # raw_value = raw_value.replace('<br />', '\n')
            val = BeautifulSoup(raw_value, "html.parser").get_text('\n')
            value_norm = unicodedata.normalize("NFKD", val)
            if field not in field_clob and len(value_norm) > 2000:  # 2 bytes per character
                value_norm = value_norm[:2000]
                long_field = field  # for future use /Do Not Remove/
            value = value_norm

        elif field_type == 'DATE':
            value = dt.datetime.strptime(raw_value, '%Y-%m-%dT%H:%M:%S.%fZ')
        elif field_type == 'OBJECT':
            try:
                ref = raw_value.split('/')
                value = ref[-1]
            except:
                value = raw_value
        else:
            value = raw_value

    return value


def prepare_db_rows(l_run_id, c_log, df, entity_type, fields, batches, hdlr,
                    field_clob=None):
    """
    Parameters
    ----------
    l_run_id : int
        process_run_id for DB logging
    c_log  : TYPE cursor object for DB logging
    df    : TYPE pandas dataframe
        DESCRIPTION. Dataframe object of API response with at most 500 rows
    entity_type : TYPE string
        DESCRIPTION. Type of Rally entity like Portfolio Item, UserStory, Tasks, Epics, Defect etc.
    fields : TYPE list
        DESCRIPTION. List of required Rally fields
    field_clob : TYPE list
        DESCRIPTION. List of fields which are CLOB type in the DB
    batches : TYPE int
        DESCRIPTION. value of the current batch (Each batch contains max 500 rows)
    hdlr    : File logging handler

    Returns
    -------
    tup : TYPE List of tuples
        DESCRIPTION. Recordset to be loaded into DB

    """
    if entity_type in tag_entity:
        df_tag = df[['_type', 'ObjectID', 'Tags._tagsNameArray']].copy()
        cursor_tag_insert = conn.cursor()
        try:
            stage_tag_assignment_data(df_tag, entity_type, batches, l_run_id, c_log, cursor_tag_insert, hdlr)

        except Exception:
            exc_type, exc_value, exc_tb = sys.exc_info()
            stat = 'TAG Assignment Insertion statement failed for batch - {}. Error Type : {}, error_value : {}, ' \
                   'error_trace : {}'.format(batches, exc_type, exc_value, exc_tb)
            logging.exception(stat)
            c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag + ' - TAG Assignment', stat))
            c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
            c_log.close()
            cursor_tag_insert.close()
            hdlr.close()
            sys.exit()

    # TAG Assignment load function call ends here

    if entity_type in ['PortfolioItem', 'Feature', 'Epic', 'Theme']:
        tp = rally.typedef('Feature')  # This will remain default for all Portfolio Items
    else:
        tp = rally.typedef(entity_type)

    tpa = tp.Attributes
    field_types = {}
    for item in tpa:
        item_name = item.ElementName
        if item_name in fields:
            field_types[item_name] = item.AttributeType

    for f in fields:
        if f not in field_types.keys():
            field_types[f] = None

    # check if datatypes for all fields are available or not
    # if len(fields) != len(field_types.keys()):
    #     stat = 'Rally Field Type ERROR - Datatypes mismacthed for some of the fields.'
    #     logging.error(stat)
    #     c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag, stat))

    field_mod = []
    for col in fields:
        if field_types[col] == 'OBJECT':
            if col == 'Artifact' and entity_type == 'ConversationPost':
                col_type = col + '._type'
                field_mod.append(col_type)
            # _name column will be added only for these attributes
            if col in ['Release', 'Iteration', 'User', 'Owner', 'State', 'PreliminaryEstimate', 'SubmittedBy', 'Tester', 'BlockedBy'] :
                col_name = col + '._refObjectName'
                field_mod.append(col_name)
            col += '._ref'
        field_mod.append(col)

    for col in df.columns:
        if col not in field_mod:
            df = df.drop(labels=col, axis=1)

    for col in field_mod:
        if col not in df.columns:
            idx = field_mod.index(col)
            df.insert(loc=idx, column=col, value=None)

    # if entity_type == 'User_Iter_Cap':
    #     miss = []
    #     for col in field_mod:
    #         if col not in df.columns:
    #             miss.append(col)
    #             idx = field_mod.index(col)
    #             df.insert(loc=idx, column=col, value=None)

    field_df_col = []
    field_type_add = []
    for item in field_mod:
        if '.' in item:
            row_mod = item.split('.')
            if row_mod[1] == '_refObjectName':
                row_new = row_mod[0] + '_Name'
                field_type_add.append(row_new)
            elif row_mod[1] == '_type':
                row_new = row_mod[0] + '_type'
                field_type_add.append(row_new)
            elif row_mod[1] == '_ref':
                row_new = row_mod[0]  #
            item = row_new
        field_df_col.append(item)

    stat = 'Appropriate DataFrame Columns defined according to the DB Table columns'
    logging.info(stat)
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag, stat))

    for item in field_type_add:
        field_types[item] = 'OBJECT'

    df = df.reindex(columns=field_mod)
    df.columns = field_df_col

    for col, v in field_types.items():
        if v in ['DECIMAL', 'INTEGER', 'OBJECT', 'QUANTITY']:
            if v == 'OBJECT':
                df[col] = df[col].apply(lambda x: parse_text_field(x, col, 'OBJECT'))
            df[col] = df[col].where(pd.notnull(df[col]), None)
        elif v == 'DATE':
            df[col] = df[col].apply(lambda x: parse_text_field(x, col, 'DATE'))
            df[col] = df[col].astype(object).where(df[col].notnull(), None)
        elif v == 'TEXT':
            df[col] = df[col].apply(lambda x: parse_text_field(x, col, 'TEXT', field_clob))
        elif v == 'STRING':
            df[col] = df[col].apply(lambda x: parse_text_field(x, col, 'STRING', field_clob))

    # GAP assignment load function call starts here
    if entity_type == 'PortfolioItem':
        df_gap = df[['PortfolioItemTypeName', 'ObjectID', 'c_GapID']].copy()
        cursor_gap_insert = conn.cursor()
        try:
            stage_gap_data(df_gap, batches, l_run_id, c_log, cursor_gap_insert, hdlr)

        except Exception:
            exc_type, exc_value, exc_tb = sys.exc_info()
            stat = 'Gap Assignment Insertion statement failed for batch - {}. Error Type : {}, error_value : {}, ' \
                   'error_trace : {}'.format(batches, exc_type, exc_value, exc_tb)
            logging.exception(stat)
            c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag + ' - Gap Assignment', stat))
            c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
            c_log.close()
            cursor_gap_insert.close()
            hdlr.close()
            sys.exit()

    # GAP assignment load function call ends here

    tup = [tuple(r) for r in df.to_numpy()]

    stat = 'Final Recordset to be inserted into DB Table, created '
    logging.info(stat)
    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag, stat))

    return tup


def paginate_all_rows(l_run_id, c_log, total, batches, offset, sql_insert, response, entity_type, fields,
                      flag_stat,
                      query, hdlr, field_clob=None):
    """
    One function to control all the other functions and apply pagination to the resultset of API fetch

    Parameters
    ----------
    l_run_id : int
        process_run_id for DB logging
    c_log  : TYPE cursor object for DB logging
    total : TYPE int
        DESCRIPTION. Holds count of the total records fetched with the API call
    batches : TYPE int
        DESCRIPTION. value of the current batch (Each batch contains max 500 rows)
    offset : TYPE int
        DESCRIPTION. Value of index position for API GET call
    sql_insert : int
        sql statement to insert the data into DB
    response : TYPE iterator
        DESCRIPTION. Contains iterator object resulted from the API call
    entity_type : TYPE string
        DESCRIPTION.Type of Rally entity like Portfolio Item, UserStory, Tasks, Defects etc.
    fields : TYPE list
        DESCRIPTION. List of required Rally fields
    flag_stat : TYPE string
        DESCRIPTION. Flag to indicate if Target job could be started or not.
    field_clob : TYPE list
        DESCRIPTION. List of fields which are CLOB type in the DB
    query   : str
        API GET call query
    hdlr    : file logging handler

    Returns
    -------
    flag_stat : TYPE string
        DESCRIPTION. If 'Y' then update T_SYS_CONFIG with current date, else don't.


    """

    record_set = prepare_db_rows(l_run_id, c_log, response, entity_type, fields, batches, hdlr, field_clob)

    try:
        c1 = conn.cursor()
        error_list = insert_rows(l_run_id, c_log, sql_insert, record_set, c1)

        if error_list:

            flag_stat = 'N'
            stat = 'DATA ERROR - Error found with batch - {}'.format(batches)
            logging.error(stat)
            c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag, stat))
            c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
            c_log.close()
            c1.close()
            conn.close()
            hdlr.close()
            sys.exit()
        else:
            stat = 'records inserted without any errors for batch - {}'.format(batches)
            logging.info(stat)
            c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag, stat))

    except Exception:
        exc_type, exc_value, exc_tb = sys.exc_info()
        stat = 'Insertion statement failed for batch - {}. Error Type : {}, error_value : {}, error_trace : {}'.format(
            batches, exc_type, exc_value, exc_tb)
        logging.exception(stat)
        c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, warn, stag, stat))
        stat = 'Trying the rows one by one to isolate the error for batch - {}. Check Log file for full traceback'.format(
            batches)
        logging.warning(stat)
        c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, warn, stag, stat))

        # flag_stat = 'N'

        for row in record_set:
            try:
                c2 = conn.cursor()
                error_list = insert_rows(l_run_id, c_log, sql_insert, record_set, c2)

                if error_list:
                    flag_stat = 'N'
                    stat = 'DATA ERROR - Error found with batch - {}'.format(batches)
                    logging.error(stat)
                    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag, stat))

            except Exception:
                flag_stat = 'N'
                stat = 'Error found. Check log file for erroneous record'
                logging.exception(stat)
                c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag, stat))
                stat = 'Error found. Error record - {}'.format(row)
                logging.exception(stat)
                c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
                c_log.close()
                c2.close()
                c1.close()
                conn.close()
                hdlr.close()
                sys.exit()

        c2.close()

    c1.close()

    if total - 500 > 0:
        batches += 1
        offset = ((batches - 1) * 500) + 1
        total -= 500
        if entity_type == 'User':
            fetch_fields = fields
        else:
            fetch_fields = True

        response_iterative = rally.get(entity_type, fetch=fetch_fields, query=query, projectScopeDown=True,
                                       start=offset, limit=500)
        flag_stat = paginate_all_rows(l_run_id, c_log, total, batches, offset, sql_insert, response_iterative,
                                      entity_type, fields, flag_stat, query, hdlr, field_clob)

    return flag_stat


def paginate_all_rows_batch(l_run_id, c_log, batches, sql_insert, df, entity_type, fields,
                            flag_stat, hdlr, field_clob=None):
    """
    One function to control all the other functions and apply pagination to the result-set of API fetch

    Parameters
    ----------
    l_run_id : int
        process_run_id for DB logging
    c_log  : TYPE cursor object for DB logging
    batches : TYPE int
        DESCRIPTION. value of the current batch (Each batch contains max 500 rows)
    df    : TYPE pandas dataframe
        DESCRIPTION. Dataframe object of API response with at most 500 rows
    sql_insert : str
        sql statement to insert the data into DB
    entity_type : TYPE string
        DESCRIPTION.Type of Rally entity like Portfolio Item, UserStory, Tasks, Defects etc.
    fields : TYPE list
        DESCRIPTION. List of required Rally fields
    flag_stat : TYPE string
        DESCRIPTION. Flag to indicate if Target job could be started or not.
    field_clob : TYPE list
        DESCRIPTION. List of fields which are CLOB type in the DB
    hdlr    : file logging handler

    Returns
    -------
    flag_stat : TYPE string
        DESCRIPTION. If 'Y' then update T_SYS_CONFIG with current date, else don't.

    """

    record_set = prepare_db_rows(l_run_id, c_log, df, entity_type, fields, batches, hdlr, field_clob)

    try:
        c1 = conn.cursor()
        error_list = insert_rows(l_run_id, c_log, sql_insert, record_set, c1)

        if error_list:
            flag_stat = 'N'
            stat = 'DATA ERROR - Error found with batch - {}'.format(batches)
            logging.error(stat)
            c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag, stat))
            c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
            c_log.close()
            c1.close()
            conn.close()
            hdlr.close()
            sys.exit()
        else:
            stat = 'records inserted without any errors for batch - {}'.format(batches)
            logging.info(stat)
            c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, info, stag, stat))

    except Exception:
        exc_type, exc_value, exc_tb = sys.exc_info()
        stat = 'Insertion statement failed for batch - {}. Error Type : {}, error_value : {}, error_trace : {}'.format(
            batches, exc_type, exc_value, exc_tb)
        logging.exception(stat)
        c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, warn, stag, stat))
        stat = 'Trying the rows one by one to isolate the error for batch - {}. Check Log file for full traceback'.format(
            batches)
        logging.warning(stat)
        c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, warn, stag, stat))

        # flag_stat = 'N'

        for row in record_set:
            try:
                c2 = conn.cursor()
                error_list = insert_rows(l_run_id, c_log, sql_insert, record_set, c2)

                if error_list:
                    flag_stat = 'N'
                    stat = 'DATA ERROR - Error found with batch - {}'.format(batches)
                    logging.error(stat)
                    c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag, stat))

            except Exception:
                flag_stat = 'N'
                stat = 'Error found. Check log file for erroneous record'
                logging.exception(stat)
                c_log.callproc('BMR_LOG.INSERT_PROCESS_RUN_LOG_DTL', (l_run_id, err, stag, stat))
                stat = 'Error found. Error record - {}'.format(row)
                logging.exception(stat)
                c_log.callproc('BMR_LOG.UPDATE_PROCESS_RUN_DTL', (l_run_id, 'F'))
                c_log.close()
                c2.close()
                c1.close()
                conn.close()
                hdlr.close()
                sys.exit()

        c2.close()

    c1.close()

    return flag_stat
