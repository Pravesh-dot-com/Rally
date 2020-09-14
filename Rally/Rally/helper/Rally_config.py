# -*- coding: utf-8 -*-
"""
Created on Tue May 12 09:58:05 2020

@author: pjoshi
"""
# --DO NOT USE Dynamaic Variables DIRECTLY IN THE MAIN PROGRAM--##
# --I REPEAT JUST DON'T--#
# -Use the Final key-value pair instead which you'll find at the end-#
# --Thank Me Later--#

import Rally

# constant Variables
# Use directly in the main entity programs#

env = 'RALLY'
run_stat = 'S'

server = 'hostname.domain.com'
user = 'pjoshi'
password = 'password'
workspace = 'Default worksapce'
project = 'Sample Project 1'

PROJECTS = ['Sample Project 1', 'Sample Project 2']

info = 'INFO'
warn = 'WARNING'
err = 'ERROR'
flag_status = 'Y'
stag = 'STAGING AREA'
target = 'TARGET AREA'

db = {'usr': 'usr123', 'pwd': 'pwd123', 'host': 'host123', 'port': '1521', 'svc': 'service'}
db_usr, db_pwd, db_host, db_port, db_service = db['usr'], db['pwd'], db['host'], db['port'], db['svc']
db_con_str = str(db_usr) + '/' + str(db_pwd) + '@' + str(db_host) + ':' + str(db_port) + '/' + str(db_service)

tab_sys = 't_sys_config'
sql_fetch_syscon = "select code_value from " + str(tab_sys) + " where code_name = '{}'"

log_file_path = 'D:/Pravesh/work/logs/RALLY_DATA_PULL/Rally_'
# log_file_path = 'D:/Sched_jobs/Logs/Rally_Extract/Rally_'

sql_active_artifacts = """
insert into {}(objectid, type) values
( :1, :2)
"""

sql_trunc = 'truncate table {}'

# CreationDate as query group
creation_dt_query_grp = ['ConversationPost', 'UserIterationCapacity', 'Revision', 'TestCaseResult', 'TestFolder',
                         'Blocker', 'TestCaseStep']

# Dimension Group specific static variables
dimension_group = ['Release', 'Project', 'Iteration', 'Tag', 'User', 'State', 'PreliminaryEstimate']
dim_grp_str = 'Dim_Group'
dim_grp_start_date = '2010-01-01'

# Gap Assignment related variables#
stg_tab_gap = 'stg_rally_ext_gap_assignment'
sql_trunc_gap_assignment = sql_trunc.format(stg_tab_gap)
sql_insert_gap = "insert into {0}(artifact_type, artifact_id, gap_id) values (:1,:2,:3)".format(str(stg_tab_gap))

# Tag Assignment related variables#
tag_entity = ['PortfolioItem', 'Defect', 'DefectSuite', 'UserStory', 'Task', 'TestSet', 'TestCase']
stg_tab_tag_assign = 'stg_rally_ext_tag_assignment'
sql_insert_tag_assign = "insert into {0}(artifact_type, artifact_id, tag_id, tag_name) values (:1,:2,:3,:4)".format(
    str(stg_tab_tag_assign))
sql_delete_tag_assignment = "delete from {0} where artifact_type = :type ".format(str(stg_tab_tag_assign))

################################ -Dynamic Variables-##################################

process_id_pi = 231
process_id_defect = 232
process_id_us = 233
process_id_task = 234
process_id_dim_grp = 235
process_id_test_set = 239
process_id_test_case = 240
process_id_defect_suite = 244

# CreationDate as query - group
process_id_disc = 236
process_id_user_iter_cap = 237
process_id_rev_hist = 238
process_id_test_case_result = 241
process_id_test_folder = 242
process_id_blocker = 243
process_id_test_case_step = 245

log_file_path_pi = log_file_path + 'portfolio_item_'
log_file_path_defect = log_file_path + 'defect_'
log_file_path_us = log_file_path + 'us_'
log_file_path_task = log_file_path + 'task_'
log_file_path_dim_grp = log_file_path + 'dim_group_'
log_file_path_test_set = log_file_path + 'test_set_'
log_file_path_test_case = log_file_path + 'test_case_'
log_file_path_defect_suite = log_file_path + 'defect_suite_'

# CreationDate as query - group
log_file_path_disc = log_file_path + 'disc_'
log_file_path_user_iter_cap = log_file_path + 'user_iter_cap_'
log_file_path_rev_hist = log_file_path + 'rev_hist_'
log_file_path_test_case_result = log_file_path + 'test_case_result_'
log_file_path_test_folder = log_file_path + 'test_folder_'
log_file_path_blocker = log_file_path + 'blocker_'
log_file_path_test_case_step = log_file_path + 'test_case_step_'

# Staging Tables
stg_tab_pi = 'stg_rally_ext_pi'
stg_tab_active_pi = 'stg_rally_ext_active_pi'

stg_tab_defect = 'stg_rally_ext_defect'
stg_tab_active_defect = 'stg_rally_ext_active_defect'

stg_tab_defect_suite = 'stg_rally_ext_defect_suite'
stg_tab_active_defect_suite = 'stg_rally_ext_active_defect_st'

stg_tab_us = 'stg_rally_ext_us'
stg_tab_active_us = 'stg_rally_ext_active_us'

stg_tab_task = 'stg_rally_ext_task'
stg_tab_active_task = 'stg_rally_ext_active_task'

stg_tab_test_set = 'stg_rally_ext_test_set'
stg_tab_active_test_set = 'stg_rally_ext_active_test_set'

stg_tab_test_case = 'stg_rally_ext_test_case'
stg_tab_active_test_case = 'stg_rally_ext_active_test_case'

# CreationDate as query - group
stg_tab_disc = 'stg_rally_ext_discussion'
stg_tab_user_iter_cap = 'stg_rally_ext_user_iter_cap'
stg_tab_rev_hist = 'stg_rally_ext_rev_hist'
stg_tab_test_case_result = 'stg_rally_ext_test_case_result'
stg_tab_test_case_step = 'stg_rally_ext_test_case_step'
stg_tab_test_folder = 'stg_rally_ext_test_folder'
stg_tab_blocker = 'stg_rally_ext_blocker'

# dim_group staging tables
stg_tab_release = 'stg_rally_ext_release'
stg_tab_project = 'stg_rally_ext_project'
stg_tab_iteration = 'stg_rally_ext_iteration'
stg_tab_user = 'stg_rally_ext_user'
stg_tab_tag = 'stg_rally_ext_tag'
stg_tab_kanban_state = 'stg_rally_ext_kanban_state'
stg_tab_prelim_estmt = 'stg_rally_ext_prelim_estmt'

code_name_pi = 'RALLY_EXTRACT_PI'
code_name_defect = 'RALLY_EXTRACT_DEFECT'
code_name_us = 'RALLY_EXTRACT_US'
code_name_task = 'RALLY_EXTRACT_TASK'
code_name_test_set = 'RALLY_EXTRACT_TEST_SET'
code_name_test_case = 'RALLY_EXTRACT_TEST_CASE'
code_name_defect_suite = 'RALLY_EXTRACT_DEFECT_SUITE'

# CreationDate as query - group
code_name_disc = 'RALLY_EXTRACT_DISCUSSION'
code_name_user_iter_cap = 'RALLY_EXTRACT_USER_ITER_CAP'
code_name_rev_hist = 'RALLY_EXTRACT_REV_HIST'
code_name_test_case_result = 'RALLY_EXTRACT_TEST_CASE_RESULT'
code_name_test_case_step = 'RALLY_EXTRACT_TEST_CASE_STEP'
code_name_test_folder = 'RALLY_EXTRACT_TEST_FOLDER'
code_name_blocker = 'RALLY_EXTRACT_BLOCKER'

sql_fetch_syscon_pi = sql_fetch_syscon.format(code_name_pi)
sql_fetch_syscon_defect = sql_fetch_syscon.format(code_name_defect)
sql_fetch_syscon_defect_suite = sql_fetch_syscon.format(code_name_defect_suite)
sql_fetch_syscon_us = sql_fetch_syscon.format(code_name_us)
sql_fetch_syscon_task = sql_fetch_syscon.format(code_name_task)
sql_fetch_syscon_test_set = sql_fetch_syscon.format(code_name_test_set)
sql_fetch_syscon_test_case = sql_fetch_syscon.format(code_name_test_case)

# CreationDate as query - group
sql_fetch_syscon_disc = sql_fetch_syscon.format(code_name_disc)
sql_fetch_syscon_user_iter_cap = sql_fetch_syscon.format(code_name_user_iter_cap)
sql_fetch_syscon_rev_hist = sql_fetch_syscon.format(code_name_rev_hist)
sql_fetch_syscon_test_case_result = sql_fetch_syscon.format(code_name_test_case_result)
sql_fetch_syscon_test_case_step = sql_fetch_syscon.format(code_name_test_case_step)
sql_fetch_syscon_test_folder = sql_fetch_syscon.format(code_name_test_folder)
sql_fetch_syscon_blocker = sql_fetch_syscon.format(code_name_blocker)

sql_trunc_defect = sql_trunc.format(stg_tab_defect)
sql_trunc_defect_suite = sql_trunc.format(stg_tab_defect_suite)
sql_trunc_pi = sql_trunc.format(stg_tab_pi)
sql_trunc_us = sql_trunc.format(stg_tab_us)
sql_trunc_task = sql_trunc.format(stg_tab_task)
sql_trunc_test_set = sql_trunc.format(stg_tab_test_set)
sql_trunc_test_case = sql_trunc.format(stg_tab_test_case)

# CreationDate as query - group
sql_trunc_disc = sql_trunc.format(stg_tab_disc)
sql_trunc_user_iter_cap = sql_trunc.format(stg_tab_user_iter_cap)
sql_trunc_rev_hist = sql_trunc.format(stg_tab_rev_hist)
sql_trunc_test_case_result = sql_trunc.format(stg_tab_test_case_result)
sql_trunc_test_case_step = sql_trunc.format(stg_tab_test_case_step)
sql_trunc_test_folder = sql_trunc.format(stg_tab_test_folder)
sql_trunc_blocker = sql_trunc.format(stg_tab_blocker)

# dim_group truncate statements
sql_trunc_release = sql_trunc.format(stg_tab_release)
sql_trunc_project = sql_trunc.format(stg_tab_project)
sql_trunc_iteration = sql_trunc.format(stg_tab_iteration)
sql_trunc_user = sql_trunc.format(stg_tab_user)
sql_trunc_tag = sql_trunc.format(stg_tab_tag)
sql_trunc_kanban_state = sql_trunc.format(stg_tab_kanban_state)
sql_trunc_prelim_estmt = sql_trunc.format(stg_tab_prelim_estmt)
sql_trunc_dim_grp = [sql_trunc_release, sql_trunc_project, sql_trunc_iteration, sql_trunc_user, sql_trunc_tag,
                     sql_trunc_kanban_state, sql_trunc_prelim_estmt]

# active artifacts table truncate
sql_trunc_aa_pi = sql_trunc.format(stg_tab_active_pi)
sql_trunc_aa_defect = sql_trunc.format(stg_tab_active_defect)
sql_trunc_aa_defect_suite = sql_trunc.format(stg_tab_active_defect_suite)
sql_trunc_aa_us = sql_trunc.format(stg_tab_active_us)
sql_trunc_aa_task = sql_trunc.format(stg_tab_active_task)
sql_trunc_aa_test_set = sql_trunc.format(stg_tab_active_test_set)
sql_trunc_aa_test_case = sql_trunc.format(stg_tab_active_test_case)

# active artifacts table load
sql_active_artifacts_pi = sql_active_artifacts.format(stg_tab_active_pi)
sql_active_artifacts_defect = sql_active_artifacts.format(stg_tab_active_defect)
sql_active_artifacts_defect_suite = sql_active_artifacts.format(stg_tab_active_defect_suite)
sql_active_artifacts_us = sql_active_artifacts.format(stg_tab_active_us)
sql_active_artifacts_task = sql_active_artifacts.format(stg_tab_active_task)
sql_active_artifacts_test_set = sql_active_artifacts.format(stg_tab_active_test_set)
sql_active_artifacts_test_case = sql_active_artifacts.format(stg_tab_active_test_case)

fields_pi = ['ObjectID', 'LastUpdateDate', 'CreationDate', 'FormattedID', 'LatestDiscussionAgeInMinutes', 'Name',
             'Owner', 'Project', 'Ready', 'AcceptedLeafStoryCount', 'AcceptedLeafStoryPlanEstimateTotal',
             'ActualEndDate', 'ActualStartDate', 'Archived', 'DirectChildrenCount', 'InvestmentCategory', 'JobSize',
             'LeafStoryCount', 'LeafStoryPlanEstimateTotal', 'PercentDoneByStoryCount',
             'PercentDoneByStoryPlanEstimate', 'PlannedEndDate', 'PlannedStartDate', 'PortfolioItemTypeName',
             'PreliminaryEstimate', 'RefinedEstimate', 'RiskScore', 'RROEValue', 'StateChangedDate', 'TimeCriticality',
             'UnEstimatedLeafStoryCount', 'UserBusinessValue', 'ValueScore', 'WSJFScore', 'Parent', 'State', 'Release',
             'c_Actionable', 'c_AnalysisAssignedDate', 'c_AnalysisStatus', 'c_BusAdvisorMOImpact',
             'c_BusOpsBOImpactbyMarket', 'c_BusOpsBOOwnerUK', 'c_BusOpsBOOwnerUS', 'c_BusOpsBOTesting',
             'c_BusOpsPrimaryTeamImpacted', 'c_BusOpsTeamFYIonly', 'c_BusUATAdvisorMOOwner',
             'c_BusUATAdvisorSolutionsOwner', 'c_BusinessAnalyst', 'c_BusinessEvent', 'c_Client', 'c_ClientConsumable',
             'c_DeleteMe', 'c_DeleteMe11', 'c_DeleteMe2', 'c_DeleteMe3', 'c_DeleteMe4', 'c_DeleteMe8', 'c_DeleteMe9',
             'c_DeployedOffCycleDuringPSI', 'c_DocumentStatementURL', 'c_EpicAnalysisNeeded', 'c_EpicOwner',
             'c_EpicRankEffortEstimate', 'c_EpicRankInvestmentThemeAlignment', 'c_EpicRankPotentialValueEstimate',
             'c_EpicRankValueEffort', 'c_FeatureType', 'c_PPMEstimatedStoryPoints', 'c_ReadOutUpdateDate',
             'c_ReadOutCompletionDt', 'c_RN1Title', 'c_RN5RelatedTrackingNumber', 'c_RN6Status', 'c_Service',
             'c_ServiceBundle', 'c_SolutionArchitect', 'c_Sponsor', 'c_SubService', 'c_SubmittedBy',
             'c_SysArchitectureNew', 'c_SysArchitectureNewDeploymentProcess', 'c_SysArtifactServiceCatolagueStatus',
             'c_SysArtifactServiceWorkflowUserGuide', 'c_SysImpactImplementationTools',
             'c_SysImpactImplementationToolsDetails', 'c_SysImpactInterfaces', 'c_SysImpactInterfacesDetails',
             'c_SysImpactLoaders', 'c_SysImpactLoadersDetails', 'c_SysImpactReporting', 'c_SysImpactReportingDetails',
             'c_SysOperationsTeam', 'c_SysProductMgtTeamMgr', 'c_SysRequiresMigrationScripts', 'c_SysUKConfiguration',
             'c_SysUKOpsConfiguration', 'c_SysUKOpsRolloutStrategy', 'c_SysUKRolloutStrategy',
             'c_SysUSAdvisorConfiguration', 'c_SysUSAdvisorRolloutStrategy', 'c_SysUSBankConfiguration',
             'c_SysUSBankRolloutStrategy', 'c_SysUSOpsConfiguration', 'c_SysUSOpsRolloutStrategy',
             'c_SysWebServicesDetails', 'c_TargetPSI', 'c_TargetPSIInitiated', 'c_TargetSprint', 'c_TargetTrain',
             'c_VirtualTeamOps', 'c_WSJFCoDUservalueTimevalueRROE', 'c_WSJFJobSize', 'c_WSJFRROEValue',
             'c_WSJFTimeValue', 'c_WSJFUserValue', 'c_WSJFUserValueTimeValueRROEJobSize', 'RevisionHistory',
             'DragAndDropRank', 'c_RoadmapCategoryADVMarket', 'c_ProductStrategyRoadmap', 'c_BusinessProcess',
             'c_CoreRefined', 'c_RoadmapCategoryOPS', 'c_RoadmapCategoryPBUK', 'c_RoadmapCategoryPBUS',
             'c_RoadmapSubCategory', 'c_GapID', 'c_WFPriority', 'c_WellsBusinessProcess', 'c_WellsIDMM',
             'c_RequiredforBridgeTesting', 'c_ClientUATTestable', 'c_SysUSHTSConfiguration', 'c_SysLoaderWebService',
             'c_SysUSHTSRolloutStrategy', 'c_SEIInvestmentTheme', 'c_AcceptanceCriteria', 'Description', 'Notes',
             'c_Benefit', 'c_BusAdvisorComments', 'c_BusOpsComments', 'c_DeleteMe10', 'c_NonActionableReason',
             'c_RN2Description', 'c_RN3PreviousFunctionality', 'c_RN4NewImprovedFunctionality']
fields_clob_pi = ['c_AcceptanceCriteria', 'Description', 'Notes', 'c_Benefit', 'c_BusAdvisorComments',
                  'c_BusOpsComments', 'c_DeleteMe10', 'c_NonActionableReason', 'c_RN2Description',
                  'c_RN3PreviousFunctionality', 'c_RN4NewImprovedFunctionality']

fields_defect = ['ObjectID', 'LastUpdateDate', 'CreationDate', 'FormattedID', 'LatestDiscussionAgeInMinutes', 'Name',
                 'Owner', 'Project', 'Ready', 'AcceptedDate', 'AffectsDoc', 'Blocked', 'BlockedReason', 'Blocker',
                 'ClosedDate', 'OpenedDate', 'Environment', 'FixedInBuild', 'FoundInBuild', 'InProgressDate',
                 'Iteration', 'Package',
                 'PlanEstimate', 'Priority', 'Recycled', 'Release', 'ReleaseNote', 'Requirement', 'Resolution',
                 'SalesforceCaseID', 'SalesforceCaseNumber', 'ScheduleState', 'Severity', 'State', 'SubmittedBy',
                 'TargetBuild', 'TargetDate', 'TaskActualTotal', 'TaskEstimateTotal', 'TaskRemainingTotal',
                 'TaskStatus', 'TestCase', 'TestCaseResult', 'TestCaseStatus', 'VerifiedInBuild', 'c_DefectExternalID',
                 'c_DeleteMe1', 'c_FirmName', 'c_FoundIn', 'c_FoundInCASOffCycleRelease', 'c_PDActivityPhase',
                 'c_PDActivityType', 'c_PDIStripeEnvironment', 'c_TestPhase', 'c_BusinessProcess', 'c_StandardizedRCA',
                 'RevisionHistory', 'Description', 'Notes', 'c_QC10Attachments', 'c_RootCauseDetails']
fields_clob_defect = ['Description', 'Notes', 'c_QC10Attachments', 'c_RootCauseDetails']

fields_defect_suite = ['ObjectID', 'CreationDate', 'LastUpdateDate', 'Expedite', 'FormattedID',
                       'LatestDiscussionAgeInMinutes', 'Name', 'Owner', 'Project', 'Ready', 'RevisionHistory',
                       'LastBuild', 'LastRun', 'PassingTestCaseCount', 'ScheduleState', 'TestCaseCount', 'AcceptedDate',
                       'Blocked', 'BlockedReason', 'Blocker', 'ClosedDefectCount', 'DefectStatus', 'DragAndDropRank',
                       'InProgressDate', 'Iteration', 'Package', 'PlanEstimate', 'Release', 'TaskActualTotal',
                       'TaskEstimateTotal', 'TaskRemainingTotal', 'TaskStatus', 'TotalDefectCount', 'Description',
                       'Notes']
fields_clob_defect_suite = ['Description', 'Notes']

fields_us = ['ObjectID', 'LastUpdateDate', 'CreationDate', 'FormattedID', 'LatestDiscussionAgeInMinutes', 'Name',
             'Owner', 'Project', 'Ready', 'Package', 'AcceptedDate', 'Blocked', 'BlockedReason', 'Blocker',
             'DefectStatus', 'DirectChildrenCount', 'HasParent', 'InProgressDate', 'Iteration', 'Parent',
             'PlanEstimate', 'Recycled', 'Release', 'ScheduleState', 'TaskActualTotal', 'TaskEstimateTotal',
             'TaskRemainingTotal', 'TaskStatus', 'TestCaseStatus', 'c_AdvisorStabilityBacklog', 'Feature',
             'c_MaintenanceCategory', 'PortfolioItem', 'c_ProdReadinessDetails', 'c_ProdReadinessNeeded', 'c_Risk',
             'c_Service', 'c_ServiceBundle', 'c_SubService', 'c_SubmittedBy', 'c_TargetEndDate', 'c_UserStoryType',
             'c_Value', 'RevisionHistory', 'c_WellsRabid', 'c_FunctionalArea', 'c_TrackingType', 'c_TrackingNumber',
             'c_ResolutionType', 'c_Priority', 'c_OffTrainTeam', 'c_Environment', 'c_BusinessUnit', 'c_InvestmentTheme',
             'Description', 'Notes', 'c_AcceptanceCriteria', 'c_BlockedReasonCustom']
fields_clob_us = ['Description', 'Notes', 'c_AcceptanceCriteria', 'c_BlockedReasonCustom']

fields_task = ['ObjectID', 'LastUpdateDate', 'CreationDate', 'FormattedID', 'LatestDiscussionAgeInMinutes', 'Name',
               'Owner', 'Ready', 'Actuals', 'Blocked', 'BlockedReason', 'Estimate', 'Iteration', 'Project', 'Recycled',
               'Release', 'State', 'TaskIndex', 'ToDo', 'WorkProduct', 'c_SubmittedBy', 'c_TargetEndDate',
               'RevisionHistory', 'Description', 'Notes', 'c_AcceptanceCriteria', 'c_BlockedReasonCustom']
fields_clob_task = ['Description', 'Notes', 'c_AcceptanceCriteria', 'c_BlockedReasonCustom']

fields_test_set = ['ObjectID', 'LastUpdateDate', 'CreationDate', 'FormattedID', 'LatestDiscussionAgeInMinutes', 'Name',
                   'Owner', 'Project', 'Ready', 'AcceptedDate', 'Blocker', 'Blocked', 'BlockedReason',
                   'TaskActualTotal', 'TaskEstimateTotal', 'TaskRemainingTotal', 'PlanEstimate', 'Iteration', 'Release',
                   'ScheduleState', 'TaskStatus', 'TestCaseStatus', 'RevisionHistory', 'Description', 'Notes']
fields_clob_test_set = ['Description', 'Notes']

fields_test_case = ['ObjectID', 'LastUpdateDate', 'CreationDate', 'FormattedID', 'LatestDiscussionAgeInMinutes', 'Name',
                    'Owner', 'Project', 'Ready', 'DefectStatus', 'LastBuild', 'LastRun', 'LastVerdict', 'Method',
                    'Package', 'Priority', 'Recycled', 'Risk', 'TestFolder', 'WorkProduct', 'c_HistoricalRelease',
                    'c_PlannedEndDt', 'c_PlannedStartDt', 'c_SprintWrittenFor', 'c_TestPhase', 'c_TestCaseQCExternalID',
                    'c_WorkTrack', 'c_BusinessProcess', 'c_MarketUnit', 'RevisionHistory', 'Description', 'Notes',
                    'Objective', 'PostConditions', 'PreConditions']
fields_clob_test_case = ['Description', 'Notes', 'Objective', 'PostConditions', 'PreConditions']

# Fields for Dimension Group
fields_user = ['ObjectID', 'CreationDate', 'CostCenter', 'Department', 'Disabled', 'DisplayName', 'EmailAddress',
               'FirstName', 'LandingPage', 'LastLoginDate', 'LastName', 'LastPasswordUpdateDate', 'MiddleName',
               'NetworkID', 'OfficeLocation', 'OnpremLdapUsername', 'Phone', 'Role', 'ShortDisplayName',
               'SubscriptionAdmin', 'UserName', 'UserProfile', 'c_TestLocation', 'c_TestRole', 'c_WorkStatus',
               'RevisionHistory']
fields_clob_user = [None]

fields_project = ['ObjectID', 'CreationDate', 'Name', 'Owner', 'Parent', 'SchemaVersion', 'State', 'RevisionHistory',
                  'Description', 'Notes']
fields_clob_project = ['Description', 'Notes']

fields_release = ['ObjectID', 'CreationDate', 'GrossEstimateConversionRatio', 'Name', 'PlannedVelocity', 'Project',
                  'ReleaseDate', 'ReleaseStartDate', 'State', 'RevisionHistory', 'Notes', 'Theme']
fields_clob_release = ['Notes', 'Theme']

fields_iteration = ['ObjectID', 'CreationDate', 'EndDate', 'Name', 'PlannedVelocity', 'Project', 'StartDate', 'State',
                    'RevisionHistory', 'Notes', 'Theme']
fields_clob_iteration = ['Notes', 'Theme']

fields_tag = ['ObjectID', 'CreationDate', 'Archived', 'Name']
fields_clob_tag = [None]

fields_kanban_state = ['ObjectID', 'CreationDate', 'AcceptedMarker', 'Enabled', 'InProgressMarker', 'Name',
                       'OrderIndex', 'WIPLimit', 'RevisionHistory', 'Description']
fields_clob_kanban_state = ['Description']

fields_prelim_estmt = ['ObjectID', 'CreationDate', 'Name', 'Value', 'RevisionHistory', 'Description']
fields_clob_prelim_estmt = ['Description']

# Fields for CreationDate as query - group

fields_disc = ['ObjectID', 'CreationDate', 'Artifact', 'PostNumber', 'User', 'Text']
fields_clob_disc = ['Text']

fields_user_iter_cap = ['ObjectID', 'CreationDate', 'Capacity', 'Iteration', 'Load', 'Project', 'TaskEstimates', 'User']
fields_clob_user_iter_cap = [None]

fields_rev_hist = ['ObjectID', 'CreationDate', 'RevisionHistory', 'RevisionNumber', 'User', 'Description']
fields_clob_rev_hist = ['Description']

fields_test_case_result = ['ObjectID', 'CreationDate', 'Build', 'Date', 'Duration', 'TestCase', 'TestSet', 'Tester',
                           'Verdict', 'Notes' ]
fields_clob_test_case_result = ['Notes']

fields_test_case_step = ['ObjectID','CreationDate','StepIndex','TestCase','ExpectedResult','Input']
fields_clob_test_case_step = ['ExpectedResult','Input']

fields_test_folder = ['ObjectID', 'CreationDate', 'FormattedID', 'Name', 'Parent', 'Project']
fields_clob_test_folder = [None]

fields_blocker = ['ObjectID', 'CreationDate', 'BlockedBy', 'WorkProduct']
fields_clob_blocker = [None]

sql_insert_pi = """
insert into """ + str(stg_tab_pi) + \
                """(objectid, lastupdatedate, creationdate, formattedid, latestdiscussionageinminutes, name, 
                owner_name, owner, project, ready, acceptedleafstorycount, 
                acceptedleafstoryplan_est_tot, actualenddate, actualstartdate, archived, directchildrencount, 
                investmentcategory, jobsize, leafstorycount, leafstoryplan_est_tot, percentdonebystorycount, 
                percentdonebystoryplan_est, plannedenddate, plannedstartdate, portfolioitemtypename, 
                preliminaryestimate_name, preliminaryestimate, refinedestimate, riskscore, rroevalue, 
                statechangeddate, timecriticality, unestimatedleafstorycount, userbusinessvalue, valuescore, 
                wsjfscore, parent, state_name, state, release_name, release, c_actionable, 
                c_analysisassigneddate, c_analysisstatus, c_busadvisormoimpact, c_busopsboimpactbymarket, 
                c_busopsboowneruk, c_busopsboownerus, c_busopsbotesting, c_busopsprimaryteamimpacted, 
                c_busopsteamfyionly, c_busuatadvisormoowner, c_busuatadvisorsolutionsowner, c_businessanalyst, 
                c_businessevent, c_client, c_clientconsumable, c_deleteme, c_deleteme11, c_deleteme2, c_deleteme3, 
                c_deleteme4, c_deleteme8, c_deleteme9, c_deployedoffcycleduringpsi, c_documentstatementurl, 
                c_epicanalysisneeded, c_epicowner, c_epicrankeffort_est, c_epicrankinvstmntthemealnmnt, 
                c_epicrankpotentialvalue_est, c_epicrankvalueeffort, c_featuretype, c_ppm_estdstorypoints, 
                c_readoutupdatedate, c_readoutcompletiondt, c_rn1title, c_rn5relatedtrackingnumber, c_rn6status, 
                c_svc, c_svcbundle, c_solutionarchitect, c_sponsor, c_subsvc, c_submittedby, c_sysarchnew, 
                c_sysarchnewdplmntprocess, c_sysartfct_svc_ctlg_status, c_sysartfct_svc_wrkflw_usrgd, 
                c_sysimpact_implmntn_tools, c_sysimpact_implmntntools_dtls, c_sysimpactinterfaces, 
                c_sysimpactinterfaces_dtls, c_sysimpactloaders, c_sysimpactloaders_dtls, c_sysimpactreporting, 
                c_sysimpactreporting_dtls, c_sysoperationsteam, c_sysproductmgtteammgr, 
                c_sysrequiresmigrationscripts, c_sysukconfiguration, c_sysukopsconfiguration, 
                c_sysukopsrolloutstrategy, c_sysukrolloutstrategy, c_sysusadvisorconfiguration, 
                c_sysusadvisorrolloutstrategy, c_sysusbankconfiguration, c_sysusbankrolloutstrategy, 
                c_sysusopsconfiguration, c_sysusopsrolloutstrategy, c_syswebsvc_dtls, c_targetpsi, 
                c_targetpsiinitiated, c_targetsprint, c_targettrain, c_virtualteamops, c_wsjfcodusrvaltimevalrroe, 
                c_wsjfjobsize, c_wsjfrroevalue, c_wsjftimevalue, c_wsjfuservalue, c_wsjfuser_val_tm_valrroejobsz, 
                revisionhistory, draganddroprank, c_roadmapcategoryadvmarket, 
                c_productstrategyroadmap, c_businessprocess, c_corerefined, c_roadmapcategoryops, 
                c_roadmapcategorypbuk, c_roadmapcategorypbus, c_roadmapsubcategory, c_gapid, c_wfpriority, 
                c_wellsbusinessprocess, c_wellsidmm, c_requiredforbridgetesting, c_clientuattestable, 
                c_sysushtsconfiguration, c_sysloaderwebsvc, c_sysushtsrolloutstrategy, c_SEIInvestmentTheme,
                c_AcceptanceCriteria, 
                Description, Notes, c_Benefit, c_BusAdvisorComments, c_BusOpsComments, c_DeleteMe10, 
                c_NonActionableReason, c_RN2Description, c_RN3PreviousFunctionality, c_RN4NewImprovedFunctionality) 
                values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,
                :26,:27,:28,:29,:30,:31,:32,:33,:34,:35,:36,:37,:38,:39,:40,:41,:42,:43,:44,:45,:46,:47,:48,:49,:50,
                :51,:52,:53,:54,:55,:56,:57,:58,:59,:60,:61,:62,:63,:64,:65,:66,:67,:68,:69,:70,:71,:72,:73,:74,:75,
                :76,:77,:78,:79,:80,:81,:82,:83,:84,:85,:86,:87,:88,:89,:90,:91,:92,:93,:94,:95,:96,:97,:98,:99,:100,
                :101,:102,:103,:104,:105,:106,:107,:108,:109,:110,:111,:112,:113,:114,:115,:116,:117,:118,:119,:120,
                :121,:122,:123,:124,:125,:126,:127,:128,:129,:130,:131,:132,:133,:134,:135,:136,:137,:138,:139,:140,
                :141,:142,:143,:144,:145,:146,:147,:148,:149, :150,:151,:152,:153) """

sql_insert_defect = """
insert into """ + str(stg_tab_defect) + """(OBJECTID,LASTUPDATEDATE,CREATIONDATE,FORMATTEDID, 
LATESTDISCUSSIONAGEINMINUTES,NAME,OWNER_NAME,OWNER,PROJECT,READY,ACCEPTEDDATE,AFFECTSDOC,BLOCKED, 
BLOCKEDREASON,BLOCKER,CLOSEDDATE,OpenedDate,ENVIRONMENT,FIXEDINBUILD,FOUNDINBUILD,INPROGRESSDATE,ITERATION_NAME, 
ITERATION,PACKAGE,PLANESTIMATE,PRIORITY,RECYCLED,RELEASE_NAME,RELEASE,RELEASENOTE,REQUIREMENT, 
RESOLUTION,SALESFORCECASEID,SALESFORCECASENUMBER,SCHEDULESTATE,SEVERITY,STATE,SUBMITTEDBY_NAME,SUBMITTEDBY, 
TARGETBUILD,TARGETDATE,TASKACTUALTOTAL,TASKESTIMATETOTAL,TASKREMAININGTOTAL,TASKSTATUS,TESTCASE, 
TESTCASERESULT,TESTCASESTATUS,VERIFIEDINBUILD,C_DEFECTQCEXTERNALID,C_DELETEME1,C_FIRMNAME, 
C_FOUNDIN,C_FOUNDINCASOFFCYCLERELEASE,C_PDACTIVITYPHASE,C_PDACTIVITYTYPE,C_PDISTRIPEENVIRONMENT,C_TESTPHASE, 
C_BUSINESSPROCESS,c_StandardizedRCA,RevisionHistory,DESCRIPTION,NOTES,C_QC10ATTACHMENTS,C_ROOTCAUSEDETAILS) values (
:1,:2,:3,:4,:5, :6,:7,:8, :9,:10, :11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,
:31,:32,:33,:34, :35,:36,:37, :38,:39, :40,:41,:42,:43,:44,:45,:46,:47,:48,:49,:50,:51,:52,:53,:54,:55,:56,:57,:58,
:59,:60,:61,:62, :63, :64,:65) """

sql_insert_defect_suite = """
insert into """ + str(stg_tab_defect_suite) + """(OBJECTID,CreationDate,LastUpdateDate,Expedite,
FormattedID,LatestDiscussionAgeInMinutes,Name,Owner_NAME,Owner,Project,Ready,RevisionHistory,LastBuild,LastRun,
PassingTestCaseCount,ScheduleState,TestCaseCount,AcceptedDate,Blocked,BlockedReason,Blocker,ClosedDefectCount,
DefectStatus,DragAndDropRank,InProgressDate,Iteration_NAME,Iteration,Package,PlanEstimate,RELEASE_NAME,Release,
TaskActualTotal,TaskEstimateTotal,TaskRemainingTotal,TaskStatus,TotalDefectCount,Description,Notes) values ( :1,:2,
:3,:4,:5, :6,:7,:8, :9,:10, :11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30, :31,:32,
:33,:34, :35,:36,:37, :38) """

sql_insert_us = """
insert into """ + str(stg_tab_us) + """(ObjectID,LastUpdateDate,CreationDate,FormattedID,
LatestDiscussionAgeInMinutes,Name,Owner_Name,Owner,Project,Ready,Package,AcceptedDate,Blocked,
BlockedReason,Blocker,DefectStatus,DirectChildrenCount,HasParent,InProgressDate,Iteration_Name,
Iteration,Parent,PlanEstimate,Recycled,Release_Name,Release,ScheduleState,TaskActualTotal,
TaskEstimateTotal,TaskRemainingTotal,TaskStatus,TestCaseStatus,c_AdvisorStabilityBacklog,Feature,
c_MaintenanceCategory,PortfolioItem,c_ProdReadinessDetails,c_ProdReadinessNeeded,c_Risk,c_Service,
c_ServiceBundle,c_SubService,c_SubmittedBy,c_TargetEndDate,c_UserStoryType,c_Value,
RevisionHistory,c_WellsRabid,c_FunctionalArea,c_TrackingType,c_TrackingNumber,c_ResolutionType,c_Priority,
c_OffTrainTeam,c_Environment,c_BusinessUnit,c_InvestmentTheme,Description,Notes,c_AcceptanceCriteria,
c_BlockedReasonCustom) values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,
:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34,:35,:36,:37,:38,:39,:40,:41,:42,:43,:44,:45,:46,:47,:48,:49,:50,:51,:52,
:53,:54,:55,:56,:57,:58,:59,:60,:61) """

sql_insert_task = """
insert into """ + str(stg_tab_task) + """(OBJECTID,LASTUPDATEDATE,CREATIONDATE,FORMATTEDID, 
LATESTDISCUSSIONAGEINMINUTES,NAME,OWNER_NAME,OWNER,READY,ACTUALS,BLOCKED,BLOCKEDREASON,ESTIMATE,ITERATION_NAME, 
ITERATION,PROJECT,RECYCLED,RELEASE_NAME,RELEASE,STATE,TASKINDEX,TODO,WORKPRODUCT, 
C_SUBMITTEDBY,C_TARGETENDDATE,RevisionHistory,DESCRIPTION,NOTES,C_ACCEPTANCECRITERIA,C_BLOCKEDREASONCUSTOM) values (
:1,:2, :3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23, :24,:25,:26,:27,:28,:29,:30) """

sql_insert_test_set = """
insert into """ + str(stg_tab_test_set) + """(ObjectID,LastUpdateDate,CreationDate,FormattedID, 
LatestDiscussionAgeInMinutes,Name,Owner_name,Owner,Project,Ready,AcceptedDate,Blocker,Blocked,BlockedReason, 
TaskActualTotal,TaskEstimateTotal,TaskRemainingTotal,PlanEstimate,Iteration_name,Iteration,Release_name,Release, 
ScheduleState,TaskStatus,TestCaseStatus,RevisionHistory,Description,Notes) values ( :1,:2, :3,:4,:5,:6,:7,:8,:9,:10,
:11,:12,:13, :14,:15,:16,:17,:18,:19,:20,:21,:22,:23, :24,:25,:26,:27,:28) """

sql_insert_test_case = """
insert into """ + str(stg_tab_test_case) + """(ObjectID,LastUpdateDate,CreationDate,FormattedID,
LatestDiscussionAgeInMinutes,Name,Owner_Name,Owner,Project,Ready,DefectStatus,LastBuild,LastRun,LastVerdict,Method,Package,
Priority,Recycled,Risk,TestFolder,WorkProduct,c_HistoricalRelease,c_PlannedEndDt,c_PlannedStartDt,c_SprintWrittenFor,
c_TestPhase,c_TestCaseQCExternalID,c_WorkTrack,c_BusinessProcess,c_MarketUnit,RevisionHistory,Description,Notes,Objective,
PostConditions,PreConditions) values ( :1,:2, :3,:4,:5,:6,:7,:8,:9,:10,:11,
:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23, :24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34,:35,:36) """

# Dim Group Insert statements

sql_insert_user = """
insert into """ + str(stg_tab_user) + """(ObjectID,CreationDate,CostCenter,Department,Disabled,DisplayName, 
EmailAddress,FirstName,LandingPage,LastLoginDate,LastName,LastPasswordUpdateDate,MiddleName,NetworkID,OfficeLocation, 
OnpremLdapUsername,Phone,Role,ShortDisplayName,SubscriptionAdmin,UserName,UserProfile, c_TestLocation,c_TestRole,
c_WorkStatus,RevisionHistory) values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19, :20,:21,
:22,:23,:24,:25, :26) """

sql_insert_project = """
insert into """ + str(stg_tab_project) + """(ObjectID,CreationDate,Name,Owner_name,Owner,Parent,
SchemaVersion,State,RevisionHistory,Description,Notes) values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11) """

sql_insert_release = """
insert into """ + str(stg_tab_release) + """(ObjectID,CreationDate,GrossEstimateConversionRatio,Name,PlannedVelocity, 
Project,ReleaseDate,ReleaseStartDate,State,RevisionHistory,Notes,Theme ) values (:1,:2,:3,:4,:5,:6,:7,
:8,:9,:10,:11,:12) """

sql_insert_iteration = """
insert into """ + str(stg_tab_iteration) + """(ObjectID,CreationDate,EndDate,Name,PlannedVelocity,
Project,StartDate,State,RevisionHistory,Notes,Theme ) values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11) """

sql_insert_tag = """
insert into """ + str(stg_tab_tag) + """(ObjectID,CreationDate,Archived, Name ) values (:1,:2,:3,:4) """

sql_insert_kanban_state = """
insert into """ + str(stg_tab_kanban_state) + """(ObjectID,CreationDate,AcceptedMarker,Enabled,
InProgressMarker,Name,OrderIndex,WIPLimit,RevisionHistory,Description) values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10) """

sql_insert_prelim_estmt = """
insert into """ + str(stg_tab_prelim_estmt) + """(ObjectID,CreationDate,Name,Value,RevisionHistory,Description) 
values (:1,:2,:3,:4, :5,:6) """

# SQL insert statement for CreationDate as query - group

sql_insert_disc = """
insert into """ + str(stg_tab_disc) + """(OBJECTID,CREATIONDATE,SRC_TYPE,SRC_OBJECTID,PostNumber,User_Name,
User_OBJECTID,TEXT) values (:1,:2,:3,:4,:5,:6,:7,:8) """

sql_insert_user_iter_cap = """
insert into """ + str(stg_tab_user_iter_cap) + """(OBJECTID,CREATIONDATE,Capacity,Iteration_Name,Iteration,Load,
Project,TaskEstimates,USER_NAME,User_id) values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10) """

sql_insert_rev_hist = """
insert into """ + str(stg_tab_rev_hist) + """(ObjectID,CreationDate,RevisionHistory,RevisionNumber,User_name,USER_ID,
Description) values (:1,:2,:3,:4,:5,:6,:7) """

sql_insert_test_case_result = """
insert into """ + str(stg_tab_test_case_result) + """(ObjectID,CreationDate,Build,TESTCASERESULT_Date,Duration,
TestCase,TestSet,Tester_name,Tester,Verdict,Notes) values ( :1,:2, :3,:4,:5,:6,:7,:8,:9,:10,:11) """

sql_insert_test_case_step = """
insert into """ + str(stg_tab_test_case_step) + """(ObjectID,CreationDate,StepIndex,TestCase,ExpectedResult,step_input) 
values ( :1,:2, :3,:4,:5,:6) """

sql_insert_test_folder = """
insert into """ + str(stg_tab_test_folder) + """(ObjectID,CreationDate,FormattedID,Name,Parent,Project) values ( :1,
:2, :3,:4,:5,:6) """

sql_insert_blocker = """
insert into """ + str(stg_tab_blocker) + """(ObjectID,CreationDate,BlockedBy_Name,BlockedBy,WorkProduct) values ( :1,
:2, :3,:4,:5) """

db_pkg_name = 'PKG_RALLY_EXTRACT.{}'
db_pkg_name_pi = db_pkg_name.format('P_RALLY_EXT_PI')
db_pkg_name_defect = db_pkg_name.format('P_RALLY_EXT_DEFECT')
db_pkg_name_defect_suite = db_pkg_name.format('P_RALLY_EXT_DEFECT_SUITE')
db_pkg_name_us = db_pkg_name.format('P_RALLY_EXT_US')
db_pkg_name_task = db_pkg_name.format('P_RALLY_EXT_TASK')
db_pkg_name_test_set = db_pkg_name.format('P_RALLY_EXT_TEST_SET')
db_pkg_name_test_case = db_pkg_name.format('P_RALLY_EXT_TEST_CASE')

# Dim Group PKGs
db_pkg_name_user = db_pkg_name.format('P_RALLY_EXT_USER')
db_pkg_name_project = db_pkg_name.format('P_RALLY_EXT_PROJECT')
db_pkg_name_release = db_pkg_name.format('P_RALLY_EXT_RELEASE')
db_pkg_name_iteration = db_pkg_name.format('P_RALLY_EXT_ITERATION')
db_pkg_name_tag = db_pkg_name.format('P_RALLY_EXT_TAG')
db_pkg_name_kanban_state = db_pkg_name.format('P_RALLY_EXT_KANBAN_STATE')
db_pkg_name_prelim_estmt = db_pkg_name.format('P_RALLY_EXT_PRELIM_ESTMT')

# CreationDate as query - group
db_pkg_name_disc = db_pkg_name.format('P_RALLY_EXT_DISCUSSION')
db_pkg_name_user_iter_cap = db_pkg_name.format('P_RALLY_EXT_USER_ITER_CAP')
db_pkg_name_rev_hist = db_pkg_name.format('P_RALLY_EXT_REV_HIST')
db_pkg_name_test_case_result = db_pkg_name.format('P_RALLY_EXT_TEST_CASE_RESULT')
db_pkg_name_test_case_step = db_pkg_name.format('P_RALLY_EXT_TEST_CASE_STEP')
db_pkg_name_test_folder = db_pkg_name.format('P_RALLY_EXT_TEST_FOLDER')
db_pkg_name_blocker = db_pkg_name.format('P_RALLY_EXT_BLOCKER')

# Dimension group Key-Value pair
fields_dim_grp = {'User': fields_user, 'Project': fields_project, 'Release': fields_release,
                  'Iteration': fields_iteration, 'Tag': fields_tag, 'State': fields_kanban_state,
                  'PreliminaryEstimate': fields_prelim_estmt}
fields_clob_dim_grp = {'User': fields_clob_user, 'Project': fields_clob_project,
                       'Release': fields_clob_release,
                       'Iteration': fields_clob_iteration, 'Tag': fields_clob_tag, 'State': fields_clob_kanban_state,
                       'PreliminaryEstimate': fields_clob_prelim_estmt}
sql_insert_dim_grp = {'User': sql_insert_user, 'Project': sql_insert_project, 'Release': sql_insert_release,
                      'Iteration': sql_insert_iteration, 'Tag': sql_insert_tag, 'State': sql_insert_kanban_state,
                      'PreliminaryEstimate': sql_insert_prelim_estmt}
db_pkg_name_dim_grp = {'User': db_pkg_name_user, 'Project': db_pkg_name_project,
                       'Release': db_pkg_name_release,
                       'Iteration': db_pkg_name_iteration, 'Tag': db_pkg_name_tag, 'State': db_pkg_name_kanban_state,
                       'PreliminaryEstimate': db_pkg_name_prelim_estmt}
