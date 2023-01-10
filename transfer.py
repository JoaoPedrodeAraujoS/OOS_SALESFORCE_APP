#Removing try catch
import pandas as pd
import re
#Add library os.path
import os, os.path
from datetime import datetime, timedelta
from api import SFApi, read_preferences, dict_from_df
from simple_salesforce import format_soql
import parsers as ps
import sys
import subprocess



def split_dataframe(df_new, df, col, remove_prefix):
    df_new = pd.concat(
        [df_new, df[col]], axis=1, sort=False)
    df_new = df_new.rename(
        columns={col: col.replace(remove_prefix, '')})
    return df_new


def get_records(parser, fname, df_records, df_popped_records):
    msg, df = parser.get_cleaned_df(fname)
    df['ORIGINAL_FILE'] = fname
    if msg == "OK":
        df_records = df_records.append(df)
    else:
        df_popped_records = df_popped_records.append(
            {'ORIGINAL_FILE': fname, 'ERROR': msg},
            ignore_index=True
        )              
    return df_records, df_popped_records


def parse_failed_records(preferences, fnames, df_records=pd.DataFrame(), df_popped_records=pd.DataFrame()):
    for fname in fnames:
        parser = ps.Parser.discover_parser(fname)('', '')
        df_records, df_popped_records = get_records(parser, fname, df_records, df_popped_records)
    return df_records, df_popped_records


def parse_records(preferences):
    analyzed_files = []
    df_popped_records = pd.DataFrame()
    df_records = pd.DataFrame()
    
    for parser_model in [ps.AzulParser, ps.WideroeParser, ps.HelveticParser, ps.AstanaParser, ps.BelaviaParser, ps.KLMParser]:
        p = parser_model(
            '//flmfs05/vss/suporte_tecnico/FPR/6 - DADOS OPERACIONAIS/1 - OPERADORES/1 - Dados recebidos',
            preferences['entry_pattern']
        )
        for fname in p.get_unprocessed_files():
            date_ref = ps.Parser.get_reference_date(fname, 1)
            date_ref_aux = date_ref[0]
            
            if datetime.strptime(preferences['date_entry_upload_since'], '%d/%m/%Y').date() <= \
               datetime.strptime(date_ref_aux, '%Y-%m-%d').date():
                analyzed_files.append(
                    fname.replace('ø', 'o').replace('@', 'a')
                )
                df_records, df_popped_records = get_records(p, fname, df_records, df_popped_records)
    return df_records, df_popped_records, analyzed_files


def replace_register_by_sn(sf_api, df_records, df_popped_records, reject_ejets=False):
    ac_registers = tuple(set(df_records['Aircraft_Register__c'].values.tolist()))
    ac_ids = sf_api.query(
            '''SELECT Registration__c, Id, Fleet_Type__c FROM Aircraft__c
            WHERE Registration__c IN {}'''.format(ac_registers)
    ).rename(columns={'Registration__c': 'Aircraft_Register__c'})
    df_records = df_records.merge(ac_ids, on='Aircraft_Register__c', how='left')
    
    df_records = df_records.drop(columns=['Aircraft_Register__c'])\
        .rename(columns={'Id': 'Serial_Number__c'})
    
    nan_mask = pd.isna(df_records.Serial_Number__c)
    df_records_nan_mask = df_records[nan_mask].drop(columns=['Serial_Number__c'])
    df_records_nan_mask['ERROR'] = 'AC REGISTER DOES NOT EXIST IN SF'
    df_popped_records = df_popped_records.append(df_records_nan_mask[['ORIGINAL_FILE', 'ERROR']])
    return df_records[~nan_mask], df_popped_records

def improve_error_message(df):
    df['STATUS_CODE'] = df['ERROR'].apply(lambda x: x['statusCode'] if isinstance(x, dict) else '')
    df.loc[df.STATUS_CODE == 'DUPLICATE_VALUE','ERROR'] = 'FAIL - DUPLICATED - AIRCRAFT THAT STARTED EVENT AT ' \
        + df.loc[df.STATUS_CODE == 'DUPLICATE_VALUE','Start_Date__c'] + ' ALREADY EXISTS IN SF'
    return df
'''
def merge_information_transfer(self):
    ps.Parser.merge(self)
'''
#READING THE PATHS TO MERGE INTER AND PIREP INFORMATION
def merge_information_transfer(self):

    #SEPARATING YEAR AND MONTH REFERENCE
    preferences = read_preferences()
    ano = preferences["date_entry_upload_since"].split('/')[2]
    mes = preferences["date_entry_upload_since"].split('/')[1]

    #PATH NAMES
    fname_klm = '//flmfs05/vss/suporte_tecnico/FPR/6 - DADOS OPERACIONAIS/1 - OPERADORES/1 - Dados recebidos/4 - EMEA/KLM CITYHOPPER' + '/' + ano + '/' + mes + '/' + 'OOS_DATA-KLM.xlsx'
    fname_azul = '//flmfs05/vss/suporte_tecnico/FPR/6 - DADOS OPERACIONAIS/1 - OPERADORES/1 - Dados recebidos/5 - LATIN AMERICA/AZUL/' + ano + '/' + mes + '/' + 'OOS_DATA-AZUL.xlsx'
    fname_astana = '//flmfs05/vss/suporte_tecnico/FPR/6 - DADOS OPERACIONAIS/1 - OPERADORES/1 - Dados recebidos/4 - EMEA/AIR ASTANA' + '/' + ano + '/' + mes + '/' + 'OOS_DATA-ASTANA.xlsx'
    fname_wideroe = '//flmfs05/vss/suporte_tecnico/FPR/6 - DADOS OPERACIONAIS/1 - OPERADORES/1 - Dados recebidos/4 - EMEA/WIDEROE' + '/' + ano + '/' + mes + '/' + 'OOS_DATA-WIDEROE.xlsx'
    fname_helvetic = '//flmfs05/vss/suporte_tecnico/FPR/6 - DADOS OPERACIONAIS/1 - OPERADORES/1 - Dados recebidos/4 - EMEA/HELVETIC AIRWAYS' + '/' + ano + '/' + mes + '/' + 'E2' + '/' + 'OOS_DATA-HELVETIC.xlsx'

    #VERIFY IF EXIST THE FILE WITH THAT PATH NAME
    if os.path.isfile(fname_klm):
        ps.Parser.merge_klm(self)
    else:
        pass
    if os.path.isfile(fname_wideroe):
        ps.Parser.merge_wideroe(self)
    else:
        pass
    if os.path.isfile(fname_azul):
        ps.Parser.merge_azul(self)
    else:
        pass
    if os.path.isfile(fname_astana):
        ps.Parser.merge_astana(self)
    else:
        pass
    if os.path.isfile(fname_helvetic):
        ps.Parser.merge_helvetic(self)
    else:
        sys.exit()

def auto_update_records_from_operators_sheets():
    preferences = read_preferences()
    fname_failed_records = 'errors/FAILED_RECORDS.csv'
    
    df_records, df_popped_records, analyzed_files = parse_records(preferences)
    if os.path.isfile(fname_failed_records):
        df_failed_records = pd.read_csv(fname_failed_records, sep=';')
        df_records, df_popped_records = parse_failed_records(
            preferences, df_failed_records.ORIGINAL_FILE.drop_duplicates().values, df_records, df_popped_records
        )
        df_records.drop_duplicates(inplace=True)
    messages = []
    messages.append('DATE:'+datetime.now().strftime("%Hh%Mm, %d/%m/%Y"))
    if len(df_records) > 0:
        sf_api = SFApi()
        sf_api.connect()
        df_records, df_popped_records = replace_register_by_sn(
                                            sf_api, df_records, df_popped_records
                                        )
        df_records = df_records[df_records.Fleet_Type__c == 'E-JET E2'].reset_index(drop=True)
        fnames = df_records['ORIGINAL_FILE']
        df_records.drop(columns=['ORIGINAL_FILE', 'Fleet_Type__c'], inplace=True)
        results = sf_api.insert('Out_of_service__c', dict_from_df(df_records))
        if len(results) > 0:
            df_results = pd.DataFrame(results).apply(pd.Series)
            

            df_records['SUCCESS'] = df_results.success
            df_records['ORIGINAL_FILE'] = fnames
            df_records['ERROR'] = df_results.errors.apply(lambda x: x[0] if len(x) > 0 else '')
            
            
            df_popped_records = df_popped_records.append(
                improve_error_message(
                    df_records[df_records.SUCCESS == False]
                )[['ORIGINAL_FILE', 'ERROR']]            
            )
            df_records = df_records[df_records.SUCCESS == True].drop(columns=['SUCCESS', 'ERROR'])
        with open('settings/history_files.txt', 'a+') as f:
            f.write('\n'.join(analyzed_files) + '\n')

        if len(df_popped_records) > 0:
            messages.append('UPLOAD:[FAIL] CHECK THE \'errors\' FOLDER')
        else:
            messages.append('UPLOAD:DATA WERE SENT TO SF')
    else:
        messages.append('UPLOAD:EVERYTHING IS UP-TO-DATE')
    
    with open('settings/logs.txt', 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write('\n'.join(messages) + '\n' + content)
    
    if len(df_popped_records) > 0:
        df_popped_records.reset_index(drop=True).drop_duplicates().to_csv(
            fname_failed_records, sep=";", index=False
        )
    else:
        if os.path.isfile(fname_failed_records):
            try:
                os.remove(fname_failed_records)
            finally:
                pass
        else:
            pass
    # with open('settings/logs.txt', 'a+') as f:
    #     f.write('\n'.join(messages) + '\n')
    
    return messages



def put_set_of_columns_after_anchor(all_columns, set_of_columns, anchor_column):
    for column_name in set_of_columns[::-1]:
        all_columns.pop(all_columns.index(column_name))
        all_columns.insert(all_columns.index(anchor_column) + 1, column_name)
    
    return all_columns


def download_records_as_sheet(folder, from_date='01/01/2000', to_date='31/12/9999'):
    '''
        This function is responsible to download records from SalesForce.
        In the process, this function will be used in conjunction with upload
        
        params
            :folder - Which folder the spreadsheet will be downloaded
            :from_date - From which date the records will be exported
            :to_date - Until what date the records will be exported

    '''
    
    sf_api = SFApi()
    sf_api.connect()

    from_date = datetime.strptime(
        from_date, '%d/%m/%Y').date()
    to_date = datetime.strptime(
        to_date, '%d/%m/%Y').date()
    
    df = sf_api.query(
        format_soql('''
            SELECT Id, Name, Inter_ID__c, Flight_Number__c, Log_Number__c, Aircraft_Register__c,
            Operator__c, Start_Date__c, Start_Time__c, Release_Date__c, Release_Time__c,
            Header__c, Event_Description__c, Action_Description__c, Chargeable__c,
            Exclusion_Code__c, Solution_Description__c, Solution_Release_Date__c,
            Issue_Status__c, Dispatched_on_MEL__c, PCR__c, EPR__c, JIRA__c, eFleet__c,
            CMC_Message__c, Component_Serial_Number__c, Component_Part_Number__c,
            EFTC_Comments__c, Troubleshooting_Analysis_Status__c, Troubleshooting_Category__c,
            EMIT_Comments__c, RTS_Comments__c, Meeting_Comments__c, Component_Analysis_Status__c,
            Component_Category__c, Materials_Comments__c, Quality_Investigation_Status__c,
            Quality_Item_Classification__c, Quality_Comments__c, Corrective_Action_Document__c
            FROM Out_of_service__c WHERE Reference_Date__c >= {} AND Reference_Date__c <= {}''',
            from_date, to_date)
        )
    
    cols_fc = ['Fail_Code__r.Name']
    cols_rc = ['Root_Code__r.Name', 'Root_Code__r.ATA__c', 'Root_Code__r.Supplier__r.Name']
    cols_pn = ['Name', 'PN_ON__c', 'SN_OFF__c', 'SN_ON__c', 'Pool__c', 'RSPL__c']
    _cols_pn = ['PN_Removal__r.' + col for col in cols_pn]
    
    df_fc = sf_api.query(format_soql('''
                         SELECT {:literal}, Out_of_service__r.Id FROM FC_OOS_Association__c
                         WHERE Out_of_service__c IN {}
                         ''', ','.join(cols_fc), df.Id.values.tolist()))
                         
    df_rc = sf_api.query(format_soql('''
                         SELECT {:literal}, Out_of_service__r.Id FROM RC_OOS_Association__c
                         WHERE Out_of_service__c IN {}
                         ''', ','.join(cols_rc), df.Id.values.tolist()))
                         
    df_pn = sf_api.query(format_soql('''
                         SELECT {:literal}, Out_of_service__r.Id FROM PN_Removal__c
                         WHERE Out_of_service__c IN {}
                         ''', ','.join(cols_pn), df.Id.values.tolist()))
                         
    df_pn.rename(columns=dict(zip(cols_pn, _cols_pn)), inplace=True)    
    
    df = pd.merge(df, df_fc, how='left', left_on='Id',
                  right_on='Out_of_service__r.Id')
    df = pd.merge(df, df_rc, how='left', left_on='Id',
                  right_on='Out_of_service__r.Id')
    df = pd.merge(df, df_pn, how='left', left_on='Id',
                  right_on='Out_of_service__r.Id')
    
    cols = [col for col in df.columns if 'Out_of_service__r.Id' not in col]
    df = df[cols]
    
    cols = put_set_of_columns_after_anchor(list(df.columns), cols_fc, 'Action_Description__c')
    cols = put_set_of_columns_after_anchor(cols, cols_rc, 'Exclusion_Code__c')
    cols = put_set_of_columns_after_anchor(cols, _cols_pn, 'Meeting_Comments__c')
    
    
    df = df[cols].sort_values('Start_Date__c')
    
    #Data conversion to str
    df['Header__c'] = df['Header__c'].map(str)
    df['Event_Description__c'] = df['Event_Description__c'].map(str)
    df['Action_Description__c'] = df['Action_Description__c'].map(str)
    df['Solution_Description__c'] = df['Solution_Description__c'].map(str)
    df['EFTC_Comments__c'] = df['EFTC_Comments__c'].map(str)
    df['Troubleshooting_Analysis_Status__c'] = df['Troubleshooting_Analysis_Status__c'].map(str)
    df['EMIT_Comments__c'] = df['EMIT_Comments__c'].map(str)
    df['RTS_Comments__c'] = df['RTS_Comments__c'].map(str)
    df['Meeting_Comments__c'] = df['Meeting_Comments__c'].map(str)
    df['Materials_Comments__c'] = df['Materials_Comments__c'].map(str)
    df['Quality_Comments__c'] = df['Quality_Comments__c'].map(str)
    
    #get index of the columns
    index_header = df.columns.get_loc('Header__c')
    index_event = df.columns.get_loc('Event_Description__c')
    index_action = df.columns.get_loc('Action_Description__c')
    index_sulution = df.columns.get_loc('Solution_Description__c')
    index_eftc = df.columns.get_loc('EFTC_Comments__c')
    index_trubleshooting = df.columns.get_loc('Troubleshooting_Analysis_Status__c')
    index_emit = df.columns.get_loc('EMIT_Comments__c')
    index_rts = df.columns.get_loc('RTS_Comments__c')
    index_meeting = df.columns.get_loc('Meeting_Comments__c')
    index_materials = df.columns.get_loc('Materials_Comments__c')
    index_quality = df.columns.get_loc('Quality_Comments__c')
    
    #Regular Expression
    pattern = r'<.*?>'
    
    #Replace the HTML tag <br> to //
    df['Header__c'] = df['Header__c'].replace('<br>', ' // ')
    df['Event_Description__c'] = df['Event_Description__c'].replace('<br>', ' // ')
    df['Action_Description__c'] = df['Action_Description__c'].replace('<br>', ' // ')
    df['Solution_Description__c'] = df['Solution_Description__c'].replace('<br>', ' // ')
    df['EFTC_Comments__c'] = df['EFTC_Comments__c'].replace('<br>', ' // ')
    df['EFTC_Comments__c'] = df['EFTC_Comments__c'].replace('<br>', ' // ')
    df['Troubleshooting_Analysis_Status__c'] = df['Troubleshooting_Analysis_Status__c'].replace('<br>', ' // ')
    df['EMIT_Comments__c'] = df['EMIT_Comments__c'].replace('<br>', ' // ')
    df['RTS_Comments__c'] = df['RTS_Comments__c'].replace('<br>', ' // ')
    df['Meeting_Comments__c'] = df['Meeting_Comments__c'].replace('<br>', ' // ')
    df['Materials_Comments__c'] = df['Materials_Comments__c'].replace('<br>', ' // ')
    df['Quality_Comments__c'] = df['Quality_Comments__c'].replace('<br>', ' // ')
    
    
    
    #Remove the HTML tags
    for row in range(0, len(df)):
        header = re.sub(pattern, '', df.iat[row, index_header], flags=re.IGNORECASE)
        df.iat[row, index_header] = header
    for row in range(0, len(df)):
        event = re.sub(pattern, '', df.iat[row, index_event], flags=re.IGNORECASE)
        df.iat[row, index_event] = event  
    for row in range(0, len(df)):
        action = re.sub(pattern, '', df.iat[row, index_action], flags=re.IGNORECASE)
        df.iat[row, index_action] = action       
    for row in range(0, len(df)):
        solution = re.sub(pattern, '', df.iat[row, index_sulution], flags=re.IGNORECASE)
        df.iat[row, index_sulution] = solution
    for row in range(0, len(df)):
        eftc = re.sub(pattern, '', df.iat[row, index_eftc], flags=re.IGNORECASE)
        df.iat[row, index_eftc] = eftc
    for row in range(0, len(df)):
        trubleshooting = re.sub(pattern, '', df.iat[row, index_trubleshooting], flags=re.IGNORECASE)
        df.iat[row, index_trubleshooting] = trubleshooting
    for row in range(0, len(df)):
        emit = re.sub(pattern, '', df.iat[row, index_emit], flags=re.IGNORECASE)
        df.iat[row, index_emit] = emit
    for row in range(0, len(df)):
        rts = re.sub(pattern, '', df.iat[row, index_rts], flags=re.IGNORECASE)
        df.iat[row, index_rts] = rts 
    for row in range(0, len(df)):
        meeting = re.sub(pattern, '', df.iat[row, index_meeting], flags=re.IGNORECASE)
        df.iat[row, index_meeting] = meeting
    for row in range(0, len(df)):
        materials = re.sub(pattern, '', df.iat[row, index_materials], flags=re.IGNORECASE)
        df.iat[row, index_materials] = materials
    for row in range(0, len(df)):
        quality = re.sub(pattern, '', df.iat[row, index_quality], flags=re.IGNORECASE)
        df.iat[row, index_quality] = quality
    
    #Replace 'None' to empty data 
    df['Header__c'] = df['Header__c'].str.replace('None', '')
    df['Action_Description__c'] = df['Action_Description__c'].str.replace('None', '')
    df['Solution_Description__c'] = df['Solution_Description__c'].str.replace('None', '')
    df['EFTC_Comments__c'] = df['EFTC_Comments__c'].str.replace('None', '')
    df['Troubleshooting_Analysis_Status__c'] = df['Troubleshooting_Analysis_Status__c'].str.replace('None', '')
    df['EMIT_Comments__c'] = df['EMIT_Comments__c'].str.replace('None', '')
    df['RTS_Comments__c'] = df['RTS_Comments__c'].str.replace('None', '')
    df['Meeting_Comments__c'] = df['Meeting_Comments__c'].str.replace('None', '')
    df['Materials_Comments__c'] = df['Materials_Comments__c'].str.replace('None', '')
    df['Quality_Comments__c'] = df['Quality_Comments__c'].str.replace('None', '')
    
    df.to_excel(folder+'/EXPORTED_OOS_DATA_' +
                      re.sub(r'[^A-z0-9_]', '_', datetime.now().isoformat()
                             ) + '.xlsx', index=False)


def get_errors(errors_series, results):
    df = pd.DataFrame(results).apply(pd.Series)
    return errors_series.append(df[df['success'] == False]
                                ['errors'], ignore_index=True)


def upload_modified_sheet(fname):
    sf_api = SFApi()
    sf_api.connect()
    
    isTrue = lambda x: x in [True, 'True', 'Verdadeiro']
    
    converters = {
        'PN_Removal__r.RSPL__c': isTrue,
        'PN_Removal__r.Pool__c': isTrue,
        'Chargeable__c': isTrue
    }
    
    schema = {
        'Id': str,
        'Name': str,
        'Inter_ID__c': str,
        'Flight_Number__c': str,
        'Aircraft_Register__c': str,
        'Operator__c': str,
        'Start_Date__c': 'datetime64[ns]',
        'Start_Time__c': str,
        'Release_Date__c': 'datetime64[ns]',
        'Release_Time__c': str,
        'Header__c': str,
        'Event_Description__c': str,
        'Fail_Code__r.Name': str,
        'Chargeable__c': 'boolean',
        'Exclusion_Code__c': str,
        'Root_Code__r.Name': str,
        'Root_Code__r.ATA__c': str,
        'Root_Code__r.Supplier__r.Name': str,
        'Solution_Description__c': str,
        'Solution_Release_Date__c': 'datetime64[ns]',
        'Issue_Status__c':  str,
        'Dispatched_on_MEL__c':  str,
        'PCR__c': str,
        'EPR__c': str,
        'JIRA__c': str,
        'eFleet__c': str,
        'CMC_Message__c': str,
        'Component_Serial_Number__c': str,
        'Component_Part_Number__c': str,
        'EFTC_Comments__c': str,
        'Troubleshooting_Analysis_Status__c': str,
        'Troubleshooting_Category__c': str,
        'EMIT_Comments__c': str,
        'RTS_Comments__c': str,
        'Meeting_Comments__c': str,
        'PN_Removal__r.Name': str,
        'PN_Removal__r.PN_ON__c': str,
        'PN_Removal__r.SN_OFF__c': str,
        'PN_Removal__r.SN_ON__c': str,
        'PN_Removal__r.RSPL__c': 'boolean',
        'PN_Removal__r.Pool__c': 'boolean',
        'Component_Analysis_Status__c': str,
        'Component_Category__c': str,
        'Materials_Comments__c': str,
        'Quality_Investigation_Status__c': str,
        'Quality_Item_Classification__c': str,
        'Corrective_Action_Document__c': str,
        'Quality_Comments__c': str
    }
    
    df = pd.read_excel(fname, dtype = schema, converters=converters)
    dates_cols = [key for key, value in schema.items() if value == 'datetime64[ns]']
    for col in dates_cols:
        df[col] = df[col].dt.date.fillna('').astype(str)
    
    df.rename(columns={
            'Id': 'Out_of_service__c',
            'Root_Code__r.Supplier__r.Name': 'Root_Code__r.Supplier__c'
        }, inplace=True)
    
    
    sf_api.update_oos(df)
    sf_api.upsert_fail_codes(df)
    sf_api.upsert_root_codes(df)
    sf_api.upsert_pn_removals(df)

    return ['SUCCESS:DATA WAS UPLOADED TO SF']


# download_records_as_sheet('//flmfs05/vss/suporte_tecnico/FPR/6 - DADOS OPERACIONAIS/1 - OPERADORES/2 - Dados trabalhados/11 - OOS/OOS_SALESFORCE_APP_v2', '01/09/2020')
# upload_modified_sheet('//fmlfs05/vss/suporte_tecnico/FPR/6 - DADOS OPERACIONAIS/1 - OPERADORES/2 - Dados trabalhados/11 - OOS/OOS_SALESFORCE_APP_v2/EXPORTED_OOS_DATA_2021_03_15T09_29_51_888213.xlsx')
