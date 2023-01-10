import json
import pandas as pd
import numpy as np
from simple_salesforce import Salesforce, format_soql
import os
import re
from time import sleep

pd.set_option('display.max_columns', 5)
path = r'\\flmfs05\\VSS\\suporte_tecnico\\FPR\\6 - DADOS OPERACIONAIS\\1 - OPERADORES\\2 - Dados trabalhados\\11 - OOS\\OOS_SALESFORCE_APP_v2\\settings\\preferences.json'
preference_fname = os.path.abspath(path)


def read_preferences():
    preferences = {}
    if os.path.isfile(preference_fname):
        preferences = json.load(open(preference_fname))
    return preferences


def save_preferences(preferences):
    with open(preference_fname, 'w') as f:
        json.dump(preferences, f)


def dict_from_df(df):
    if isinstance(df, pd.Series):
        df = df.to_frame()
    records_dict = df.to_dict('records')
    for record in records_dict:
        coppied_record = record.copy()
        for key, value in coppied_record.items():
            if value is None or value == '' or pd.isna(value):
                del record[key]
    return records_dict


def records_message_error(df, main_sObject):
    renamed_df = df.rename(columns={'Id': 'id'})
    
    if isinstance(renamed_df['id'], pd.DataFrame):
        ids = renamed_df['id'].iloc[:, 0]
        renamed_df.drop(columns=['id'], inplace=True)
        renamed_df['id'] = ids
        
    if pd.isna(renamed_df['id']).sum() > 0:
        print('The following {} records could NOT be sent to SF'.format(
            main_sObject))

        renamed_df['errors'] = renamed_df['errors'].apply(
            lambda x: x[0]['message'] if len(x) > 0 else x)
        print(renamed_df[pd.isna(renamed_df['id'])]['errors'])
        print()


class SFApi:

    def __init__(self):
        self.sf = None

    @staticmethod
    def _normalize_records(df):
        if not isinstance(df, pd.Series):
            df = pd.DataFrame(df)

        df = df.apply(pd.Series)
        if len(df) > 0:
            df.drop(labels=['attributes'], axis=1, inplace=True)
        else:
            df = None
        return df
    '''
    @staticmethod
    def _get_domain(org):
        if org == 'prod':
            return 'login'
        elif org == 'qa':
            return 'testqa'
        else:
            return 'testdev'
    
    def connect(self):
        self.sf = None
        preferences = read_preferences()
        message = []
        domain = self._get_domain(preferences['variable_radio'])
        
        if domain == 'login':
            try:
                self.sf = Salesforce(instance_url='https://commercialaviation.my.salesforce.com/',
                                     session_id=preferences['entry_token_security'])
                message.append("LOGIN:SUCCESS")
            except Exception:
                message.append('LOGIN:FAILED')
                message.append('PROBLEM:MAYBE YOUR CREDENTIALS ARE INCORRECT!')
    
            return message
        elif domain == 'testqa':
            try:
                self.sf = Salesforce(instance_url='https://commercialaviation--qa.my.salesforce.com/',
                                     session_id=preferences['entry_token_security'])
                message.append("LOGIN:SUCCESS")
            except Exception:
                message.append('LOGIN:FAILED')
                message.append('PROBLEM:MAYBE YOUR CREDENTIALS ARE INCORRECT!')
    
            return message
        else:
            try:
                self.sf = Salesforce(instance_url='https://commercialaviation--fltperf.my.salesforce.com/',
                                     session_id=preferences['entry_token_security'])
                message.append("LOGIN:SUCCESS")
            except Exception:
                message.append('LOGIN:FAILED')
                message.append('PROBLEM:MAYBE YOUR CREDENTIALS ARE INCORRECT!')
    
            return message
    '''
    @staticmethod
    def _get_domain(org):
        if org == 'prod':
            return 'login'
        elif org == 'qa':
            return 'testqa'
        else:
            return 'testdev'
    
    def connect(self):
        self.sf = None
        preferences = read_preferences()
        message = []
        d = self._get_domain(preferences['variable_radio'])
        
        if d == 'login':
            try:
                self.sf = Salesforce(username=preferences['entry_username'],
                                    password=preferences['entry_password'],
                                    security_token=preferences['entry_token_security'],
                                    domain=self._get_domain(preferences['variable_radio']))
                message.append("LOGIN:SUCCESS")
            except Exception:
                message.append('LOGIN:FAILED')
                message.append('PROBLEM:MAYBE YOUR CREDENTIALS ARE INCORRECT!')

            return message
        elif d == 'testqa':
            
            try:
                self.sf = Salesforce(instance_url='https://commercialaviation--qa.my.salesforce.com/',
                                     session_id=preferences['entry_token_security'])
                message.append("LOGIN:SUCCESS")
            except Exception:
                message.append('LOGIN:FAILED')
                message.append('PROBLEM:MAYBE YOUR CREDENTIALS ARE INCORRECT!')
    
            return message
            '''
            try:
                self.sf = Salesforce(username=preferences['entry_username'],
                                    password=preferences['entry_password'],
                                    security_token=preferences['entry_token_security'],
                                    domain=self._get_domain(preferences['variable_radio']))
                message.append("LOGIN:SUCCESS")
            except Exception:
                message.append('LOGIN:FAILED')
                message.append('PROBLEM:MAYBE YOUR CREDENTIALS ARE INCORRECT!')
            return message
            '''
        else:
            try:
                self.sf = Salesforce(instance_url='https://commercialaviation--fltperf.my.salesforce.com/',
                                     session_id=preferences['entry_token_security'])
                message.append("LOGIN:SUCCESS")
            except Exception:
                message.append('LOGIN:FAILED')
                message.append('PROBLEM:MAYBE YOUR CREDENTIALS ARE INCORRECT!')
    
            return message   
         
    
    def _flat_relation_columns(self, df_records):
        for col in df_records.columns:
            if col[-3:] == '__r':
                s = self._normalize_records(
                    df_records[col]).add_prefix(col+'.')
                s = self._flat_relation_columns(s)
                df_records.drop(columns=[col], inplace=True)
                df_records = pd.concat(
                    [df_records, s], axis=1, sort=False)
            
        return df_records
        
    def get_all_fields(self, obj_api, just_editable=False, just_custom=False):
        fields = pd.DataFrame(getattr(self.sf, obj_api).describe()['fields'])
        if just_editable:
            fields = fields[fields.calculated == False]
        if just_custom:
            fields = fields[fields.custom == True]
        
        return fields.name.values.tolist()
        
    @staticmethod
    def _create_empty_df(soql):
        df_records = pd.DataFrame()
        for col in re.findall(r'SELECT ([\s\S]*?) FROM ', soql)[0].split(','):
            df_records[col.replace('\n', '').replace(
                '\t', '').strip()] = np.nan
            
        return df_records
    
    @staticmethod
    def _discover_bulk_obj_api(soql):
        return re.findall(r'SELECT [\s\S]*? FROM ([A-z_]+)', soql, re.IGNORECASE)[0]
    
    def query(self, soql):
        obj_api = self._discover_bulk_obj_api(soql)
        df_records = pd.Series(getattr(self.sf.bulk, obj_api).query(soql)).apply(pd.Series)
        df_records.drop(columns=['attributes'], inplace=True)
        if len(df_records) > 0:
            return self._flat_relation_columns(df_records)
        else:
            return self._create_empty_df(soql)

    def upsert(self, obj_api, data_dict, what='Id'):
        return getattr(self.sf.bulk, obj_api).upsert(data_dict, what, batch_size=5000)

    def update(self, obj_api, data_dict):
        return getattr(self.sf.bulk, obj_api).update(data_dict, batch_size=5000)
    
    def delete(self, obj_api, data_dict):
        return getattr(self.sf.bulk, obj_api).delete(data_dict, batch_size=5000)

    def insert(self, obj_api, data_dict):
        # sobject = SFType(obj_api, self.sf.session_id, self.sf.sf_instance)
        return getattr(self.sf.bulk, obj_api).insert(data_dict, batch_size=5000)
    
    def query_associations_between_objects(self, child_obj, parent_obj, association_obj,
                                           id_limits=None):
        if id_limits is not None:
            return self.query(
                format_soql(
                    'SELECT {:literal}, {:literal} FROM {:literal} WHERE {:literal} IN {}',
                    parent_obj, child_obj, association_obj, parent_obj, id_limits))
        else:
            return self.query(
                format_soql(
                    'SELECT {:literal}, {:literal} FROM {:literal}',
                    parent_obj, child_obj, association_obj))
    
        
    def get_new_associations_between_sf_and_df(self, df, child_obj, parent_obj, association_obj,
                                               info_new_to_whom='df'):
        sf_associations = self.query_associations_between_objects(
            child_obj, parent_obj, association_obj, df[parent_obj].values.tolist())
        
        df_associations = df[[parent_obj, child_obj]].dropna()
        
        if info_new_to_whom == 'df':
            how = 'left'
            sf_associations['new_associations_returns_nan'] = False
        else:
            how = 'right'
            df_associations['new_associations_returns_nan'] = False
            sf_associations['test'] = False
        
        new_associations = df_associations.merge(
            sf_associations, on=[child_obj, parent_obj], how=how)

        return new_associations[
            pd.isna(new_associations.new_associations_returns_nan)][[parent_obj, child_obj]]


    def insert_association_between_objects(self, df, child_obj, parent_obj, association_obj=None):
        if association_obj is None:
            child_obj_id = 'Id'
            association_obj = child_obj
        else:
            child_obj_id = child_obj
            
        new_associations = self.get_new_associations_between_sf_and_df(
            df, child_obj_id, parent_obj, association_obj)
        
        if len(new_associations) > 0:
            results = self.insert(association_obj, dict_from_df(
                new_associations))
            
            results_df = pd.Series(results).apply(pd.Series)
            records_message_error(results_df, association_obj) 
        
    
    def _upsert_set_columns(self, child_obj_df, child_obj, parent_obj='Out_of_service__c',
                            association_obj=None, external_id='Name', 
                            parent_field_on_child=False):
            
        child_obj_df['Name'] = child_obj_df['Name'].replace({'': np.nan})
        child_obj_df.dropna(subset=['Name'], inplace=True)
        
        unique_child_obj_df = child_obj_df.drop_duplicates(subset=[external_id])
        
        unecessary_id_fields = [child_obj]
        if not parent_field_on_child:
            unecessary_id_fields.append(parent_obj)
            
        dict_records = dict_from_df(
            unique_child_obj_df.drop(columns=unecessary_id_fields, errors='ignore'))
            
        results = self.upsert(child_obj, dict_records, external_id)
        
        df_with_created_ids = pd.concat([pd.Series(results).apply(pd.Series),
                                        unique_child_obj_df.reset_index(drop=True)],
                                        axis=1)
    
        records_message_error(df_with_created_ids, child_obj)
    
        if external_id.lower() != 'id':    
    
            map_dict = df_with_created_ids[['id', 'Name']]
            
            map_dict = map_dict[~pd.isna(map_dict['id'])]\
                        .set_index('Name', drop=True)\
                        .iloc[:, 0]\
                        .to_dict()
        
            child_obj_df[child_obj] = child_obj_df['Name']\
                                        .replace(map_dict)\
                                        .rename(child_obj)
                                        
            if association_obj is not None:
                self.insert_association_between_objects(
                    child_obj_df, parent_obj, child_obj, association_obj)
                
        return child_obj_df
    
    
    @staticmethod
    def _normalize_ata(x):
        if pd.isna(x) or x == '':
            return ''
        else:
            x = str(int(float(x)))
            if len(x) == 1:
                x = '0' + x
            return x
        
        
    def df_with_child_and_parent_objects(self, df, child_obj, parent_obj):
        parent_ids = df[parent_obj]
        df = df[[
                c for c in df.columns if child_obj[:-3] + '__r' in c
            ]].copy()
        df.columns = [c.split('.')[-1] for c in df.columns]
        df[parent_obj] = parent_ids
        
        return df.rename(columns={'Id': child_obj})
    
    
    def delete_sf_associations(self, child_df, child_name, parent_obj, association_obj):
        sf_associations = self.query(
            format_soql(
                '''SELECT Id, {:literal}, {:literal} FROM {:literal} 
                WHERE {:literal} IN {}''',
                child_name, parent_obj, association_obj, parent_obj,
                child_df[parent_obj].values.tolist()))
        
        if len(sf_associations) == 0:
            return
        
        if child_name == 'Name':
            sf_associations.rename(columns={child_name: 'sf_' + child_name}, inplace=True)
            child_name = 'sf_' + child_name
        
        associations_product_left = sf_associations.merge(
            child_df, on=[parent_obj], how='left')
        
        sf_slice = associations_product_left[['Id', parent_obj, child_name]].fillna('')
        df_slice = associations_product_left[['Id', parent_obj, 'Name']]\
            .rename(columns={'Name': child_name}).fillna('')
        
        merged_associations = sf_slice.merge(
            df_slice, on=[parent_obj, child_name])
        
        merged_associations['hasRelation'] = True
        
        results = associations_product_left.merge(
            merged_associations, on=[parent_obj, child_name], how='left')
        
        results['hasRelation'] = results['hasRelation'].fillna(False)
        
        associations_to_delete = results[results.hasRelation == False]['Id'].drop_duplicates()
        
        if len(associations_to_delete) > 0:
            results = self.delete(association_obj, dict_from_df(associations_to_delete))

    
    def update_oos(self, sf_df):
        
        unique_oos_df = sf_df.drop_duplicates(subset=['Out_of_service__c'])\
            .rename(columns={'Out_of_service__c': 'Id'}).drop(columns='Name')
        
        oos_fields = [col for col in unique_oos_df.columns if '__r.' not in col]
        
        described_oos_fields = pd.Series(
            self.sf.Out_of_service__c.describe()['fields']).apply(pd.Series)
        
        fields_to_remove = described_oos_fields[described_oos_fields.calculated == True].name
        
        for col in fields_to_remove:
            if col in oos_fields:
                oos_fields.remove(col)  
                
        
        results = self.update('Out_of_service__c', dict_from_df(unique_oos_df[oos_fields]))
        print(pd.DataFrame(results))
        pd.DataFrame(results).to_csv('ERRO_ENVIO_UPDATE.csv')
        
    def upsert_pn_removals(self, sf_df):
        pn_removal_df = self.df_with_child_and_parent_objects(
            sf_df, 'PN_Removal__c', 'Out_of_service__c'
        )
        
        pn_removal_df[[
            'Name', 'PN_ON__c', 'SN_OFF__c', 'SN_ON__c'
        ]] = pn_removal_df[['Name', 'PN_ON__c', 'SN_OFF__c', 'SN_ON__c']]\
                .replace(r'^[A-z\\\/\- ]+$', np.nan, regex=True)
                
        pn_removal_df['RSPL__c'] = pn_removal_df['RSPL__c']      
        pn_removal_df['Pool__c'] = pn_removal_df['Pool__c']
        
        self.delete_sf_associations(
            pn_removal_df, 'Name', 'Out_of_service__c', 'PN_Removal__c')
        
        
        if (~((pd.isna(pn_removal_df['Name'])) | (pn_removal_df['Name'] == ''))).sum() > 0:
            sf_pn_removal = self.query(
                format_soql('''SELECT Id, Name, Out_of_service__c FROM PN_Removal__c
                            WHERE Out_of_service__c IN {}''', 
                            pn_removal_df.Out_of_service__c.values.tolist()))
            
            pn_removal_df = pn_removal_df.merge(sf_pn_removal,
                                                on=['Name', 'Out_of_service__c'],
                                                how='left')
            
            
            self._upsert_set_columns(pn_removal_df, 'PN_Removal__c', 'Out_of_service__c',
                                     external_id='Id', parent_field_on_child=True)
    
    
    def upsert_fail_codes(self, sf_df):
        fail_code_df = self.df_with_child_and_parent_objects(
            sf_df, 'Fail_Code__c', 'Out_of_service__c'
        )
                                                
        if 'Technology__c' in fail_code_df.columns:
            fail_code_df.drop(columns=['Technology__c'], inplace=True)
        
        self.delete_sf_associations(
            fail_code_df, 'Fail_Code__r.Name', 'Out_of_service__c', 'FC_OOS_Association__c')
        
        if (~((pd.isna(fail_code_df['Name'])) | (fail_code_df['Name'] == ''))).sum() > 0:          
            fail_code_df = self._upsert_set_columns(
                fail_code_df, 'Fail_Code__c', association_obj='FC_OOS_Association__c')
        
    
    def upsert_root_codes(self, sf_df):
        root_code_df = self.df_with_child_and_parent_objects(
            sf_df, 'Root_Code__c', 'Out_of_service__c')

        root_code_df[[
            'Name', 'Supplier__c'
        ]] = root_code_df[['Name', 'Supplier__c']]\
                .fillna('')\
                .astype(str)\
                .applymap(lambda x: x.upper().strip())\
                .replace({'NOT CLASSIFIED': np.nan,
                          'N/A': np.nan,
                          0: np.nan,
                          '0': np.nan,
                          'NI': np.nan})\
                .fillna('')
                
        root_code_df['ATA__c'] = root_code_df['ATA__c'].apply(self._normalize_ata)
    
        # adding supplier
        if (~((pd.isna(root_code_df['Supplier__c'])) | (root_code_df['Supplier__c'] == ''))).sum() > 0: 
            supplier_df = self._upsert_set_columns(
                root_code_df[['Supplier__c']].rename(columns={'Supplier__c': 'Name'}),
                'Supplier__c', 'Root_Code__c')
            root_code_df['Supplier__c'] = root_code_df['Supplier__c']\
                                            .replace(supplier_df\
                                                         .set_index('Name', drop=True)\
                                                         .iloc[:, 0]\
                                                         .to_dict())
                                                
        # adding root codes
        if 'Technology__c' in root_code_df.columns:
            root_code_df.drop(columns=['Technology__c'], inplace=True)
        
        self.delete_sf_associations(
            root_code_df, 'Root_Code__r.Name', 'Out_of_service__c', 'RC_OOS_Association__c')
        
        if (~((pd.isna(root_code_df['Name'])) | (root_code_df['Name'] == ''))).sum() > 0:        
            sf_df = self._upsert_set_columns(
                root_code_df,
                'Root_Code__c',
                association_obj='RC_OOS_Association__c')
