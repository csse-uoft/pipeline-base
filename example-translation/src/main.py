#############################################
# Main file for the pipeline.
# setup configuration variables to configure environment, input directory, and output direcotry.
#   file: config.yml
import config

import os
LOG_FILE = f"logging_{config.YEAR}.txt"    # Log file used to log errors or warnings that should not stop the processing (e.g. invalid address is found)
if os.path.exists(LOG_FILE):
    os.remove(LOG_FILE)

# Set where data exists
if config.IN_DIR.startswith('~/'):
    IN_PATH = os.path.expanduser("~") + config.IN_DIR.replace('~/','/')
else:
    IN_PATH = config.IN_DIR
# Set where output files should be created
OUT_PATH = IN_PATH + config.OUT_DIR
_ = os.makedirs(OUT_PATH) if not os.path.exists(OUT_PATH) else None


from mapping.utils import global_db
import pandas as pd, tqdm, numpy as np

from mapping.namespaces import namespaces, compass
exec(f"from mapping.namespaces import {','.join([nm for nm in namespaces.keys() if nm != ''])}")

from mapping.utils import read_csv, save_global_db, load_global_db, save_db_as_ttl, format_strptime

from mapping.translator import generate_organization
from mapping.logics import load_fundings,load_financials

#------------------------------------------------------------------------
# here for debugging purposes, imports from other files
#------------------------------------------------------------------------
# import re, collections, uuid

# from mapping.translator import generate_organization, generate_funding, generate_program, generate_service,\
#     map_service_codes, map_client_codes, generate_address
# from mapping.codes import sm_tag2uoft_codes, sm_tag2sm_code, normalize_sm_tag, ClientCode, ServiceCode,\
#    map_language, map_ownership, expands_and_donation_type, strip_nm, normalize_code

# from mapping.utils import escape_str, get_instance, get_blank_instance, get_instance_label, default_to_regular

if __name__ == '__main__':


    if config.LOAD_DATA:
        # TODO: import CSV and map columns
        # 1. organizations
        organization_mapping = {
            'orgID'     : 'BN/Registration Number',
            'desc'      : 'org:hasDescription',
            'url'       : 'cids:hasWebAddress',
            'regDate'   : 'sch:dateCreated',
            'name'      : 'Name',
            'address'   : 'Mailing address',
            'city'      : 'City',
            'province'  : 'Province',
            'postal'    : 'Postal code',
        }
        organizations = pd.DataFrame(read_csv(IN_PATH + '/example-organizations.csv'))
        organizations['regDate'] = organizations['regDate'].apply(lambda d: format_strptime(d))
        organizations = organizations.rename(columns=organization_mapping)
        organizations['Legal Name'] = organizations['Name']
        organizations['Country'] = 'Canada'
        print("Done loading orgnizations.")


        # 2. fundings
        fundings_mapping = {
            'from':'BN/Registration Number',
            'to':'Donee BN/Registration Number',
            'received':'receivedAmount',
            'requested':'requestedAmount',
        }
        fundings = pd.DataFrame(read_csv(IN_PATH + '/example-gifts.csv'))
        fundings = fundings.rename(columns=fundings_mapping)
        fundings = fundings.merge(organizations[['BN/Registration Number', 'Name','Legal Name']], on=['BN/Registration Number']). \
            rename(columns={'BN/Registration Number':'Donor BN/Registration Number', 'Name':'Donor Name', 'Legal Name':'Donor Legal Name'})
        fundings = fundings.merge(organizations[['BN/Registration Number', 'Name','Legal Name']], left_on=['Donee BN/Registration Number'], right_on=['BN/Registration Number']). \
            rename(columns={'Name':'Donee Name', 'Legal Name':'Donee Legal Name'})
        fundings = fundings.drop(columns=['BN/Registration Number']).rename(columns={'Donor BN/Registration Number':'BN/Registration Number'})
        fundings['Fiscal year'] = config.YEAR



        # 3. financials (CRA data)
        print("Loading financials file...")
        financials = pd.DataFrame(read_csv(IN_PATH + '/example-financials.csv'))
        # financials = .rename(columns=fundings_mapping)
        financials = financials.merge(organizations, on=['BN/Registration Number'])
        financials['Fiscal year'] = config.YEAR
        financials['Category English Desc'] = np.nan
        financials['Sub-Category English Desc'] = np.nan

        with compass:
            print("Generating organizations...")
            # TODO: convert Dataframe to OWL concepts
            for _,row_data in tqdm.tqdm(organizations.iterrows(), total=organizations.shape[0]):
                generate_organization(row_data)

            load_fundings(fundings)

            load_financials(financials)

            save_global_db(filename=OUT_PATH+f"/global_db_{config.YEAR}.pickle")

    else:
        print("Loading global_db...")
        load_global_db(filename=OUT_PATH+f"/global_db_{config.YEAR}.pickle")
 
    print("Saving to ttl file...")
    if config.DEBUG:
        save_db_as_ttl(filename=OUT_PATH + f"/knowledge_graph_example_debug_{config.YEAR}.ttl")

    else:

        if config.BATCHES <=1:
            # write everything as one file
            fileout = OUT_PATH + f"/knowledge_graph_example_{config.YEAR}.ttl"
            save_db_as_ttl(filename=fileout)
            if config.ZIP_TTL:
                os.system("gzip -c \""+fileout + "\" > \""+fileout + ".gz\"")
        else:
            # split graph into multiple files
            from mapping.utils import global_db
            global_db_hold = global_db.copy()
            global_db_keys = list(global_db_hold.keys())
            batch_size = round(len(global_db_keys) / config.BATCHES)
            for i,b in enumerate(range(0,len(global_db_keys)+1, batch_size+1)):
                print(i,b, b+batch_size+1)
                tmp_db = dict(zip(global_db_keys[b:b+batch_size+1], list(global_db_hold.values())[b:b+batch_size+1]))
                fileout = OUT_PATH + f"/knowledge_graph_example_{i}_{config.YEAR}.ttl"
                save_db_as_ttl(filename=fileout, dict_db=tmp_db)
                if config.ZIP_TTL:
                    os.system("gzip -c \""+fileout + "\" > \""+fileout + ".gz\"")

