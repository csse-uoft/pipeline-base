from owlready2 import default_world
from email import charset
import tqdm, re
import pandas as pd
from ..utils import read_csv, write_csv, global_db, get_instance, get_blank_instance
from mapping.namespaces import PREFIX, TURTLE_PREAMBLE, namespaces, prop_ranges_preset, compass
exec(f"from mapping.namespaces import {','.join([nm for nm in namespaces.keys() if nm != ''])}")
from ..translator import generate_organization, generate_service, generate_address, escape_str, \
    generate_program, generate_funding, map_service_codes, map_client_codes
from ..codes import sm_tag2uoft_codes, sm_tag2sm_code, normalize_sm_tag, \
    map_language, map_ownership, expands_and_donation_type

def load_fundings(fundings):
    # build funding
    for _, funding in tqdm.tqdm(fundings.iterrows(), total=fundings.shape[0]):
        _ = generate_funding(funding) 


def load_financials(financials):
    print("Generating financials...")
    for _,row_data in tqdm.tqdm(financials.iterrows(), total=financials.shape[0]):
        # break
        organization = generate_organization(row_data)

        charity_types = [5000]
        charity_client_codes = []
        for value in row_data[['Category English Desc','Sub-Category English Desc']].dropna().tolist():
            tags = map_client_codes(value)
            charity_client_codes += [get_instance(klass='cids.Code', inst_id=tag) for tag in tags]

        # Federal Gov
        fed_gov = row_data[['Country']]
        fed_gov = fed_gov.rename({'Country':'BN/Registration Number'})
        fed_gov['Name'] = 'Federal Government of Canada'
        fed_gov['Legal Name'] = fed_gov['Name']
        fed_gov['org:has_Ownership'] = 'government_owned'
        fed_org = generate_organization(fed_gov, klass='org.GovernmentOrganization')
        fund_tmp = pd.Series({'Fiscal year':row_data['Fiscal year'], 'receivedAmount':row_data['FedGov_Revenue']}, dtype=object)
        fund_fed = generate_funding(fund_tmp, donor_org=fed_org, donee_org=organization)

        # Provincial Gov
        prov_gov = row_data[['Country','Province']]
        prov_gov['BN/Registration Number'] = '_'.join(prov_gov[['Country','Province']].dropna().values)
        prov_gov['Name'] = f"Government of {prov_gov['Province']}"
        prov_gov['Legal Name'] = prov_gov['Name']
        prov_gov['org:has_Ownership'] = 'government_owned'
        prov_org = generate_organization(prov_gov, klass='org.GovernmentOrganization')
        fund_tmp = pd.Series({'Fiscal year':row_data['Fiscal year'], 'receivedAmount':row_data['ProvGov_Revenue']},dtype=object)
        fund_prov = generate_funding(fund_tmp, donor_org=prov_org, donee_org=organization)


        # Municipal Gov
        mun_gov = row_data[['Country','Province', 'City']]
        mun_gov['BN/Registration Number'] = '_'.join(mun_gov[['Country','Province','City']].dropna().values)
        mun_gov['Name'] = f"City of {mun_gov['City']}"
        mun_gov['Legal Name'] = mun_gov['Name']
        mun_gov['org:has_Ownership'] = 'government_owned'
        mun_org = generate_organization(mun_gov, klass='org.GovernmentOrganization')
        fund_tmp = pd.Series({'Fiscal year': row_data['Fiscal year'], 'receivedAmount':row_data['MunGov_Revenue']}, dtype=object)
        fund_mun = generate_funding(fund_tmp, donor_org=mun_org, donee_org=organization)
            # fund_mun = generate_funding(fund_tmp, donor_org=organization, donee_org=organization, donor_program=expenses_program, donee_program=program)


        #-------------------------------------------------
        # process expenses, by code.
        #-------------------------------------------------

        # main program for funding other sub-programs
        code_tags = expands_and_donation_type['0']
        program_name = '_'.join([re.sub(r".*EXPENSES\-(.+)\-[0-9]+", r"\1",normalize_sm_tag(str(code_tag))) for code_tag in code_tags])
        # program_id = f"{re.sub(r'.*organization_', '',str(organization))}_{program_name}"

        tmp = pd.Series({'Name':program_name}, dtype=object)
        expenses_program = generate_program(tmp, organization, program_name=program_name, is_blank=True, beneficiary_code_tags=code_tags)

        address = global_db[organization['ic.hasAddress'][0]] if len(organization['ic.hasAddress'])>0 else None
        # individual programs that received funding they spend
        for c in row_data.index:
            if str(c).strip() in expands_and_donation_type.keys():
                amount = row_data[c]
                service_codes = []
                for code_tag in expands_and_donation_type[str(c).strip()]:
                    service_code = get_instance(klass='cids.Code', inst_id=code_tag)
                    if c in charity_types:
                        service_codes.append(service_code)
                        client_codes = charity_client_codes
                    else:
                        service_codes = []
                        client_codes = []

                program_name = '_'.join([re.sub(r".*EXPENSES\-(.+)\-[0-9]+", r"\1",normalize_sm_tag(str(code_tag['ID']))) for code_tag in service_codes])
                # program_id = f"{re.sub(r'.*organization_', '',str(organization))}_{program_name}"
                tmp = pd.Series({'Name':program_name}, dtype=object)
                # profile = cProfile.Profile()
                # with cProfile.Profile() as profile:
                program = generate_program(tmp, organization, program_name=program_name, is_blank=False, address=address, beneficiary_codes=client_codes)
                # ps = pstats.Stats(profile)
                # ps.sort_stats(pstats.SortKey.TIME)
                # ps.dump_stats("generate_program.prof") #

                # profile = cProfile.Profile()
                # with cProfile.Profile() as profile:
                service = generate_service(f"service_{program_name}", organization, program, address=address, service_codes=service_codes, client_codes=client_codes)
                # ps = pstats.Stats(profile)
                # ps.sort_stats(pstats.SortKey.TIME)
                # ps.dump_stats("generate_service.prof") #


                # profile = cProfile.Profile()
                # with cProfile.Profile() as profile:
                fund_tmp = pd.Series({'Fiscal year':row_data['Fiscal year'], 'receivedAmount':amount, 'requestedAmount':amount},dtype=object)
                fund_mun = generate_funding(fund_tmp, donor_org=organization, donee_org=organization, donor_program=expenses_program, donee_program=program)
                # ps = pstats.Stats(profile)
                # ps.sort_stats(pstats.SortKey.TIME)
                # ps.dump_stats("generate_funding.prof") #
