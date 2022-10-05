import re
import pandas as pd
from mapping.namespaces import PREFIX, TURTLE_PREAMBLE, namespaces, prop_ranges_preset, compass
exec(f"from mapping.namespaces import {','.join([nm for nm in namespaces.keys() if nm != ''])}")
from .codes import sm_tag2uoft_codes, sm_tag2sm_code, normalize_sm_tag,normalize_code, strip_nm, \
    ClientCode, ServiceCode,map_language, map_ownership, expands_and_donation_type, loaded_org_codes

from .utils import escape_str, get_instance, get_blank_instance, logger, global_db
from datetime import datetime, timedelta

# translations for street types to OWL
street_type_str2ic_street_type = {
    'ave': ic.Avenue,
    'avenue': ic.Avenue,
    "l'avenue": ic.Avenue,
    'blvd': ic.Boulevard,
    'boul': ic.Boulevard,
    'boulevard': ic.Boulevard,
    'cir': ic.Circle,
    # 'square':ic.Circle,
    # 'sq':ic.Circle,
    'cres': ic.Crescent,
    'dr': ic.Drive,
    'rd': ic.Road,
    'road': ic.Road,
    'st': ic.Street,
    'street': ic.Street,
    'str': ic.Street,

}

# transalation for street direction types for OWL
street_direction_str2ic = {
    's': ic.south,
    'south':ic.south,
    'sud':ic.south,
    'w': ic.west,
    'west':ic.west,
    'ouest':ic.west,
    'n': ic.north,
    'north': ic.north,
    'nord':ic.north,
    'e': ic.east,
    'east':ic.east,
    'est':ic.east,
}

# trnlsation for province types to OWL
canada_provinces = {
    "AB": "Alberta",
    "BC": "British Columbia",
    "MB": "Manitoba",
    "NB": "New Brunswick",
    "NL": "Newfoundland and Labrador",
    "NS": "Nova Scotia",
    "ON": "Ontario",
    "PE": "Prince Edward Island",
    "QC": "Quebec",
    "SK": "Saskatchewan",
    "NT": "Northwest Territories",
    "NU": "Nunavut",
    "YT": "Yukon"
}
canada_provinces_regex = ''
for short_name, province in canada_provinces.items():
    canada_provinces_regex += f'{short_name}|{province}|'
canada_provinces_regex = canada_provinces_regex[:-2]


def generate_address(row_data):
    """
    convert metadata to Address object
    :param row_data : dict with metadata
                Exepcting: # Mailing address	City	Province	Postal code	Country
    :return address : dict of address object
    """
    
    addr_data1 = row_data.get('Mailing address') or '' if not pd.isnull(row_data.get('Mailing address')) else ''
    city = row_data.get('City') or ''  if not pd.isnull(row_data.get('City')) else ''
    state = row_data.get('Province') or ''  if not pd.isnull(row_data.get('Provnice')) else ''
    postal_code = row_data.get('Postal code') or ''  if not pd.isnull(row_data.get('Postal code')) else ''

    street_direction = None
    street_type = None
    street_number = None
    street_name = None
    unit_number = None
    street = ''

    if addr_data1 and not pd.isnull(addr_data1):
        # Extract postal code from addr_data1
        if re.search(r"^[0-9]+ *\- *[0-9]+",addr_data1) is not None:
            parts = re.search(r"^([0-9]+) *\- *([0-9]+)(.*)",addr_data1)
            unit_number = parts[1]
            street_number = parts[2]
            street = parts[3].strip()

        if re.search(r"^[0-9]+[a-zA-Z]*[ ,]+(.*)",addr_data1) is not None:
            parts = re.search(r"^([0-9]+[a-zA-Z]*)[ ,]+(.*)",addr_data1)
            unit_number = ''
            street_number = parts[1]
            street = parts[2].strip()

        if re.search(r"^[^0-9]",addr_data1) is not None:
            street = addr_data1.strip()
            unit_number = ''
            street_number = ''


        split_address = addr_data1.split(',')
        split_address = street.split(',')
        # Trim all white spaces
        split_address = [x.strip() for x in split_address]
        for data in split_address:
            if data == '':
                split_address.remove(data)


        # Now the split_address should have a length of 1, if not, the last item is the city name
        if len(split_address) == 2:
            city = split_address.pop(-1)
        if len(split_address) > 1:
            logger(f'Error 3 parsing address {addr_data1} for {row_data["BN/Registration Number"]}')
            # caught an error with the address, mae a logger entry and return None; wil not stop process.
            return None
        if len(split_address) == 1:
            street = split_address[0]
            street_name = street
            split_street = [x.strip() for x in street.split(' ')]
            
            re_dir = re.compile("^"+'|'.join(street_direction_str2ic.keys())+"$", re.IGNORECASE)
            if re.match(re_dir, split_street[-1]):
                street_direction = split_street.pop(-1)
            re_type = re.compile("^"+"|".join(street_type_str2ic_street_type.keys())+"$", re.IGNORECASE)
            if len(split_street) > 0 and re.match(re_type, split_street[-1]):
                street_type = split_street.pop(-1)
            elif len(split_street) > 0 and re.match(re_type, split_street[0]):
                street_type = split_street.pop(0)
            street_name = ' '.join(split_street)


    address_props = {}
    address_props['ic.hasCountry'] = 'ic.canada'
    if state and (re.match(canada_provinces_regex, state) is not None):
        if state.upper() in canada_provinces.keys():
            state = canada_provinces[state.upper()]
        state = state.title().replace('And',' ').replace(' ','')
        state = state[0].lower() + state[1:]
        address_props['ic.hasState'] = str(ic[state])
    if city:
        address_props['ic.hasCityS'] = city
    if row_data.get('Latitude') and not pd.isnull(row_data['Latitude']):
        address_props['address.lat'] = float(row_data['Latitude'])
    if row_data.get('Longitude') and not pd.isnull(row_data['Longitude']):
        address_props['address.long'] = float(row_data['Longitude'])
    if postal_code:
        address_props['ic.hasPostalCode'] = postal_code
    if street_name:
        address_props['ic.hasStreet'] = street_name
    if street_number:
        address_props['ic.hasStreetNumber'] = street_number
    if unit_number:
        address_props['ic.hasUnitNumber'] = unit_number

    # TODO: Support more street type and direction
    if street_type and street_type_str2ic_street_type.get(street_type.lower()):
        address_props['ic.hasStreetType'] = str(street_type_str2ic_street_type[street_type.lower()])
    if street_direction and street_direction_str2ic.get(street_direction.lower()):
        address_props['ic.hasStreetDirection'] = str(street_direction_str2ic[street_direction.lower()])

    address = get_instance(klass='ic.Address', props=address_props)
    return address


def generate_funding(row_data, donor_org=None, donee_org=None,donor_program=None, donee_program=None):
    """
    Generate Funding object for row_data
    Return an .
    :param row_data: row data as pd.Series
    :param donor_org: instance of donor organization
    :param donee_org: instance of donee organization
    :param donor_program: dict, donor organization program for funding 
    :param donee_program: dict. donee organization program for funding
    :return fund: instance of funding object
    """

    if not row_data.any():
        row_data = {}
    if donor_org is None:
        tmp = row_data.rename({'Donor Legal Name':'Legal Name'})
        tmp['Name'] = tmp['Legal Name']
        donor_org = generate_organization(tmp)

    if donee_org is None:
        tmp = row_data[['Donee BN/Registration Number','Donee Name','Donee Legal Name']].rename({
            'Donee BN/Registration Number':'BN/Registration Number','Donee Name':'Name','Donee Legal Name':'Legal Name'
        })
        donee_org = generate_organization(tmp)

    fund = get_blank_instance(klass='cp.Funding')
    # setup date as current date range as Jan 01 to Dec 31 of Fiscal year
    start_date = datetime.fromisoformat(f"{row_data['Fiscal year']}-01-01")
    end_date = datetime.fromisoformat(f"{row_data['Fiscal year']+1}-01-01")-timedelta(days=1)
    start = get_instance(klass='time.Instant', props={'time.inXSDDateTimeStamp':start_date})
    end = get_instance(klass='time.Instant', props={'time.inXSDDateTimeStamp':end_date})
    interval = get_instance(klass='time.DateTimeInterval', props={'time.hasBeginning':start['ID'],'time.hasEnd':end['ID']})

    fund['time.hasTime'].append(interval['ID'])

    if row_data.get('receivedAmount') and not pd.isnull(row_data.get('receivedAmount')):
       fund['cp.receivedAmount'] = [float(row_data['receivedAmount'])]
    if row_data.get('requestedAmount') and not pd.isnull(row_data.get('requestedAmount')):
        fund['cp.requestedAmount'] = [float(row_data['requestedAmount'])]
    fund['cp.receivedFrom'] = [donor_org['ID']]

    # Donor's program that send the funding
    if donor_program is None:
        prog_name = escape_str(f"funding_program_from_{donor_org['cp.hasUUID']}")

        donor_program = get_blank_instance(klass='cp.Program')
        donor_program['org.hasName'].append(str(prog_name))

    for col in ['Category English Desc','Sub-Category English Desc']:
        if row_data.get(col) and not pd.isnull(row_data.get(col)).any():
            # i.e. Advancement of Religion
            for value in row_data.get(col):
                code_tags = set(map_client_codes(value))
                for code_tag in code_tags:
                    code = get_instance(klasss='cids.Code', inst_id=code_tag)
                    char = get_instance(klasss='cids.Characteristic', inst_id=escape_str('characteristic_'+normalize_code(code['ID'])), props={'cids.hasCode':code['ID']})
                    stakeholder = get_instance(klass='cids.BeneficialStakeholder', props={'cids.hasCharacteristic':char['ID']})
                    
                    donor_program['cids.hasBeneficialStakeholder'].append(stakeholder['ID'])
                    donee_program['cids.hasBeneficialStakeholder'].append(stakeholder['ID'])
                    fund['cp.forStakeholder'].append(stakeholder['ID'])


    donor_org['cids.hasProgram'].append(donor_program['ID'])
    fund['cp.fundersProgram'].append(donor_program['ID'])

    donor_lm = get_instance(klass='cids.LogicModel', props={'cids.hasProgram':donor_program['ID'], 'cids.forOrganization':donor_org['ID']})

    # donee's program that receives the funding
    # TODO: is there a way to know what amount was requested?
    # TODO: is there a way to tell which money went to which program/service?
    if donee_program is None:
        prog_name = escape_str(f"funded_program_for_{donee_org['cp.hasUUID']}")
        donee_program = get_instance(klass='cp.Program', props={'org.hasName':str(prog_name)})

    donee_org['cids.hasProgram'].append(donee_program['ID'])
    fund['cp.forProgram'].append(donee_program['ID'])

    donee_lm = get_instance(klass='cids.LogicModel', props={'cids.hasProgram':donee_program['ID'], 'cids.forOrganization':donee_org['ID']})

    return fund

def generate_organization(row_data, klass=None):
    """
    Generate organization object instance
    :param row_data: row data as pd.Series
    :param klass: Class type of the organization, e.g. cp.Organization, org.GovernmentOrganization, etc
    :return: organization
    """
    if not row_data.any():
        row_data = {}

    if klass is None:
        klass = 'cp.Organization'
    org_id = get_instance(klass='org.OrganizationID', props={'org.hasIdentifier':row_data['BN/Registration Number']})
    organization = get_instance(klass=klass, props={'org.hasID':org_id['ID']})

    if len(organization['org.hasName']) > 0:
        return organization

    organization['org.hasName'] = [row_data['Name'] or row_data['Legal Name']]
    organization['org.hasLegalName'] = [row_data['Legal Name']]

    address = generate_address(row_data)
    if address is not None:
        organization['ic.hasAddress'].append(address['ID'])


    if row_data.get('Contact Phone') and isinstance(row_data['Contact Phone'], str):
        phone_number_str = re.sub(r'[^0-9]', '', row_data['Contact Phone'])
        telephone_props = {}
        if len(phone_number_str) == 11:
            telephone_props['ic.hasCountryCode'] = int(phone_number_str[0])
            telephone_props['ic.hasPhoneNumber'] = int(phone_number_str[1:])
        elif len(phone_number_str) == 10:
            telephone_props['ic.hasPhoneNumber'] = int(phone_number_str)
        else:
            logger(f"Invalid phone number for {row_data['BN/Registration Number']} ({phone_number_str})")
        if telephone_props != {}:
            telephone = get_instance(klass='ic.PhoneNumber', props=telephone_props)
            organization['ic.hasTelephone'].append(telephone['ID'])
    if row_data.get('org:hasDescription') and not pd.isnull(row_data.get('org:hasDescription')):
        organization['cids.hasDescription'].append(row_data['org:hasDescription'])
    if row_data.get('cids:hasWebAddress') and not pd.isnull(row_data.get('cids:hasWebAddress')):
        organization['cids.hasWebAddress'].append(row_data['cids:hasWebAddress'])        
    if row_data.get('org:hasMission') and not pd.isnull(row_data.get('org:hasMission')):
        organization['cids.hasMission'].append(row_data['org:hasMission'])
    if row_data.get('sch:dateCreated') and not pd.isnull(row_data.get('sch:dateCreated')):
        # expecting "1974-01-01  00:00:00"
        formatted_date = datetime.fromisoformat(str(row_data['sch:dateCreated']))
        organization['sch.dateCreated'].append(formatted_date)

    for col in ['Category English Desc','Sub-Category English Desc']:
        if row_data.get(col) and not pd.isnull(row_data.get(col)):
            # i.e. Advancement of Religion
            value = row_data.get(col)
            service_tags = map_service_codes(value)
            client_tags = map_client_codes(value)
            code_tags = set(service_tags + client_tags)
            for code_tag in code_tags:
                code = get_instance(klass='cids.Code', inst_id=code_tag)
                char = get_instance(klass='cids.Characteristic', inst_id=escape_str('characteristic_'+normalize_code(code['ID'])))

                char['cids.hasCode'].append(code['ID'])
                organization['cids.hasCharacteristic'].append(char['ID'])

    if row_data.get('cp:hasLanguage') and not pd.isnull(row_data.get('cp:hasLanguage')):
        # TODO: attach to CL-Language code first
        code_tag = map_language[row_data['cp:hasLanguage']]
        code = get_instance(klass='cids.Code', inst_id=code_tag)
        organization['cp.hasLanguage'].append(code['ID'])

    if row_data.get('org:has_Ownership') and not pd.isnull(row_data.get('org:has_Ownership')):
        ownership_value = map_ownership[row_data['org:has_Ownership']]
        ownership = get_instance(klass='org.Ownership', inst_id=ownership_value)
        organization['org.has_Ownership'].append(ownership['ID'])

    return organization


def map_service_codes(value):
    matches = []
    for c,df in loaded_org_codes.items():
        tmp = df[df[c]==value]
        for v in tmp['Service'].dropna().tolist():
            matches.append(v)
        for v in tmp['Employment'].dropna().tolist():
            matches.append(v)
    return [re.sub(r'^\[|\]$', '', m) for m in matches]

def map_client_codes(value):
    matches = []
    for c,df in loaded_org_codes.items():
        tmp = df[df[c]==value]
        for v in tmp['Client'].dropna().tolist():
            matches.append(v)
    return [re.sub(r'^\[|\]$', '', m) for m in matches]


def generate_program(row_data, organization, program_name=None, is_blank=False, beneficiary_codes=[],beneficiary_code_tags=[], address=None):
    """
    Generate Program instance.
    :param row_data:
    :param organization: dict of program's organization
    :param program_name: str, name of program
    :param is_blank: Blank program does not contain additional information from the row_data.
    :param beneficiary_codes: list of codes for the beneficary stakeholder
    :param beneficiary_code_tags: list of tags for codes for beneficary stakeholder
    :param address: dict, an address instance
    :return program: dict, instanceo the program created.
    """
    program = get_blank_instance(klass='cp.Program')
    if program_name:
        program['org.hasName'].append(str(program_name))
    elif row_data.get("Name"):
        program['org.hasName'].append(str(row_data["Name"]))

    organization['cids.hasProgram'].append(program['ID'])
    program['cp.providedBy'].append(organization['ID'])

    if not is_blank:
        if row_data.get('Phone'):
            telephone = get_instance(klass='ic.PhoneNumber', props={'ic.hasCountryCode': [int(row_data['Phone'][0])], 'ic.hasPhoneNumber': [int(row_data['Phone'][1:])]})
            program['ic.hasTelephone'].append(telephone['ID'])
        if row_data.get('Description'):
            program['cids.hasDescription'].append(row_data['Description'])
        if row_data.get('SiteUrl'):
            program['cids.hasWebAddress'].append(row_data['SiteUrl'])

        if address is not None:
            program['ic.hasAddress'].append(address['ID'])

        for code_tag in beneficiary_code_tags:
            code = get_instance(klasss='cids.Code', inst_id=code_tag)
            beneficiary_codes.append(code)

        char = generate_characteristic(beneficiary_codes)

        if char is not None:
            stakeholder = get_instance(klass='cids.BeneficialStakeholder', props={'cids.hasCharacteristic':char['ID']})
            program['cids.hasBeneficialStakeholder'].append(stakeholder['ID'])
            
            community = generate_community(address, char)
            if community is not None and len(stakeholder['i72.located_in']) == 0:
                try:
                    land_areas_id = community['landuse_50872.hasLandArea']
                except AttributeError as e:
                    # Caught a serious error with the adress and community. Display debug information and throw exception.
                    print(1,e)
                    print(2,organization)
                    print(3,char)
                    print(4,stakeholder)
                    print(5,address)
                    print(6,community)
                    print(7,is_blank)
                    print(8,row_data)
                    raise(e)
                for land_area_id in land_areas_id:
                    stakeholder['i72.located_in'].append(land_area_id)


    return program

def generate_characteristic(codes):
    """
    Generate cids.Charactersitics from passed codes.
    :param codes: list of dict Code instances.
    :return res_char: dict, Charactersitic instance with associated Codes in codes.
    """
    res_char = None
    if len(codes)>1:
        char_label =     '_'.join([normalize_code(str(code['ID'])) for code in codes])
        res_char = get_instance(klass='cids.CompositeCharacteristic', inst_id=escape_str(f'compositecharacteristic_{char_label}'))
        if len(res_char['oep.hasPart'])==0:
            for code in codes:
                char = get_instance(klass='cids.Characteristic', inst_id=escape_str('characteristic_'+normalize_code(code['ID'])))
                char['cids.hasCode'].append(code['ID'])
                res_char['oep.hasPart'].append(char['ID'])

    elif len(codes) == 1:
        code = codes[0]
        res_char = get_instance(klass='cids.Characteristic', inst_id=escape_str('characteristic_'+normalize_code(code['ID'])))
        if len(res_char['cids.hasCode']) == 0:
            res_char['cids.hasCode'].append(code['ID'])

    return res_char

def generate_community(address, char):
    """
    Generate cids.Community object instance
    :param address: dict, Address intance for the community
    :param char: dict, Characteristic intance for the community
    :return community: dict, instance of the newly created Community
    """
    if address is None or char is None:
        return None

    location_label = []
    for values in [address['ic.hasCountry'], address['ic.hasState'], address['ic.hasCityS']]:
        if values is not None:
            for value in values:
                location_label.append(strip_nm(value))
    location_label = re.sub(r' |\.', '_','_'.join(location_label))

    land_area = get_instance(klass='landuse_50872.LandArea', inst_id="landarea_"+location_label)
    if len(land_area['landuse_50872.parcelHasLocation'])==0:
        feature = get_instance(klass='i72.Feature', inst_id="feature_"+location_label)
        land_area['landuse_50872.parcelHasLocation'].append(feature['ID'])

    community = get_instance(klass='cp.Community', inst_id="community_"+location_label)
    if len(community['landuse_50872.hasLandArea'])==0:
        community['landuse_50872.hasLandArea'].append(land_area['ID'])

    comm_char = get_instance(klass='cp.CommunityCharacteristic', inst_id="communitycharacteristic_"+location_label)
    if len(community['cp.hasCommunityCharacteristic'])==0:
        community['cp.hasCommunityCharacteristic'].append(comm_char['ID'])
        comm_char['cids.hasCharacteristic'].append(char['ID'])
    return community

def generate_service(name, organization, program, address=None, service_codes=[], service_code_tags=[],service_code_values=[], client_codes=[], client_code_tags=[],client_code_values=[]):
    """"
    Generate a cp.Service object instance.
    :param name: string for name of service
    :param organization: dict for service's organization
    :param program: dict for service's program
    :param address: dict for services address
    :param service_codes: list of service code instances
    :param service_code_tags: list of service code tags (formatted strings)
    :param service_code_values: list of service code strings
    :param client_codes: list of client codes for this service
    :param client_code_tags: list of client code tags for this service (formatted strings)
    :param client_code_values: list of cliet codes strings for this service
    :return service: dict instance of the Service object
    """

    service = get_instance(klass='cp.Service', props={'org.hasName':name, 'cp.providedBy':organization['ID']})

    program['cids.hasService'].append(service['ID'])
    for value in service_code_values:
        service_code_tags += map_service_codes(value)
    for tag in service_code_tags:
        code = get_instance(klass='cids.Code', inst_id=tag)
        service_codes.append(code)
    for code in service_codes:
        service['cids.hasCode'].append(code['ID'])

    for value in client_code_values:
        client_code_tags += map_client_codes(value)
    for client_code_tag in client_code_tags:
        code = get_instance(klass='cids.Code', inst_id=client_code_tag)
        client_codes.append(code)

    char = generate_characteristic(client_codes)

    if char is not None:
        stakeholder = get_instance(klass='cids.BeneficialStakeholder', props={'cids.hasCharacteristic':char['ID']})
        service['cids.hasBeneficialStakeholder'].append(stakeholder['ID'])
        service['cp.hasRequirement'].append(char['ID'])
        service['cp.hasFocus'].append(char['ID'])
        
        community = generate_community(address, char)
        if community is not None and len(stakeholder['i72.located_in']) == 0:
            for land_area_id in community['landuse_50872.hasLandArea']:
                stakeholder['i72.located_in'].append(land_area_id)

    return service


