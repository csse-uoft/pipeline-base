import os, re
import pandas as pd
import xlsxwriter
# from .namespaces import cids, compass, org, dcterms
from mapping.namespaces import PREFIX, TURTLE_PREAMBLE, namespaces, prop_ranges_preset, compass
exec(f"from mapping.namespaces import {','.join([nm for nm in namespaces.keys() if nm != ''])}")
from .utils import escape_str

# A dictionary that maps the SM tags to a list of uoft ontology classes which are subclass of cids:Code
# key is in lowercase
sm_tag2uoft_codes = {}
# A dictionary that maps the SM tag to an SM code
# key is in lowercase
sm_tag2sm_code = {}


class ClientCode(cids.Code):
    namespace = compass


class ServiceCode(cids.Code):
    namespace = compass


class SMCode(cids.Code):
    namespace = compass


def strip_nm(e):
    e=str(e).strip()
    return re.sub(r"^[^\.]+\.",'',e)

def normalize_code(code):
    # remove leading namespace and hashtags from code value
    return re.sub(r'(^[^\.]+\.)|(^#)', '',str(code)).lower()

def normalize_sm_tag(tag):
    """Special function for SM tags"""
    mapper = {
        'accessibility / mobility': 'Accessibility',
        'agricultural': 'Agriculture',
        'environment & nature': 'Environmental & Nature',
        'hiv / aids': 'HIV/ AIDS',
        'free wi-fi': 'Free WiFi',
        'info and referral': 'Info & Referral',
        'newcomer/ immigrant': 'Newcomer/Immigrant',
        'seniors': 'Senior',
    }
    tag = tag.lower()
    if mapper.get(tag):
        return mapper[tag].lower()
    else:
        return tag


def get_levels(row, num_levels):
    levels = []
    i = 0
    while i < num_levels and row[f'level-{i}']:
        levels.append(row[f'level-{i}'].strip())  # Remove trailing spaces
        i += 1
    return levels


def load_codes(data, parent):
    """
    Example Structure:
    level-0,level-1,level-2,level-3,CodeValue
    Shelter,,,,[SHELTER-Shelter-0]
    Shelter, Temporary , Temporary Shelter,,[SHELTER-Temporary_Shelter-2]
    Shelter, Temporary , Temporary Housing,,[SHELTER-Temporary_Housing-3]
    Shelter, Temporary , Temporary Bed,,[SHELTER-Temporary_Bed-4]
    Shelter, Social & Affordable Housing ,,,[SHELTER-Social_&_Affordable_Housing-5]
    """
    tags2code = {}
    prefix = parent.iri[parent.iri.index('#') + 1:]

    column_names = data[0].keys()
    level_columns = list(filter(lambda x: x.startswith('level'), column_names))

    for i in range(len(data)):
        row = data[i]
        next_row = None

        # A state variable for current row.
        # If True, the CodeValue is an instance of a code that is located in the second last level;
        #          and don't create a code class for the last level.
        # If False, We create a code class of the last level and an instance of that code class.
        is_last_level_a_class = True

        curr_row_levels = get_levels(row, len(level_columns))
        if i != len(data) - 1:
            next_row = data[i + 1]

            # Get all levels
            next_row_levels = get_levels(next_row, len(level_columns))

            if len(next_row_levels) > len(curr_row_levels) and curr_row_levels[-1] == next_row_levels[-2]:
                is_last_level_a_class = False

        code_value = row['CodeValue'].strip()[1:-1]  # Remove brackets
        if is_last_level_a_class:
            code_class = tags2code['-'.join([prefix, *curr_row_levels][:-1])]
            code_class(escape_str(code_value))
        else:
            class_name = escape_str("-".join([prefix, *curr_row_levels]), lower=False)
            if len(curr_row_levels) == 1:
                code_class = tags2code['-'.join([prefix, *curr_row_levels])] = type(class_name, (parent,), {})
            else:
                super_class = tags2code['-'.join([prefix, *curr_row_levels][:-1])]
                code_class = tags2code['-'.join([prefix, *curr_row_levels])] = type(class_name, (super_class,), {})
            code_class.label = ' - '.join(curr_row_levels)
            code_class(escape_str(code_value))


def load_sm_codes(mapping_data, code_def_data):
    # Load definition and create sm codes
    prev_class = None
    for row in code_def_data:
        c1 = row['Class'].strip()
        if not c1:
            continue
        tag = row['Tag'].strip()
        description = row['Description'].strip()
        if not c1[0].isdigit():
            name = escape_str(c1, lower=False)
            prev_class = type(f'SM-{name}', (SMCode,), {})
            prev_class.label = c1
        else:
            name = escape_str(tag)
            sm_code = prev_class(f'sm-{name}-{c1}')
            sm_code.label = tag
            cids.hasDescription[sm_code].append(description)

            sm_tag2sm_code[tag.lower()] = sm_code

    # Load mapping
    for row in mapping_data:
        tag_lower = row['SM_tags'].lower()
        if sm_tag2uoft_codes.get(tag_lower) is None:
            sm_tag2uoft_codes[tag_lower] = []
        uoft_code = compass[escape_str(row['code_instance'][1:-1])]
        if uoft_code:
            sm_tag2uoft_codes[tag_lower].append(uoft_code)
            # Set the mapping in ontology
            sm_code = sm_tag2sm_code[tag_lower]
            compass.mappedTo[sm_code].append(uoft_code)
        else:
            raise ValueError(f"Cannot find {row['code_instance']}")

    return sm_tag2uoft_codes




map_language = {
    1:'LANGUAGE-English-1', 
    2:'LANGUAGE-French-2'
}

map_ownership = {
    'Charitable Organization':'charitable_owned',
    'Private Foundation':'privately_owned',
    'Public Foundation':'publicaly_owned',
    'government_owned':'government_owned',
}

        
expands_and_donation_type = {
    '0':['EXPENSES-Expenses-0'],
    '4800':['EXPENSES-Advertising-1'],
    '4810':['EXPENSES-Travel-2'],
    '4820':['EXPENSES-Banking_Fee_Interest-3'],
    '4830':['EXPENSES-Memberships-4'],
    '4840':['EXPENSES-Facility-5'],
    '4850':['EXPENSES-Rent-6'],
    '4860':['EXPENSES-Consulting-7'],
    '4870':['EXPENSES-Training-8'],
    '4880':['EXPENSES-Salary-9'],
    '4890':['EXPENSES-Charity_Good-10'],
    '4891':['EXPENSES-Purchased_Asset-11'],
    '4900':['EXPENSES-Amortization-12'],
    '4910':['EXPENSES-Research-13','EXPENSES-Scholarship-14'],
    '4920':['EXPENSES-Other-99'],
    '5000':['EXPENSES-Charitable_Activity-15'],
    '5010':['EXPENSES-Administration-16'],
    '5020':['EXPENSES-Fundrasing_Cost-17'],
    '5030':['EXPENSES-Politics-18'],
    '5040':['EXPENSES-Administration-Other-98'],
}

def load_org_codes():
    dir_path = os.path.expanduser("~") + '/Dropbox/Compass Shared Folder/Helpseeker Data/CRA Data'
    # dir_path = 'e:/Dropbox/Compass Shared Folder/Helpseeker Data/CRA Data'


    # print("Loading org codes file...")
    xls_codes = pd.ExcelFile(dir_path + '/lookup_codes_4.xlsx')
    org_codes = {}
    for sheet_name in xls_codes.sheet_names:
        org_codes[sheet_name] = pd.read_excel(xls_codes, sheet_name)
    print("Done loading org codes.")
    return org_codes
loaded_org_codes = load_org_codes()
