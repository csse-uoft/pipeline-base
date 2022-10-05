import csv, re, datetime, collections, pickle, tqdm, unicodedata, io
import datetime
from mapping.namespaces import PREFIX, TURTLE_PREAMBLE, namespaces, prop_ranges_preset
# importing napspqces as variables allows them to be used as owlready2 variables, with additional validation
exec(f"from mapping.namespaces import {','.join([nm for nm in namespaces.keys() if nm != ''])}")

import collections
import uuid


from __main__ import LOG_FILE

# global variable that stores the working dict of data.
# It is used in place of the owlready2 sqllite3, as its more efficient.
global_db = {}

def logger(text, filename=LOG_FILE):
    """
    logs errors and warnings to the file at filename
    :param text : string value of text to write to log
    :param filename : string value of log's filename
    """
    # Open a file with access mode 'a'
    file_object = open(filename, 'a')
    # Append 'hello' at the end of file
    file_object.write(str(datetime.datetime.today()) + "\t" + str(text) + "\n")
    # Close the file
    file_object.close()

def escape_str(s: str, lower=True):
    """
    Generate an individual/class name that complies the ontology format.
    :param s : string value of string to escape.
    :param lower : whetehr to convert to lower case or not.
    :return s : formatted string
    """
    if lower:
        s = s.lower()
    # string.replace('+', '_plus_')
    s.replace('<', '_lt_')
    s.replace('>', '_gt_')
    s = re.sub(r'[^-_0-9a-zA-Z]', '_', s)
    return re.sub(r'_-_|-_-|_+', '_', s)


def is_bom(file_path):
    """
    check if file contains BOM characters.
    :param file_path: filename of file to check BOM for
    :return True/False whetehr fiel is BOM formatted.
    """
    f = open(file_path, mode='r', encoding='utf-8')
    chars = f.read(4)[0]
    f.close()
    return chars[0] == '\ufeff'


def read_csv(csv_path: str, encoding=None):
    """
    read CSV file, with error handling.
    :param csv_path : string with csv file path
    :param encoding : string with file nencoding to use, if any
    :return data : list of data read from CSV file
    """
    data = []
    if not encoding:
        encoding = 'utf-8-sig' if is_bom(csv_path) else 'utf-8'
    print(f'Loaded CSV "{csv_path}"; Encoding: {encoding}')
    with open(csv_path, encoding=encoding, newline='') as file:
        # reader = csv.DictReader(file)
        reader = csv.DictReader(file, quotechar='"', delimiter=',',  quoting=csv.QUOTE_ALL, skipinitialspace=True)
        for row in reader:
            data.append(row)
    return data


def write_csv(csv_path, data: list):
    """
    write CSV file
    :param csv_path : string with CSV file name to write to
    :param data : list with records to wrote to CSV file
    """
    with open(csv_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

def format_strptime(d):
    """
    parse a date string into formatted Timestamp value
    :param d : string with date value to format
    :return : string formatted as Timestamp type
    """
    try:
        return datetime.datetime.strptime(d, "%Y-%m-%d 00:00:00")
    except ValueError:
        try:
            return datetime.datetime.strptime(d, "%Y-%m-%d 00:00")
        except ValueError:
            return datetime.datetime.strptime(d, "%Y-%m-%d")


###########################################################
# Methods for handling instance creation
# Uses internal glbol_db dict for storage
# Created UUID if needed for each instance.
###########################################################
def get_instance_label(klass, uuid_inst=None):
    """
    return instance label with class and UUID
    :param klass: type of instance
    :param uuid_inst: uuid instance, e.g. from uuid.uuid4()
    :return string value of instance label
    """
    if uuid_inst is None:
        uuid_inst = uuid.uuid4()
    klass_label = re.sub(r'^[^\.]+\.','',str(klass).lower())
    return f"{klass_label}_{uuid_inst}"

def get_instance(klass,nm=PREFIX, inst_id=None, props={}):
    global global_db
    '''
    return an instance with matching properties, or creates a new instance and returns that
    :param klass: type of the instance
    :param nm: namespace to use, defaults to PREFIX
    :param props: properties used to find or create the instance. 
            Includes namespace in property name (e.g. "time.hasEnd")
            Namespace is removed when runing seaarch, but used when creating a new instance, using exec().
    :return inst
    '''
    inst = None
    uuid_inst = None
    key = None
    key_list = [key for key in props.keys() if 'hasUUID' in key]

    if inst_id is not None and inst_id in global_db.keys():
        # was created/found before under the inst_id
        inst = global_db[inst_id]
    elif len(key_list)>0:
        uuid_inst = key = properties[key_list[0]][0]
        inst_id = get_instance_label(klass=klass, uuid_inst=uuid_inst)
        if inst_id in global_db.keys():
            # was created/found before under the inst_id
            inst = global_db[inst_id]
    
    properties = dict(collections.OrderedDict(sorted(props.items())))
    if inst is None and properties != {}:
        # inst not found and parameters not empty, hence can use parameters as unique search key.
        key = f"{klass}_{properties}"
        if key in global_db.keys():
            inst = global_db[key]
            inst_id = inst['ID']

    if inst is not None:
        if inst_id is None and inst['ID']==[]:
            uuid_inst = uuid.uuid4()
            inst_id = get_instance_label(klass=klass, uuid_inst=uuid_inst)
            inst[nm+'.hasUUID'] = str(uuid_inst)
        for prop,val in properties.items():
            if val is None: continue
            if not isinstance(val, list):
                inst[prop] = [val]
            else:
                inst[prop].append(val)
    else:
        inst = collections.defaultdict(lambda:[], properties)
        inst['is_a'] = klass
        if inst_id is None:
            uuid_inst = uuid.uuid4()
            inst_id = get_instance_label(klass=klass, uuid_inst=uuid_inst)
            inst[nm+'.hasUUID'] = str(uuid_inst)
        for prop,val in properties.items():
            if val is None: continue       
            if not isinstance(val, list):
                inst[prop] = [val]
            else:
                inst[prop].append(val)
    inst['ID'] = inst_id
    if key:  global_db[key] = inst
    global_db[inst_id] = inst
    tmp = inst
    for prop,val in tmp.items():
        if isinstance(val,list):
            try:
                inst[prop] = list(set(val))
            except TypeError  as e:
                print(inst_id, prop, val)
                print(e)
                raise(e)
    if inst_id is None:
        # Something went wrong. Display data and throw exception.
        print("")
        print(inst)
        print(prop)
        print(properties)
        print("")
        raise("No inst_id")
    return inst

def get_blank_instance(klass, nm=PREFIX, inst_id = None):
    global global_db
    '''
    return new an instance without any properites, ONLY hasUUID is included
    :param klass: type of the instance
    :param nm: namespace to use, defaults to PREFIX
    :return inst: instance label
    '''
    uuid_inst = uuid.uuid4()
    if inst_id is None:
        inst_id = get_instance_label(klass=klass, uuid_inst=uuid_inst)
    inst = collections.defaultdict(lambda:[])
    inst['ID'] = inst_id
    inst['is_a'] = klass
    # inst=klass(inst_id)
    inst[nm+'.hasUUID'] = str(uuid_inst)
    global_db[inst_id] = inst

    return inst

def encode_inst(val):
    """
    encode OWL instance name to remove any non-OWL characters (punctuation)
    return val : string value of instance label
    """
    puncts = '!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~ '
    val = unicodedata.normalize('NFD', val).encode('ascii', 'ignore').decode()
    val = re.sub('|'.join([re.escape(s) for s in puncts]), '_',val)
    val = re.sub(r'_+','_',val)
    return val

def resolve_nm(val):
    """
    Resolve the namespace on a property range value. Uses rules and look up table for predefined property range types.
    return res : string value for property value
    """
    res = ''
    if isinstance(val, str):
        match = re.findall(r'(.*)\.([^\.]+)', val)
        if len(match) == 0:
            res = f"{PREFIX}:{encode_inst(val)}"
        else:
            if match[0][0] == '': res = f"{PREFIX}:{encode_inst(match[0][1])}"
            else: res = f"{match[0][0]}:{encode_inst(match[0][1])}"
    elif isinstance(val, datetime.datetime):
        res = '"'+val.strftime("%Y-%m-%dT%H:%M:%S")+'"'
    else:
        res = val
    return res

def default_to_regular(d):
    """convert collection.defalutdict to dict"""
    if isinstance(d, collections.defaultdict):
        d = {k: default_to_regular(v) for k, v in d.items()}
    return d

def save_global_db(filename='global_db.pickle'):
    """
    save dictionary in global_db to a pickle file.
    value vcan be used later to generate .ttl file without loading all data, or process can be restarted.
    """
    global global_db
    tmp = {}
    for key,val in tqdm.tqdm(global_db.items(), total=len(global_db.keys())):
        tmp[key] = default_to_regular(val)

    with open(filename, 'wb') as handle:
        pickle.dump(tmp, handle, protocol=pickle.HIGHEST_PROTOCOL)

def load_global_db(filename='global_db.pickle'):
    """load global_db from pickle file"""
    global global_db

    with open(filename, 'rb') as handle:
        global_db = pickle.load(handle)
    
    for key,val in tqdm.tqdm(global_db.items(), total=len(global_db.keys())):
        global_db[key] = collections.defaultdict(lambda: [],val)

def row_to_turtle(inst, prop_ranges={}):
    """convert global_db value to Turtle format"""
    s = resolve_nm(inst['ID'])
    text = f"{s} a {resolve_nm(inst['is_a'])};\n"
    for prop,vals in inst.items():
        if prop in ['is_a', 'ID']:
            continue
        if not isinstance(vals,list):
            vals = [vals]
        for val in vals:
            prop_eval = eval(prop)
            ranges = []
            if prop_eval in prop_ranges.keys() and len(prop_ranges[prop_eval])>0:
                ranges = prop_ranges[prop_eval]
            if str in ranges:       o = '"'+str(val).replace('\\', '\\\\').replace('"','\\"') + '"'
            elif int in ranges:     o = f'{val}'
            elif float in ranges:   o = f'{val}'
            else:                   o = resolve_nm(val)
            text += f"    {resolve_nm(prop)} {o};\n"
    text += ".\n"
    return text


def save_db_as_ttl(filename='global_db.ttl', dict_db=None):
    """Save global_db as .ttl file"""
    global global_db
    if dict_db is None:
        dict_db = global_db

    f = io.FileIO(filename, 'w')
    writer = io.BufferedWriter(f,buffer_size=100000000)
    writer.write(TURTLE_PREAMBLE.encode(encoding='UTF-8'))

    flush_i = 0
    flush_cycle = 100000

    records = [inst for inst_id,inst in dict_db.items() if inst_id == inst['ID']]
    for inst in tqdm.tqdm(records, total=len(records)):
        if flush_i % flush_cycle == 0:
            writer.flush()
        flush_i += 1
        klass = eval(inst['is_a'])
        prop_ranges = dict([(prop,prop.range) for prop in list(klass.INDIRECT_get_class_properties())])
        writer.write(row_to_turtle(inst, prop_ranges=prop_ranges|prop_ranges_preset).encode(encoding='UTF-8'))

    writer.flush()

