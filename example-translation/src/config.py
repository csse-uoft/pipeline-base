import yaml
from yaml.loader import SafeLoader

# Open the file and load the file
with open('example-translation/src/config.yml') as f:
    data = yaml.load(f, Loader=SafeLoader)
    for var,val in data.items():
        if isinstance(val,str): exec(f"{var} = \"{val}\"")
        else: exec(f"{var} = {val}")
