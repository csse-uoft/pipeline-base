### Data Translation Pipeline
This repository contains one way to translate CSV-liked data into Knowledge Graph.
1. [Manual scripting with owlready2 (OWL format)](./example-translation)
   > This is used to provide the demo.
   
    Instantiating ontology class instances using owlready2 based on the CSV data.
    Then save the knowledge graph using dict, following the owlready2 syntax. 
    This provides a high flexibility and efficiency, without requiring the developer 
    to be proficient with owlready2. 
    owlready2 is used for validation of fields and namespaces.

# Installation
run `conda env create -f PyPipeline.yml`

# running
1. update config file ``example-translation/src/config.yml`
2. update global variables in `example-translation/src/main.py`
3. run `conda activate PyPipeline`
4. run `python example-translation/src/main.py`