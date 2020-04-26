# JsonBuilder
This is a tool for converting .csv data to a structured JSON format. The only requirement for using this tool is to have Pandas installed on your machine.


## Example usage:
```Python
from JsonBuilder import JsonBuilder
import json

csv = 'path/to/some/csv/file.csv' # can also be str or file-like object  
mapping = {TODO}
functions = [TODO]
transforms = [TODO]

output_native = JsonBuilder().parse_mapping(mapping)
                             .load_csv(csv)
                             .add_functions(functions)
                             .apply_transforms(transforms)
                             .build()
                             .value

output_json = json.dumps(output_native, indent=2)
```

## Mapping
The mapping is a Python dict describing the shape of the desired output JSON, and instructions for how to build it based on the data in the csv. The mapping consists of JSON nodes, and each node can have the following attributes:
  
- type  
- name  
- name_col  
- value  
- value_col  
- children  
- multiple  
- group_by  
- filter  
- func  
  
type is either 'object', 'array' or 'primitive' (default)
  
an object is parent to nodes with names  
an array is parent to nodes without names  
a primitive can not be parent to further nodes  
  
children is a list of all child nodes  
  
each child to an object must have a name  
use name_col to fetch name dynamically from a column  
use name to set a hard coded name
  
each primitive has a value (string, float, int, bool, or none)  
use value_col to fetch value dynamically from a column  
use value to set a hard coded value 
  
'multiple': false	(default)  
means the current scope should build one instances of this node  
  
'multiple': true  
means the current scope should build several instances of this node  
the default behaviour is to build one instance for each row in the scope  
but if group_by is present then each _group_ of the scope builds one node  
  
filter is used to reduce the scope of the dataframe

func is used to apply a function to a value after it has been created


## Functions
Functions are passed as a List of functions, represented as strings:
``` python
functions = [
  "def f1(x,r,df): x = 15; x += 2; return x",
  "def f2(df): return df['region']",
  "def f3(df): return df['currency'] == 'SEK'"
]
```
  

This allows the user to apply them anywhere in the mapping: 
```python
"func": "lambda x,r,df: f1(x,r,df)"
"group_by": "lambda df: f2(df)"
"filter": "lambda df: f3(df)"
```
on a node.

In these examples it may be more convenient to use the lambda directly,
but if the functions are complex or one wants to re-use the same function
multiple times then this can be useful.
