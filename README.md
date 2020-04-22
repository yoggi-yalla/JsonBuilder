# table2json
This is a tool for converting Pandas DataFrames to a structured JSON format.


## Example usage:
```Python
from JsonBuilder import JsonBuilder
import pandas as pd
mapping = {TODO}
df = pd.DataFrame()
functions = {'melt', pd.melt}
name, value = JsonBuilder(mapping, df, functions=functions).build()
```

## Mapping
The mapping is a Python dict describing the shape of the desired output JSON, and instructions for how to build it based on the date from the DataFrame. The mapping consists of JSON objects, and each object can have the following attributes:
  
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
  
type is either 'object', 'array' or 'primitive'  
  
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
  
'multiple': false	(this is the default value)  
means the current scope should build one instances of this node  
  
'multiple': true  
means the current scope should build several instances of this node  
the default behaviour is to build one instance for each row in the scope  
but if group_by is present then each _group_ of the scope builds one node  
  
filter is used to reduce the scope of the dataframe

func is used to apply a function to a value after it has been created

## DataFrame
Any valid DataFrame should work with this tool
DataFrames can easily be built from .csv using:
```Python
import pandas as pd
df = pd.read_csv('path_to_csv.csv')
```

## Functions
Functions are passed as a Dict with <function name, function obj> pairs.
This allows the user to apply them anywhere in the mapping by setting 
"func": "<function name>" 
on a node.
