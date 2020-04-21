# table2json
This is a tool for converting Pandas DataFrames to a structured JSON format.

The three input arguments for the JsonBuilder object are:
1. Mapping
2. DataFrame
3. Functions

1. 
A 'mapping' describes the output JSON as a tree of nodes,
a node can have the following attributes:

type
name
name_col
value
value_col
children
multiple
group_by
filter
func

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

'multiple': false	#(this is the default value)
means the current scope should build one instances of this node

'multiple': true
means the current scope should build several instances of this node
the default behaviour is to build one instance for each row in the scope
but if group_by is present then each _group_ of the scope builds one node

filter is used to reduce the scope of the dataframe

func is used to apply a function to a value after it has been created

2. 
Any valid DataFrame should work with this tool
DataFrames can easily be built from .csv using:
>>import pandas as pd
>>df = pd.read_csv('path_to_csv.csv')

3. 
Functions are passed as a Dict with <function name, function obj> pairs.
This allows the user to apply them anywhere in the mapping by setting 
"func": "<function name>" 
on a node. This makes it easy to extend the scope of functions available
during the mapping. 


Example usage:
>>from JsonBuilder import JsonBuilder
>>import pandas as pd
>>mapping = {TODO}
>>df = pd.DataFrame()
>>functions = {'melt', pd.melt}
>>name, value = JsonBuilder(mapping, df, functions).build()

The above example allows the mapping to use the Pandas function 'melt'