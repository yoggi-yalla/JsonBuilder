# JsonBuilder
JsonBuilder is a tool for converting .csv data to a structured JSON format. It is built on top of Pandas, and as such it requires the user to have  Pandas installed on their machine.

## Example usage
```Python
from JsonBuilder import JsonBuilder
import json

csv = 'path/to/some/csv/file.csv' # can also be str or file-like object  
mapping = {TODO} # see below for examples
functions = [TODO] # see below for examples
transforms = [TODO] # see below for examples


output_native = JsonBuilder().parse_mapping(mapping)
                             .load_csv(csv)
                             .add_functions(functions) # optional
                             .apply_transforms(transforms) # optional
                             .build()
                             .value

output_json = json.dumps(output_native, indent=2)
```
<br>

## Intro
Consider a simple .csv file with EURSEK fixing data:

|currency1|currency2|fixing |date      |
|---------|---------|-------|----------|
|EUR      |SEK      |10.8623|2020-04-20|
|EUR      |SEK      |10.9543|2020-04-21|
|EUR      |SEK      |10.9423|2020-04-22|
|EUR      |SEK      |10.8883|2020-04-23|
|EUR      |SEK      |10.8723|2020-04-24|

There are many ways of expressing this data in JSON format:

````python
# One example:
[
  {
    "currency1": "EUR",
    "currency2": "SEK",
    "fixing": 10.8623,
    "fixing_date": "2020-04-20"
  },
  {
    "currency1": "EUR",
    "currency2": "SEK",
    "fixing": 10.9543,
    "fixing_date": "2020-04-21"
  },
  ...
]

# Another example:
{
  "EURSEK": {
    "dates":[
      "2020-04-20",
      "2020-04-21",
      ...
    ],
    "fixings":[
      10.8623,
      10.9543,
      ...
    ]
  }

}
````

Even in this simple scenario there are virtually endless ways of converting the .csv into JSON format, some more sensible than others of course. If you have an application that consumes JSON data in a certain format, and you receive data in a flat .csv format, then this tool allows you to easily convert the .csv data to the specific JSON format that can be consumed by your application.

<br>

## Mapping
The mapping is in itself a JSON object, specifying the shape of the desired output JSON. The mapping is best described as a tree of nodes, where each node can have the following attributes:

|Attribute|Description|
|-------------:|-----------|
|``"type"``          | Can be ``"object"``, ``"array"``, or ``"primitive"``, defaults to ``"primitive"``|
|``"value_col"``     | The column containing the value to be fetched|
|``"name_col"``      | The column containing the name to be fetched |
|``"value"``         | Can be used for setting a default value|
|``"name"``          | Can be used for setting a default name|
|``"children"``      | An array of all child nodes. Any child of an ``"object"`` must have a name, either using ``"name"`` or ``"name_col"``. Conversely, all children of an ``"array"`` have no name, any provided name will be ignored. ``"primitive"`` nodes have no children.|
|``"filter"``        | Applies a filter to the DataFrame by checking for truth values, for example: <br>``"df['currency1'] == 'EUR' & df['currency2'] == 'SEK'"``.<br>  Let ``s`` denote the raw string, the DataFrame is filtered according to ``df=df[eval(s)]``.|
|``"split"``      | Splits the DataFrame into groups accoding to a Pandas group_by expression, for example: <br>``"df['some_column_name'].str[:3]"``.<br> Let ``s`` denote the raw string, the DataFrame is split into groups according to ``df.group_by(eval(s))``. A simple ``"r"`` splits the DataFrame into rows.|
|``"transmute"``          | Allows the user to provide an arbitrary expression with ``x``, ``r``, and ``df`` as the variables at their disposal. The evaluated expression is assigned directly to the output value, for example: <br><br>``"x if r['date']>"2020-04-03" else 0"``<br><br>If it seems magical to you then it's because it is, you can read more about the behavior [here](TODO). It is normally a good idea to avoid complex transmutes and instead prepare the data as needed in the [transforms](TODO).|

<br>

### Example mapping:
```python
mapping = \
{
  "type": "object",
  "children":[
    {
      "type":"array",
      "name":"fixings",
      "children":[
        {
          "type":"object",
          "split":"r",
          "children":[
            {
              "name":"fixing",
              "value_col": "fixing"
            },
            {
              "name":"currency_pair",
              "transmute":"r['currency1']+r['currency2']"
            },
            {
              "name":"fixing_date",
              "value_col":"date"
            }
          ]
        }
      ]
    }
  ]
}
```
### Which would generate a JSON with this shape:
```python
{
  "fixings":[
    {
      "currency_pair": "EURSEK",
      "fixing": 10.8623,
      "fixing_date": "2020-04-20"
    },
    {
      "currency_pair": "EURSEK",
      "fixing": 10.9543,
      "fixing_date": "2020-04-21"
    },
    ...
  ]
}
```

<br>

## Functions
Functions are used to define functions that can be re-used in more than one place in the mapping (or in the [transforms](TODO). They are passed as a list of named functions, represented as strings:
``` python
functions = [
  "def f1(x,r,df): x = 15; x += 2; return x",
  "def f2(df): return df['region']",
  "def f3(df): return df['currency'] == 'SEK'",
  "def f4(df): return df['currency_pair'].str.lower()"
]
```
  

The functions can later be used in the mapping like this: 
```python
"transmute": "f1(x,r,df)"
"group_by": "f2(df)"
"filter": "f3(df)"
```

For simple operations it is often more convenient to write an expression directly in the string, but for large/complex functions this functionality is essential.

<br>

## Transforms
Transforms are used for column-wise transforms of the data before the mapping. They are passed as a list of expressions in a string format:
```python
transforms = [
  # Converts the fixings to Swedish Öre
  "df['fixing']*100",

  # Creates a new column called 'currency_pair'
  "(df['currency1'] + df['currency2']).rename('currency_pair')",

  # User defined functions can be used here as well, 
  # as long as they have been added using add_function first!
  "f4(df)"
]
```

The expression is expected to evaluate to a Pandas Series object, i.e. a column. The JsonBuilder will attach the generated column to the DataFrame.

Let ``s`` denote the expression, the transforms are applied according to:
```python
column = eval(s)
name = column.name
df[name] = column
```
If only one column is used in the transform then the output Series will have the same name as this column, which means the corresponding column in the DataFrame will be overwritten with the output Series. 

If more than one column is used, the resulting Series will have ``name == None``. This Series should be renamed by the user to get a sensible column header, as shown in the second transform above. 

Renaming can also be useful if one wants to transform a single column, but save the output into a _new_ column.
