# JsonBuilder
JsonBuilder is a tool for converting .csv data to a structured JSON format. It is built on top of Pandas, and as such it requires the user to have  Pandas installed on their machine.

## Example usage
```Python
from JsonBuilder import JsonBuilder
import json

csv = 'path/to/some/csv/file.csv'
mapping = {TODO} # see below for examples
functions = [TODO] # see below for examples
transforms = [TODO] # see below for examples


output_native = JsonBuilder.parse_mapping(mapping)        \
                           .load_csv(csv)                 \
                           .add_functions(functions)      \
                           .apply_transforms(transforms)  \
                           .build()                       \
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

Even in this simple scenario there are countless ways of converting the .csv into JSON format, some more sensible than others of course. If you have an application that consumes JSON data in a certain format, and you receive data in a flat .csv format, then this tool allows you to easily convert the .csv data to the specific JSON format that can be consumed by your application.

<br>

## Mapping
The mapping is in itself a JSON object, specifying the shape of the desired output JSON. The nomenclature used in this project is similar to the official [JSON documentation](https://www.json.org/json-en.html), so if you are not familiar with it already I suggest you have a look at their webpage. The mapping is best described as a tree of nodes, where each node can have the following attributes:

|Attribute|Description|
|-------------:|-----------|
|``"type"``          | Can be ``"object"``, ``"array"``, or ``"primitive"``, defaults to ``primitive``|
|``"name"``      | The name of a value within an `object` node, e.g. ``"some_name"`` |
|``"value"``     | This is typically left blank but can be used for setting a hardcoded value. <br> May contain any valid JSON value such as ``"some_value"`` or ``0.5`` or ``[true, false]`` or ``{}`` etc.|
|``"column"``         | The column in the DataFrame containing the value to be extracted, e.g. ``"some_column_name"`` |
|``"children"``      | An array of all child nodes. Any child of an ``object`` must have a name. Conversely, the children of an ``array`` have no names, and any provided name will be ignored. ``primitive`` nodes have no children.|
|``"filter"``        | Applies a filter to the DataFrame by checking for truth values, for example: <br>``"currency1 == 'EUR' and currency2 == 'SEK'"``. <br>See [df.query](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html) for more informaiton.|
|``"iterate"``      |  Should contain a column name, e.g. ``"some_other_column_name"``. The JsonBuilder will iterate over each unique group in this column and generate one value for each group. To iterate over all rows, the keyword ``"index"`` may be used, or any column that only contains unique elements.|
|``"transmute"``          | Allows the user to provide an arbitrary expression with ``x``, ``r``, and ``df`` as the variables at their disposal. The evaluated expression is assigned directly to the output value, for example: <br><br>``"x if r['date']>"2020-04-03" else 0"``<br><br>If it seems magical to you then it's because it is, you can read more about the behavior [here](#Transmutes). It is normally a good idea to avoid complex transmutes and instead prepare the data as needed in the [transforms](#Transforms).|

<br>

### Here's an example mapping:
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
          "iterate":"index",
          "children":[
            {
              "name":"fixing",
              "column": "fixing"
            },
            {
              "name":"currency_pair",
              "transmute":"r['currency1']+r['currency2']"
            },
            {
              "name":"fixing_date",
              "column":"date"
            }
          ]
        }
      ]
    }
  ]
}
```
### Which would generate the following output:
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
JsonBuilder allows the user to define their own functions that can be accessed in the [transforms](#Transforms) and in the [transmutes](#Transmutes). They are passed as a list of named functions, represented as strings:
``` python
functions = [
  "def f1(x,r,df): y = len(df.index); x += y; return x",
  "def f2(df): return df.fillna(0)"
]
```
  

The functions can later be accessed like this: 
```python
mapping: {
  ...
  "transmute": "f1(x,r,df)"
  ...
}

transforms:[
  ...
  "f2(df)",
  ...
]
```

For simple operations it is often more convenient to write an expression directly in the string, but for large/complex operations this functionality is essential.

<br>

## Transforms
Transforms are used for column-wise (or table-wise) transforms of the DataFrame before the mapping. They are passed as a list of expressions in a string format:
```python
transforms = [
  # Converts the fixings to Swedish Öre
  "df['fixing']*100",

  # Creates a new column called 'currency_pair'
  "(df['currency1'] + df['currency2']).rename('currency_pair')",

  # User defined functions can be used here as well, 
  # as long as they have been added using add_functions first!
  "f2(df)"
]
```

The expression is expected to evaluate to a Pandas Series object, i.e. a column. The JsonBuilder will attach the generated column to the DataFrame.

If only one column is used in the transform then the output Series will have the same name as the input column, which means the corresponding column in the DataFrame will be _overwritten_ with the output Series. 

If more than one column is used, the resulting Series will have an empty name. This Series should be renamed by the user to get a sensible column header, as shown in the second transform above. 

Renaming can also be useful if one wants to transform a single column, but save the output into a _new_ column.


## Transmutes

TODO
