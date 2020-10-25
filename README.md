# JsonBuilder
JsonBuilder is a tool for converting .csv data to a structured JSON format. It is built on top of Pandas, and as such it requires the user to have  Pandas installed on their machine.

## Example usage
```Python
import jsonbuilder
import json

# Examples of all inputs are found further down

csv = 'path/to/csv/file.csv' # Required
fmt = {
  "mapping": {TODO} # Required
  "functions": [TODO] # Optional
  "df_transforms": [TODO] # Optional
}

jbTree = jsonbuilder.Tree(fmt, csv)

# At this stage one can access jbTree.df and jbTree.intermediate_dfs
# This may be used in order to inspect the results of the df_transforms

output_native = jbTree.build().value
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
The mapping is in itself a JSON object, specifying the shape of the desired output JSON. The nomenclature used in this project is similar to the official JSON documentation, so if you are not familiar with it already I suggest you have a look at their [webpage](https://www.json.org/json-en.html). The mapping is best described as a tree of nodes, where each node can have the following attributes:

|Attribute|Description|
|-------------:|-----------|
|``"type"``          | Can be ``"object"``, ``"array"``, or ``"primitive"``, defaults to ``"primitive"``|
|``"name"``      | The `name` of a `value` within an `object` node |
|``"value"``     | This is typically left blank but can be used for setting a hard-coded value on `primitive` nodes. May contain *any* valid JSON value such as ``"foo"`` or ``0.5`` or ``[true, false, 123]`` or ``{"foo":"bar"}`` etc.|
|``"column"``         | The column in the DataFrame containing the value to be extracted, e.g. ``"some_column_name"``. This can only be used on primitive nodes. |
|``"children"``      | An array of all child nodes. Any child of an ``object`` should have a name. Conversely, the children of an ``array`` should not have a name, and any provided name will be ignored. ``primitive`` nodes have no children.|
|``"filter"``        | Applies a filter to the DataFrame by checking for truth values, for example: <br>``"currency1 == 'EUR' and currency2 == 'SEK'"``. <br>See [df.query](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html) for more informaiton.|
|``"group_by"``      |  Should contain a column name, e.g. ``"some_other_column_name"``. The JsonBuilder will iterate over each unique group in this column and generate one value for each group.|
|``"iterate"``      |  Similar to ``group_by`` but it is much faster. This is a bool, and if set to ``true`` the JsonBuilder will build one value for each *row*. While doing so the JsonBuilder drops the DataFrame from memory, so it's not possible to use ``filter``, ``group_by``, or ``iterate`` on any descendant node.|
|``"transmute"``          | Allows the user to provide an arbitrary expression with ``x``, ``r``, and ``df`` as the variables at their disposal. The evaluated expression is assigned directly to the output value, for example: <br><br>``"x if r['date']>"2020-04-03" else 0"``<br><br>You can read more about the behavior [here](#Transmutes). It is normally a good idea to avoid complex transmutes and instead prepare the data as needed in the [transforms](#Transforms).|

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
Transforms are used for column-wise (or table-wise) transforms of the DataFrame before building the final JSON. They are passed as a list of expressions in a string format:
```python
transforms = [
  # Converts the fixings to Swedish Öre
  "df['fixing']*100",

  # Creates a new column called 'currency_pair'
  "(df['currency1'] + df['currency2']).rename('currency_pair')",

  # Replaces all NaN-values in the DataFrame with 0
  "df.fillna(0)",

  # User defined functions can be used here as well, 
  # as long as they have been added using add_functions first!
  "f2(df)"
]
```

Here, `df` is used to access the DataFrame to be transformed. The expression is expected to evaluate to either a Pandas Series object (i.e. a column, see first two examples above) *or* to a DataFrame (see third example above). If it evaluates to a DataFrame it will replace the input DataFrame, otherwise the generated column is simply attached to the DataFrame. The expressions will be executed one at a time in the order they appear in the list. 

If only one column is used in the transform then the output Series will have the same name as the input column, which means the corresponding column in the DataFrame will be _overwritten_ with the output Series.

If more than one column is used, the resulting Series will have an empty name. This Series should be renamed by the user to get a sensible column header, as shown in the second transform above. 

Renaming can also be useful if one wants to transform a single column, but save the output into a _new_ column.


## Transmutes

Transmutes allow the user to manipulate the value generated by one JsonBuilder node before returning itself to its parent. To fully understand how it works it helps to understand how the JsonBuilder "build"-process works. When a node is asked to build itself, given a DataFrame, it will:

1. Apply the filter specified on this node to the DataFrame
2. Build itself, which for primitive nodes means either extracting one value from the DataFrame or returning a hard-coded value, and for objects or arrays mean:
    1. Initialize its value as an empty container (`{}` for objects, `[]` for arrays)
    2. Ask all of its children to build themselves based on the filtered DataFrame
    3. Append the value built by each child to the container
3. Transmute the value that was built
4. Return itself to the caller


This is slightly simplified, step 2 also involves iterating over rows or groups of the DataFrame. What that means in practice is that in each iteration, only one row or group of the dataframe will be passed down to the child node.

The transmute is essentially a lambda expression with three parameters: `x`, `r`, and `df`. Within the expression, `x` represents the *value* stored on this node, `r` represents the *row* currently being processed, and `df` represents the *group* (i.e. DataFrame) currently being processed. When iterating over groups, `r` represents the top row of the current group, but when iterating over rows there is no DataFrame available, so `df` is equal to `None`. 

Here are a few examples of how it may be used:

```Python
   # Multiply the value by 100
1. "transmute": "x*100"

   # Set value to r['fixing'] or fall back to x
2. "transmute": "r['fixing'] or x"

   # Set value to a list containing the entire fixings column
   # This is significantly faster than iterating over each row
3. "transmute": "df['fixings'].tolist()" 

   # Add a new key:value pair to the object being built
   # This will only work on an object node, here x is a python dict
4. "transmute": "x['foo'] = 'bar'"

   # Append a new value to the array being built
   # This will only work on an array node, here x is a python list
5. "transmute": "x.append(5)"

   # x, r, and df can be passed to your own functions like this
6. "transmute": "f1(x,r,df)"
```

Keep in mind that `x` will be of different type depending on whether the transmute is specified on a `primitive-`, `object-`, or `array` node. Whatever the transmute evaluates to will be the final value of the node. 