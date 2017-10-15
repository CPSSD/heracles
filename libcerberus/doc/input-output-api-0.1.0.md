# I/O API for payloads created with libcerberus

**Version: 0.1.0**

This API will use JSON objects for its input and output.

## Map

### Input

*Fields*

* `key` - A *string* containing the key for the map operation.
* `value` - A *string* containing the value for the map operation.

*Example*

```json
{
    "key": "foo",
    "value": "bar"
}
```

### Output

*Fields*

*Note*: Duplicate keys are allowed, and expected.

* `pairs` - An *array* of *objects*, each of which has the following fields:
    * `key` - A *string* containing the intermediate key from the map operation.
    * `value` - A *string* containing a value corresponding to the intermediate key.

*Example*

```json
{
    "pairs": [
        {
            "key": "foo_intermediate",
            "value": "bar"
        },
        {
            "key": "foo_intermediate",
            "value": "baz"
        }
    ]
}
```

## Reduce

### Input

*Fields*

* `key` - A *string* containing an intermediate key outputted from a map operation.
* `values` - An *array* of *strings* each containing an intermediate value as outputted from a map operation.

*Example*

```json
{
    "key": "foo_intermediate",
    "values": [
        "bar",
        "baz"
    ]
}
```

### Output

*Fields*

* `values` - An *array* of *strings* representing part of the final output of the map-reduce pipeline.

*Example*

```json
{
    "values": [
        "barbaz",
        "bazbar"
    ]
}
```