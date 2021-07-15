(:JIQS: ShouldRun; Output="({ "label" : 0, "name" : "a", "stringCol" : "i am data entry 1", "indexedStringCol" : 0 }, { "label" : 1, "name" : "b", "stringCol" : "i am data entry 2", "indexedStringCol" : 2 }, { "label" : 2, "name" : "c", "stringCol" : "i am data entry 3", "indexedStringCol" : 4 }, { "label" : 3, "name" : "d", "stringCol" : "i am data entry 4", "indexedStringCol" : 5 }, { "label" : 4, "name" : "e", "stringCol" : "i am data entry 5", "indexedStringCol" : 1 }, { "label" : 5, "name" : "f", "stringCol" : "i am data entry 6", "indexedStringCol" : 3 })" :)
let $data := annotate(
    json-file("../../../../queries/rumbleML/sample-ml-data-flat.json"),
    { "label": "integer", "binaryLabel": "integer", "name": "string", "age": "double", "weight": "double", "booleanCol": "boolean", "nullCol": "null", "stringCol": "string", "stringArrayCol": ["string"], "intArrayCol": ["integer"],  "doubleArrayCol": ["double"],  "doubleArrayArrayCol": [["double"]] }
)

let $est := get-estimator("StringIndexer")
let $tra := $est(
    $data,
    { "inputCol": "stringCol", "outputCol": "indexedStringCol" }
)
for $result in $tra(
    $data,
    { }
)
return {
    "label": $result.label,
    "name": $result.name,
    "stringCol": $result.stringCol,
    "indexedStringCol": $result.indexedStringCol
}
