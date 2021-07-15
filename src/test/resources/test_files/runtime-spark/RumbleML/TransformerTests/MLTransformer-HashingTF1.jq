(:JIQS: ShouldRun; Output="({ "label" : 0, "stringArrayCol" : [ "i", "am", "data", "entry", "1" ], "hashingtf" : { "4" : 1, "7" : 1, "9" : 1, "13" : 1, "15" : 1 } }, { "label" : 1, "stringArrayCol" : [ "i", "am", "data", "entry", "2" ], "hashingtf" : { "7" : 1, "9" : 2, "13" : 1, "15" : 1 } }, { "label" : 2, "stringArrayCol" : [ "i", "am", "data", "entry", "3" ], "hashingtf" : { "7" : 1, "9" : 1, "13" : 1, "15" : 1, "18" : 1 } }, { "label" : 3, "stringArrayCol" : [ "i", "am", "data", "entry", "4" ], "hashingtf" : { "6" : 1, "7" : 1, "9" : 1, "13" : 1, "15" : 1 } }, { "label" : 4, "stringArrayCol" : [ "i", "am", "data", "entry", "5" ], "hashingtf" : { "7" : 1, "9" : 2, "13" : 1, "15" : 1 } }, { "label" : 5, "stringArrayCol" : [ "i", "am", "data", "entry", "6" ], "hashingtf" : { "7" : 1, "9" : 1, "13" : 1, "15" : 2 } })" :)
let $data := annotate(
    json-file("../../../../queries/rumbleML/sample-ml-data-flat.json"),
    { "label": "integer", "binaryLabel": "integer", "name": "string", "age": "double", "weight": "double", "booleanCol": "boolean", "nullCol": "null", "stringCol": "string", "stringArrayCol": ["string"], "intArrayCol": ["integer"],  "doubleArrayCol": ["double"],  "doubleArrayArrayCol": [["double"]] }
)

let $transformer := get-transformer("HashingTF")
for $result in $transformer(
    $data,
    {"inputCol": "stringArrayCol", "outputCol": "hashingtf", "numFeatures": 20}
)
return {
    "label": $result.label,
    "stringArrayCol": $result.stringArrayCol,
    "hashingtf": $result.hashingtf
}
