(:JIQS: ShouldRun; Output="({ "label" : 0, "name" : "a", "age" : 20, "weight" : 50, "topicDistribution" : [ 0.9833443300169635, 0.0166556699830365 ] }, { "label" : 1, "name" : "b", "age" : 21, "weight" : 55.3, "topicDistribution" : [ 0.9851674303454577, 0.01483256965454223 ] }, { "label" : 2, "name" : "c", "age" : 22, "weight" : 60.6, "topicDistribution" : [ 0.986619424673936, 0.013380575326063963 ] }, { "label" : 3, "name" : "d", "age" : 23, "weight" : 65.9, "topicDistribution" : [ 0.9878039575392187, 0.012196042460781317 ] }, { "label" : 4, "name" : "e", "age" : 24, "weight" : 70.3, "topicDistribution" : [ 0.9886142429767494, 0.011385757023250669 ] }, { "label" : 5, "name" : "f", "age" : 25, "weight" : 75.6, "topicDistribution" : [ 0.9894688632580931, 0.01053113674190688 ] })" :)
let $data := annotate(
    json-file("../../../../queries/rumbleML/sample-ml-data-flat.json"),
    { "label": "integer", "binaryLabel": "integer", "name": "string", "age": "double", "weight": "double", "booleanCol": "boolean", "nullCol": "null", "stringCol": "string", "stringArrayCol": ["string"], "intArrayCol": ["integer"],  "doubleArrayCol": ["double"],  "doubleArrayArrayCol": [["double"]] }
)
let $vector-assembler := get-transformer("VectorAssembler")
let $data := $vector-assembler($data, {"inputCols" : [ "age", "weight" ], "outputCol" : "features" })

let $est := get-estimator("LDA")
let $tra := $est(
    $data,
    { "k": 2, "maxIter": 10}
)
for $result in $tra(
    $data,
    { }
)
return {
    "label": $result.label,
    "name": $result.name,
    "age": $result.age,
    "weight": $result.weight,
    "topicDistribution": $result.topicDistribution
}
