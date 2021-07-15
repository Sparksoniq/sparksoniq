(:JIQS: ShouldRun; Output="({ "label" : 0, "name" : "a", "binaryLabel" : 0, "age" : 20, "enc_binaryLabel" : { "0" : 1 }, "enc_age" : { "20" : 1 } }, { "label" : 1, "name" : "b", "binaryLabel" : 0, "age" : 21, "enc_binaryLabel" : { "0" : 1 }, "enc_age" : { "21" : 1 } }, { "label" : 2, "name" : "c", "binaryLabel" : 0, "age" : 22, "enc_binaryLabel" : { "0" : 1 }, "enc_age" : { "22" : 1 } }, { "label" : 3, "name" : "d", "binaryLabel" : 1, "age" : 23, "enc_binaryLabel" : { }, "enc_age" : { "23" : 1 } }, { "label" : 4, "name" : "e", "binaryLabel" : 1, "age" : 24, "enc_binaryLabel" : { }, "enc_age" : { "24" : 1 } }, { "label" : 5, "name" : "f", "binaryLabel" : 1, "age" : 25, "enc_binaryLabel" : { }, "enc_age" : { } })" :)
let $data := annotate(
    json-file("../../../../queries/rumbleML/sample-ml-data-flat.json"),
    { "label": "integer", "binaryLabel": "integer", "name": "string", "age": "double", "weight": "double", "booleanCol": "boolean", "nullCol": "null", "stringCol": "string", "stringArrayCol": ["string"], "intArrayCol": ["integer"],  "doubleArrayCol": ["double"],  "doubleArrayArrayCol": [["double"]] }
)

let $est := get-estimator("OneHotEncoderEstimator")
let $tra := $est(
    $data,
    { "inputCols": ["binaryLabel", "age"], "outputCols": ["enc_binaryLabel", "enc_age"]}
)
for $result in $tra(
    $data,
    { }
)
return {
    "label": $result.label,
    "name": $result.name,
    "binaryLabel": $result.binaryLabel,
    "age": $result.age,
    "enc_binaryLabel": $result.enc_binaryLabel,
    "enc_age": $result.enc_age
}

(: https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/OneHotEncoder.html :)
(: WARNING - OneHotEncoderEstimator will be renamed OneHotEncoder and this OneHotEncoder will be removed in 3.0.0. :)
