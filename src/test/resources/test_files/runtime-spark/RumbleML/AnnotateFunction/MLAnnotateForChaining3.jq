(:JIQS: ShouldRun; Output="({ "id" : 1, "sentence" : "Hi I heard about Spark", "tokenized" : [ "hi", "i", "heard", "about", "spark" ], "hashingTF" : { "0" : 1, "1" : 4 } }, { "id" : 2, "sentence" : "I wish Java could use case classes", "tokenized" : [ "i", "wish", "java", "could", "use", "case", "classes" ], "hashingTF" : { "0" : 1, "1" : 6 } }, { "id" : 3, "sentence" : "Logistic regression models are neat", "tokenized" : [ "logistic", "regression", "models", "are", "neat" ], "hashingTF" : { "0" : 3, "1" : 2 } })" :)
let $data := structured-json-file("../../../../queries/rumbleML/sample-ml-data-sentence.json")

let $tokenizer := get-transformer("Tokenizer")
let $intermediate-data := $tokenizer($data, {"inputCol": "sentence", "outputCol": "tokenized"})
let $df-intermediate-data := annotate($intermediate-data, {"id": "decimal", "sentence": "string", "tokenized": ["string"]})

let $hashingTF := get-transformer("HashingTF")
return $hashingTF($df-intermediate-data, {"inputCol": "tokenized", "outputCol": "hashingTF", "numFeatures": 2})

(: chain two different transformers together :)
