# RumbleDB ML
RumbleDB ML is a Machine Learning library built on top of the RumbleDB engine that makes it more productive and easier to perform ML tasks thanks to the abstraction layer provided by JSONiq.

The machine learning capabilities are exposed through JSONiq function items. The concepts of "estimator" and "transformer", which are core to Machine Learning, are naturally function items and fit seamlessly in the JSONiq data model.

Training sets, test sets, and validation sets, which contain features and labels, are exposed through JSONiq sequences of object items: the keys of these objects are the features and labels.

The names of the estimators and of the transformers, as well as the functionality they encapsulate, are directly inherited from the [SparkML](https://spark.apache.org/docs/latest/ml-guide.html) library which RumbleDB ML is based on: we chose not to reinvent the wheel.

## Transformers

A **transformer** is a function item that maps a sequence of objects to a sequence of objects.

It is an abstraction that either performs a feature transformation or generates predictions based on trained models. For example:

- _Tokenizer_ is a feature transformer that receives textual input data and splits it into individual terms (usually words), which are called tokens.

- _KMeansModel_ is a trained model and a transformer that can read a dataset containing features and generate predictions as its output.

## Estimators

An **estimator** is a function item that maps a sequence of objects to a transformer (yes, you got it right: that's a function item returned by a function item. This is why they are also called higher-order functions!).

Estimators abstract the concept of a Machine Learning algorithm or any algorithm that fits or trains on data. For example, a learning algorithm such as _KMeans_ is implemented as an Estimator. Calling this estimator on data essentially trains a KMeansModel, which is a Model and hence a Transformer.

## Parameters

Transformers and estimators are function items in the RumbleDB Data Model. Their first argument is the sequence of objects that represents, for example, the training set or test set. Parameters can be provided as their second argument. This second argument is expected to be an object item. The machine learning parameters form the fields of the said object item as key-value pairs.

## Type Annotations

RumbleDB ML works on highly structured data, because it requires full type information for all the fields in the training set or test set. It is on our development plan to automate the detection of these types when the sequence of objects gets created in the fly.

RumbleDB supports a user-defined type system with which you can validate and annotate datasets against a JSound schema.

This annotation is required to be applied on any dataset that must be used as input to RumbleDB ML, but it is superfluous if the data was directly read from a structured input format such as Parquet, CSV, Avro, SVM or ROOT.


## Examples

- Tokenizer Example:
```

declare type local:id-and-sentence as {
  "id": "integer",
  "sentence": "string"
};


let $local-data := (
    {"id": 1, "sentence": "Hi I heard about Spark"},
    {"id": 2, "sentence": "I wish Java could use case classes"},
    {"id": 3, "sentence": "Logistic regression models are neat"}
)
let $df-data := validate type local:id-and-sentence* { $local-data }

let $transformer := get-transformer("Tokenizer")
for $i in $transformer(
    $df-data,
    {"inputCol": "sentence", "outputCol": "output"}
)
return $i

// returns
// { "id" : 1, "sentence" : "Hi I heard about Spark", "output" : [ "hi", "i", "heard", "about", "spark" ] }
// { "id" : 2, "sentence" : "I wish Java could use case classes", "output" : [ "i", "wish", "java", "could", "use", "case", "classes" ] }
// { "id" : 3, "sentence" : "Logistic regression models are neat", "output" : [ "logistic", "regression", "models", "are", "neat" ] }
```

- KMeans Example:
```
declare type local:col-1-2-3 as {
  "id": "integer",
  "col1": "decimal",
  "col2": "decimal",
  "col3": "decimal"
};

let $vector-assembler := get-transformer("VectorAssembler")(
  ?,
  { "inputCols" : [ "col1", "col2", "col3" ], "outputCol" : "features" }
)

let $local-data := (
    {"id": 0, "col1": 0.0, "col2": 0.0, "col3": 0.0},
    {"id": 1, "col1": 0.1, "col2": 0.1, "col3": 0.1},
    {"id": 2, "col1": 0.2, "col2": 0.2, "col3": 0.2},
    {"id": 3, "col1": 9.0, "col2": 9.0, "col3": 9.0},
    {"id": 4, "col1": 9.1, "col2": 9.1, "col3": 9.1},
    {"id": 5, "col1": 9.2, "col2": 9.2, "col3": 9.2}
)
let $df-data := validate type local:col-1-2-3* {$local-data }
let $df-data := $vector-assembler($df-data)

let $est := get-estimator("KMeans")
let $tra := $est(
    $df-data,
    {"featuresCol": "features"}
)

for $i in $tra(
    $df-data,
    {"featuresCol": "features"}
)
return $i

// returns
// { "id" : 0, "col1" : 0, "col2" : 0, "col3" : 0, "prediction" : 0 }
// { "id" : 1, "col1" : 0.1, "col2" : 0.1, "col3" : 0.1, "prediction" : 0 }
// { "id" : 2, "col1" : 0.2, "col2" : 0.2, "col3" : 0.2, "prediction" : 0 }
// { "id" : 3, "col1" : 9, "col2" : 9, "col3" : 9, "prediction" : 1 }
// { "id" : 4, "col1" : 9.1, "col2" : 9.1, "col3" : 9.1, "prediction" : 1 }
// { "id" : 5, "col1" : 9.2, "col2" : 9.2, "col3" : 9.2, "prediction" : 1 }
```

# RumbleDB ML Functionality Overview:
## RumbleDB ML - Catalogue of Estimators:
### [AFTSurvivalRegression](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/regression/AFTSurvivalRegression.html)

#### Parameters:
```
- aggregationDepth: integer
- censorCol: string
- featuresCol: string
- fitIntercept: boolean
- labelCol: string
- maxIter: integer
- predictionCol: string
- quantileProbabilities: array (of double)
- quantilesCol: string
- tol: double
```
### [ALS](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/recommendation/ALS.html)
#### Parameters:
```
- alpha: double
- checkpointInterval: integer
- coldStartStrategy: string
- finalStorageLevel: string
- implicitPrefs: boolean
- intermediateStorageLevel: string
- itemCol: string
- maxIter: integer
- nonnegative: boolean
- numBlocks: integer
- numItemBlocks: integer
- numUserBlocks: integer
- predictionCol: string
- rank: integer
- ratingCol: string
- regParam: double
- seed: double
- userCol: string
```
### [BisectingKMeans](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/clustering/BisectingKMeans.html)
#### Parameters:
```
- distanceMeasure: string
- featuresCol: string
- k: integer
- maxIter: integer
- minDivisibleClusterSize: double
- predictionCol: string
- seed: double
```
### [BucketedRandomProjectionLSH](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/BucketedRandomProjectionLSH.html)
#### Parameters:
```
- bucketLength: double
- inputCol: string
- numHashTables: integer
- outputCol: string
- seed: double
```
### [ChiSqSelector](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/ChiSqSelector.html)
#### Parameters:
```
- fdr: double
- featuresCol: string
- fpr: double
- fwe: double
- labelCol: string
- numTopFeatures: integer
- outputCol: string
- percentile: double
- selectorType: string
```
### [CountVectorizer](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/CountVectorizer.html)
#### Parameters:
```
- binary: boolean
- inputCol: string
- maxDF: double
- minDF: double
- minTF: double
- outputCol: string
- vocabSize: integer
```
### [CrossValidator](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/tuning/CrossValidator.html)
#### Parameters:
```
- collectSubModels: boolean
- estimator: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- numFolds: integer
- parallelism: integer
- seed: double
```
### [DecisionTreeClassifier](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/classification/DecisionTreeClassifier.html)
#### Parameters:
```
- cacheNodeIds: boolean
- checkpointInterval: integer
- featuresCol: string
- impurity: string
- labelCol: string
- maxBins: integer
- maxDepth: integer
- maxMemoryInMB: integer
- minInfoGain: double
- minInstancesPerNode: integer
- predictionCol: string
- probabilityCol: string
- rawPredictionCol: string
- seed: double
- thresholds: array (of double)
```
### [DecisionTreeRegressor](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/regression/DecisionTreeRegressor.html)
#### Parameters:
```
- cacheNodeIds: boolean
- checkpointInterval: integer
- featuresCol: string
- impurity: string
- labelCol: string
- maxBins: integer
- maxDepth: integer
- maxMemoryInMB: integer
- minInfoGain: double
- minInstancesPerNode: integer
- predictionCol: string
- seed: double
- varianceCol: string
```
### [FPGrowth](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/fpm/FPGrowth.html)
#### Parameters:
```
- itemsCol: string
- minConfidence: double
- minSupport: double
- numPartitions: integer
- predictionCol: string
```
### [GBTClassifier](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/classification/GBTClassifier.html)
#### Parameters:
```
- cacheNodeIds: boolean
- checkpointInterval: integer
- featuresCol: string
- featureSubsetStrategy: string
- impurity: string
- labelCol: string
- lossType: string
- maxBins: integer
- maxDepth: integer
- maxIter: integer
- maxMemoryInMB: integer
- minInfoGain: double
- minInstancesPerNode: integer
- predictionCol: string
- probabilityCol: string
- rawPredictionCol: string
- seed: double
- stepSize: double
- subsamplingRate: double
- thresholds: array (of double)
- validationIndicatorCol: string
```
### [GBTRegressor](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/regression/GBTRegressor.html)
#### Parameters:
```
- cacheNodeIds: boolean
- checkpointInterval: integer
- featuresCol: string
- featureSubsetStrategy: string
- impurity: string
- labelCol: string
- lossType: string
- maxBins: integer
- maxDepth: integer
- maxIter: integer
- maxMemoryInMB: integer
- minInfoGain: double
- minInstancesPerNode: integer
- predictionCol: string
- seed: double
- stepSize: double
- subsamplingRate: double
- validationIndicatorCol: string
```
### [GaussianMixture](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/clustering/GaussianMixture.html)
#### Parameters:
```
- featuresCol: string
- k: integer
- maxIter: integer
- predictionCol: string
- probabilityCol: string
- seed: double
- tol: double
```
### [GeneralizedLinearRegression](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/regression/GeneralizedLinearRegression.html)
#### Parameters:
```
- family: string
- featuresCol: string
- fitIntercept: boolean
- labelCol: string
- link: string
- linkPower: double
- linkPredictionCol: string
- maxIter: integer
- offsetCol: string
- predictionCol: string
- regParam: double
- solver: string
- tol: double
- variancePower: double
- weightCol: string
```
### [IDF](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/IDF.html)
#### Parameters:
```
- inputCol: string
- minDocFreq: integer
- outputCol: string
```
### [Imputer](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/Imputer.html)
#### Parameters:
```
- inputCols: array (of string)
- missingValue: double
- outputCols: array (of string)
- strategy: string
```
### [IsotonicRegression](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/regression/IsotonicRegression.html)
#### Parameters:
```
- featureIndex: integer
- featuresCol: string
- isotonic: boolean
- labelCol: string
- predictionCol: string
- weightCol: string
```
### [KMeans](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/clustering/KMeans.html)
#### Parameters:
```
- distanceMeasure: string
- featuresCol: string
- initMode: string
- initSteps: integer
- k: integer
- maxIter: integer
- predictionCol: string
- seed: double
- tol: double
```
### [LDA](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/clustering/LDA.html)
#### Parameters:
```
- checkpointInterval: integer
- docConcentration: double
- docConcentration: array (of double)
- featuresCol: string
- k: integer
- keepLastCheckpoint: boolean
- learningDecay: double
- learningOffset: double
- maxIter: integer
- optimizeDocConcentration: boolean
- optimizer: string
- seed: double
- subsamplingRate: double
- topicConcentration: double
- topicDistributionCol: string
```
### [LinearRegression](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/regression/LinearRegression.html)
#### Parameters:
```
- aggregationDepth: integer
- elasticNetParam: double
- epsilon: double
- featuresCol: string
- fitIntercept: boolean
- labelCol: string
- loss: string
- maxIter: integer
- predictionCol: string
- regParam: double
- solver: string
- standardization: boolean
- tol: double
- weightCol: string
```
### [LinearSVC](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/classification/LinearSVC.html)
#### Parameters:
```
- aggregationDepth: integer
- featuresCol: string
- fitIntercept: boolean
- labelCol: string
- maxIter: integer
- predictionCol: string
- rawPredictionCol: string
- regParam: double
- standardization: boolean
- threshold: double
- tol: double
- weightCol: string
```
### [LogisticRegression](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/classification/LogisticRegression.html)
#### Parameters:
```
- aggregationDepth: integer
- elasticNetParam: double
- family: string
- featuresCol: string
- fitIntercept: boolean
- labelCol: string
- lowerBoundsOnCoefficients: object (of object of double)
- lowerBoundsOnIntercepts: object (of double)
- maxIter: integer
- predictionCol: string
- probabilityCol: string
- rawPredictionCol: string
- regParam: double
- standardization: boolean
- threshold: double
- thresholds: array (of double)
- tol: double
- upperBoundsOnCoefficients: object (of object of double)
- upperBoundsOnIntercepts: object (of double)
- weightCol: string
```
### [MaxAbsScaler](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/MaxAbsScaler.html)
#### Parameters:
```
- inputCol: string
- outputCol: string
```
### [MinHashLSH](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/MinHashLSH.html)
#### Parameters:
```
- inputCol: string
- numHashTables: integer
- outputCol: string
- seed: double
```
### [MinMaxScaler](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/MinMaxScaler.html)
#### Parameters:
```
- inputCol: string
- max: double
- min: double
- outputCol: string
```
### [MultilayerPerceptronClassifier](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/classification/MultilayerPerceptronClassifier.html)
#### Parameters:
```
- blockSize: integer
- featuresCol: string
- initialWeights: object (of double)
- labelCol: string
- layers: array (of integer)
- maxIter: integer
- predictionCol: string
- probabilityCol: string
- rawPredictionCol: string
- seed: double
- solver: string
- stepSize: double
- thresholds: array (of double)
- tol: double
```
### [NaiveBayes](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/classification/NaiveBayes.html)
#### Parameters:
```
- featuresCol: string
- labelCol: string
- modelType: string
- predictionCol: string
- probabilityCol: string
- rawPredictionCol: string
- smoothing: double
- thresholds: array (of double)
- weightCol: string
```
### [OneHotEncoderEstimator](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/OneHotEncoderEstimator.html)
#### Parameters:
```
- dropLast: boolean
- handleInvalid: string
- inputCols: array (of string)
- outputCols: array (of string)
```
### [OneVsRest](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/classification/OneVsRest.html)
#### Parameters:
```
- featuresCol: string
- labelCol: string
- parallelism: integer
- predictionCol: string
- rawPredictionCol: string
- weightCol: string
```
### [PCA](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/PCA.html)
#### Parameters:
```
- inputCol: string
- k: integer
- outputCol: string
```
### [Pipeline](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/Pipeline.html)
#### Parameters:
```
```
### [QuantileDiscretizer](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/QuantileDiscretizer.html)
#### Parameters:
```
- handleInvalid: string
- inputCol: string
- inputCols: array (of string)
- numBuckets: integer
- numBucketsArray: array (of integer)
- outputCol: string
- outputCols: array (of string)
- relativeError: double
```
### [RFormula](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/RFormula.html)
#### Parameters:
```
- featuresCol: string
- forceIndexLabel: boolean
- formula: string
- handleInvalid: string
- labelCol: string
- stringIndexerOrderType: string
```
### [RandomForestClassifier](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/classification/RandomForestClassifier.html)
#### Parameters:
```
- cacheNodeIds: boolean
- checkpointInterval: integer
- featuresCol: string
- featureSubsetStrategy: string
- impurity: string
- labelCol: string
- maxBins: integer
- maxDepth: integer
- maxMemoryInMB: integer
- minInfoGain: double
- minInstancesPerNode: integer
- numTrees: integer
- predictionCol: string
- probabilityCol: string
- rawPredictionCol: string
- seed: double
- subsamplingRate: double
- thresholds: array (of double)
```
### [RandomForestRegressor](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/regression/RandomForestRegressor.html)
#### Parameters:
```
- cacheNodeIds: boolean
- checkpointInterval: integer
- featuresCol: string
- featureSubsetStrategy: string
- impurity: string
- labelCol: string
- maxBins: integer
- maxDepth: integer
- maxMemoryInMB: integer
- minInfoGain: double
- minInstancesPerNode: integer
- numTrees: integer
- predictionCol: string
- seed: double
- subsamplingRate: double
```
### [StandardScaler](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/StandardScaler.html)
#### Parameters:
```
- inputCol: string
- outputCol: string
- withMean: boolean
- withStd: boolean
```
### [StringIndexer](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/StringIndexer.html)
#### Parameters:
```
- handleInvalid: string
- inputCol: string
- outputCol: string
- stringOrderType: string
```
### [TrainValidationSplit](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/tuning/TrainValidationSplit.html)
#### Parameters:
```
- collectSubModels: boolean
- estimator: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- parallelism: integer
- seed: double
- trainRatio: double
```
### [VectorIndexer](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/VectorIndexer.html)
#### Parameters:
```
- handleInvalid: string
- inputCol: string
- maxCategories: integer
- outputCol: string
```
### [Word2Vec](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/Word2Vec.html)
#### Parameters:
```
- inputCol: string
- maxIter: integer
- maxSentenceLength: integer
- minCount: integer
- numPartitions: integer
- outputCol: string
- seed: double
- stepSize: double
- vectorSize: integer
- windowSize: integer
```

## RumbleDB ML - Catalogue of Transformers:
### [AFTSurvivalRegressionModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/regression/AFTSurvivalRegressionModel.html)

#### Parameters:
```
- featuresCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- predictionCol: string
- quantileProbabilities: array (of double)
- quantilesCol: string
```
### [ALSModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/recommendation/ALSModel.html)
#### Parameters:
```
- coldStartStrategy: string
- itemCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- predictionCol: string
- userCol: string
```
### [Binarizer](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/Binarizer.html)
#### Parameters:
```
- inputCol: string
- outputCol: string
- threshold: double
```
### [BisectingKMeansModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/clustering/BisectingKMeansModel.html)
#### Parameters:
```
- featuresCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- predictionCol: string
```
### [BucketedRandomProjectionLSHModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/BucketedRandomProjectionLSHModel.html)
#### Parameters:
```
- inputCol: string
- outputCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
```
### [Bucketizer](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/Bucketizer.html)
#### Parameters:
```
- handleInvalid: string
- inputCol: string
- inputCols: array (of string)
- outputCol: string
- outputCols: array (of string)
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- splits: array (of double)
- splitsArray: array (of array of double)
```
### [ChiSqSelectorModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/ChiSqSelectorModel.html)
#### Parameters:
```
- featuresCol: string
- outputCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
```
### [ColumnPruner](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/ColumnPruner.html)
#### Parameters:
```
```
### [CountVectorizerModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/CountVectorizerModel.html)
#### Parameters:
```
- binary: boolean
- inputCol: string
- minTF: double
- outputCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
```
### [CrossValidatorModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/tuning/CrossValidatorModel.html)
#### Parameters:
```
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
```
### [DCT](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/DCT.html)
#### Parameters:
```
- inputCol: string
- inverse: boolean
- outputCol: string
```
### [DecisionTreeClassificationModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/classification/DecisionTreeClassificationModel.html)
#### Parameters:
```
- cacheNodeIds: boolean
- checkpointInterval: integer
- featuresCol: string
- impurity: string
- maxBins: integer
- maxDepth: integer
- maxMemoryInMB: integer
- minInfoGain: double
- minInstancesPerNode: integer
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- predictionCol: string
- probabilityCol: string
- rawPredictionCol: string
- seed: double
- thresholds: array (of double)
```
### [DecisionTreeRegressionModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/regression/DecisionTreeRegressionModel.html)
#### Parameters:
```
- cacheNodeIds: boolean
- checkpointInterval: integer
- featuresCol: string
- impurity: string
- maxBins: integer
- maxDepth: integer
- maxMemoryInMB: integer
- minInfoGain: double
- minInstancesPerNode: integer
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- predictionCol: string
- seed: double
- varianceCol: string
```
### [DistributedLDAModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/clustering/DistributedLDAModel.html)
#### Parameters:
```
- featuresCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- seed: double
- topicDistributionCol: string
```
### [ElementwiseProduct](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/ElementwiseProduct.html)
#### Parameters:
```
- inputCol: string
- outputCol: string
- scalingVec: object (of double)
```
### [FPGrowthModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/fpm/FPGrowthModel.html)
#### Parameters:
```
- itemsCol: string
- minConfidence: double
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- predictionCol: string
```
### [FeatureHasher](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/FeatureHasher.html)
#### Parameters:
```
- categoricalCols: array (of string)
- inputCols: array (of string)
- numFeatures: integer
- outputCol: string
```
### [GBTClassificationModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/classification/GBTClassificationModel.html)
#### Parameters:
```
- cacheNodeIds: boolean
- checkpointInterval: integer
- featuresCol: string
- featureSubsetStrategy: string
- impurity: string
- maxBins: integer
- maxDepth: integer
- maxIter: integer
- maxMemoryInMB: integer
- minInfoGain: double
- minInstancesPerNode: integer
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- predictionCol: string
- probabilityCol: string
- rawPredictionCol: string
- seed: double
- stepSize: double
- subsamplingRate: double
- thresholds: array (of double)
```
### [GBTRegressionModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/regression/GBTRegressionModel.html)
#### Parameters:
```
- cacheNodeIds: boolean
- checkpointInterval: integer
- featuresCol: string
- featureSubsetStrategy: string
- impurity: string
- maxBins: integer
- maxDepth: integer
- maxIter: integer
- maxMemoryInMB: integer
- minInfoGain: double
- minInstancesPerNode: integer
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- predictionCol: string
- seed: double
- stepSize: double
- subsamplingRate: double
```
### [GaussianMixtureModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/clustering/GaussianMixtureModel.html)
#### Parameters:
```
- featuresCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- predictionCol: string
- probabilityCol: string
```
### [GeneralizedLinearRegressionModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/regression/GeneralizedLinearRegressionModel.html)
#### Parameters:
```
- featuresCol: string
- linkPredictionCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- predictionCol: string
```
### [HashingTF](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/HashingTF.html)
#### Parameters:
```
- binary: boolean
- inputCol: string
- numFeatures: integer
- outputCol: string
```
### [IDFModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/IDFModel.html)
#### Parameters:
```
- inputCol: string
- outputCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
```
### [ImputerModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/ImputerModel.html)
#### Parameters:
```
- inputCols: array (of string)
- outputCols: array (of string)
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
```
### [IndexToString](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/IndexToString.html)
#### Parameters:
```
- inputCol: string
- labels: array (of string)
- outputCol: string
```
### [Interaction](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/Interaction.html)
#### Parameters:
```
- inputCols: array (of string)
- outputCol: string
```
### [IsotonicRegressionModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/regression/IsotonicRegressionModel.html)
#### Parameters:
```
- featureIndex: integer
- featuresCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- predictionCol: string
```
### [KMeansModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/clustering/KMeansModel.html)
#### Parameters:
```
- featuresCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- predictionCol: string
```
### [LinearRegressionModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/regression/LinearRegressionModel.html)
#### Parameters:
```
- featuresCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- predictionCol: string
```
### [LinearSVCModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/classification/LinearSVCModel.html)
#### Parameters:
```
- featuresCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- predictionCol: string
- rawPredictionCol: string
- threshold: double
- weightCol: double
```
### [LocalLDAModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/clustering/LocalLDAModel.html)
#### Parameters:
```
- featuresCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- seed: double
- topicDistributionCol: string
```
### [LogisticRegressionModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/classification/LogisticRegressionModel.html)
#### Parameters:
```
- featuresCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- predictionCol: string
- probabilityCol: string
- rawPredictionCol: string
- threshold: double
- thresholds: array (of double)
```
### [MaxAbsScalerModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/MaxAbsScalerModel.html)
#### Parameters:
```
- inputCol: string
- outputCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
```
### [MinHashLSHModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/MinHashLSHModel.html)
#### Parameters:
```
- inputCol: string
- outputCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
```
### [MinMaxScalerModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/MinMaxScalerModel.html)
#### Parameters:
```
- inputCol: string
- max: double
- min: double
- outputCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
```
### [MultilayerPerceptronClassificationModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/classification/MultilayerPerceptronClassificationModel.html)
#### Parameters:
```
- featuresCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- predictionCol: string
- probabilityCol: string
- rawPredictionCol: string
- thresholds: array (of double)
```
### [NGram](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/NGram.html)
#### Parameters:
```
- inputCol: string
- n: integer
- outputCol: string
```
### [NaiveBayesModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/classification/NaiveBayesModel.html)
#### Parameters:
```
- featuresCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- predictionCol: string
- probabilityCol: string
- rawPredictionCol: string
- thresholds: array (of double)
```
### [Normalizer](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/Normalizer.html)
#### Parameters:
```
- inputCol: string
- outputCol: string
- p: double
```
### [OneHotEncoder](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/OneHotEncoder.html)
#### Parameters:
```
- dropLast: boolean
- inputCol: string
- outputCol: string
```
### [OneHotEncoderModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/OneHotEncoderModel.html)
#### Parameters:
```
- dropLast: boolean
- handleInvalid: string
- inputCols: array (of string)
- outputCols: array (of string)
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
```
### [OneVsRestModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/classification/OneVsRestModel.html)
#### Parameters:
```
- featuresCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- predictionCol: string
- rawPredictionCol: string
```
### [PCAModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/PCAModel.html)
#### Parameters:
```
- inputCol: string
- outputCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
```
### [PipelineModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/PipelineModel.html)
#### Parameters:
```
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
```
### [PolynomialExpansion](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/PolynomialExpansion.html)
#### Parameters:
```
- degree: integer
- inputCol: string
- outputCol: string
```
### [RFormulaModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/RFormulaModel.html)
#### Parameters:
```
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
```
### [RandomForestClassificationModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/classification/RandomForestClassificationModel.html)
#### Parameters:
```
- cacheNodeIds: boolean
- checkpointInterval: integer
- featuresCol: string
- featureSubsetStrategy: string
- impurity: string
- maxBins: integer
- maxDepth: integer
- maxMemoryInMB: integer
- minInfoGain: double
- minInstancesPerNode: integer
- numTrees: integer
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- predictionCol: string
- probabilityCol: string
- rawPredictionCol: string
- seed: double
- subsamplingRate: double
- thresholds: array (of double)
```
### [RandomForestRegressionModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/regression/RandomForestRegressionModel.html)
#### Parameters:
```
- cacheNodeIds: boolean
- checkpointInterval: integer
- featuresCol: string
- featureSubsetStrategy: string
- impurity: string
- maxBins: integer
- maxDepth: integer
- maxMemoryInMB: integer
- minInfoGain: double
- minInstancesPerNode: integer
- numTrees: integer
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
- predictionCol: string
- seed: double
- subsamplingRate: double
```
### [RegexTokenizer](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/RegexTokenizer.html)
#### Parameters:
```
- gaps: boolean
- inputCol: string
- minTokenLength: integer
- outputCol: string
- pattern: string
- toLowercase: boolean
```
### [SQLTransformer](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/SQLTransformer.html)
#### Parameters:
```
- statement: string
```
### [StandardScalerModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/StandardScalerModel.html)
#### Parameters:
```
- inputCol: string
- outputCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
```
### [StopWordsRemover](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/StopWordsRemover.html)
#### Parameters:
```
- caseSensitive: boolean
- inputCol: string
- locale: string
- outputCol: string
- stopWords: array (of string)
```
### [StringIndexerModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/StringIndexerModel.html)
#### Parameters:
```
- handleInvalid: string
- inputCol: string
- outputCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
```
### [Tokenizer](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/Tokenizer.html)
#### Parameters:
```
- inputCol: string
- outputCol: string
```
### [TrainValidationSplitModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/tuning/TrainValidationSplitModel.html)
#### Parameters:
```
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
```
### [VectorAssembler](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/VectorAssembler.html)
#### Parameters:
```
- handleInvalid: string
- inputCols: array (of string)
- outputCol: string
```
### [VectorAttributeRewriter](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/VectorAttributeRewriter.html)
#### Parameters:
```
```
### [VectorIndexerModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/VectorIndexerModel.html)
#### Parameters:
```
- inputCol: string
- outputCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
```
### [VectorSizeHint](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/VectorSizeHint.html)
#### Parameters:
```
- handleInvalid: string
- inputCol: string
- size: integer
```
### [VectorSlicer](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/VectorSlicer.html)
#### Parameters:
```
- indices: array (of integer)
- inputCol: string
- names: array (of string)
- outputCol: string
```
### [Word2VecModel](https://spark.apache.org/docs/2.4.4/api/java/org/apache/spark/ml/feature/Word2VecModel.html)
#### Parameters:
```
- inputCol: string
- outputCol: string
- parent: estimator (i.e., function(object*, object) as function(object*, object) as object*)
```

