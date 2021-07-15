package sparksoniq.spark.ml;

import org.rumbledb.exceptions.ExceptionMetadata;
import org.rumbledb.exceptions.OurBadException;
import org.rumbledb.exceptions.UnrecognizedRumbleMLClassReferenceException;
import org.rumbledb.exceptions.UnrecognizedRumbleMLParamReferenceException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Maps within these catalog are auto generated (also the hashmap.put(...) statements) with the SparkML scraper python
 * tool
 */
public class RumbleMLCatalog {
    private static final HashMap<String, String> estimatorFullClassNames;
    private static final HashMap<String, String> transformerFullClassNames;

    private static final HashMap<String, String> paramJavaTypeNames;

    private static final HashMap<String, List<String>> estimatorParams;
    private static final HashMap<String, List<String>> transformerParams;

    static {
        estimatorFullClassNames = new HashMap<>();
        transformerFullClassNames = new HashMap<>();

        estimatorFullClassNames.put("AFTSurvivalRegression", "org.apache.spark.ml.regression.AFTSurvivalRegression");
        estimatorFullClassNames.put("ALS", "org.apache.spark.ml.recommendation.ALS");
        estimatorFullClassNames.put("BisectingKMeans", "org.apache.spark.ml.clustering.BisectingKMeans");
        estimatorFullClassNames.put(
            "BucketedRandomProjectionLSH",
            "org.apache.spark.ml.feature.BucketedRandomProjectionLSH"
        );
        estimatorFullClassNames.put("ChiSqSelector", "org.apache.spark.ml.feature.ChiSqSelector");
        estimatorFullClassNames.put("CountVectorizer", "org.apache.spark.ml.feature.CountVectorizer");
        estimatorFullClassNames.put("CrossValidator", "org.apache.spark.ml.tuning.CrossValidator");
        estimatorFullClassNames.put(
            "DecisionTreeClassifier",
            "org.apache.spark.ml.classification.DecisionTreeClassifier"
        );
        estimatorFullClassNames.put("DecisionTreeRegressor", "org.apache.spark.ml.regression.DecisionTreeRegressor");
        estimatorFullClassNames.put("FPGrowth", "org.apache.spark.ml.fpm.FPGrowth");
        estimatorFullClassNames.put("GBTClassifier", "org.apache.spark.ml.classification.GBTClassifier");
        estimatorFullClassNames.put("GBTRegressor", "org.apache.spark.ml.regression.GBTRegressor");
        estimatorFullClassNames.put("GaussianMixture", "org.apache.spark.ml.clustering.GaussianMixture");
        estimatorFullClassNames.put(
            "GeneralizedLinearRegression",
            "org.apache.spark.ml.regression.GeneralizedLinearRegression"
        );
        estimatorFullClassNames.put("IDF", "org.apache.spark.ml.feature.IDF");
        estimatorFullClassNames.put("Imputer", "org.apache.spark.ml.feature.Imputer");
        estimatorFullClassNames.put("IsotonicRegression", "org.apache.spark.ml.regression.IsotonicRegression");
        estimatorFullClassNames.put("KMeans", "org.apache.spark.ml.clustering.KMeans");
        estimatorFullClassNames.put("LDA", "org.apache.spark.ml.clustering.LDA");
        estimatorFullClassNames.put("LinearRegression", "org.apache.spark.ml.regression.LinearRegression");
        estimatorFullClassNames.put("LinearSVC", "org.apache.spark.ml.classification.LinearSVC");
        estimatorFullClassNames.put("LogisticRegression", "org.apache.spark.ml.classification.LogisticRegression");
        estimatorFullClassNames.put("MaxAbsScaler", "org.apache.spark.ml.feature.MaxAbsScaler");
        estimatorFullClassNames.put("MinHashLSH", "org.apache.spark.ml.feature.MinHashLSH");
        estimatorFullClassNames.put("MinMaxScaler", "org.apache.spark.ml.feature.MinMaxScaler");
        estimatorFullClassNames.put(
            "MultilayerPerceptronClassifier",
            "org.apache.spark.ml.classification.MultilayerPerceptronClassifier"
        );
        estimatorFullClassNames.put("NaiveBayes", "org.apache.spark.ml.classification.NaiveBayes");
        estimatorFullClassNames.put("OneHotEncoderEstimator", "org.apache.spark.ml.feature.OneHotEncoderEstimator");
        estimatorFullClassNames.put("OneVsRest", "org.apache.spark.ml.classification.OneVsRest");
        estimatorFullClassNames.put("PCA", "org.apache.spark.ml.feature.PCA");
        estimatorFullClassNames.put("Pipeline", "org.apache.spark.ml.Pipeline");
        estimatorFullClassNames.put("QuantileDiscretizer", "org.apache.spark.ml.feature.QuantileDiscretizer");
        estimatorFullClassNames.put("RFormula", "org.apache.spark.ml.feature.RFormula");
        estimatorFullClassNames.put(
            "RandomForestClassifier",
            "org.apache.spark.ml.classification.RandomForestClassifier"
        );
        estimatorFullClassNames.put("RandomForestRegressor", "org.apache.spark.ml.regression.RandomForestRegressor");
        estimatorFullClassNames.put("StandardScaler", "org.apache.spark.ml.feature.StandardScaler");
        estimatorFullClassNames.put("StringIndexer", "org.apache.spark.ml.feature.StringIndexer");
        estimatorFullClassNames.put("TrainValidationSplit", "org.apache.spark.ml.tuning.TrainValidationSplit");
        estimatorFullClassNames.put("VectorIndexer", "org.apache.spark.ml.feature.VectorIndexer");
        estimatorFullClassNames.put("Word2Vec", "org.apache.spark.ml.feature.Word2Vec");


        transformerFullClassNames.put(
            "AFTSurvivalRegressionModel",
            "org.apache.spark.ml.regression.AFTSurvivalRegressionModel"
        );
        transformerFullClassNames.put("ALSModel", "org.apache.spark.ml.recommendation.ALSModel");
        transformerFullClassNames.put("Binarizer", "org.apache.spark.ml.feature.Binarizer");
        transformerFullClassNames.put("BisectingKMeansModel", "org.apache.spark.ml.clustering.BisectingKMeansModel");
        transformerFullClassNames.put(
            "BucketedRandomProjectionLSHModel",
            "org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel"
        );
        transformerFullClassNames.put("Bucketizer", "org.apache.spark.ml.feature.Bucketizer");
        transformerFullClassNames.put("ChiSqSelectorModel", "org.apache.spark.ml.feature.ChiSqSelectorModel");
        transformerFullClassNames.put("ColumnPruner", "org.apache.spark.ml.feature.ColumnPruner");
        transformerFullClassNames.put("CountVectorizerModel", "org.apache.spark.ml.feature.CountVectorizerModel");
        transformerFullClassNames.put("CrossValidatorModel", "org.apache.spark.ml.tuning.CrossValidatorModel");
        transformerFullClassNames.put("DCT", "org.apache.spark.ml.feature.DCT");
        transformerFullClassNames.put(
            "DecisionTreeClassificationModel",
            "org.apache.spark.ml.classification.DecisionTreeClassificationModel"
        );
        transformerFullClassNames.put(
            "DecisionTreeRegressionModel",
            "org.apache.spark.ml.regression.DecisionTreeRegressionModel"
        );
        transformerFullClassNames.put("DistributedLDAModel", "org.apache.spark.ml.clustering.DistributedLDAModel");
        transformerFullClassNames.put("ElementwiseProduct", "org.apache.spark.ml.feature.ElementwiseProduct");
        transformerFullClassNames.put("FPGrowthModel", "org.apache.spark.ml.fpm.FPGrowthModel");
        transformerFullClassNames.put("FeatureHasher", "org.apache.spark.ml.feature.FeatureHasher");
        transformerFullClassNames.put(
            "GBTClassificationModel",
            "org.apache.spark.ml.classification.GBTClassificationModel"
        );
        transformerFullClassNames.put("GBTRegressionModel", "org.apache.spark.ml.regression.GBTRegressionModel");
        transformerFullClassNames.put("GaussianMixtureModel", "org.apache.spark.ml.clustering.GaussianMixtureModel");
        transformerFullClassNames.put(
            "GeneralizedLinearRegressionModel",
            "org.apache.spark.ml.regression.GeneralizedLinearRegressionModel"
        );
        transformerFullClassNames.put("HashingTF", "org.apache.spark.ml.feature.HashingTF");
        transformerFullClassNames.put("IDFModel", "org.apache.spark.ml.feature.IDFModel");
        transformerFullClassNames.put("ImputerModel", "org.apache.spark.ml.feature.ImputerModel");
        transformerFullClassNames.put("IndexToString", "org.apache.spark.ml.feature.IndexToString");
        transformerFullClassNames.put("Interaction", "org.apache.spark.ml.feature.Interaction");
        transformerFullClassNames.put(
            "IsotonicRegressionModel",
            "org.apache.spark.ml.regression.IsotonicRegressionModel"
        );
        transformerFullClassNames.put("KMeansModel", "org.apache.spark.ml.clustering.KMeansModel");
        transformerFullClassNames.put("LinearRegressionModel", "org.apache.spark.ml.regression.LinearRegressionModel");
        transformerFullClassNames.put("LinearSVCModel", "org.apache.spark.ml.classification.LinearSVCModel");
        transformerFullClassNames.put("LocalLDAModel", "org.apache.spark.ml.clustering.LocalLDAModel");
        transformerFullClassNames.put(
            "LogisticRegressionModel",
            "org.apache.spark.ml.classification.LogisticRegressionModel"
        );
        transformerFullClassNames.put("MaxAbsScalerModel", "org.apache.spark.ml.feature.MaxAbsScalerModel");
        transformerFullClassNames.put("MinHashLSHModel", "org.apache.spark.ml.feature.MinHashLSHModel");
        transformerFullClassNames.put("MinMaxScalerModel", "org.apache.spark.ml.feature.MinMaxScalerModel");
        transformerFullClassNames.put(
            "MultilayerPerceptronClassificationModel",
            "org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel"
        );
        transformerFullClassNames.put("NGram", "org.apache.spark.ml.feature.NGram");
        transformerFullClassNames.put("NaiveBayesModel", "org.apache.spark.ml.classification.NaiveBayesModel");
        transformerFullClassNames.put("Normalizer", "org.apache.spark.ml.feature.Normalizer");
        transformerFullClassNames.put("OneHotEncoder", "org.apache.spark.ml.feature.OneHotEncoder");
        transformerFullClassNames.put("OneHotEncoderModel", "org.apache.spark.ml.feature.OneHotEncoderModel");
        transformerFullClassNames.put("OneVsRestModel", "org.apache.spark.ml.classification.OneVsRestModel");
        transformerFullClassNames.put("PCAModel", "org.apache.spark.ml.feature.PCAModel");
        transformerFullClassNames.put("PipelineModel", "org.apache.spark.ml.PipelineModel");
        transformerFullClassNames.put("PolynomialExpansion", "org.apache.spark.ml.feature.PolynomialExpansion");
        transformerFullClassNames.put("RFormulaModel", "org.apache.spark.ml.feature.RFormulaModel");
        transformerFullClassNames.put(
            "RandomForestClassificationModel",
            "org.apache.spark.ml.classification.RandomForestClassificationModel"
        );
        transformerFullClassNames.put(
            "RandomForestRegressionModel",
            "org.apache.spark.ml.regression.RandomForestRegressionModel"
        );
        transformerFullClassNames.put("RegexTokenizer", "org.apache.spark.ml.feature.RegexTokenizer");
        transformerFullClassNames.put("SQLTransformer", "org.apache.spark.ml.feature.SQLTransformer");
        transformerFullClassNames.put("StandardScalerModel", "org.apache.spark.ml.feature.StandardScalerModel");
        transformerFullClassNames.put("StopWordsRemover", "org.apache.spark.ml.feature.StopWordsRemover");
        transformerFullClassNames.put("StringIndexerModel", "org.apache.spark.ml.feature.StringIndexerModel");
        transformerFullClassNames.put("Tokenizer", "org.apache.spark.ml.feature.Tokenizer");
        transformerFullClassNames.put(
            "TrainValidationSplitModel",
            "org.apache.spark.ml.tuning.TrainValidationSplitModel"
        );
        transformerFullClassNames.put("VectorAssembler", "org.apache.spark.ml.feature.VectorAssembler");
        transformerFullClassNames.put("VectorAttributeRewriter", "org.apache.spark.ml.feature.VectorAttributeRewriter");
        transformerFullClassNames.put("VectorIndexerModel", "org.apache.spark.ml.feature.VectorIndexerModel");
        transformerFullClassNames.put("VectorSizeHint", "org.apache.spark.ml.feature.VectorSizeHint");
        transformerFullClassNames.put("VectorSlicer", "org.apache.spark.ml.feature.VectorSlicer");
        transformerFullClassNames.put("Word2VecModel", "org.apache.spark.ml.feature.Word2VecModel");

    }

    static {
        paramJavaTypeNames = new HashMap<>();

        paramJavaTypeNames.put("aggregationDepth", "int");
        paramJavaTypeNames.put("alpha", "double");
        paramJavaTypeNames.put("binary", "boolean");
        paramJavaTypeNames.put("blockSize", "int");
        paramJavaTypeNames.put("bucketLength", "double");
        paramJavaTypeNames.put("cacheNodeIds", "boolean");
        paramJavaTypeNames.put("caseSensitive", "boolean");
        paramJavaTypeNames.put("categoricalCols", "String[]");
        paramJavaTypeNames.put("censorCol", "String");
        paramJavaTypeNames.put("checkpointInterval", "int");
        paramJavaTypeNames.put("coldStartStrategy", "String");
        paramJavaTypeNames.put("collectSubModels", "boolean");
        paramJavaTypeNames.put("degree", "int");
        paramJavaTypeNames.put("distanceMeasure", "String");
        // ml.clustering.LDA offers two setters for docConcentration, one is disabled for the time being
        // paramJavaTypeNames.put("docConcentration", "double");
        paramJavaTypeNames.put("docConcentration", "double[]");
        paramJavaTypeNames.put("dropLast", "boolean");
        paramJavaTypeNames.put("elasticNetParam", "double");
        paramJavaTypeNames.put("epsilon", "double");
        paramJavaTypeNames.put("estimator", "Estimator<?>");
        paramJavaTypeNames.put("family", "String");
        paramJavaTypeNames.put("fdr", "double");
        paramJavaTypeNames.put("featureIndex", "int");
        paramJavaTypeNames.put("featureSubsetStrategy", "String");
        paramJavaTypeNames.put("featuresCol", "String");
        paramJavaTypeNames.put("finalStorageLevel", "String");
        paramJavaTypeNames.put("fitIntercept", "boolean");
        paramJavaTypeNames.put("forceIndexLabel", "boolean");
        paramJavaTypeNames.put("formula", "String");
        paramJavaTypeNames.put("fpr", "double");
        paramJavaTypeNames.put("fwe", "double");
        paramJavaTypeNames.put("gaps", "boolean");
        paramJavaTypeNames.put("handleInvalid", "String");
        paramJavaTypeNames.put("implicitPrefs", "boolean");
        paramJavaTypeNames.put("impurity", "String");
        paramJavaTypeNames.put("indices", "int[]");
        paramJavaTypeNames.put("initMode", "String");
        paramJavaTypeNames.put("initSteps", "int");
        paramJavaTypeNames.put("initialWeights", "Vector");
        paramJavaTypeNames.put("inputCol", "String");
        paramJavaTypeNames.put("inputCols", "String[]");
        paramJavaTypeNames.put("intermediateStorageLevel", "String");
        paramJavaTypeNames.put("inverse", "boolean");
        paramJavaTypeNames.put("isotonic", "boolean");
        paramJavaTypeNames.put("itemCol", "String");
        paramJavaTypeNames.put("itemsCol", "String");
        paramJavaTypeNames.put("k", "int");
        paramJavaTypeNames.put("keepLastCheckpoint", "boolean");
        paramJavaTypeNames.put("labelCol", "String");
        paramJavaTypeNames.put("labels", "String[]");
        paramJavaTypeNames.put("layers", "int[]");
        paramJavaTypeNames.put("learningDecay", "double");
        paramJavaTypeNames.put("learningOffset", "double");
        paramJavaTypeNames.put("link", "String");
        paramJavaTypeNames.put("linkPower", "double");
        paramJavaTypeNames.put("linkPredictionCol", "String");
        paramJavaTypeNames.put("locale", "String");
        paramJavaTypeNames.put("loss", "String");
        paramJavaTypeNames.put("lossType", "String");
        paramJavaTypeNames.put("lowerBoundsOnCoefficients", "Matrix");
        paramJavaTypeNames.put("lowerBoundsOnIntercepts", "Vector");
        paramJavaTypeNames.put("max", "double");
        paramJavaTypeNames.put("maxBins", "int");
        paramJavaTypeNames.put("maxCategories", "int");
        paramJavaTypeNames.put("maxDF", "double");
        paramJavaTypeNames.put("maxDepth", "int");
        paramJavaTypeNames.put("maxIter", "int");
        paramJavaTypeNames.put("maxMemoryInMB", "int");
        paramJavaTypeNames.put("maxSentenceLength", "int");
        paramJavaTypeNames.put("min", "double");
        paramJavaTypeNames.put("minConfidence", "double");
        paramJavaTypeNames.put("minCount", "int");
        paramJavaTypeNames.put("minDF", "double");
        paramJavaTypeNames.put("minDivisibleClusterSize", "double");
        paramJavaTypeNames.put("minDocFreq", "int");
        paramJavaTypeNames.put("minInfoGain", "double");
        paramJavaTypeNames.put("minInstancesPerNode", "int");
        paramJavaTypeNames.put("minSupport", "double");
        paramJavaTypeNames.put("minTF", "double");
        paramJavaTypeNames.put("minTokenLength", "int");
        paramJavaTypeNames.put("missingValue", "double");
        paramJavaTypeNames.put("modelType", "String");
        paramJavaTypeNames.put("n", "int");
        paramJavaTypeNames.put("names", "String[]");
        paramJavaTypeNames.put("nonnegative", "boolean");
        paramJavaTypeNames.put("numBlocks", "int");
        paramJavaTypeNames.put("numBuckets", "int");
        paramJavaTypeNames.put("numBucketsArray", "int[]");
        paramJavaTypeNames.put("numFeatures", "int");
        paramJavaTypeNames.put("numFolds", "int");
        paramJavaTypeNames.put("numHashTables", "int");
        paramJavaTypeNames.put("numItemBlocks", "int");
        paramJavaTypeNames.put("numPartitions", "int");
        paramJavaTypeNames.put("numTopFeatures", "int");
        paramJavaTypeNames.put("numTrees", "int");
        paramJavaTypeNames.put("numUserBlocks", "int");
        paramJavaTypeNames.put("offsetCol", "String");
        paramJavaTypeNames.put("optimizeDocConcentration", "boolean");
        paramJavaTypeNames.put("optimizer", "String");
        paramJavaTypeNames.put("outputCol", "String");
        paramJavaTypeNames.put("outputCols", "String[]");
        paramJavaTypeNames.put("p", "double");
        paramJavaTypeNames.put("parallelism", "int");
        paramJavaTypeNames.put("parent", "Estimator<M>");
        paramJavaTypeNames.put("pattern", "String");
        paramJavaTypeNames.put("percentile", "double");
        paramJavaTypeNames.put("predictionCol", "String");
        paramJavaTypeNames.put("probabilityCol", "String");
        paramJavaTypeNames.put("quantileProbabilities", "double[]");
        paramJavaTypeNames.put("quantilesCol", "String");
        paramJavaTypeNames.put("rank", "int");
        paramJavaTypeNames.put("ratingCol", "String");
        paramJavaTypeNames.put("rawPredictionCol", "String");
        paramJavaTypeNames.put("regParam", "double");
        paramJavaTypeNames.put("relativeError", "double");
        paramJavaTypeNames.put("scalingVec", "Vector");
        paramJavaTypeNames.put("seed", "long");
        paramJavaTypeNames.put("selectorType", "String");
        paramJavaTypeNames.put("size", "int");
        paramJavaTypeNames.put("smoothing", "double");
        paramJavaTypeNames.put("solver", "String");
        paramJavaTypeNames.put("splits", "double[]");
        paramJavaTypeNames.put("splitsArray", "double[][]");
        paramJavaTypeNames.put("stages", "PipelineStage[]");
        paramJavaTypeNames.put("standardization", "boolean");
        paramJavaTypeNames.put("statement", "String");
        paramJavaTypeNames.put("stepSize", "double");
        paramJavaTypeNames.put("stopWords", "String[]");
        paramJavaTypeNames.put("strategy", "String");
        paramJavaTypeNames.put("stringIndexerOrderType", "String");
        paramJavaTypeNames.put("stringOrderType", "String");
        paramJavaTypeNames.put("subsamplingRate", "double");
        paramJavaTypeNames.put("threshold", "double");
        paramJavaTypeNames.put("thresholds", "double[]");
        paramJavaTypeNames.put("toLowercase", "boolean");
        paramJavaTypeNames.put("tol", "double");
        paramJavaTypeNames.put("topicConcentration", "double");
        paramJavaTypeNames.put("topicDistributionCol", "String");
        paramJavaTypeNames.put("trainRatio", "double");
        paramJavaTypeNames.put("upperBoundsOnCoefficients", "Matrix");
        paramJavaTypeNames.put("upperBoundsOnIntercepts", "Vector");
        paramJavaTypeNames.put("userCol", "String");
        paramJavaTypeNames.put("validationIndicatorCol", "String");
        paramJavaTypeNames.put("varianceCol", "String");
        paramJavaTypeNames.put("variancePower", "double");
        paramJavaTypeNames.put("vectorSize", "int");
        paramJavaTypeNames.put("vocabSize", "int");
        paramJavaTypeNames.put("weightCol", "String");
        // disabled b/c this param is deprecated in ml.classification.LinearSVCModel
        // paramJavaTypeNames.put("weightCol", "double");
        paramJavaTypeNames.put("windowSize", "int");
        paramJavaTypeNames.put("withMean", "boolean");
        paramJavaTypeNames.put("withStd", "boolean");

    }

    static {
        estimatorParams = new HashMap<>();
        transformerParams = new HashMap<>();

        estimatorParams.put(
            "AFTSurvivalRegression",
            new ArrayList<>(
                    Arrays.asList(
                        "aggregationDepth",
                        "censorCol",
                        "featuresCol",
                        "fitIntercept",
                        "labelCol",
                        "maxIter",
                        "predictionCol",
                        "quantileProbabilities",
                        "quantilesCol",
                        "tol"
                    )
            )
        );
        estimatorParams.put(
            "ALS",
            new ArrayList<>(
                    Arrays.asList(
                        "alpha",
                        "checkpointInterval",
                        "coldStartStrategy",
                        "finalStorageLevel",
                        "implicitPrefs",
                        "intermediateStorageLevel",
                        "itemCol",
                        "maxIter",
                        "nonnegative",
                        "numBlocks",
                        "numItemBlocks",
                        "numUserBlocks",
                        "predictionCol",
                        "rank",
                        "ratingCol",
                        "regParam",
                        "seed",
                        "userCol"
                    )
            )
        );
        estimatorParams.put(
            "BisectingKMeans",
            new ArrayList<>(
                    Arrays.asList(
                        "distanceMeasure",
                        "featuresCol",
                        "k",
                        "maxIter",
                        "minDivisibleClusterSize",
                        "predictionCol",
                        "seed"
                    )
            )
        );
        estimatorParams.put(
            "BucketedRandomProjectionLSH",
            new ArrayList<>(Arrays.asList("bucketLength", "inputCol", "numHashTables", "outputCol", "seed"))
        );
        estimatorParams.put(
            "ChiSqSelector",
            new ArrayList<>(
                    Arrays.asList(
                        "fdr",
                        "featuresCol",
                        "fpr",
                        "fwe",
                        "labelCol",
                        "numTopFeatures",
                        "outputCol",
                        "percentile",
                        "selectorType"
                    )
            )
        );
        estimatorParams.put(
            "CountVectorizer",
            new ArrayList<>(Arrays.asList("binary", "inputCol", "maxDF", "minDF", "minTF", "outputCol", "vocabSize"))
        );
        estimatorParams.put(
            "CrossValidator",
            new ArrayList<>(Arrays.asList("collectSubModels", "estimator", "numFolds", "parallelism", "seed"))
        );
        estimatorParams.put(
            "DecisionTreeClassifier",
            new ArrayList<>(
                    Arrays.asList(
                        "cacheNodeIds",
                        "checkpointInterval",
                        "featuresCol",
                        "impurity",
                        "labelCol",
                        "maxBins",
                        "maxDepth",
                        "maxMemoryInMB",
                        "minInfoGain",
                        "minInstancesPerNode",
                        "predictionCol",
                        "probabilityCol",
                        "rawPredictionCol",
                        "seed",
                        "thresholds"
                    )
            )
        );
        estimatorParams.put(
            "DecisionTreeRegressor",
            new ArrayList<>(
                    Arrays.asList(
                        "cacheNodeIds",
                        "checkpointInterval",
                        "featuresCol",
                        "impurity",
                        "labelCol",
                        "maxBins",
                        "maxDepth",
                        "maxMemoryInMB",
                        "minInfoGain",
                        "minInstancesPerNode",
                        "predictionCol",
                        "seed",
                        "varianceCol"
                    )
            )
        );
        estimatorParams.put(
            "FPGrowth",
            new ArrayList<>(Arrays.asList("itemsCol", "minConfidence", "minSupport", "numPartitions", "predictionCol"))
        );
        estimatorParams.put(
            "GBTClassifier",
            new ArrayList<>(
                    Arrays.asList(
                        "cacheNodeIds",
                        "checkpointInterval",
                        "featuresCol",
                        "featureSubsetStrategy",
                        "impurity",
                        "labelCol",
                        "lossType",
                        "maxBins",
                        "maxDepth",
                        "maxIter",
                        "maxMemoryInMB",
                        "minInfoGain",
                        "minInstancesPerNode",
                        "predictionCol",
                        "probabilityCol",
                        "rawPredictionCol",
                        "seed",
                        "stepSize",
                        "subsamplingRate",
                        "thresholds",
                        "validationIndicatorCol"
                    )
            )
        );
        estimatorParams.put(
            "GBTRegressor",
            new ArrayList<>(
                    Arrays.asList(
                        "cacheNodeIds",
                        "checkpointInterval",
                        "featuresCol",
                        "featureSubsetStrategy",
                        "impurity",
                        "labelCol",
                        "lossType",
                        "maxBins",
                        "maxDepth",
                        "maxIter",
                        "maxMemoryInMB",
                        "minInfoGain",
                        "minInstancesPerNode",
                        "predictionCol",
                        "seed",
                        "stepSize",
                        "subsamplingRate",
                        "validationIndicatorCol"
                    )
            )
        );
        estimatorParams.put(
            "GaussianMixture",
            new ArrayList<>(
                    Arrays.asList("featuresCol", "k", "maxIter", "predictionCol", "probabilityCol", "seed", "tol")
            )
        );
        estimatorParams.put(
            "GeneralizedLinearRegression",
            new ArrayList<>(
                    Arrays.asList(
                        "family",
                        "featuresCol",
                        "fitIntercept",
                        "labelCol",
                        "link",
                        "linkPower",
                        "linkPredictionCol",
                        "maxIter",
                        "offsetCol",
                        "predictionCol",
                        "regParam",
                        "solver",
                        "tol",
                        "variancePower",
                        "weightCol"
                    )
            )
        );
        estimatorParams.put("IDF", new ArrayList<>(Arrays.asList("inputCol", "minDocFreq", "outputCol")));
        estimatorParams.put(
            "Imputer",
            new ArrayList<>(Arrays.asList("inputCols", "missingValue", "outputCols", "strategy"))
        );
        estimatorParams.put(
            "IsotonicRegression",
            new ArrayList<>(
                    Arrays.asList("featureIndex", "featuresCol", "isotonic", "labelCol", "predictionCol", "weightCol")
            )
        );
        estimatorParams.put(
            "KMeans",
            new ArrayList<>(
                    Arrays.asList(
                        "distanceMeasure",
                        "featuresCol",
                        "initMode",
                        "initSteps",
                        "k",
                        "maxIter",
                        "predictionCol",
                        "seed",
                        "tol"
                    )
            )
        );
        estimatorParams.put(
            "LDA",
            new ArrayList<>(
                    Arrays.asList(
                        "checkpointInterval",
                        "docConcentration",
                        "docConcentration",
                        "featuresCol",
                        "k",
                        "keepLastCheckpoint",
                        "learningDecay",
                        "learningOffset",
                        "maxIter",
                        "optimizeDocConcentration",
                        "optimizer",
                        "seed",
                        "subsamplingRate",
                        "topicConcentration",
                        "topicDistributionCol"
                    )
            )
        );
        estimatorParams.put(
            "LinearRegression",
            new ArrayList<>(
                    Arrays.asList(
                        "aggregationDepth",
                        "elasticNetParam",
                        "epsilon",
                        "featuresCol",
                        "fitIntercept",
                        "labelCol",
                        "loss",
                        "maxIter",
                        "predictionCol",
                        "regParam",
                        "solver",
                        "standardization",
                        "tol",
                        "weightCol"
                    )
            )
        );
        estimatorParams.put(
            "LinearSVC",
            new ArrayList<>(
                    Arrays.asList(
                        "aggregationDepth",
                        "featuresCol",
                        "fitIntercept",
                        "labelCol",
                        "maxIter",
                        "predictionCol",
                        "rawPredictionCol",
                        "regParam",
                        "standardization",
                        "threshold",
                        "tol",
                        "weightCol"
                    )
            )
        );
        estimatorParams.put(
            "LogisticRegression",
            new ArrayList<>(
                    Arrays.asList(
                        "aggregationDepth",
                        "elasticNetParam",
                        "family",
                        "featuresCol",
                        "fitIntercept",
                        "labelCol",
                        "lowerBoundsOnCoefficients",
                        "lowerBoundsOnIntercepts",
                        "maxIter",
                        "predictionCol",
                        "probabilityCol",
                        "rawPredictionCol",
                        "regParam",
                        "standardization",
                        "threshold",
                        "thresholds",
                        "tol",
                        "upperBoundsOnCoefficients",
                        "upperBoundsOnIntercepts",
                        "weightCol"
                    )
            )
        );
        estimatorParams.put("MaxAbsScaler", new ArrayList<>(Arrays.asList("inputCol", "outputCol")));
        estimatorParams.put(
            "MinHashLSH",
            new ArrayList<>(Arrays.asList("inputCol", "numHashTables", "outputCol", "seed"))
        );
        estimatorParams.put("MinMaxScaler", new ArrayList<>(Arrays.asList("inputCol", "max", "min", "outputCol")));
        estimatorParams.put(
            "MultilayerPerceptronClassifier",
            new ArrayList<>(
                    Arrays.asList(
                        "blockSize",
                        "featuresCol",
                        "initialWeights",
                        "labelCol",
                        "layers",
                        "maxIter",
                        "predictionCol",
                        "probabilityCol",
                        "rawPredictionCol",
                        "seed",
                        "solver",
                        "stepSize",
                        "thresholds",
                        "tol"
                    )
            )
        );
        estimatorParams.put(
            "NaiveBayes",
            new ArrayList<>(
                    Arrays.asList(
                        "featuresCol",
                        "labelCol",
                        "modelType",
                        "predictionCol",
                        "probabilityCol",
                        "rawPredictionCol",
                        "smoothing",
                        "thresholds",
                        "weightCol"
                    )
            )
        );
        estimatorParams.put(
            "OneHotEncoderEstimator",
            new ArrayList<>(Arrays.asList("dropLast", "handleInvalid", "inputCols", "outputCols"))
        );
        estimatorParams.put(
            "OneVsRest",
            new ArrayList<>(
                    Arrays.asList(
                        "featuresCol",
                        "labelCol",
                        "parallelism",
                        "predictionCol",
                        "rawPredictionCol",
                        "weightCol"
                    )
            )
        );
        estimatorParams.put("PCA", new ArrayList<>(Arrays.asList("inputCol", "k", "outputCol")));
        estimatorParams.put("Pipeline", new ArrayList<>(Arrays.asList("stages")));
        estimatorParams.put(
            "QuantileDiscretizer",
            new ArrayList<>(
                    Arrays.asList(
                        "handleInvalid",
                        "inputCol",
                        "inputCols",
                        "numBuckets",
                        "numBucketsArray",
                        "outputCol",
                        "outputCols",
                        "relativeError"
                    )
            )
        );
        estimatorParams.put(
            "RFormula",
            new ArrayList<>(
                    Arrays.asList(
                        "featuresCol",
                        "forceIndexLabel",
                        "formula",
                        "handleInvalid",
                        "labelCol",
                        "stringIndexerOrderType"
                    )
            )
        );
        estimatorParams.put(
            "RandomForestClassifier",
            new ArrayList<>(
                    Arrays.asList(
                        "cacheNodeIds",
                        "checkpointInterval",
                        "featuresCol",
                        "featureSubsetStrategy",
                        "impurity",
                        "labelCol",
                        "maxBins",
                        "maxDepth",
                        "maxMemoryInMB",
                        "minInfoGain",
                        "minInstancesPerNode",
                        "numTrees",
                        "predictionCol",
                        "probabilityCol",
                        "rawPredictionCol",
                        "seed",
                        "subsamplingRate",
                        "thresholds"
                    )
            )
        );
        estimatorParams.put(
            "RandomForestRegressor",
            new ArrayList<>(
                    Arrays.asList(
                        "cacheNodeIds",
                        "checkpointInterval",
                        "featuresCol",
                        "featureSubsetStrategy",
                        "impurity",
                        "labelCol",
                        "maxBins",
                        "maxDepth",
                        "maxMemoryInMB",
                        "minInfoGain",
                        "minInstancesPerNode",
                        "numTrees",
                        "predictionCol",
                        "seed",
                        "subsamplingRate"
                    )
            )
        );
        estimatorParams.put(
            "StandardScaler",
            new ArrayList<>(Arrays.asList("inputCol", "outputCol", "withMean", "withStd"))
        );
        estimatorParams.put(
            "StringIndexer",
            new ArrayList<>(Arrays.asList("handleInvalid", "inputCol", "outputCol", "stringOrderType"))
        );
        estimatorParams.put(
            "TrainValidationSplit",
            new ArrayList<>(Arrays.asList("collectSubModels", "estimator", "parallelism", "seed", "trainRatio"))
        );
        estimatorParams.put(
            "VectorIndexer",
            new ArrayList<>(Arrays.asList("handleInvalid", "inputCol", "maxCategories", "outputCol"))
        );
        estimatorParams.put(
            "Word2Vec",
            new ArrayList<>(
                    Arrays.asList(
                        "inputCol",
                        "maxIter",
                        "maxSentenceLength",
                        "minCount",
                        "numPartitions",
                        "outputCol",
                        "seed",
                        "stepSize",
                        "vectorSize",
                        "windowSize"
                    )
            )
        );



        transformerParams.put(
            "AFTSurvivalRegressionModel",
            new ArrayList<>(
                    Arrays.asList("featuresCol", "parent", "predictionCol", "quantileProbabilities", "quantilesCol")
            )
        );
        transformerParams.put(
            "ALSModel",
            new ArrayList<>(Arrays.asList("coldStartStrategy", "itemCol", "parent", "predictionCol", "userCol"))
        );
        transformerParams.put("Binarizer", new ArrayList<>(Arrays.asList("inputCol", "outputCol", "threshold")));
        transformerParams.put(
            "BisectingKMeansModel",
            new ArrayList<>(Arrays.asList("featuresCol", "parent", "predictionCol"))
        );
        transformerParams.put(
            "BucketedRandomProjectionLSHModel",
            new ArrayList<>(Arrays.asList("inputCol", "outputCol", "parent"))
        );
        transformerParams.put(
            "Bucketizer",
            new ArrayList<>(
                    Arrays.asList(
                        "handleInvalid",
                        "inputCol",
                        "inputCols",
                        "outputCol",
                        "outputCols",
                        "parent",
                        "splits",
                        "splitsArray"
                    )
            )
        );
        transformerParams.put(
            "ChiSqSelectorModel",
            new ArrayList<>(Arrays.asList("featuresCol", "outputCol", "parent"))
        );
        transformerParams.put("ColumnPruner", new ArrayList<>(Arrays.asList()));
        transformerParams.put(
            "CountVectorizerModel",
            new ArrayList<>(Arrays.asList("binary", "inputCol", "minTF", "outputCol", "parent"))
        );
        transformerParams.put("CrossValidatorModel", new ArrayList<>(Arrays.asList("parent")));
        transformerParams.put("DCT", new ArrayList<>(Arrays.asList("inputCol", "inverse", "outputCol")));
        transformerParams.put(
            "DecisionTreeClassificationModel",
            new ArrayList<>(
                    Arrays.asList(
                        "cacheNodeIds",
                        "checkpointInterval",
                        "featuresCol",
                        "impurity",
                        "maxBins",
                        "maxDepth",
                        "maxMemoryInMB",
                        "minInfoGain",
                        "minInstancesPerNode",
                        "parent",
                        "predictionCol",
                        "probabilityCol",
                        "rawPredictionCol",
                        "seed",
                        "thresholds"
                    )
            )
        );
        transformerParams.put(
            "DecisionTreeRegressionModel",
            new ArrayList<>(
                    Arrays.asList(
                        "cacheNodeIds",
                        "checkpointInterval",
                        "featuresCol",
                        "impurity",
                        "maxBins",
                        "maxDepth",
                        "maxMemoryInMB",
                        "minInfoGain",
                        "minInstancesPerNode",
                        "parent",
                        "predictionCol",
                        "seed",
                        "varianceCol"
                    )
            )
        );
        transformerParams.put(
            "DistributedLDAModel",
            new ArrayList<>(Arrays.asList("featuresCol", "parent", "seed", "topicDistributionCol"))
        );
        transformerParams.put(
            "ElementwiseProduct",
            new ArrayList<>(Arrays.asList("inputCol", "outputCol", "scalingVec"))
        );
        transformerParams.put(
            "FPGrowthModel",
            new ArrayList<>(Arrays.asList("itemsCol", "minConfidence", "parent", "predictionCol"))
        );
        transformerParams.put(
            "FeatureHasher",
            new ArrayList<>(Arrays.asList("categoricalCols", "inputCols", "numFeatures", "outputCol"))
        );
        transformerParams.put(
            "GBTClassificationModel",
            new ArrayList<>(
                    Arrays.asList(
                        "cacheNodeIds",
                        "checkpointInterval",
                        "featuresCol",
                        "featureSubsetStrategy",
                        "impurity",
                        "maxBins",
                        "maxDepth",
                        "maxIter",
                        "maxMemoryInMB",
                        "minInfoGain",
                        "minInstancesPerNode",
                        "parent",
                        "predictionCol",
                        "probabilityCol",
                        "rawPredictionCol",
                        "seed",
                        "stepSize",
                        "subsamplingRate",
                        "thresholds"
                    )
            )
        );
        transformerParams.put(
            "GBTRegressionModel",
            new ArrayList<>(
                    Arrays.asList(
                        "cacheNodeIds",
                        "checkpointInterval",
                        "featuresCol",
                        "featureSubsetStrategy",
                        "impurity",
                        "maxBins",
                        "maxDepth",
                        "maxIter",
                        "maxMemoryInMB",
                        "minInfoGain",
                        "minInstancesPerNode",
                        "parent",
                        "predictionCol",
                        "seed",
                        "stepSize",
                        "subsamplingRate"
                    )
            )
        );
        transformerParams.put(
            "GaussianMixtureModel",
            new ArrayList<>(Arrays.asList("featuresCol", "parent", "predictionCol", "probabilityCol"))
        );
        transformerParams.put(
            "GeneralizedLinearRegressionModel",
            new ArrayList<>(Arrays.asList("featuresCol", "linkPredictionCol", "parent", "predictionCol"))
        );
        transformerParams.put(
            "HashingTF",
            new ArrayList<>(Arrays.asList("binary", "inputCol", "numFeatures", "outputCol"))
        );
        transformerParams.put("IDFModel", new ArrayList<>(Arrays.asList("inputCol", "outputCol", "parent")));
        transformerParams.put("ImputerModel", new ArrayList<>(Arrays.asList("inputCols", "outputCols", "parent")));
        transformerParams.put("IndexToString", new ArrayList<>(Arrays.asList("inputCol", "labels", "outputCol")));
        transformerParams.put("Interaction", new ArrayList<>(Arrays.asList("inputCols", "outputCol")));
        transformerParams.put(
            "IsotonicRegressionModel",
            new ArrayList<>(Arrays.asList("featureIndex", "featuresCol", "parent", "predictionCol"))
        );
        transformerParams.put("KMeansModel", new ArrayList<>(Arrays.asList("featuresCol", "parent", "predictionCol")));
        transformerParams.put(
            "LinearRegressionModel",
            new ArrayList<>(Arrays.asList("featuresCol", "parent", "predictionCol"))
        );
        transformerParams.put(
            "LinearSVCModel",
            new ArrayList<>(
                    Arrays.asList(
                        "featuresCol",
                        "parent",
                        "predictionCol",
                        "rawPredictionCol",
                        "threshold",
                        "weightCol"
                    )
            )
        );
        transformerParams.put(
            "LocalLDAModel",
            new ArrayList<>(Arrays.asList("featuresCol", "parent", "seed", "topicDistributionCol"))
        );
        transformerParams.put(
            "LogisticRegressionModel",
            new ArrayList<>(
                    Arrays.asList(
                        "featuresCol",
                        "parent",
                        "predictionCol",
                        "probabilityCol",
                        "rawPredictionCol",
                        "threshold",
                        "thresholds"
                    )
            )
        );
        transformerParams.put("MaxAbsScalerModel", new ArrayList<>(Arrays.asList("inputCol", "outputCol", "parent")));
        transformerParams.put("MinHashLSHModel", new ArrayList<>(Arrays.asList("inputCol", "outputCol", "parent")));
        transformerParams.put(
            "MinMaxScalerModel",
            new ArrayList<>(Arrays.asList("inputCol", "max", "min", "outputCol", "parent"))
        );
        transformerParams.put(
            "MultilayerPerceptronClassificationModel",
            new ArrayList<>(
                    Arrays.asList(
                        "featuresCol",
                        "parent",
                        "predictionCol",
                        "probabilityCol",
                        "rawPredictionCol",
                        "thresholds"
                    )
            )
        );
        transformerParams.put("NGram", new ArrayList<>(Arrays.asList("inputCol", "n", "outputCol")));
        transformerParams.put(
            "NaiveBayesModel",
            new ArrayList<>(
                    Arrays.asList(
                        "featuresCol",
                        "parent",
                        "predictionCol",
                        "probabilityCol",
                        "rawPredictionCol",
                        "thresholds"
                    )
            )
        );
        transformerParams.put("Normalizer", new ArrayList<>(Arrays.asList("inputCol", "outputCol", "p")));
        transformerParams.put("OneHotEncoder", new ArrayList<>(Arrays.asList("dropLast", "inputCol", "outputCol")));
        transformerParams.put(
            "OneHotEncoderModel",
            new ArrayList<>(Arrays.asList("dropLast", "handleInvalid", "inputCols", "outputCols", "parent"))
        );
        transformerParams.put(
            "OneVsRestModel",
            new ArrayList<>(Arrays.asList("featuresCol", "parent", "predictionCol", "rawPredictionCol"))
        );
        transformerParams.put("PCAModel", new ArrayList<>(Arrays.asList("inputCol", "outputCol", "parent")));
        transformerParams.put("PipelineModel", new ArrayList<>(Arrays.asList("parent")));
        transformerParams.put("PolynomialExpansion", new ArrayList<>(Arrays.asList("degree", "inputCol", "outputCol")));
        transformerParams.put("RFormulaModel", new ArrayList<>(Arrays.asList("parent")));
        transformerParams.put(
            "RandomForestClassificationModel",
            new ArrayList<>(
                    Arrays.asList(
                        "cacheNodeIds",
                        "checkpointInterval",
                        "featuresCol",
                        "featureSubsetStrategy",
                        "impurity",
                        "maxBins",
                        "maxDepth",
                        "maxMemoryInMB",
                        "minInfoGain",
                        "minInstancesPerNode",
                        "numTrees",
                        "parent",
                        "predictionCol",
                        "probabilityCol",
                        "rawPredictionCol",
                        "seed",
                        "subsamplingRate",
                        "thresholds"
                    )
            )
        );
        transformerParams.put(
            "RandomForestRegressionModel",
            new ArrayList<>(
                    Arrays.asList(
                        "cacheNodeIds",
                        "checkpointInterval",
                        "featuresCol",
                        "featureSubsetStrategy",
                        "impurity",
                        "maxBins",
                        "maxDepth",
                        "maxMemoryInMB",
                        "minInfoGain",
                        "minInstancesPerNode",
                        "numTrees",
                        "parent",
                        "predictionCol",
                        "seed",
                        "subsamplingRate"
                    )
            )
        );
        transformerParams.put(
            "RegexTokenizer",
            new ArrayList<>(Arrays.asList("gaps", "inputCol", "minTokenLength", "outputCol", "pattern", "toLowercase"))
        );
        transformerParams.put("SQLTransformer", new ArrayList<>(Arrays.asList("statement")));
        transformerParams.put("StandardScalerModel", new ArrayList<>(Arrays.asList("inputCol", "outputCol", "parent")));
        transformerParams.put(
            "StopWordsRemover",
            new ArrayList<>(Arrays.asList("caseSensitive", "inputCol", "locale", "outputCol", "stopWords"))
        );
        transformerParams.put(
            "StringIndexerModel",
            new ArrayList<>(Arrays.asList("handleInvalid", "inputCol", "outputCol", "parent"))
        );
        transformerParams.put("Tokenizer", new ArrayList<>(Arrays.asList("inputCol", "outputCol")));
        transformerParams.put("TrainValidationSplitModel", new ArrayList<>(Arrays.asList("parent")));
        transformerParams.put(
            "VectorAssembler",
            new ArrayList<>(Arrays.asList("handleInvalid", "inputCols", "outputCol"))
        );
        transformerParams.put("VectorAttributeRewriter", new ArrayList<>(Arrays.asList()));
        transformerParams.put("VectorIndexerModel", new ArrayList<>(Arrays.asList("inputCol", "outputCol", "parent")));
        transformerParams.put("VectorSizeHint", new ArrayList<>(Arrays.asList("handleInvalid", "inputCol", "size")));
        transformerParams.put(
            "VectorSlicer",
            new ArrayList<>(Arrays.asList("indices", "inputCol", "names", "outputCol"))
        );
        transformerParams.put("Word2VecModel", new ArrayList<>(Arrays.asList("inputCol", "outputCol", "parent")));
    }

    private static void validateEstimatorByName(String name, ExceptionMetadata metadata) {
        if (!estimatorFullClassNames.containsKey(name)) {
            throw new UnrecognizedRumbleMLClassReferenceException(
                    "Unrecognized RumbleML class; Make sure \"" + name + "\" is a valid estimator of RumbleML API.",
                    metadata
            );
        }
    }

    private static void validateTransformerByName(String name, ExceptionMetadata metadata) {
        if (!transformerFullClassNames.containsKey(name)) {
            throw new UnrecognizedRumbleMLClassReferenceException(
                    "Unrecognized RumbleML class; Make sure \"" + name + "\" is a valid transformer of RumbleML API.",
                    metadata
            );
        }
    }

    public static String getEstimatorFullClassName(String name, ExceptionMetadata metadata) {
        validateEstimatorByName(name, metadata);
        return estimatorFullClassNames.get(name);
    }

    public static String getTransformerFullClassName(String name, ExceptionMetadata metadata) {
        validateTransformerByName(name, metadata);
        return transformerFullClassNames.get(name);
    }

    public static List<String> getEstimatorParams(String name, ExceptionMetadata metadata) {
        validateEstimatorByName(name, metadata);
        return estimatorParams.get(name);
    }

    public static List<String> getTransformerParams(String name, ExceptionMetadata metadata) {
        validateTransformerByName(name, metadata);
        return transformerParams.get(name);
    }

    public static void validateEstimatorParameterByName(
            String estimatorShortName,
            String paramName,
            ExceptionMetadata metadata
    ) {
        validateEstimatorByName(estimatorShortName, metadata);

        if (!estimatorParams.get(estimatorShortName).contains(paramName)) {
            throw new UnrecognizedRumbleMLParamReferenceException(
                    "Make sure \""
                        + paramName
                        + "\" is a valid parameter of \""
                        + estimatorShortName
                        + "\".",
                    metadata
            );
        }
    }

    public static void validateTransformerParameterByName(
            String transformerShortName,
            String paramName,
            ExceptionMetadata metadata
    ) {
        validateTransformerByName(transformerShortName, metadata);

        if (!transformerParams.get(transformerShortName).contains(paramName)) {
            throw new UnrecognizedRumbleMLParamReferenceException(
                    "Make sure \""
                        + paramName
                        + "\" is a valid parameter of \""
                        + transformerShortName
                        + "\".",
                    metadata
            );
        }
    }

    private static void validateParamName(String name, ExceptionMetadata metadata) {
        if (!paramJavaTypeNames.containsKey(name)) {
            throw new UnrecognizedRumbleMLParamReferenceException(
                    "Parameter \""
                        + name
                        + "\" does not exist in in RumbleML API.\"",
                    metadata
            );
        }
    }


    static String getJavaTypeNameOfParamByName(String name, ExceptionMetadata metadata) {
        validateParamName(name, metadata);
        return paramJavaTypeNames.get(name);
    }

    static String getRumbleMLShortName(String javaFullClassName) {
        int indexOfLastDot = javaFullClassName.lastIndexOf(".");
        return javaFullClassName.substring(indexOfLastDot + 1);
    }

    /**
     * SparkMl parameters that require a column of vectors are re-purposed in RumbleML
     * SparkML expects a single string to refers to a column of vectors
     * RumbleML collects an array of strings and generates a Vector from referenced columns
     */
    static final List<String> specialParamsThatMayReferToAColumnOfVectors = new ArrayList<>();
    private static final HashMap<String, String> defaultSparkMLValuesOfSpecialParams = new HashMap<>();
    static final String javaTypeNameOfSpecialParams = "String[]";
    private static final HashMap<String, String> UUIDsForSpecialParams = new HashMap<>();

    private static final String featuresColParamName = "featuresCol";
    private static final String inputColParamName = "inputCol";
    static {
        // specialParamsThatMayReferToAColumnOfVectors.add(featuresColParamName);
        // specialParamsThatMayReferToAColumnOfVectors.add(inputColParamName);

        defaultSparkMLValuesOfSpecialParams.put(featuresColParamName, "features");

        UUIDsForSpecialParams.put(featuresColParamName, "59992242-914d-4357-bcc8-10b1c134476b");
        UUIDsForSpecialParams.put(inputColParamName, "fd289b98-4410-4423-96bd-001bb703e8d3");

    }

    static boolean specialParamHasNoDefaultSparkMLValue(String paramName) {
        if (!specialParamsThatMayReferToAColumnOfVectors.contains(paramName)) {
            throw new OurBadException("Special param with name '" + paramName + "' not found.");
        }
        return !defaultSparkMLValuesOfSpecialParams.containsKey(paramName);
    }

    static String getDefaultSparkMLValueOfSpecialParam(String paramName) {
        if (!specialParamsThatMayReferToAColumnOfVectors.contains(paramName)) {
            throw new OurBadException("Special param with name '" + paramName + "' not found.");
        }
        if (!defaultSparkMLValuesOfSpecialParams.containsKey(paramName)) {
            throw new OurBadException("Default SparkML value for special param '" + paramName + "' not found.");
        }
        return defaultSparkMLValuesOfSpecialParams.get(paramName);
    }


    static String getUUIDOfOfSpecialParam(String paramName) {
        if (!specialParamsThatMayReferToAColumnOfVectors.contains(paramName)) {
            throw new OurBadException("Special param with name '" + paramName + "' not found.");
        }
        if (!UUIDsForSpecialParams.containsKey(paramName)) {
            throw new OurBadException("UUID for special param '" + paramName + "' not found.");
        }
        return UUIDsForSpecialParams.get(paramName);
    }

    static boolean shouldEstimatorColumnReferencedBySpecialParamContainVectors(
            String estimatorName,
            String paramName,
            ExceptionMetadata metadata
    ) {
        validateEstimatorByName(estimatorName, metadata);
        validateEstimatorParameterByName(estimatorName, paramName, metadata);
        if (paramName.equals(featuresColParamName) && !estimatorName.equals("RFormula")) {
            return true;
        }
        if (
            paramName.equals(inputColParamName)
                && (estimatorName.equals("BucketedRandomProjectionLSH")
                    || estimatorName.equals("IDF")
                    || estimatorName.equals("MaxAbsScaler")
                    || estimatorName.equals("MinHashLSH")
                    || estimatorName.equals("MinMaxScaler")
                    || estimatorName.equals("PCA")
                    || estimatorName.equals("StandardScaler")
                    || estimatorName.equals("VectorIndexer"))
        ) {
            return true;
        }
        return false;
    }

    static boolean shouldTransformerColumnReferencedBySpecialParamContainVectors(
            String transformerName,
            String paramName,
            ExceptionMetadata metadata
    ) {
        validateTransformerByName(transformerName, metadata);
        validateTransformerParameterByName(transformerName, paramName, metadata);
        if (paramName.equals(featuresColParamName) && !transformerName.equals("RFormulaModel")) {
            return true;
        }
        if (
            paramName.equals(inputColParamName)
                && (transformerName.equals("BucketedRandomProjectionLSHModel")
                    || transformerName.equals("IDFModel")
                    || transformerName.equals("MaxAbsScalerModel")
                    || transformerName.equals("MinHashLSHModel")
                    || transformerName.equals("MinMaxScalerModel")
                    || transformerName.equals("PCAModel")
                    || transformerName.equals("StandardScalerModel")
                    || transformerName.equals("VectorIndexerModel")
                    || transformerName.equals("DCT")
                    || transformerName.equals("ElementwiseProduct")
                    || transformerName.equals("Normalizer")
                    || transformerName.equals("PolynomialExpansion")
                    || transformerName.equals("VectorSizeHint")
                    || transformerName.equals("VectorSlicer"))
        ) {
            return true;
        }
        return false;
    }
}
