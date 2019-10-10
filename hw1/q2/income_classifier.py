import numpy as np
import time
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, \
    VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.evaluation import BinaryClassificationMetrics, \
    MulticlassMetrics
from matplotlib import pyplot as plt

CSV_PATH = "data/adult.data.csv"


def load(sqlContext, csv_path):
    column_names = ["age", "workclass", "fnlwgt", "education", "education_num",
                    "marital_status", "occupation", "relationship", "race",
                    "sex",
                    "capital_gain", "capital_loss", "hours_per_week",
                    "native_country", "income"]
    income_df = sqlContext.read.format("com.databricks.spark.csv").options(
        header="false", inferschema="true").load(csv_path)
    for old_name, new_name in zip(income_df.columns, column_names):
        income_df = income_df.withColumnRenamed(old_name, new_name)
        income_df = income_df.dropna()
    # print(income_df.dtypes)
    print("Load csv file correctly.")
    return income_df


def preprocess(data_frame):
    category_columns = ["workclass", "education", "marital_status",
                        "occupation", "relationship", "race", "sex",
                        "native_country", "income"]
    index_columns = [col + "_index" for col in category_columns]
    vec_columns = [col + "_vec" for col in category_columns]
    for col in category_columns:
        stringIndexer = StringIndexer(inputCol=col,
                                      outputCol=col + "_index",
                                      handleInvalid='error')
        model = stringIndexer.fit(data_frame)
        data_frame = model.transform(data_frame)
        data_frame = data_frame.drop(col)
    index_columns.pop(-1)
    vec_columns.pop(-1)

    ohe = OneHotEncoderEstimator(inputCols=index_columns,
                                 outputCols=vec_columns)
    ohe_model = ohe.fit(data_frame)
    ohe_df = ohe_model.transform(data_frame)
    ohe_df = ohe_df.drop(*index_columns)
    # ohe_df.show()
    cols = ohe_df.columns
    cols.remove("income_index")
    vector_assembler = VectorAssembler(inputCols=cols, outputCol="features")
    vdata_frame = vector_assembler.transform(ohe_df)
    vdata_frame = vdata_frame.drop(*cols)
    # vdata_frame.show()
    print("Preprocess input data correctly.")
    return vdata_frame


def plot_roc(FPR, TPR, img_path):
    fig = plt.figure(figsize=(6, 5))
    plt.plot([0, 1], [0, 1], 'r--')
    plt.plot(FPR, TPR)
    plt.xlabel('FPR')
    plt.ylabel('TPR')
    fig.savefig(img_path)


def plot_pr(recall, precision, img_path):
    fig = plt.figure(figsize=(6, 5))
    plt.plot([0, 1], [1, 0], 'r--')
    plt.plot(recall, precision)
    plt.xlabel('recall')
    plt.ylabel('precision')
    fig.savefig(img_path)


def main():
    start = time.time()
    conf = SparkConf().setMaster("local").setAppName("income")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    income_df = load(sqlContext, csv_path=CSV_PATH)
    # income_df.show()
    # print(income_df.dtypes)
    # print(income_df.count())

    features_df = preprocess(data_frame=income_df)

    # train, test split
    train_df, test_df = features_df.randomSplit([7.0, 3.0], 100)

    # logistic regression

    income_lr = LogisticRegression(featuresCol="features",
                                   labelCol="income_index",
                                   regParam=0.0, elasticNetParam=0.0,
                                   maxIter=200)
    income_model = income_lr.fit(train_df)

    # modeling
    print("Training:")
    training_summary = income_model.summary
    training_FPR = training_summary.roc.select('FPR').collect()
    training_TPR = training_summary.roc.select('TPR').collect()
    plot_roc(training_FPR, training_TPR, "pic/training_roc.jpg")

    training_recall = training_summary.pr.select('recall').collect()
    training_precision = training_summary.pr.select('precision').collect()
    # Area under ROC curve
    print("Training Area under ROC = %s" % training_summary.areaUnderROC)
    # accuracy
    print("Training Accuracy = %s" % training_summary.accuracy)
    plot_pr(training_recall, training_precision, "pic/training_pr.jpg")

    # evaluation
    print()
    print("Evaluation:")
    pred_df = income_model.transform(test_df).select("prediction",
                                                     "income_index")
    raw_pred_df = income_model.transform(test_df).select("probability",
                                                     "income_index"
                                                     ).rdd.map(
        lambda l: (float(l[0][1]), l[1]))
    metrics = BinaryClassificationMetrics(raw_pred_df)
    # Area under ROC curve
    print("Testing Area under ROC = %s" % metrics.areaUnderROC)
    # accuracy
    metrics = MulticlassMetrics(pred_df.rdd)
    print("Testing Accuracy = %s" % metrics.accuracy)

    # confusion matrix
    print("Testing Confusion Matrix:")
    print(metrics.confusionMatrix().toArray())
    print("Total cost %fs" % (time.time() - start))
    print("Done!")


if __name__ == '__main__':
    main()
