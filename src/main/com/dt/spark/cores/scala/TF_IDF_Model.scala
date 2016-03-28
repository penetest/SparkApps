package com.dt.spark.cores.scala

import breeze.linalg.{norm, SparseVector}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.{Word2Vec, IDF, HashingTF}
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}

/**
  * make by macal
  * 使用TF-IDF模型处理文本-Spark高级文本处理技术
  * Created by Administrator on 2016/2/23.
  */
object TF_IDF_Model {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("TF-IDF text deal with")
    //conf.setMaster("local[3]")
    val sc = new SparkContext(conf)

   // val path = "E:/workspases/data/20news-bydate-train/*"
    val path = "/data/20news-bydate-train/*"
    val rdd = sc.wholeTextFiles(path,8)//read texta
    val texts = rdd.map{case(fileName,content) => content}

//    val whiteSpaseSplit = texts.flatMap(t => t.split(" ").map(_.toLowerCase))//分割文档的词语，将每个单词转化成小写
    val nonWordSplit = texts.flatMap(t => t.split("""\W+""").map(_.toLowerCase))//分割单词，并将非单词的字符去掉，将每个单词转化成小写
    val regex = """[^0-9]*""".r //设置过滤规则
    val filterNumbers = nonWordSplit.filter(token => regex.pattern.matcher(token).matches())//过滤掉数字和包含数字的单词
    //设置停用词的集合
    val stopwords = Set("the","a","an","of","or","in","for","by","on","but","is","not",
      "with","as","was","if","they","are","this","and","it","have","form","at","my","be","that","to")
//    val tokenCountsFilteredStopwords = filterNumbers.filter(token => !stopwords.contains(token))//过滤停留词
//    val tokenCountsFilteredSize = tokenCountsFilteredStopwords.filter(token => token.size >= 2)//过滤单个字符的单词
    //过滤掉频次较低的单词
    val tokenCounts = filterNumbers.map(t => (t,1)).reduceByKey(_+_)
    val rareTokens = tokenCounts.filter{case(k,v) => v < 2}.map{case(k,v) => k}.collect().toSet
//    val tokenCountFilteredAll = tokenCountsFilteredSize.filter( token =>  rareTokens.contains(token))

    def tokenize(doc:String) :Seq[String] ={
      doc.split("""\W+""")//分词，并将非单词字符过滤掉
        .map(_.toLowerCase)//将单词转换成小写
        .filter(token => regex.pattern.matcher(token).matches())//过滤数字和包括数据的单词
        .filterNot(token => stopwords.contains(token))//过滤停留单词
        .filterNot(token => rareTokens.contains(token))//过滤词频小于2的单词
        .filter(token => token.size >= 2)//过滤单个字符
        .toSeq
    }
    val tokens = texts.map(doc => tokenize(doc))

    //训练TF-IDF模型
    val dim = math.pow(2,18).toInt
    val hashingTF =  new HashingTF(dim)
    val tf = hashingTF.transform(tokens)
    tf.cache()


    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)
    tfidf.cache()
    //验证TF-IDF模型
//    val common = sc.parallelize(Seq(Seq("you","do","we")))
//    val tfCommon = hashingTF.transform(common)
//    val tfidfCommon = idf.transform(tfCommon)
//    val commonVector = tfidfCommon.first().asInstanceOf[SV]
//    println(commonVector.values.toSeq)

    //文本相似度和TF-IDF特征
//    val hockeyText = rdd.filter{case(fileName,content) => fileName.contains("hockey")}
//    val hokeyTF = hockeyText.mapValues(doc => hashingTF.transform(tokenize(doc)))
//    val hockeyTfIdf = idf.transform(hokeyTF.map(_._2))
//
//    val hockey1 = hockeyTfIdf.sample(true,.1,42).first().asInstanceOf[SV]
//    val breeze1 = new SparseVector(hockey1.indices,hockey1.values,hockey1.size)
//    val hockey2 = hockeyTfIdf.sample(true,0.1,43).first().asInstanceOf[SV]
//    val breeze2 = new SparseVector(hockey2.indices,hockey2.values,hockey2.size)
//    val cosineSim = breeze1.dot(breeze2)/(norm(breeze1) * norm(breeze2))
//    println("******1: "+cosineSim)
//
//    val graphicsText = rdd.filter{case(fileName,content) => fileName.contains("comp.graphics")}
//    val graphicsTF = graphicsText.mapValues(doc => hashingTF.transform(tokenize(doc)))
//    val graphicsTfIdf = idf.transform(graphicsTF.map(_._2))
//
//    val graphics = graphicsTfIdf.sample(true,0.1,42).first().asInstanceOf[SV]
//    val breezeGraphics = new SparseVector(graphics.indices,graphics.values,graphics.size)
//    val cosineSim2 = breeze1.dot(breezeGraphics) / (norm(breeze1) * norm(breezeGraphics))
//    println("******2: "+cosineSim2)
//
//    val baseballText = rdd.filter{case(fileName,content) => fileName.contains("baseball")}
//    val baseballTF = baseballText.mapValues(doc => hashingTF.transform(tokenize(doc)))
//    val baseballTfIdf = idf.transform(baseballTF.map(_._2))
//
//    val baseball = baseballTfIdf.sample(true,0.1,42).first().asInstanceOf[SV]
//    val breazeBaseball = new SparseVector(baseball.indices,baseball.values,baseball.size)
//    val cosineSim3 = breeze1.dot(breazeBaseball) / (norm(breeze1) * norm(breazeBaseball))
//    println("******3: "+cosineSim3)

    //使用TF-IDF训练文本分类器
    val newsgroups = rdd.map{ case(fileName,content) => fileName.split("/").takeRight(2).head}
    val newsgroupsMap = newsgroups.distinct().collect().zipWithIndex.toMap
    val zipped = newsgroups.zip(tfidf)
    val train = zipped.map{case (topic,vector) => LabeledPoint(newsgroupsMap(topic),vector)}
    train.cache()

    val model = NaiveBayes.train(train,lambda = 0.1)//朴素贝叶斯训练模型

    //read测试集
   // val testPath = "E:/workspases/data/20news-bydate-test/*"
    val testPath = "/data/20news-bydate-test/*"
    val testRDD = sc.wholeTextFiles(testPath,8)
    val testLabels = testRDD.map{
      case(fileName,content) =>
      val topic = fileName.split("/").takeRight(2).head
        newsgroupsMap(topic)
    }

    val testTf = testRDD.map{ case(fileName,content) => hashingTF.transform(tokenize(content))}
    val testIdfTf = idf.transform(testTf)
    val zippedTest = testLabels.zip(testIdfTf)
    val test = zippedTest.map{ case(topic,vector) => LabeledPoint(topic,vector)}

    //计算模型的准确度和多酚类加权F-指标
    val predictionAndLabel = test.map( p => (model.predict(p.features),p.label))
    predictionAndLabel.cache()
    val accuracy = 1.0 * predictionAndLabel.filter(x=> x._1 == x._2).count() / test.count()
    val metrics = new MulticlassMetrics(predictionAndLabel)
    println("accuracy:"+accuracy)
    println("metrics:"+metrics.weightedFMeasure)

    //训练Word2Vec
//    val word2vec = new Word2Vec()
//    word2vec.setSeed(42)
//    val word2vecModel = word2vec.fit(tokens)
//    println("********************")
//    word2vecModel.findSynonyms("hockey",20).foreach(println)
//    println("********************")
//    word2vecModel.findSynonyms("legislation",20).foreach(println)
//    println("********************")
//    word2vecModel.findSynonyms("telephony",20).foreach(println)
//    println("********************")
    sc.stop()

  }

}
