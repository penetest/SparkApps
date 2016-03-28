package com.dt.spark.cores.scala

import java.awt.image.BufferedImage
import java.io.File
import javax.imageio.ImageIO

import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.{SparkContext, SparkConf}

/**
  * make by macal
  * 数据降维
 * Created by Administrator on 2016/2/22.
 */
object DataReduction {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("datas reduction")
    val sc = new SparkContext(conf)

    //read datas
    val path  = "/data/lfw/*"
    val rdd = sc.wholeTextFiles(path)
    val files = rdd.map{case(fileName,content) => fileName.replace("hdfs://master:9000","")}
//    files.take(10).foreach(f => {
//      val pixels = extractPixels(f,50,50)
//      println(pixels.take(10).mkString("",",",", ..."))
//    })
    val pixels = files.map(f =>{
      val p =  extractPixels(f,50,50)
        p
      })
    val vectors = pixels.map(p => Vectors.dense(p))
    vectors.setName("image-vectors")
    vectors.cache()
    val scaler = new StandardScaler(true,false).fit(vectors)
    val scaledVectors = vectors.map(v => scaler.transform(v))
    val matrix = new RowMatrix(scaledVectors)
    val K = 10
    val pc = matrix.computePrincipalComponents(K)

    val rows = pc.numRows
    val cols = pc.numCols
    println(rows,cols)
   // println(pixels.take(10).map(_.take(10).mkString("",",",", ...")).mkString("\n"))
    sc.stop()
  }

  /**
    * load inages
    *
    * @param path
    * @return
    */
  def loadImageFromFile(path:String) : BufferedImage ={
    ImageIO.read(new File(path))
  }

  /**
    * transform images
    *
    * @param image
    * @param width
    * @param height
    * @return
    */
  def processImage(image:BufferedImage,width:Int,height:Int) :BufferedImage ={
    val bwImage = new BufferedImage(width,height,BufferedImage.TYPE_BYTE_GRAY)
    val g = bwImage.getGraphics
    g.drawImage(image,0,0,width,height,null)
    g.dispose()
    bwImage
  }

  /**
    * get Pixels
    *
    * @param image
    * @return
    */
  def getPixelsFromImage(image:BufferedImage):Array[Double] ={
    val width = image.getWidth
    val height = image.getHeight
    val pixels = Array.ofDim[Double](width * height)
    image.getData.getPixels(0,0,width,height,pixels)
  }

  def extractPixels(path:String,width:Int,height:Int) :Array[Double]={
    val raw = loadImageFromFile(path)
    val processed = processImage(raw,width,height)
    getPixelsFromImage(processed)
  }
}
