package com.gravity.hbase.mapreduce.tests

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import com.gravity.hbase.mapreduce._
import org.apache.hadoop.io.{NullWritable, Text, IntWritable}
import scala.collection.JavaConversions._
import junit.framework.TestCase
import org.junit._
import org.apache.hadoop.mapred.MiniMRCluster
import org.apache.hadoop.hdfs.MiniDFSCluster
import java.io.{InputStreamReader, BufferedReader, File}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.gravity.hbase.schema.HPasteMapReduceTestCase
import com.gravity.hbase.mapreduce.HMapCombineReduceTask
import com.gravity.hbase.mapreduce.HTaskConfigs
import com.gravity.hbase.mapreduce.BigMemoryConf
import com.gravity.hbase.mapreduce.HIO
import com.gravity.hbase.mapreduce.HPathInput
import com.gravity.hbase.mapreduce.HTaskID
import com.gravity.hbase.mapreduce.ReducerCountConf
import com.gravity.hbase.mapreduce.HPathOutput

/**
 * User: mtrelinski
 */

object TemporaryIOPaths {

  val NONCE = "com-gravity-hpaste-"
  val INPUT = NONCE + "input"

  val ClassicWordCount_OUTPUT = NONCE + "classicwordcount-output"
  val ClassicWordCountWithCombiner_OUTPUT = NONCE + "classicwordcountcombiner-output"
  val ClassicWordCountWithCombinerDifferentReducer_OUTPUT = NONCE + "wordcountcombinerdiffreducer-output"

}

// The Jobs:

class ClassicWordCount extends HJob[NoSettings] (
  "A Contrived and Classic Word Count",
  HMapReduceTask(
    HTaskID("AContrivedAndClassicWordCount"),
    HTaskConfigs(ReducerCountConf(2), BigMemoryConf(512, 512, 500, 500)),
    HIO(
      HPathInput(TemporaryIOPaths.INPUT :: Nil),
      HPathOutput(TemporaryIOPaths.ClassicWordCount_OUTPUT)
    ),
    new LineToWordsWithCounts,
    new WordWithCountAggregator
  )
)

class ClassicWordCountWithCombiner extends HJob[NoSettings] (
  "Another Contrived and Classic Word Count using a Combiner",
  HMapCombineReduceTask(
    HTaskID("AnotherContrivedWordCountWithACombiner"),
    HTaskConfigs(ReducerCountConf(2), BigMemoryConf(512, 512, 500, 500)),
    HIO(
      HPathInput(TemporaryIOPaths.INPUT :: Nil),
      HPathOutput(TemporaryIOPaths.ClassicWordCountWithCombiner_OUTPUT)
    ),
    new LineToWordsWithCounts,
    new WordWithCountAggregator,
    new WordWithCountAggregator
  )
)

class ClassicWordCountWithCombinerAndDifferentReducer extends HJob[NoSettings] (
  "Another Contrived and Classic Word Count using a Combiner with a Reducer that doesn't match the output K, V types",
  HMapCombineReduceTask(
    HTaskID("YetAnotherContrivedWordCountWithACombiner"),
    HTaskConfigs(ReducerCountConf(2), BigMemoryConf(512, 512, 500, 500)),
    HIO(
      HPathInput(TemporaryIOPaths.INPUT :: Nil),
      HPathOutput(TemporaryIOPaths.ClassicWordCountWithCombinerDifferentReducer_OUTPUT)
    ),
    new LineToWordsWithCounts,
    new WordWithCountAggregator,
    new WordWithCountFinalAggregator
  )
)

// The Transformers and Aggregators:

class LineToWordsWithCounts extends HMapper[IntWritable, Text, Text, IntWritable] {

  val wordWritable = new Text
  val one = new IntWritable(1)

  override def map() {
    this.value.toString.split("\\s").foreach {
      word =>
        wordWritable.set(word)
        this.write(wordWritable, one)
    }

  }

}

class WordWithCountAggregator extends HReducer[Text, IntWritable, Text, IntWritable] {

  override def reduce() {

    var total: Int = 0
    val valuesIterator = this.values.iterator()
    while(valuesIterator.hasNext)
      total = total + valuesIterator.next().get()

    this.write(this.key, new IntWritable(total))
  }

}

class WordWithCountFinalAggregator extends HReducer[Text, IntWritable, Text, NullWritable] {

  override def reduce() {

    var total: Int = 0
    val valuesIterator = this.values.iterator()
    while(valuesIterator.hasNext)
      total = total + valuesIterator.next().get()

    this.write(new Text(this.key.toString + "\t" + total), NullWritable.get())
  }

}

// The Tests:

class MapReduceTests extends HPasteMapReduceTestCase(TemporaryIOPaths.INPUT :: TemporaryIOPaths.ClassicWordCount_OUTPUT :: TemporaryIOPaths.ClassicWordCountWithCombiner_OUTPUT :: TemporaryIOPaths.ClassicWordCountWithCombinerDifferentReducer_OUTPUT :: Nil) {

  val sentences = "the quick brown fox jumped over the lazy dog" :: "cat and dog" :: Nil
  val sentencesWordFrequencies = Map("the" -> 2, "quick" -> 1, "brown" -> 1, "fox" -> 1, "jumped" -> 1, "over" -> 1, "lazy" -> 1, "dog" -> 2, "and" -> 1, "cat" -> 1)

  def verifyOutput(path: String) = {
    val output: String = getJobOutput(path)
    println("Job Output: \n" + output)
    output.split("\n").foreach {
      line =>
        val lineSplit = line.split("\t")
        val (word, freq) = (lineSplit(0) -> lineSplit(1))
        Assert.assertEquals( "'" + word + "' should be " + sentencesWordFrequencies(word) + " but got " + freq + " instead", sentencesWordFrequencies(word).toLong, freq.toLong)
    }
  }

  def writeSentences() {
    val stream = getFileSystem().create(new Path(TemporaryIOPaths.INPUT + "/sentences.txt"), true)
    sentences.foreach {
      sentence => stream.write((sentence + "\n").getBytes())
    }
    stream.flush()
    stream.sync()
    stream.close()
  }

  @Test
  def testClassicWordCount() {
    writeSentences()
    val classicWordCount = new ClassicWordCount
    val resultOfClassicWordCount = classicWordCount.run(new com.gravity.hbase.mapreduce.NoSettings, conf)
    Assert.assertTrue("job failed", resultOfClassicWordCount._1)
    verifyOutput(TemporaryIOPaths.ClassicWordCount_OUTPUT)
  }

  @Test
  def testClassicWordCountWithCombiner() {
    writeSentences()
    val classicWordCountWithCombiner = new ClassicWordCountWithCombiner
    val resultOfClassicWordCountWithCombiner = classicWordCountWithCombiner.run(new com.gravity.hbase.mapreduce.NoSettings, conf)
    Assert.assertTrue("job failed", resultOfClassicWordCountWithCombiner._1)
    verifyOutput(TemporaryIOPaths.ClassicWordCountWithCombiner_OUTPUT)
  }

  @Test
  def testClassicWordCountWithCombinerAndDifferentReducer() {
    writeSentences()
    val classicWordCountWithCombinerAndDifferentReducer = new ClassicWordCountWithCombinerAndDifferentReducer
    val resultOfClassicWordCountWithCombinerAndDifferentReducer = classicWordCountWithCombinerAndDifferentReducer.run(new com.gravity.hbase.mapreduce.NoSettings, conf)
    Assert.assertTrue("job failed", resultOfClassicWordCountWithCombinerAndDifferentReducer._1)
    verifyOutput(TemporaryIOPaths.ClassicWordCountWithCombinerDifferentReducer_OUTPUT)
  }

}
