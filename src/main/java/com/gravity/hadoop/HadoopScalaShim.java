package com.gravity.hadoop;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;

/**
 * This class exists to get around an issue in Scala 2.9.x
 * https://issues.scala-lang.org/browse/SI-4603
 */
public class HadoopScalaShim {

  public static void registerInputFormat(Job job, Class inputFormat) {
    job.setInputFormatClass(inputFormat);
  }

  public static void registerMapper(Job job, Class mapper) {
    job.setMapperClass(mapper);
  }

  public static void registerReducer(Job job, Class reducer) {
    job.setReducerClass(reducer);
  }


  public static void registerCombiner(Job job, Class combiner) {
    job.setCombinerClass(combiner);
  }

  public static void setMultithreadedMapperClass(Job job,Class mapper) {
    MultithreadedMapper.setMapperClass(job, mapper);
  }

  public static void registerOutputFormat(Job job, Class output) {
    job.setOutputFormatClass(output);
  }}
