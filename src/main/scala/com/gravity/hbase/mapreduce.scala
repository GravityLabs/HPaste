package com.gravity.hbase

import java.util.Random

package object mapreduce {
  type MapperFunc[MK,MV,MOK,MOV, S <: SettingsBase] = (HMapContext[MK,MV,MOK,MOV,S]) => Unit

  type ReducerFunc[MOK,MOV,ROK,ROV, S<:SettingsBase] = (HReduceContext[MOK,MOV,ROK,ROV,S]) => Unit

  def genTmpFile = "/tmp/htemp-" + new Random().nextInt(Int.MaxValue)
}