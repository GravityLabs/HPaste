package com.gravity.hbase.schema

import org.junit.Test

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
class NewScalaTest {

  @Test def testMe() {


    val coll = Some("hi")


    val hi = Seq(1,5,7,9,10)

    val res = hi.flatMap(itm=> if(itm > 3) Some(itm) else None)
    println(res)

    println((for(itm <- hi) yield Some(itm)).flatten)
  }

}
