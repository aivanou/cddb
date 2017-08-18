package org.cddb.lsmt

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LevelHandlerSuite extends TableSuite {

  //  val config = Config("./.tempdata", "index.dat", TableMetadata(500, 0.5, 300))

  test("level append records should be consistent") {
    val lhandler = LevelHandler(config)
    val recs = genRecords()
    recs foreach lhandler.append
    recs foreach { rec =>
      val dbRec = lhandler.read(rec.key)
      assert(dbRec.isDefined)
      assert(rec.timestamp <= dbRec.get.timestamp)
      if (rec.timestamp == dbRec.get.timestamp) {
        assertResult(rec.value)(dbRec.get.value)
      }
    }
  }

}
