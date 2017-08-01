package org.cddb.lsmt

import org.cddb.io.Config
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LevelHandler extends FunSuite {

  test("level handler 1") {
    val config = Config("./.tempdata", "index.dat", TableMetadata(500, 0.5, 300))
  }

}
