package demo

import infrastructure.test.BaseTest
import org.scalatest.Ignore

//@Ignore
class ShellTest extends BaseTest {

  it should "use as shell" in {
    val path = "out/delta/"
    val tablename = "scores_1574849993318"

    spark.read
      .format("delta")
      .load(path+tablename)
      .repartition(1)
      .write
      .format("delta")
      .mode("overwrite")
      .save(path+tablename)

  }

}
