import infrastructure.test.BaseTest
class ShellTest extends BaseTest {

  it should "use as shell" in {
    val path = "out/parquets/scores_1574777278502"
    spark.read.parquet(path).show(false)
  }

}
