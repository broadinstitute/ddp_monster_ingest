import _root_.io.circe.Json

lazy val `ddp-schema` = project
  .in(file("schema"))
  .enablePlugins(MonsterJadeDatasetPlugin)
  .settings(
    jadeTablePackage := "org.broadinstitute.monster.hca.jadeschema.table",
    jadeTableFragmentPackage := "org.broadinstitute.monster.hca.jadeschema.fragment",
    jadeStructPackage := "org.broadinstitute.monster.hca.jadeschema.struct",
  )
