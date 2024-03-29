/*
 * Copyright (c) 2018 Schibsted Media Group. All rights reserved
 */
package infrastructure.docker

import com.whisk.docker.impl.dockerjava.DockerKitDockerJava
import com.whisk.docker.scalatest.DockerTestKit
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.prop.PropertyChecks
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

trait DockerIntegrationTest
    extends FlatSpec with Matchers with GivenWhenThen
    with DockerTestKit
    with DockerKitDockerJava
    with PropertyChecks
    with ScalaFutures
