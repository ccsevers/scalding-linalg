/*
Copyright 2012 eBay, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package breeze.linalg

import org.scalacheck._
import breeze.linalg.parallel._
import breeze.linalg._


/**
 * Tests for the parallel tsqr. Will probably take a bit to run (~2-3 minutes).
 */

object TSQRSpecification extends Properties("TSQR") {
  import Prop.forAll

  val Epsilon = 5E-13


  implicit def arbDenseMatrix: Arbitrary[DenseMatrix[Double]] =
    Arbitrary {
      for {
        n <- Gen.choose(10,500)
        m <- Gen.choose(n+1,5000)
      } yield DenseMatrix.rand(m,n)
    }


  property("R Part")  = forAll( (mat: DenseMatrix[Double]) => {
    val buf = (math.random*(mat.rows-mat.cols)+mat.cols).toInt
    val parR = tsqr(mat,buf,true)._2
    val (q,r) = qr(mat,true)
    val r2 = r(0 until r.cols,::)
    val diff = parR.mapValues(math.abs) - r2.mapValues(math.abs)
    val truthmat = diff.mapValues(_ < Epsilon)
    truthmat.data.foldLeft(true)(_ && _)
  })

  property("Q Part")  = forAll( (mat: DenseMatrix[Double]) => {
    val buf = (math.random*(mat.rows-mat.cols)+mat.cols).toInt
    val qopt = tsqr(mat,buf,false)._1
    val q = qopt.get
    val qqt = q.t * q
    val diff = qqt.mapValues(math.abs) - DenseMatrix.eye[Double](qqt.rows)
    val truthmat = diff.mapValues(_ < Epsilon)
    truthmat.data.foldLeft(true)(_ && _)
  })


}
