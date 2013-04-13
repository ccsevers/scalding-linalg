package breeze.linalg.parallel

import org.scalacheck._
import breeze.linalg._
import breeze.linalg.parallel._

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

  property("TSQR")  = forAll( (mat: DenseMatrix[Double]) => {
    val buf = (math.random*(mat.rows-mat.cols)+mat.cols).toInt
    val parR = tsqr(mat,buf)
    val (q,r) = qr(mat,true)
    val r2 = r(0 until r.cols,::)
    val diff = parR.mapValues(math.abs) - r2.mapValues(math.abs)
    val truthmat = diff.mapValues(_ < Epsilon)
    truthmat.data.foldLeft(true)(_ && _)
  })


}
