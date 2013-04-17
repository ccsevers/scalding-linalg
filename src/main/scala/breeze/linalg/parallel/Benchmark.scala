package breeze.linalg.parallel

import breeze.linalg._

object Benchmark extends App{
    import breeze.linalg.parallel._

    // modified version to play with doing local iterations, doesn't do much so far
    def ttsqr(inputM : DenseMatrix[Double], buffer: Int, skipQ: Boolean = false) :
    (Option[DenseMatrix[Double]],DenseMatrix[Double]) = {

      type QRPair = (Option[DenseMatrix[Double]], DenseMatrix[Double])

      if (buffer > inputM.cols && buffer < inputM.rows) {
        // this chunks a matrix into vertical blocks of size buffer x n, the last might be smaller than buffer
        def blockOrMatEnd(i: Int) = if(buffer*(i) < inputM.rows) buffer*(i) else inputM.rows

        def stackAndFactor(l: DenseMatrix[Double], r: DenseMatrix[Double]) : DenseMatrix[Double] = {
          var m = DenseMatrix.vertcat(l,r)
          var s = DenseMatrix.eye[Double](inputM.cols)
          // no Q calculations, much faster
          for( i <- 0 until 2) {
            val (q,rr) = breeze.linalg.qr(m,false)
            m = q(::,0 until inputM.cols)
            val t = rr(0 until inputM.cols,::)
            s = t * s
          }
          s
        }

        val parts = for(i <- 0 until math.ceil(inputM.rows.toDouble/buffer).toInt)
        yield inputM(buffer*(i) until blockOrMatEnd(i+1),::)

        // now cat blocks and factor
        val r = parts.par.reduce{(m1,m2) => stackAndFactor(m1,m2)}
        if(skipQ) (None, r)
        else {
          val q = inputM * inv(r)
          (Some(q),r)
        }
      }
      // fall back to regular qr if the buffer is too large or too small
      else {
        breeze.util.logging.ConsoleLogger.warn("Buffer is too large or too small. Falling back to non-parallel QR")
        val (q,r) = breeze.linalg.qr(inputM,skipQ)
        (if(skipQ) None else Some(q(::,0 until inputM.cols)), r(0 until inputM.cols,::))
      }

    }

    val fixedmat = DenseMatrix.rand(10000,100)
    val (u,sigma,v) = svd(fixedmat)
    println("U is " + u.rows + "x" + u.cols + " , S is " + sigma.length + " V' is " + v.rows +"x"+v.cols)
    val zeromat = DenseMatrix.zeros[Double](10000-100,100)
    println("Done computing SVD")


    for(i <- 1 to 8) {
      val max = sigma(0)*math.pow(2,i)
      val sigmaprime = sigma
      sigmaprime(0) = max
      val mat = u*DenseMatrix.vertcat(diag(sigmaprime), zeromat)*v
      println("Done computing new matrix")
      val mmax = sigmaprime.toArray.max
      val mmin = sigmaprime.toArray.min
      val (q1,r1) = tsqr(mat,101,false)
      val r2 = tsqr(mat,101,true)._2

      val qintermed = mat * inv(r2)
      val t = tsqr(qintermed,101,true)._2
      val s = t*r2

      val qq1 = q1.get
      val qq2 = mat * inv(s)
      val diffm1 = ((qq1.t * qq1).mapValues(math.abs) - DenseMatrix.eye[Double](qq1.cols))
      val diffm2 = ((qq2.t * qq2).mapValues(math.abs) - DenseMatrix.eye[Double](qq2.cols))
      val diff1 = (0 until diffm1.rows).map(i => diffm1(i,::).sum).max
      val diff2 = (0 until diffm2.rows).map(i => diffm2(i,::).sum).max
      println("Cond is: " + mmax/mmin  + " Diff 1 is: " + diff1 + " and diff 2 is : " + diff2)

    }
}
