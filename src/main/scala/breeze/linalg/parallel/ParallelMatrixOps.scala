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


object parallel {


  /**
   * tsqr is a parallel version of the normal qr routine. It chunks the input matrix horizontally and then does a parallel
   * reduce by concating chunks and using the normal qr routine on them.
   *
   * @param inputM The input matrix. Should be dim mxn where m >= n.
   * @param buffer The size of the buffer to use when factorizing. Each factorization will happen on a matrix of size
   *               about 2*buffer
   * @param skipQ  Whether or not to compute the Q part. This is more expensive and might not be needed.
   * @return       You get an Option[ DenseMatrix[Double] ] for Q and a DenseMatrix[Double] for R. The Option is guaranteed
   *               to contain Q if you asked for it with the skipQ parameter.
   */
  def tsqr(inputM : DenseMatrix[Double], buffer: Int, skipQ: Boolean = false) :
  (Option[DenseMatrix[Double]], DenseMatrix[Double]) = {

    type QRPair = (Option[DenseMatrix[Double]], DenseMatrix[Double])

    if (buffer > inputM.cols && buffer < inputM.rows) {
      // this chunks a matrix into vertical blocks of size buffer x n, the last might be smaller than buffer
      def blockOrMatEnd(i: Int) = if(buffer*(i) < inputM.rows) buffer*(i) else inputM.rows

      def stackAndFactor(l: QRPair, r: QRPair, skipQ: Boolean) : QRPair = {
        val (lq,lr) = l
        val (rq,rr) = r
        // no Q calculations, much faster
        if(skipQ) (None,breeze.linalg.qr(DenseMatrix.vertcat(lr,rr),true)._2(0 until inputM.cols,::))
        // do the Q calculations, but do them piecewise as we go for numerical stability
        else {
          val (newQ, newR) = breeze.linalg.qr(DenseMatrix.vertcat(lr,rr),false)
          val outR = newR(0 until inputM.cols,::)
          val newQTop = lq.map{mat => mat * newQ(0 until lr.rows,0 until inputM.cols)}.getOrElse(newQ(0 until lr.rows,0 until inputM.cols))
          val newQBottom = rq.map{mat => mat * newQ(lr.rows until (lr.rows + rr.rows),0 until inputM.cols)}.getOrElse(newQ(lr.rows until (lr.rows + rr.rows),0 until inputM.cols))
          (Some(DenseMatrix.vertcat(newQTop,newQBottom)), outR)
        }
      }

      val parts = for(i <- 0 until math.ceil(inputM.rows.toDouble/buffer).toInt)
        yield (None,inputM(buffer*(i) until blockOrMatEnd(i+1),::)) : QRPair

      // now cat blocks and factor
      parts.par.reduce{(m1,m2) => stackAndFactor(m1,m2,skipQ)}
    }
    // fall back to regular qr if the buffer is too large or too small
    else {
      breeze.util.logging.ConsoleLogger.warn("Buffer is too large or too small. Falling back to non-parallel QR")
      val (q,r) = breeze.linalg.qr(inputM,skipQ)
      (if(skipQ) None else Some(q(::,0 until inputM.cols)), r(0 until inputM.cols,::))
    }

  }
}
