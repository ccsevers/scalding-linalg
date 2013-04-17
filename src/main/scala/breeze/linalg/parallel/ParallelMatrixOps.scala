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
  def tsqr(m : DenseMatrix[Double], buffer: Int, skipQ: Boolean = true) :
  (Option[DenseMatrix[Double]], DenseMatrix[Double]) = {

    type QRPair = (Option[DenseMatrix[Double]], DenseMatrix[Double])

    if (buffer > m.cols && buffer < m.rows) {
      // this chunks a matrix into vertical blocks of size buffer x n, the last might be smaller than buffer
      def blockOrMatEnd(i: Int) = if(buffer*(i) < m.rows) buffer*(i) else m.rows

      def stackAndFactor(l: QRPair, r: QRPair, skipQ: Boolean = false) : QRPair = {
        val (lq,lr) = l
        val (rq,rr) = r
        // no Q calculations, much faster
        if(skipQ) (None,breeze.linalg.qr(DenseMatrix.vertcat(lr,rr),true)._2(0 until m.cols,::))
        // do the Q calculations, but do them piecewise as we go for numerical stability
        else {
          val (newQ, newR) = breeze.linalg.qr(DenseMatrix.vertcat(lr,rr),false)
          val outR = newR(0 until m.cols,::)
          val newQTop = lq.map{mat => mat * newQ(0 until lr.rows,0 until m.cols)}.getOrElse(newQ(0 until lr.rows,0 until m.cols))
          val newQBottom = rq.map{mat => mat * newQ(lr.rows until (lr.rows + rr.rows),0 until m.cols)}.getOrElse(newQ(lr.rows until (lr.rows + rr.rows),0 until m.cols))
          (Some(DenseMatrix.vertcat(newQTop,newQBottom)), outR)
        }
      }

      val parts = for(i <- 0 until math.ceil(m.rows.toDouble/buffer).toInt)
        yield (None,m(buffer*(i) until blockOrMatEnd(i+1),::)) : QRPair

      // now cat blocks and factor
      parts.par.reduce{(m1,m2) => stackAndFactor(m1,m2,skipQ)}
    }
    // fall back to regular qr if the buffer is too large or too small
    else {
      breeze.util.logging.ConsoleLogger.warn("Buffer is too large or too small. Falling back to non-parallel QR")
      val (q,r) = breeze.linalg.qr(m,skipQ)
      (if(skipQ) None else Some(q(::,0 until m.cols)), r(0 until m.cols,::))
    }

  }
}
