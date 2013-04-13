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
  def tsqr[T](m : DenseMatrix[Double], buffer: Int) : DenseMatrix[Double] = {
    if (buffer > m.cols && buffer < m.rows) {
      // this chunks a matrix into vertical blocks of size buffer x n, the last might be smaller than buffer
      def blockOrMatEnd(i: Int) = if(buffer*(i) < m.rows) buffer*(i) else m.rows

      val parts = for(i <- 0 until math.ceil(m.rows.toDouble/buffer).toInt) yield m(buffer*(i) until blockOrMatEnd(i+1),::)

      // now cat blocks and factor
      parts.par.reduce{(m1,m2) => breeze.linalg.qr(DenseMatrix.vertcat(m1,m2),true)._2(0 until m.cols,::)}
    }
    // fall back to regular qr if the buffer is too large or too small
    else {
      breeze.linalg.qr(m,true)._2(0 until m.cols,::)
    }
  }
}
