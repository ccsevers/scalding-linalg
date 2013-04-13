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
