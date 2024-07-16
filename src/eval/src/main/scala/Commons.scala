import breeze.linalg.{CSCMatrix, sum, Axis}

import scala.collection.parallel.CollectionConverters._

object commons {
    val file_grt = "g_rt"
    
    // Source : https://biercoff.com/easily-measuring-code-execution-time-in-scala/
    def time[R](block: => R): R = {
        val t0 = System.nanoTime()
        val result = block    // call-by-name
        val t1 = System.nanoTime()
        println(f"Elapsed time: ${(t1 - t0)} ns / ${(t1 - t0)/1000000}%.3f ms / ${(t1 - t0)/1000000000}%.3f s")
        result
    } 

    def eval_polarisation(adj_matrix: CSCMatrix[Int], comm_matrix: CSCMatrix[Int]) = {
        import scala.language.postfixOps

        println("--- Substep 1/2: Calculating the auxiliary matrices.")
        val (nmc, mct, md, i, ni, inmc) = time {
            val nmc = comm_matrix.map(v => if(v == 0) 1 else 0)
            val mct = comm_matrix.t
            val md = adj_matrix * comm_matrix
            val i = md.map(v => if(v == 0) 1 else 0)
            val ni = md.map(v => if(v == 0) 0 else 1) // Essayer de passer ni en DenseMatrix
            val inmc = i *:* nmc
            (nmc, mct, md, i, ni, inmc) 
        }

        println("--- Substep 2/2: Filling the antagonism and porosity matrices.")
        val (man, mp) = (CSCMatrix.zeros[Double](comm_matrix.cols, comm_matrix.cols), CSCMatrix.zeros[Double](comm_matrix.cols, comm_matrix.cols))

        time { (0 to (comm_matrix.cols-1) par).foreach( i => {
            val ii = CSCMatrix.tabulate(comm_matrix.rows, comm_matrix.cols) { case (r, c) => inmc(r, c) * comm_matrix(r, i) }
            val tmp_mdsi = adj_matrix * ii
            val mdsi = CSCMatrix.tabulate(comm_matrix.rows, comm_matrix.cols){ case(r,c) => tmp_mdsi(r, c) * comm_matrix(r, i) * ni(r, c) }
            val mmdsi = mdsi.map(v => if(v != 0) 1.0 else 0)
            val mvani = CSCMatrix.tabulate(comm_matrix.rows, comm_matrix.cols){ case(r,c) => if(mdsi(r, c) != 0) if(mdsi(r, c) + md(r, c) != 0) (mdsi(r, c).asInstanceOf[Double] / (mdsi(r, c) + md(r, c))) - 0.5 else 0 else 0 }
            val tmp_mpi1 = mvani.map(v => if(v < 0) 1.0 else 0)
            val mbsi = sum(mmdsi.toDenseMatrix, Axis._0)
            val mcti = CSCMatrix.tabulate(1, comm_matrix.rows){ case(r,c) => mct(i,c).asInstanceOf[Double] }

            val (mani1, mani2, mpi1) = (mcti * mvani, mcti * mmdsi, mcti * tmp_mpi1)

            for(j <- 0 to (comm_matrix.cols-1)) {
                man(i,j) = if(mani2(0, j) != 0) mani1(0, j) / mani2(0, j) else 0
                mp(i,j) = mpi1(0, j) / (if(mbsi(j) != 0) mbsi(j) else 1) * 100
            }
        }) }

        (man.toDenseMatrix, mp.toDenseMatrix)
    }
}