package pridwen.dataset

import pridwen.types.opschema.SelectField

import shapeless.{HList, Witness, HNil, ::, Widen}

import org.apache.spark.sql.{Column}
import org.apache.spark.sql.functions.{col}

trait OrderOps[O, I]
object OrderOps {
    import ColumnOps._

    trait And[O1 <: OOperator, O2 <: OOperator] extends OOperator 

    trait Compute[S <: HList, O <: OOperator, I] { def toSparkColumn: Array[Column] }
    object Compute {
        implicit def default[S <: HList, O <: OOperator, I](
            implicit
            compute: ColumnOps.Compute[S, O, I]
        ): Compute[S, O, I] = new Compute[S, O, I] { def toSparkColumn: Array[Column] = Array(compute.toSparkColumn) }

        implicit def operator_is_and[
            S <: HList, O1 <: OOperator, O2 <: OOperator, I1, I2
        ](
            implicit
            compute_first: OrderOps.Compute[S, O1, I1],
            compute_second: OrderOps.Compute[S, O2, I2]
        ): Compute[S, And[O1,O2], (I1,I2)] = new Compute[S, And[O1,O2], (I1,I2)] {
            def toSparkColumn: Array[Column] = compute_first.toSparkColumn ++ compute_second.toSparkColumn
        }
    }
}