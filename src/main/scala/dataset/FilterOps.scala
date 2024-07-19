package pridwen.dataset

import pridwen.types.opschema.SelectField

import shapeless.{HList, Witness, HNil, ::, Widen}

import org.apache.spark.sql.{Column}
import org.apache.spark.sql.functions.{col}

trait FilterOps[O, I]
object FilterOps {
    import ColumnOps._

    trait And[O1 <: FOperator, O2 <: FOperator] extends FOperator ; trait Or[O1 <: FOperator, O2 <: FOperator] extends FOperator

    trait Compute[S <: HList, O <: FOperator, I] { def toSparkColumn: Column }
    object Compute {
        implicit def default[S <: HList, O <: FOperator, I](
            implicit
            compute: ColumnOps.Compute[S, O, I]
        ): Compute[S, O, I] = new Compute[S, O, I] { def toSparkColumn: Column = compute.toSparkColumn }

        implicit def operator_is_and[
            S <: HList, O1 <: FOperator, O2 <: FOperator, I1, I2
        ](
            implicit
            compute_first: ColumnOps.Compute[S, O1, I1],
            compute_second: ColumnOps.Compute[S, O2, I2]
        ): Compute[S, And[O1,O2], (I1,I2)] = new Compute[S, And[O1,O2], (I1,I2)] {
            def toSparkColumn: Column = compute_first.toSparkColumn && compute_second.toSparkColumn
        }

        implicit def operator_is_or[
            S <: HList, O1 <: FOperator, O2 <: FOperator, I1, I2
        ](
            implicit
            compute_first: ColumnOps.Compute[S, O1, I1],
            compute_second: ColumnOps.Compute[S, O2, I2]
        ): Compute[S, Or[O1,O2], (I1,I2)] = new Compute[S, Or[O1,O2], (I1,I2)] {
            def toSparkColumn: Column = compute_first.toSparkColumn || compute_second.toSparkColumn
        }
    }
}