package pridwen.dataset

import pridwen.types.opschema.SelectField

import shapeless.{HList, Witness, HNil, ::, Widen}
import shapeless.ops.hlist.Selector

import org.apache.spark.sql.{Column}
import org.apache.spark.sql.functions.{col}

import scala.reflect.runtime.universe.TypeTag

trait AddOps[O, I]
object AddOps {
    import ColumnOps._

    private type Numbers = Int :: Double :: Long :: Short :: Float :: HNil

    trait Compute[S <: HList, O <: AOperator, I] { type Out ; def toSparkColumn: Column }
    object Compute {
        type Aux[S <: HList, O <: AOperator, I, Out0] = Compute[S,O,I] { type Out = Out0 }

        implicit def second_input_is_value[
            S <: HList, WP1 <: HList, SP1 <: HList, FN1 <: Symbol, V, A >: V, FT, O <: AOperator
        ](
            implicit
            select_f1: SelectField.Aux[S, SP1, FN1, FT],
            isNumber: Selector[Numbers, FT],
            compute: ColumnOps.Compute[S, O, (Path.Aux[WP1,SP1], Widen.Aux[V, A])]
        ): Aux[S, O, (Path.Aux[WP1,SP1], Widen.Aux[V, A]), FT] = new Compute[S, O, (Path.Aux[WP1,SP1], Widen.Aux[V, A])] {
            type Out = FT
            def toSparkColumn: Column = compute.toSparkColumn
        }

        /* implicit def second_input_is_value[
            S <: HList, WP1 <: HList, SP1 <: HList, FN1 <: Symbol, V, FT >: V, O <: AOperator
        ](
            implicit
            select_f1: SelectField.Aux[S, SP1, FN1, FT],
            isNumber: Selector[Numbers, FT],
            compute: ColumnOps.Compute[S, O, (Path.Aux[WP1,SP1], Widen.Aux[V, FT])]
        ): Aux[S, O, (Path.Aux[WP1,SP1], Widen.Aux[V, FT]), FT] = new Compute[S, O, (Path.Aux[WP1,SP1], Widen.Aux[V, FT])] {
            type Out = FT
            def toSparkColumn: Column = compute.toSparkColumn
        } */

        implicit def second_input_is_path[
            S <: HList, WP1 <: HList, SP1 <: HList, WP2 <: HList, SP2 <: HList, FN1 <: Symbol, FN2 <: Symbol, FT, O <: AOperator
        ](
            implicit
            select_f1: SelectField.Aux[S, SP1, FN1, FT],
            select_f2: SelectField.Aux[S, SP2, FN2, FT],
            isNumber: Selector[Numbers, FT],
            compute: ColumnOps.Compute[S, O, (Path.Aux[WP1,SP1], Path.Aux[WP2,SP2])]
        ): Aux[S, O, (Path.Aux[WP1,SP1], Path.Aux[WP2,SP2]), FT] = new Compute[S, O, (Path.Aux[WP1,SP1], Path.Aux[WP2,SP2])] {
            type Out = FT
            def toSparkColumn: Column = compute.toSparkColumn
        }

        implicit def second_input_is_witness[
            S <: HList, WP1 <: HList, SP1 <: HList, FN1 <: Symbol, FN2 <: Symbol, V, FT >: V, O <: AOperator
        ](
            implicit
            select_f1: SelectField.Aux[S, SP1, FN1, FT],
            select_f2: SelectField.Aux[S, FN2 :: HNil, FN2, FT],
            isNumber: Selector[Numbers, FT],
            compute: ColumnOps.Compute[S, O, (Path.Aux[WP1,SP1], Witness.Aux[FN2])]
        ): Aux[S, O, (Path.Aux[WP1,SP1], Witness.Aux[FN2]), FT] = new Compute[S, O, (Path.Aux[WP1,SP1], Witness.Aux[FN2])] {
            type Out = FT
            def toSparkColumn: Column = compute.toSparkColumn
        }
    }
}