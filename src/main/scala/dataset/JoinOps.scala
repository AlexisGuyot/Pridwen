package pridwen.dataset

import pridwen.types.opschema.{SelectField, MergeSchema}
import pridwen.types.support.DecompPath

import ColumnOps._
import FilterOps.{And, Or}

import shapeless.{HList, Witness, HNil, ::, Widen}

import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions.{col}

trait JoinOps[LS <: HList, RS <: HList, O <: FOperator, I] { type Out <: HList ; def toSparkColumn(d1: Dataset[_], d2: Dataset[_]): Column }
object JoinOps {
    type Aux[LS <: HList, RS <: HList, O <: FOperator, I, Out0 <: HList] = JoinOps[LS, RS, O, I] { type Out = Out0 }

    implicit def second_input_is_path[
        LS <: HList, RS <: HList, WP1 <: HList, SP1 <: HList, WP2 <: HList, SP2 <: HList, 
        FN1 <: Symbol, FN2 <: Symbol, FT, O <: FOperator, NS <: HList, P1 <: HList, P2 <: HList
    ](
        implicit
        select_f1: SelectField.Aux[LS, SP1, FN1, FT],
        select_f2: SelectField.Aux[RS, SP2, FN2, FT],
        decomp_p1: DecompPath.Aux[SP1, P1, FN1],
        decomp_p2: DecompPath.Aux[SP2, P2, FN2],
        get_path1: Path.Aux[WP1,SP1],
        get_path2: Path.Aux[WP2,SP2],
        compare: SparkOperator[O],
        m: MergeSchema.Aux[LS, RS, P1, P2, NS]
    ): Aux[LS, RS, O, (Path.Aux[WP1,SP1], Path.Aux[WP2,SP2]), NS] = new JoinOps[LS, RS, O, (Path.Aux[WP1,SP1], Path.Aux[WP2,SP2])] {
        type Out = NS
        def toSparkColumn(d1: Dataset[_], d2: Dataset[_]): Column = compare(d1.col(get_path1.asString), d2.col(get_path2.asString))
    }

    implicit def second_input_is_witness[
        LS <: HList, RS <: HList, WP1 <: HList, SP1 <: HList, FN1 <: Symbol, FN2 <: Symbol, 
        FT, O <: FOperator, NS <: HList, P1 <: HList
    ](
        implicit
        select_f1: SelectField.Aux[LS, SP1, FN1, FT],
        select_f2: SelectField.Aux[RS, FN2 :: HNil, FN2, FT],
        decomp_p1: DecompPath.Aux[SP1, P1, FN1],
        get_path1: Path.Aux[WP1,SP1],
        get_path2: Path[Witness.Aux[FN2] :: HNil],
        compare: SparkOperator[O],
        m: MergeSchema.Aux[LS, RS, P1, HNil, NS]
    ): Aux[LS, RS, O, (Path.Aux[WP1,SP1], Witness.Aux[FN2]), NS] = new JoinOps[LS, RS, O, (Path.Aux[WP1,SP1], Witness.Aux[FN2])] {
        type Out = NS
        def toSparkColumn(d1: Dataset[_], d2: Dataset[_]): Column = compare(d1.col(get_path1.asString), d2.col(get_path2.asString))
    }

    implicit def operator_is_and[
        LS <: HList, RS <: HList, O1 <: FOperator, O2 <: FOperator, I1, I2, NS <: HList
    ](
        implicit
        compute_first: Aux[LS, RS, O1, I1, NS],
        compute_second: JoinOps[LS, RS, O2, I2]
    ): Aux[LS, RS, And[O1,O2], (I1,I2), NS] = new JoinOps[LS, RS, And[O1,O2], (I1,I2)] {
        type Out = NS
        def toSparkColumn(d1: Dataset[_], d2: Dataset[_]): Column = compute_first.toSparkColumn(d1,d2) && compute_second.toSparkColumn(d1,d2)
    }

    implicit def operator_is_or[
        LS <: HList, RS <: HList, O1 <: FOperator, O2 <: FOperator, I1, I2, NS <: HList
    ](
        implicit
        compute_first: Aux[LS, RS, O1, I1, NS],
        compute_second: JoinOps[LS, RS, O2, I2]
    ): Aux[LS, RS, Or[O1,O2], (I1,I2), NS] = new JoinOps[LS, RS, Or[O1,O2], (I1,I2)] {
        type Out = NS
        def toSparkColumn(d1: Dataset[_], d2: Dataset[_]): Column = compute_first.toSparkColumn(d1,d2) || compute_second.toSparkColumn(d1,d2)
    }
}