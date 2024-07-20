package pridwen.dataset

import pridwen.types.opschema.{SelectField, SelectMany}

import shapeless.{HList, HNil, ::}
import shapeless.labelled.{FieldType}

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf}

import scala.reflect.runtime.universe.TypeTag

trait FuncOnSchema[S <: HList, SF <: HList, F] { type Return ; def apply(f: F): UserDefinedFunction }
object FuncOnSchema {
    type Aux[S <: HList, SF <: HList, F, Return0] = FuncOnSchema[S, SF, F] { type Return = Return0 }

    implicit def function_10[
        P1 <: HList, P2 <: HList, P3 <: HList, P4 <: HList, P5 <: HList, P6 <: HList, P7 <: HList, P8 <: HList, P9 <: HList, P10 <: HList, 
        F1, F2, F3, F4, F5, F6, F7, F8, F9, F10,
        T1: TypeTag, T2: TypeTag, T3: TypeTag, T4: TypeTag, T5: TypeTag, T6: TypeTag, T7: TypeTag, T8: TypeTag, T9: TypeTag, T10: TypeTag, 
        R: TypeTag, S <: HList, NS <: HList
    ](
        implicit
        fieldsExists: SelectMany.Aux[S, P1 :: P2 :: P3 :: P4 :: P5 :: P6 :: P7 :: P8 :: P9 :: P10 :: HNil, NS],
        eq: =:=[NS, FieldType[F1,T1] :: FieldType[F2,T2] :: FieldType[F3,T3] :: FieldType[F4,T4] :: FieldType[F5,T5] :: FieldType[F6,T6] :: FieldType[F7,T7] :: FieldType[F8,T8] :: FieldType[F9,T9] :: FieldType[F10,T10] :: HNil]
    ): Aux[S, 
        P1 :: P2 :: P3 :: P4 :: P5 :: P6 :: P7 :: P8 :: P9 :: P10 :: HNil,
        Function10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R],
        R
    ] = new FuncOnSchema[S, 
        P1 :: P2 :: P3 :: P4 :: P5 :: P6 :: P7 :: P8 :: P9 :: P10 :: HNil,
        Function10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R]
    ] { 
        type Return = R 
        def apply(f: Function10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R]) = udf(f)
    }

    implicit def function_9[
        P1 <: HList, P2 <: HList, P3 <: HList, P4 <: HList, P5 <: HList, P6 <: HList, P7 <: HList, P8 <: HList, P9 <: HList, 
        F1, F2, F3, F4, F5, F6, F7, F8, F9, 
        T1: TypeTag, T2: TypeTag, T3: TypeTag, T4: TypeTag, T5: TypeTag, T6: TypeTag, T7: TypeTag, T8: TypeTag, T9: TypeTag, 
        R: TypeTag, S <: HList, NS <: HList
    ](
        implicit
        fieldsExists: SelectMany.Aux[S, P1 :: P2 :: P3 :: P4 :: P5 :: P6 :: P7 :: P8 :: P9 :: HNil, NS],
        eq: =:=[NS, FieldType[F1,T1] :: FieldType[F2,T2] :: FieldType[F3,T3] :: FieldType[F4,T4] :: FieldType[F5,T5] :: FieldType[F6,T6] :: FieldType[F7,T7] :: FieldType[F8,T8] :: FieldType[F9,T9] :: HNil]
    ): Aux[S, 
        P1 :: P2 :: P3 :: P4 :: P5 :: P6 :: P7 :: P8 :: P9 :: HNil,
        Function9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R],
        R
    ] = new FuncOnSchema[S, 
        P1 :: P2 :: P3 :: P4 :: P5 :: P6 :: P7 :: P8 :: P9 :: HNil,
        Function9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R]
    ] { 
        type Return = R 
        def apply(f: Function9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R]) = udf(f)
    }

    implicit def function_8[
        P1 <: HList, P2 <: HList, P3 <: HList, P4 <: HList, P5 <: HList, P6 <: HList, P7 <: HList, P8 <: HList, 
        F1, F2, F3, F4, F5, F6, F7, F8, 
        T1: TypeTag, T2: TypeTag, T3: TypeTag, T4: TypeTag, T5: TypeTag, T6: TypeTag, T7: TypeTag, T8: TypeTag, 
        R: TypeTag, S <: HList, NS <: HList
    ](
        implicit
        fieldsExists: SelectMany.Aux[S, P1 :: P2 :: P3 :: P4 :: P5 :: P6 :: P7 :: P8 :: HNil, NS],
        eq: =:=[NS, FieldType[F1,T1] :: FieldType[F2,T2] :: FieldType[F3,T3] :: FieldType[F4,T4] :: FieldType[F5,T5] :: FieldType[F6,T6] :: FieldType[F7,T7] :: FieldType[F8,T8] :: HNil]
    ): Aux[S, 
        P1 :: P2 :: P3 :: P4 :: P5 :: P6 :: P7 :: P8 :: HNil,
        Function8[T1, T2, T3, T4, T5, T6, T7, T8, R],
        R
    ] = new FuncOnSchema[S, 
        P1 :: P2 :: P3 :: P4 :: P5 :: P6 :: P7 :: P8 :: HNil,
        Function8[T1, T2, T3, T4, T5, T6, T7, T8, R]
    ] { 
        type Return = R 
        def apply(f: Function8[T1, T2, T3, T4, T5, T6, T7, T8, R]) = udf(f)
    }

    implicit def function_7[
        P1 <: HList, P2 <: HList, P3 <: HList, P4 <: HList, P5 <: HList, P6 <: HList, P7 <: HList, 
        F1, F2, F3, F4, F5, F6, F7, 
        T1: TypeTag, T2: TypeTag, T3: TypeTag, T4: TypeTag, T5: TypeTag, T6: TypeTag, T7: TypeTag, 
        R: TypeTag, S <: HList, NS <: HList
    ](
        implicit
        fieldsExists: SelectMany.Aux[S, P1 :: P2 :: P3 :: P4 :: P5 :: P6 :: P7 :: HNil, NS],
        eq: =:=[NS, FieldType[F1,T1] :: FieldType[F2,T2] :: FieldType[F3,T3] :: FieldType[F4,T4] :: FieldType[F5,T5] :: FieldType[F6,T6] :: FieldType[F7,T7] :: HNil]
    ): Aux[S, 
        P1 :: P2 :: P3 :: P4 :: P5 :: P6 :: P7 :: HNil,
        Function7[T1, T2, T3, T4, T5, T6, T7, R],
        R
    ] = new FuncOnSchema[S, 
        P1 :: P2 :: P3 :: P4 :: P5 :: P6 :: P7 :: HNil,
        Function7[T1, T2, T3, T4, T5, T6, T7, R]
    ] { 
        type Return = R 
        def apply(f: Function7[T1, T2, T3, T4, T5, T6, T7, R]) = udf(f)
    }

    implicit def function_6[
        P1 <: HList, P2 <: HList, P3 <: HList, P4 <: HList, P5 <: HList, P6 <: HList, 
        F1, F2, F3, F4, F5, F6, 
        T1: TypeTag, T2: TypeTag, T3: TypeTag, T4: TypeTag, T5: TypeTag, T6: TypeTag, 
        R: TypeTag, S <: HList, NS <: HList
    ](
        implicit
        fieldsExists: SelectMany.Aux[S, P1 :: P2 :: P3 :: P4 :: P5 :: P6 :: HNil, NS],
        eq: =:=[NS, FieldType[F1,T1] :: FieldType[F2,T2] :: FieldType[F3,T3] :: FieldType[F4,T4] :: FieldType[F5,T5] :: FieldType[F6,T6] :: HNil]
    ): Aux[S, 
        P1 :: P2 :: P3 :: P4 :: P5 :: P6 :: HNil,
        Function6[T1, T2, T3, T4, T5, T6, R],
        R
    ] = new FuncOnSchema[S, 
        P1 :: P2 :: P3 :: P4 :: P5 :: P6 :: HNil,
        Function6[T1, T2, T3, T4, T5, T6, R]
    ] { 
        type Return = R 
        def apply(f: Function6[T1, T2, T3, T4, T5, T6, R]) = udf(f)
    }

    implicit def function_5[
        P1 <: HList, P2 <: HList, P3 <: HList, P4 <: HList, P5 <: HList, 
        F1, F2, F3, F4, F5, 
        T1: TypeTag, T2: TypeTag, T3: TypeTag, T4: TypeTag, T5: TypeTag, 
        R: TypeTag, S <: HList, NS <: HList
    ](
        implicit
        fieldsExists: SelectMany.Aux[S, P1 :: P2 :: P3 :: P4 :: P5 :: HNil, NS],
        eq: =:=[NS, FieldType[F1,T1] :: FieldType[F2,T2] :: FieldType[F3,T3] :: FieldType[F4,T4] :: FieldType[F5,T5] :: HNil]
    ): Aux[S, 
        P1 :: P2 :: P3 :: P4 :: P5 :: HNil,
        Function5[T1, T2, T3, T4, T5, R],
        R
    ] = new FuncOnSchema[S, 
        P1 :: P2 :: P3 :: P4 :: P5 :: HNil,
        Function5[T1, T2, T3, T4, T5, R]
    ] { 
        type Return = R 
        def apply(f: Function5[T1, T2, T3, T4, T5, R]) = udf(f)
    }

    implicit def function_4[
        P1 <: HList, P2 <: HList, P3 <: HList, P4 <: HList, 
        F1, F2, F3, F4, 
        T1: TypeTag, T2: TypeTag, T3: TypeTag, T4: TypeTag, 
        R: TypeTag, S <: HList, NS <: HList
    ](
        implicit
        fieldsExists: SelectMany.Aux[S, P1 :: P2 :: P3 :: P4 :: HNil, NS],
        eq: =:=[NS, FieldType[F1,T1] :: FieldType[F2,T2] :: FieldType[F3,T3] :: FieldType[F4,T4] :: HNil]
    ): Aux[S, 
        P1 :: P2 :: P3 :: P4 :: HNil,
        Function4[T1, T2, T3, T4, R],
        R
    ] = new FuncOnSchema[S, 
        P1 :: P2 :: P3 :: P4 :: HNil,
        Function4[T1, T2, T3, T4, R]
    ] { 
        type Return = R 
        def apply(f: Function4[T1, T2, T3, T4, R]) = udf(f)
    }

    implicit def function_3[
        P1 <: HList, P2 <: HList, P3 <: HList, 
        F1, F2, F3,
        T1: TypeTag, T2: TypeTag, T3: TypeTag, 
        R: TypeTag, S <: HList, NS <: HList
    ](
        implicit
        fieldsExists: SelectMany.Aux[S, P1 :: P2 :: P3 :: HNil, NS],
        eq: =:=[NS, FieldType[F1,T1] :: FieldType[F2,T2] :: FieldType[F3,T3] :: HNil]
    ): Aux[S, 
        P1 :: P2 :: P3 :: HNil,
        Function3[T1, T2, T3, R],
        R
    ] = new FuncOnSchema[S, 
        P1 :: P2 :: P3 :: HNil,
        Function3[T1, T2, T3, R]
    ] { 
        type Return = R 
        def apply(f: Function3[T1, T2, T3, R]) = udf(f)
    }

    implicit def function_2[
        P1 <: HList, P2 <: HList, F1, F2, T1: TypeTag, T2: TypeTag, R: TypeTag, S <: HList, NS <: HList
    ](
        implicit
        fieldsExists: SelectMany.Aux[S, P1 :: P2 :: HNil, NS],
        eq: =:=[NS, FieldType[F1,T1] :: FieldType[F2,T2] :: HNil]
    ): Aux[S, P1 :: P2 :: HNil, Function2[T1, T2, R], R] 
        = new FuncOnSchema[S, P1 :: P2 :: HNil, Function2[T1, T2, R]] { 
            type Return = R 
            def apply(f: Function2[T1, T2, R]) = udf(f)
        }

    implicit def function_1[P1 <: HList, F1, T1: TypeTag, R: TypeTag, S <: HList, NS <: HList](
        implicit
        fieldsExists: SelectMany.Aux[S, P1 :: HNil, NS],
        eq: =:=[NS, FieldType[F1,T1] :: HNil]
    ): Aux[S, P1 :: HNil, Function1[T1, R], R] = new FuncOnSchema[S, P1 :: HNil, Function1[T1, R]] { 
        type Return = R 
        def apply(f: Function1[T1, R]) = udf(f)
    }
}