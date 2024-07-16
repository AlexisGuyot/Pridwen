package pridwen.types.opschema

import scala.annotation.implicitNotFound

import shapeless.{HList, HNil, ::}
import shapeless.ops.hlist.{Prepend}

import pridwen.types.support.DecompPath

/** Merge two schemas expressed as HList with several strategies.
* Computes a new schema formed by the merging.
*
* @tparam LeftSchema The first schema to merge or containing a sub-schema to merge.
* @tparam RightSchema The second schema to merge or containing a sub-schema to merge.
* @tparam LeftPath The paths to follow to find the nested schema to merge in the left schema (HNil if the whole schema needs to be merged).
* @tparam RightPath The paths to follow to find the nested schema to merge in the right schema (HNil if the whole schema needs to be merged).
* 
* Merging can be achieved through three strategies (see below for examples):
    - MergeMode.Default = appends the fields of the nested right sub-schema to the fields of the nested left sub-schema.
    - MergeMode.InLeft = replaces the left sub-schema in the left schema with the schema obtained by appending the fields of the nested right-schema to the fields of the left sub-schema.
    - MergeMode.InRight = replaces the right sub-schema in the right schema with the schema obtained by appending the fields of the nested right-schema to the fields of the left sub-schema.
*/
@implicitNotFound("[Pridwen / MergeSchema] Impossible to create a new schema Out by merging the sub-schemas reachable through paths ${LeftPath} and ${Rightpath} in schemas ${LeftSchema} and ${RightSchema}.") 
trait MergeSchema[LeftSchema <: HList, RightSchema <: HList, LeftPath, RightPath] {
    type Out <: HList
    def apply(ldata: LeftSchema, rdata: RightSchema): Out
    type Mode <: MergeMode
}

object MergeSchema {
    type Aux[LeftSchema <: HList, RightSchema <: HList, LeftPath, RightPath, Out0 <: HList] = MergeSchema[LeftSchema, RightSchema, LeftPath, RightPath] { type Out = Out0 ; type Mode = MergeMode.Default }
    type InLeft[LeftSchema <: HList, RightSchema <: HList, LeftPath, RightPath, Out0 <: HList] = MergeSchema[LeftSchema, RightSchema, LeftPath, RightPath] { type Out = Out0 ; type Mode = MergeMode.InLeft }
    type InRight[LeftSchema <: HList, RightSchema <: HList, LeftPath, RightPath, Out0 <: HList] = MergeSchema[LeftSchema, RightSchema, LeftPath, RightPath] { type Out = Out0 ; type Mode = MergeMode.InRight }

    /**
      * Introduction rule that creates terms of type MergeSchema merging schemas with the default strategy.
      * 
      * Example :
      * LeftSchema =  a ->> (
                    b ->> (
                        c ->> String :: d ->> Int :: HNil
                    ) :: 
                    e ->> String :: 
                    HNil
                ) :: 
                f ->> Int :: 
                HNil 
      * RightSchema = a' ->> Int :: b' ->> Boolean :: HNil
      * with f ->> Int <=> FieldType[Witness.`'f`.T, Int].
      *
      * LeftPath = a :: b :: HNil ; RightPath = HNil
      * Out = c ->> String :: d ->> Int :: a' ->> Int :: b' ->> Boolean :: HNil
      *
      * @param selectLSub Selects implicitly the left sub-schema. 
      * @param selectRSub Selects implicitly thr right sub-schema.
      * @param merge Appends the fields of the right sub-schema to the fields of the left sub-schema.
      * @return A term of type MergeSchema.
      */
    implicit def default_merge[LS <: HList, RS <: HList, LP, RP, LSubS <: HList, RSubS <: HList, NewS <: HList](
        implicit
        selectLSub: SelectOrFollow.Aux[LS, LP, LSubS],
        selectRSub: SelectOrFollow.Aux[RS, RP, RSubS],
        merge: Prepend.Aux[LSubS, RSubS, NewS]
    ): Aux[LS, RS, LP, RP, NewS] = new MergeSchema[LS, RS, LP, RP] {
        type Out = NewS 
        def apply(ldata: LS, rdata: RS): Out = merge(selectLSub(ldata), selectRSub(rdata))
        type Mode = MergeMode.Default
    }

    /**
      * Introduction rule that creates terms of type MergeSchema merging schemas with the inLeft strategy.
      * 
      * Example :
      * LeftSchema =  a ->> (
                    b ->> (
                        c ->> String :: d ->> Int :: HNil
                    ) :: 
                    e ->> String :: 
                    HNil
                ) :: 
                f ->> Int :: 
                HNil 
      * RightSchema = a' ->> Int :: b' ->> Boolean :: HNil
      * with f ->> Int <=> FieldType[Witness.`'f`.T, Int].
      *
      * LeftPath = a :: b :: HNil ; RightPath = HNil
      * Out = a ->> (
                    b ->> (
                        c ->> String :: 
                        d ->> Int :: 
                        a' ->> Int :: 
                        b' ->> Boolean ::
                        HNil
                    ) :: 
                    e ->> String :: 
                    HNil
                ) :: 
                f ->> Int :: 
                HNil 
      *
      * @param selectLSub Selects implicitly the left sub-schema. 
      * @param selectRSub Selects implicitly thr right sub-schema.
      * @param merge Appends implicitly the fields of the right sub-schema to the fields of the left sub-schema.
      * @param decomPath Decomposes implicitly the left path to isolate the last element (ignored) from the sub-path formed by the other ones (SLP).
      * @param decomSubPath Decomposes implicitly the sub-path to isolate the last element (PLK) from the sub-path formed by the other ones (ignored).
      * @param Updates implicitly the left schema by replacing the left sub-schema by the result of the merge.
      * @return A term of type MergeSchema.
      */
    implicit def merge_in_left[LS <: HList, RS <: HList, LP, RP, LSubS <: HList, RSubS <: HList, NewSubS <: HList, NewS <: HList](
        implicit
        selectLSub: SelectOrFollow.Aux[LS, LP, LSubS],
        selectRSub: SelectOrFollow.Aux[RS, RP, RSubS],
        merge: Prepend.Aux[LSubS, RSubS, NewSubS],
        updateLS: ReplaceOrUpdate.Aux[LS, LP, NewSubS, NewS]
    ): InLeft[LS, RS, LP, RP, NewS] = new MergeSchema[LS, RS, LP, RP] {
        type Out = NewS 
        def apply(ldata: LS, rdata: RS): Out = updateLS(ldata, merge(selectLSub(ldata), selectRSub(rdata)))
        type Mode = MergeMode.InLeft
    }

    /**
      * Introduction rule that creates terms of type MergeSchema merging schemas with the inRight strategy.
      * 
      * Example :
      * LeftSchema =  a ->> (
                    b ->> (
                        c ->> String :: d ->> Int :: HNil
                    ) :: 
                    e ->> String :: 
                    HNil
                ) :: 
                f ->> Int :: 
                HNil 
      * RightSchema = a' ->> Int :: b' ->> Boolean :: HNil
      * with f ->> Int <=> FieldType[Witness.`'f`.T, Int].
      *
      * LeftPath = a :: b :: HNil ; RightPath = HNil
      * Out =   a' ->> Int :: 
                b' ->> Boolean :: 
                c ->> String :: 
                d ->> Int ::
                HNil
      *
      * @param selectLSub Selects implicitly the left sub-schema. 
      * @param selectRSub Selects implicitly thr right sub-schema.
      * @param merge Appends implicitly the fields of the right sub-schema to the fields of the left sub-schema.
      * @param decomPath Decomposes implicitly the right path to isolate the last element (ignored) from the sub-path formed by the other ones (SRP).
      * @param decomSubPath Decomposes implicitly the sub-path to isolate the last element (PRK) from the sub-path formed by the other ones (ignored).
      * @param Updates implicitly the right schema by replacing the right sub-schema by the result of the merge.
      * @return A term of type MergeSchema.
      */
    implicit def merge_in_right[LS <: HList, RS <: HList, LP, RP, LSubS <: HList, RSubS <: HList, NewSubS <: HList, NewS <: HList](
        implicit
        selectLSub: SelectOrFollow.Aux[LS, LP, LSubS],
        selectRSub: SelectOrFollow.Aux[RS, RP, RSubS],
        merge: Prepend.Aux[RSubS, LSubS, NewSubS],
        updateRS: ReplaceOrUpdate.Aux[RS, RP, NewSubS, NewS]
    ): InRight[LS, RS, LP, RP, NewS] = new MergeSchema[LS, RS, LP, RP] {
        type Out = NewS 
        def apply(ldata: LS, rdata: RS): Out = updateRS(rdata, merge(selectRSub(rdata), selectLSub(ldata)))
        type Mode = MergeMode.InRight
    }

    /** Selects implicitly a nested schema (Out) in S by applying FollowPath if P is an empty path or SelectField otherwise.
    */
    private trait SelectOrFollow[S <: HList, P] { type Out <: HList ; def apply(d: S): Out }
    private object SelectOrFollow{
        type Aux[S <: HList, P, Out0 <: HList] = SelectOrFollow[S, P] { type Out = Out0 }

        implicit def path_is_empty[S <: HList, Out0 <: HList](
            implicit
            followPath: FollowPath.Aux[S, HNil, Out0]
        ): Aux[S, HNil, Out0] = new SelectOrFollow[S, HNil] {
            type Out = Out0
            def apply(d: S): Out = followPath(d)
        }

        implicit def path_is_not_empty[S <: HList, P, Out0 <: HList](
            implicit
            selectField: SelectField.Aux[S, P, _, Out0]
        ): Aux[S, P, Out0] = new SelectOrFollow[S, P] {
            type Out = Out0
            def apply(d: S): Out = selectField(d)
        }
    }

    private trait ReplaceOrUpdate[S <: HList, P, MS <: HList] { type Out <: HList ; def apply(d: S, md: MS): Out }
    private object ReplaceOrUpdate {
        type Aux[S <: HList, P, MS <: HList, Out0 <: HList] = ReplaceOrUpdate[S, P, MS] { type Out = Out0 }

        implicit def path_is_empty[S <: HList, MS <: HList, Out0 <: HList]: Aux[S, HNil, MS, MS] = new ReplaceOrUpdate[S, HNil, MS] {
            type Out = MS
            def apply(d: S, md: MS): Out = md
        }

        implicit def path_is_not_empty[S <: HList, MS <: HList, P, SP <: HList, FN <: Symbol, Out0 <: HList](
            implicit
            decompPath: DecompPath.Aux[P, SP, FN],
            updateS: ReplaceField.Aux[S, P, FN, MS, Out0]
        ): Aux[S, P, MS, Out0] = new ReplaceOrUpdate[S, P, MS] {
            type Out = Out0
            def apply(d: S, md: MS): Out = updateS(d, md)
        }
    }
}

trait MergeMode
object MergeMode {
    trait Default <: MergeMode
    trait InLeft <: MergeMode
    trait InRight <: MergeMode
}