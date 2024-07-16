file:///C:/Users/Alexis.DESKTOP-K9CA12K/Documents/Recherche/Code/pridwen/src/main/scala/types/opschema/MergeSchema.scala
### java.lang.AssertionError: NoDenotation.owner

occurred in the presentation compiler.

presentation compiler configuration:
Scala version: 3.3.3
Classpath:
<HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala3-library_3\3.3.3\scala3-library_3-3.3.3.jar [exists ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-library\2.13.12\scala-library-2.13.12.jar [exists ]
Options:



action parameters:
offset: 4363
uri: file:///C:/Users/Alexis.DESKTOP-K9CA12K/Documents/Recherche/Code/pridwen/src/main/scala/types/opschema/MergeSchema.scala
text:
```scala
package pridwen.types.opschema

import shapeless.{HList}
import shapeless.ops.hlist.{Prepend, Init}

import pridwen.types.support.AsHList

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
*/
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
      * @param followLPath 
      * @param followRPath
      * @param merge
      * @return
      */
    implicit def default_merge[LS <: HList, RS <: HList, LP, RP, LSubS <: HList, RSubS <: HList, NewS <: HList](
        implicit
        selectLSub: SelectField.Aux[LS, LP, _, LSubS],
        selectRSub: SelectField.Aux[RS, RP, _, RSubS],
        merge: Prepend.Aux[LSubS, RSubS, NewS]
    ): Aux[LS, RS, LP, RP, NewS] = new MergeSchema[LS, RS, LP, RP] {
        type Out = NewS 
        def apply(ldata: LS, rdata: RS): Out = merge(selectLSub(ldata), selectRSub(rdata))
        type Mode = MergeMode.Default
    }

    implicit def merge_in_left[LS <: HList, RS <: HList, LP, SLP <: HList, HLP <: HList, RLP <: HList, RP, PLK, LSubS <: HList, RSubS <: HList, NewSubS <: HList, NewS <: HList](
        implicit
        selectLSub: SelectField.Aux[LS, LP, _, LSubS],
        selectRSub: SelectField.Aux[RS, RP, _, RSubS],
        merge: Prepend.Aux[LSubS, RSubS, NewSubS],
        decompPath: DecompPath.Aux[LP, SLP, _],
        decompSubPath: DecompPath.Aux[SLP, PLK, @@]
        updateLS: ReplaceField.Aux[LS, SLP, ]
    ): InLeft[LS, RS, LP, RP, NewS] = new MergeSchema[LS, RS, LP, RP] {
        type Out = NewS 
        def apply(ldata: LS, rdata: RS): Out = updateLS(ldata, merge(followLPath(ldata), followRPath(rdata)))
        type Mode = MergeMode.InLeft
    }

    implicit def merge_in_right[LS <: HList, RS <: HList, LP, HRP <: HList, RRP <: HList, RP, NLK, TLK, PRK, NRK, TRK, LSubS <: HList, RSubS <: HList, NewSubS <: HList, NewS <: HList](
        implicit
        followLPath: FollowPath.Aux[LS, LP, NLK, TLK, LSubS],
        followRPath: FollowPath.Aux[RS, RP, NRK, TRK, RSubS],
        merge: Prepend.Aux[LSubS, RSubS, NewSubS],
        asHList: AsHList.Aux[RP, HRP],
        reduceRPath: Init.Aux[HRP, RRP],
        getParent: FollowPath.Aux[RS, RRP, PRK, _, _],
        updateRS: ReplaceField.Aux[RS, RRP, PRK, NewSubS, NewS]
    ): InRight[LS, RS, LP, RP, NewS] = new MergeSchema[LS, RS, LP, RP] {
        type Out = NewS 
        def apply(ldata: LS, rdata: RS): Out = updateRS(rdata, merge(followLPath(ldata), followRPath(rdata)))
        type Mode = MergeMode.InRight
    }
}

trait MergeMode
object MergeMode {
    trait Default <: MergeMode
    trait InLeft <: MergeMode
    trait InRight <: MergeMode
}
```



#### Error stacktrace:

```
dotty.tools.dotc.core.SymDenotations$NoDenotation$.owner(SymDenotations.scala:2607)
	scala.meta.internal.pc.SignatureHelpProvider$.isValid(SignatureHelpProvider.scala:83)
	scala.meta.internal.pc.SignatureHelpProvider$.notCurrentApply(SignatureHelpProvider.scala:94)
	scala.meta.internal.pc.SignatureHelpProvider$.$anonfun$1(SignatureHelpProvider.scala:48)
	scala.collection.StrictOptimizedLinearSeqOps.dropWhile(LinearSeq.scala:280)
	scala.collection.StrictOptimizedLinearSeqOps.dropWhile$(LinearSeq.scala:278)
	scala.collection.immutable.List.dropWhile(List.scala:79)
	scala.meta.internal.pc.SignatureHelpProvider$.signatureHelp(SignatureHelpProvider.scala:48)
	scala.meta.internal.pc.ScalaPresentationCompiler.signatureHelp$$anonfun$1(ScalaPresentationCompiler.scala:412)
```
#### Short summary: 

java.lang.AssertionError: NoDenotation.owner