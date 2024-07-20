package pridwen.dataset

import shapeless.{HList, ::, HNil, Witness}
import shapeless.labelled.{FieldType}

trait SelectAll[S <: HList] { type Out <: HList }
object SelectAll {
    type Aux[S <: HList, Out0 <: HList] = SelectAll[S] { type Out = Out0 }

    implicit def one_attribute[FN <: Symbol, FT]: Aux[FieldType[FN,FT]::HNil, Witness.Aux[FN]::HNil] 
        = new SelectAll[FieldType[FN,FT]::HNil] { type Out = Witness.Aux[FN]::HNil }

    implicit def one_attribute_nested[FN <: HList, FT, Out0 <: HList](
        implicit
        nested_select: SelectAll.Aux[FN, Out0]
    ): Aux[FieldType[FN,FT]::HNil, Out0::HNil] = new SelectAll[FieldType[FN,FT]::HNil] { type Out = Out0::HNil }
    
    implicit def multiple_attributes_unnested[FN <: Symbol, FT, SS <: HList, Out0 <: HList](
        implicit
        select_others: SelectAll.Aux[SS, Out0]
    ): Aux[FieldType[FN,FT]::SS, Witness.Aux[FN]::Out0] 
        = new SelectAll[FieldType[FN,FT]::SS] { type Out = Witness.Aux[FN]::Out0 }

    implicit def multiple_attributes_nested[FN <: HList, FT, SS <: HList, Out0 <: HList, Out1 <: HList](
        implicit
        nested_select: SelectAll.Aux[FN, Out0],
        select_others: SelectAll.Aux[SS, Out1]
    ): Aux[FieldType[FN,FT]::SS, Out0::Out1] 
        = new SelectAll[FieldType[FN,FT]::SS] { type Out = Out0::Out1 }

    /* implicit def one_attribute[FN <: Symbol, FT]: Aux[FieldType[FN,FT]::HNil, (FN::HNil)::HNil] 
        = new SelectAll[FieldType[FN,FT]::HNil] { type Out = (FN::HNil)::HNil }

    implicit def one_attribute_nested[FN <: HList, FT, Out0 <: HList](
        implicit
        nested_select: SelectAll.Aux[FN, Out0]
    ): Aux[FieldType[FN,FT]::HNil, Out0::HNil] = new SelectAll[FieldType[FN,FT]::HNil] { type Out = Out0::HNil }
    
    implicit def multiple_attributes_unnested[FN <: Symbol, FT, SS <: HList, Out0 <: HList](
        implicit
        select_others: SelectAll.Aux[SS, Out0]
    ): Aux[FieldType[FN,FT]::SS, (FN::HNil)::Out0] 
        = new SelectAll[FieldType[FN,FT]::SS] { type Out = (FN::HNil)::Out0 }

    implicit def multiple_attributes_nested[FN <: HList, FT, SS <: HList, Out0 <: HList, Out1 <: HList](
        implicit
        nested_select: SelectAll.Aux[FN, Out0],
        select_others: SelectAll.Aux[SS, Out1]
    ): Aux[FieldType[FN,FT]::SS, Out0::Out1] 
        = new SelectAll[FieldType[FN,FT]::SS] { type Out = Out0::Out1 } */
}