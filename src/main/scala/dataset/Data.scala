package pridwen.dataset

import pridwen.types.models._
import pridwen.types.opschema._
import pridwen.types.support.{DeepLabelledGeneric => LabelledGeneric}

import shapeless.{HList, HNil, ::, Witness}
import shapeless.labelled.{FieldType}

import org.apache.spark.sql.{Dataset, DataFrame, Encoder, Row}
import org.apache.spark.sql.functions.{col}

trait Data[M <: Model, S <: HList] {
    type DST
    val data: Dataset[DST]
    def toDF(): DataFrame = data.toDF
    def toDS[CS <: Product](implicit toHList: LabelledGeneric.Aux[CS,S], enc: Encoder[CS]): Dataset[CS] = data.as[CS]
    def toDS: Dataset[DST] = data

    // show
    def show(numRows: Int, truncate: Int, vertical: Boolean): Unit = data.show(numRows, truncate, vertical)
    def show(numRows: Int, truncate: Int): Unit = data.show(numRows, truncate)
    def show(numRows: Int, truncate: Boolean): Unit = data.show(numRows, truncate)
    def show(truncate: Boolean): Unit = data.show(truncate)
    def show: Unit = data.show()
    def show(numRows: Int): Unit = data.show(numRows)

    // describe
    private def inner_describe(name: String, showData: Boolean, printSchema: Schema.AsString[S], printModel: Model.AsString[M]): Unit = {
        println(s"\n============= ${name}\n")
        println(s"Model: ${printModel()}\n")
        println(s"Schema: \n${printSchema()}")
        if(showData) data.show
        println("=======================================")
    }
    def describe(implicit printSchema: Schema.AsString[S], printModel: Model.AsString[M]) = inner_describe("Dataset", false, printSchema, printModel)
    def describe(name: String)(implicit printSchema: Schema.AsString[S], printModel: Model.AsString[M]) = inner_describe(name, false, printSchema, printModel)
    def describe(showData: Boolean)(implicit printSchema: Schema.AsString[S], printModel: Model.AsString[M]) = inner_describe("Dataset", showData, printSchema, printModel)
    def describe(name: String, showData: Boolean)(implicit printSchema: Schema.AsString[S], printModel: Model.AsString[M]) = inner_describe(name, showData, printSchema, printModel)

    // count
    def count: Long = data.count
}

object implicits {
    implicit class DatasetExtension[S, HS <: HList](ds: Dataset[S])(implicit toHList: LabelledGeneric.Aux[S, HS]) {
        def asModel[M <: Model](implicit isValid: Model.As[HS, M]) = new Data[M, HS] { type DST = S ; val data = ds }
    }

    implicit def witnessToPath[FN <: Symbol](f: Witness.Aux[FN])(implicit p: Path[Witness.Aux[FN] :: HNil]): Path.Aux[Witness.Aux[FN] :: HNil, p.T] = p
}

object Data {
    def apply[S, HS <: HList](ds: Dataset[S])(implicit toHList: LabelledGeneric.Aux[S, HS]) = new {
        def as[M <: Model](implicit isValid: Model.As[HS, M]) = new Data[M, HS] { type DST = S ; val data = ds }
    }

    def apply[M <: Model] = new {
        def apply[S, HS <: HList](ds: Dataset[S])(implicit toHList: LabelledGeneric.Aux[S, HS], isValid: Model.As[HS, M]) 
            = new Data[M, HS] { type DST = S ; val data = ds }
    }

    implicit def dataOps[M <: Model, S <: HList](d: Data[M,S]): DataOps[M,S] = new DataOps[M,S](d)
}

final class DataOps[M <: Model, S <: HList](d: Data[M, S]) {
    // ============ Change model

    def as[NM <: Model](implicit isValid: Model.As[S, NM]): Data[NM, S] = new Data[NM, S] {
        type DST = d.DST
        val data = d.data
    }

    // ============ Project attribute(s)

    // Project one attribute (Path)
    def select[PW <: HList, PS <: HList, FN, FT](path: Path.Aux[PW, PS])(implicit s: SelectField.Aux[S, PS, FN, FT]): Data[M, FieldType[FN, FT] :: HNil] = new Data[M, FieldType[FN, FT] :: HNil] {
        type DST = Row
        val data = d.data.select(d.data.col(path.asString))
    }

    // Project multiple attributes (HList of Witness/Path)
    def select[MPW <: HList, MPS <: HList, NS <: HList](paths: MPW)(implicit pathsOfSymb: MultiplePaths.Aux[MPW, MPS], s: SelectMany.Aux[S, MPS, NS]): Data[M, NS] = new Data[M, NS] {
        type DST = Row
        val data = d.data.select(pathsOfSymb.asStrings.head, pathsOfSymb.asStrings.tail: _*)
    }

    // Project multiple attributes (MultiplePaths)
    def select[MPW <: HList, MPS <: HList, NS <: HList](paths: MultiplePaths.Aux[MPW, MPS])(implicit s: SelectMany.Aux[S, MPS, NS]): Data[M, NS] = new Data[M, NS] {
        type DST = Row
        val data = d.data.select(paths.asStrings.head, paths.asStrings.tail: _*)
    }

    // ============ Select rows

    def filter[O, I](f: FilterOps[O,I])(implicit filter: FilterOps.Compute[S, O, I]): Data[M, S] = new Data[M, S] {
        type DST = d.DST
        val data = d.data.filter(filter.toSparkColumn)
    }

    // add column
    // drop column
    // rename column
    // update column
    // inner join
    // left join
    // right join
    // semi join (left)
    // semi join (right)
    // cartesian product
    // group by
    // aggregate
    // order by
}