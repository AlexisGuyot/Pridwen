package pridwen.dataset

import pridwen.types.models._
import pridwen.types.opschema._
import pridwen.types.support.{DeepLabelledGeneric => LabelledGeneric, DecompPath}

import shapeless.{HList, HNil, ::, Witness}
import shapeless.labelled.{FieldType}

import org.apache.spark.sql.{Dataset, DataFrame, Encoder, Row}
import org.apache.spark.sql.functions.{col, lit, struct, typedLit}

import scala.reflect.runtime.universe.TypeTag


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

    // Project one attribute (Path with alias)
    def select[PW <: HList, NN <: Symbol, PS <: HList, FT](path: Path.As.Aux[PW, NN, PS])(implicit s: SelectField.As.Aux[S, PS, NN, FT], alias: Witness.Aux[NN]): Data[M, FieldType[NN, FT] :: HNil] = new Data[M, FieldType[NN, FT] :: HNil] {
        type DST = Row
        val data = d.data.select(d.data.col(path.asString).as(alias.value.name))
    }

    // Project multiple attributes (MultiplePaths)
    def select[MPW <: HList, MPS <: HList, NS <: HList](paths: MultiplePaths.Aux[MPW, MPS])(implicit s: SelectMany.Aux[S, MPS, NS]): Data[M, NS] = new Data[M, NS] {
        type DST = Row
        val data = d.data.select(paths.toColumns: _*)
    }

    // ============ Select rows

    def filter[O, I](f: FilterOps[O,I])(implicit filter: FilterOps.Compute[S, O, I]): Data[M, S] = new Data[M, S] {
        type DST = d.DST
        val data = d.data.filter(filter.toSparkColumn)
    }

    // ============ Add attribute

    // New value from default value
    def add[PW <: HList, PS <: HList, P <: HList, FN <: Symbol, FT: TypeTag, NS <: HList](
        field: Path.Aux[PW, PS], default: FT
    )(
        implicit
        p: DecompPath.Aux[PS, P, FN],
        a: AddField.Aux[S, P, FN, FT, NS]
    ) = new {
        private def common[A <: Model, B <: HList]: Data[A,B] = new Data[A,B]{
            type DST = Row
            val data = {
                val path = field.asString.split("\\.")
                if(path.size >= 2) {
                    val (subpath, fname) = (path.slice(0,path.size-1).mkString("."), path.last)
                    d.data.withColumn(subpath, struct(col(s"$subpath.*"), typedLit(default).as(fname)))
                } else {
                    d.data.withColumn(field.asString, typedLit(default))
                }
            }
        }

        // Without model change
        def keepModel(implicit isValid: Model.As[NS, M]): Data[M, NS] = common[M, NS]

        // With model change
        def changeModel[NM <: Model](implicit isValid: Model.As[NS, NM]): Data[NM, NS] = common[NM, NS]
    }

    // New value from another column
    /* def add[FN <: Symbol, PW <: HList, PS <: HList, CN <: Symbol, CT, FT, NS <: HList](
        fname: Witness.Aux[FN], basedOn: Path.Aux[PW,PS], values: CT => FT
    )(
        implicit
        s: SelectField.Aux[S, PS, CN, CT]
    ) = ??? */

    // New value by mapping all the columns
    //def add[FN <: Symbol, SS, FT, NS <: HList](fname: Witness.Aux[FN], values: SS => FT) = ???

    // New value by mapping some columns
    /* def add[FN <: Symbol, SS, MPW <: HList, MPS <: HList, FT, NS <: HList](
        fname: Witness.Aux[FN], basedOn: MultiplePaths.Aux[MPW,MPS], values: SS => FT
    ) = ??? */

    // ============ Drop attribute

    def drop[FN <: Symbol, NS <: HList](field: Witness.Aux[FN])(
        implicit
        r: RemoveField.Aux[S, FN :: HNil, NS]
    ): Data[M, NS] = new Data[M, NS] {
        type DST = Row
        val data = d.data.drop(field.value.name)
    }

    // With nesting, not really supported by the dataframe (as is).
    /* def drop[PW <: HList, PS <: HList, NS <: HList](field: Path.Aux[PW, PS])(
        implicit
        r: RemoveField.Aux[S, PS, NS]
    ): Data[M, NS] = new Data[M, NS] {
        type DST = Row
        val data = d.data.drop(field.asString)
    } */

    // ============ Rename attribute

    def withColumnRenamed[FN <: Symbol, FT, NN <: Symbol, NS <: HList](
        old_name: Witness.Aux[FN], new_name: Witness.Aux[NN]
    )(
        implicit
        s: SelectField.Aux[S, FN :: HNil, FN, FT],
        r: ReplaceField.Aux[S, FN :: HNil, NN, FT, NS]
    ): Data[M, NS] = new Data[M, NS] {
        type DST = Row
        val data = d.data.withColumnRenamed(old_name.value.name, new_name.value.name)
    }

    // With nesting, not really supported by the dataframe (as is).
    /* def withColumnRenamed[PW1 <: HList, PS1 <: HList, FN <: Symbol, FT, NN <: Symbol, NS <: HList](
        old_name: Path.Aux[PW1, PS1], new_name: Witness.Aux[NN]
    )(
        implicit
        s: SelectField.Aux[S, PS1, FN, FT],
        r: ReplaceField.Aux[S, PS1, NN, FT, NS]
    ): Data[M, NS] = new Data[M, NS] {
        type DST = Row
        val data = d.data.withColumnRenamed(old_name.asString, new_name.value.name)
    } */

    // ============ Inner join between datasets

    /* def join[S2 <: HList, O, I, NS <: HList](d2: Data[M, S2], cond: FilterOps[O,I])(
        implicit
        m: MergeSchema.Aux[S, S2, NS]
    ) */

    // left join
    // right join
    // semi join (left)
    // semi join (right)
    // cartesian product
    // group by
    // aggregate
    // order by
    // update column
}