package pridwen.dataset

import org.apache.spark.sql.types._

import java.math.BigDecimal
import java.time.{Instant, LocalDateTime, LocalDate, Period, Duration}
import java.sql.{Timestamp, Date}
import scala.collection.{Seq, Map}
import org.apache.spark.sql.Row

import shapeless.{HList, ::, HNil, Witness}
import shapeless.labelled.{FieldType}

import pridwen.types.support.{DeepLabelledGeneric => LabelledGeneric}

trait ToSparkType[T] { def get: DataType }
object ToSparkType {
    // Atomic
    implicit def to_byte_type: ToSparkType[Byte] = new ToSparkType[Byte] { def get = ByteType }
    implicit def to_short_type: ToSparkType[Short] = new ToSparkType[Short] { def get = ShortType }
    implicit def to_int_type: ToSparkType[Int] = new ToSparkType[Int] { def get = IntegerType }
    implicit def to_long_type: ToSparkType[Long] = new ToSparkType[Long] { def get = LongType }
    implicit def to_float_type: ToSparkType[Float] = new ToSparkType[Float] { def get = FloatType }
    implicit def to_double_type: ToSparkType[Double] = new ToSparkType[Double] { def get = DoubleType }
    //implicit def to_bigdecimal_type: ToSparkType[BigDecimal] = new ToSparkType[BigDecimal] { def get = DecimalType }
    implicit def to_string_type: ToSparkType[String] = new ToSparkType[String] { def get = StringType }
    implicit def to_array_of_bytes_type: ToSparkType[Array[Byte]] = new ToSparkType[Array[Byte]] { def get = BinaryType }
    implicit def to_boolean_type: ToSparkType[Boolean] = new ToSparkType[Boolean] { def get = BooleanType }
    implicit def to_timestamp_type1: ToSparkType[Instant] = new ToSparkType[Instant] { def get = TimestampType }
    implicit def to_timestamp_type2: ToSparkType[Timestamp] = new ToSparkType[Timestamp] { def get = TimestampType }
    implicit def to_timestamp_type3: ToSparkType[LocalDateTime] = new ToSparkType[LocalDateTime] { def get = TimestampNTZType }
    implicit def to_date_type1: ToSparkType[LocalDate] = new ToSparkType[LocalDate] { def get = DateType }
    implicit def to_date_type2: ToSparkType[Date] = new ToSparkType[Date] { def get = DateType }
    //implicit def to_yearmonthinterval_type: ToSparkType[Period] = new ToSparkType[Period] { def get = YearMonthIntervalType }
    //implicit def to_daytimeinterval_type: ToSparkType[Duration] = new ToSparkType[Duration] { def get = DayTimeIntervalType }
    
    // Multivalued
    implicit def to_array_type[T](
        implicit
        toSpark: ToSparkType[T]
    ): ToSparkType[Seq[T]] = new ToSparkType[Seq[T]] { def get = ArrayType(toSpark.get) }

    implicit def to_map_type[K, V](
        implicit
        key_toSpark: ToSparkType[K],
        val_toSpark: ToSparkType[V]
    ): ToSparkType[Map[K,V]] = new ToSparkType[Map[K,V]] { def get = MapType(key_toSpark.get, val_toSpark.get) }

    // Nested
    implicit def one_attribute[FN <: Symbol, FT](
        implicit
        fname: Witness.Aux[FN],
        toSpark: ToSparkType[FT]
    ): ToSparkType[FieldType[FN, FT]::HNil] = new ToSparkType[FieldType[FN, FT]::HNil] {
        def get = StructType(Array(StructField(fname.value.name, toSpark.get)))
    }

    implicit def multiple_attributes[FN <: Symbol, FT, SS <: HList](
        implicit
        fname: Witness.Aux[FN],
        first_toSpark: ToSparkType[FT],
        other_toSpark: ToSparkType[SS]
    ): ToSparkType[FieldType[FN, FT]::SS] = new ToSparkType[FieldType[FN, FT]::SS] {
        def get = StructType(StructField(fname.value.name, first_toSpark.get) +: other_toSpark.get.asInstanceOf[StructType].fields)
    }

    implicit def from_product[P <: Product, L <: HList](
        implicit
        toHList: LabelledGeneric.Aux[P, L],
        toSpark: ToSparkType[L]
    ): ToSparkType[P] = new ToSparkType[P] {
        def get = toSpark.get
    }
}