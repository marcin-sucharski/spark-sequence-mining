package one.off_by.sequence.mining.gsp

import one.off_by.sequence.mining.gsp.PatternJoiner.{JoinItem, JoinItemExistingElement, JoinItemNewElement, PrefixResult, PrefixSuffixResult, SuffixResult}
import one.off_by.sequence.mining.gsp.PatternMatcher.SearchableSequence
import org.apache.spark.SparkConf

import scala.collection.mutable

object GSPHelper {
  implicit class SparkConfHelper(conf: SparkConf) {
    def registerGSPKryoClasses(): SparkConf =
      conf.registerKryoClasses(Array(
        classOf[Transaction[_, _, _]],
        classOf[one.off_by.sequence.mining.gsp.Transaction$mcII$sp],
        classOf[one.off_by.sequence.mining.gsp.Transaction$mcIJ$sp],
        classOf[one.off_by.sequence.mining.gsp.Transaction$mcJI$sp],
        classOf[one.off_by.sequence.mining.gsp.Transaction$mcJJ$sp],
        classOf[Array[Transaction[_, _, _]]],
        classOf[Array[one.off_by.sequence.mining.gsp.Transaction$mcII$sp]],
        classOf[Array[one.off_by.sequence.mining.gsp.Transaction$mcIJ$sp]],
        classOf[Array[one.off_by.sequence.mining.gsp.Transaction$mcJI$sp]],
        classOf[Array[one.off_by.sequence.mining.gsp.Transaction$mcJJ$sp]],
        classOf[Element[_]],
        classOf[Array[Element[_]]],
        classOf[Pattern[_]],
        classOf[Array[Pattern[_]]],
        classOf[JoinItem[_]],
        classOf[Array[JoinItem[_]]],
        classOf[JoinItemNewElement[_]],
        classOf[Array[JoinItemNewElement[_]]],
        classOf[JoinItemExistingElement[_]],
        classOf[Array[JoinItemExistingElement[_]]],
        classOf[PrefixSuffixResult[_]],
        classOf[Array[PrefixSuffixResult[_]]],
        classOf[PrefixResult[_]],
        classOf[Array[PrefixResult[_]]],
        classOf[SuffixResult[_]],
        classOf[Array[SuffixResult[_]]],
        classOf[SearchableSequence[_, _, _]],
        classOf[Array[SearchableSequence[_, _, _]]],
        classOf[HashTree[_, _, _, _]],
        classOf[Array[HashTree[_, _, _, _]]],
        classOf[HashTreeLeaf[_, _, _, _]],
        classOf[one.off_by.sequence.mining.gsp.HashTreeLeaf$mcII$sp],
        classOf[one.off_by.sequence.mining.gsp.HashTreeLeaf$mcIJ$sp],
        classOf[one.off_by.sequence.mining.gsp.HashTreeLeaf$mcJI$sp],
        classOf[one.off_by.sequence.mining.gsp.HashTreeLeaf$mcJJ$sp],
        classOf[Array[HashTreeLeaf[_, _, _, _]]],
        classOf[Array[one.off_by.sequence.mining.gsp.HashTreeLeaf$mcII$sp]],
        classOf[Array[one.off_by.sequence.mining.gsp.HashTreeLeaf$mcIJ$sp]],
        classOf[Array[one.off_by.sequence.mining.gsp.HashTreeLeaf$mcJI$sp]],
        classOf[Array[one.off_by.sequence.mining.gsp.HashTreeLeaf$mcJJ$sp]],
        classOf[HashTreeNode[_, _, _, _]],
        classOf[one.off_by.sequence.mining.gsp.HashTreeNode$mcII$sp],
        classOf[one.off_by.sequence.mining.gsp.HashTreeNode$mcIJ$sp],
        classOf[one.off_by.sequence.mining.gsp.HashTreeNode$mcJI$sp],
        classOf[one.off_by.sequence.mining.gsp.HashTreeNode$mcJJ$sp],
        classOf[Array[HashTreeNode[_, _, _, _]]],
        classOf[Array[one.off_by.sequence.mining.gsp.HashTreeNode$mcII$sp]],
        classOf[Array[one.off_by.sequence.mining.gsp.HashTreeNode$mcIJ$sp]],
        classOf[Array[one.off_by.sequence.mining.gsp.HashTreeNode$mcJI$sp]],
        classOf[Array[one.off_by.sequence.mining.gsp.HashTreeNode$mcJJ$sp]]
      ))

    def registerMissingSparkClasses(): SparkConf =
      conf.registerKryoClasses(Array(
        Class.forName("scala.reflect.ClassTag$$anon$1"),
        classOf[Class[_]],
        classOf[mutable.WrappedArray.ofRef[_]],
        Ordering.Int.getClass,
        Ordering.String.getClass
      ))
  }
}

