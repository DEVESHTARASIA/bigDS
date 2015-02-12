
/**
 * Created by clin3 on 2015/2/2.
 */
class RuleModel(
    val lhs: String,
    val rhs: String,
    var support: Long = 0,
    var confidence: Double = 0,
    var lift:Double = 0) {

}
