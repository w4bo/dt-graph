package it.unibo.graph.utils

const val SEED = 42

val LIMIT : Int = System.getenv("THREAD")?.toInt() ?: 1

const val HIGHPRIORITY = 0
const val LOWPRIORITY = 1
const val FROM_TIMESTAMP = "fromTimestamp"
const val TO_TIMESTAMP = "toTimestamp"
const val KEY = "key"
const val TYPE = "type"
const val ID = "id"
const val LABEL = "label"
const val FROM_N = "fromN"
const val TO_N = "toN"
const val COUNT = "count"
const val EDGES = "edges"
const val VALUE = "value"
const val GRAPH_SOURCE = 0L
const val LOCATION = "location"
const val DUMMY_ID = -1L
const val NODE = true
const val EDGE = false
const val EPSILON: Long = 1

/** Test workload constants */
const val N0 = 0L
const val N1 = 1L
const val N2 = 2L
const val N3 = 3L
const val N4 = 4L
const val N5 = 5L
const val N6 = 6L
const val N7 = 7L
const val N8 = 8L

/** Toy workload locations **/
const val ERRANO_LOCATION = "POLYGON ((11.798105 44.234354, 11.801217 44.237683, 11.805286 44.235809, 11.803987 44.234851, 11.804789 44.233683, 11.80268 44.231419, 11.798105 44.234354))"
const val T0_LOCATION = "POLYGON ((11.798775 44.235004, 11.799136 44.235376, 11.800661 44.234333, 11.800189 44.234023, 11.798775 44.235004))"
const val T1_LOCATION = "POLYGON ((11.79915 44.235384, 11.799412 44.23567, 11.801042 44.234555, 11.800681 44.234343, 11.79915 44.235384))"
const val T2_LOCATION = "POLYGON ((11.799569 44.235609, 11.79977 44.235759, 11.801395 44.234782, 11.80108 44.234572, 11.799569 44.235609))"
const val POINT_IN_T0 = "POINT (11.798998 44.235024)"
const val POINT_IN_T1 = "POINT (11.799328 44.235394)"
const val POINT_IN_T2 = "POINT (11.800711 44.234904)"
const val METEO_POINT = "POINT (11.80164 44.234831)"

/** AsterixDB Constants  **/
const val MAX_ASTERIX_DATE = 253402210800000
const val FIRSTFEEDPORT: Int = 10001
const val LASTFEEDPORT: Int = 11000
const val DATASET_PREFIX = "dataset_"
const val DATAFEED_PREFIX = "EventsFeed_"
