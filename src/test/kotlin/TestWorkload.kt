import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraph
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.Labels.*
import it.unibo.graph.interfaces.PropType
import it.unibo.graph.query.*
import it.unibo.graph.structure.CustomGraph
import org.junit.jupiter.api.Assertions.assertEquals
import kotlin.test.Test


const val T0_LOCATION = "{\"coordinates\":[[[11.798775,44.235004],[11.799136,44.235376],[11.800661,44.234333],[11.800189,44.234023],[11.798775,44.235004]]],\"type\":\"Polygon\"}"
const val POINT_IN_T0 = "{\"coordinates\":[11.798998,44.235024],\"type\":\"Point\"}"

/**
 * Problemi:
 *  1. Non abbiamo possibilità di fare dei BIN temporali, e.g., SELECT timestamp, AVG(value) GROUP BY HOUR(timestamp)
 *  2. Nella search sulle TS, il toTimestamp è incluso.
 *  3. Senza Gremlin, non ho accesso alla TS come lista di elementi
 *  4. Non abbiamo percorsi di dimensione variabile e.g., [*2...5]
 *  5. Questo funziona
 *     val nowSpatialPattern = listOf(
 *            listOf(Step(AgriParcel, alias = "parcel")),
 *            listOf(Step(Device, listOf(Triple("name",Operators.EQ, it)), alias = "nowDevice")),
 *        )
 *     Questo no, perchè?
 *     val nowSpatialPattern = listOf(
 *            listOf(Step(Device, listOf(Triple("name",Operators.EQ, it)), alias = "nowDevice")),
 *            listOf(Step(AgriParcel, alias = "parcel")),
 *        )
 *    query(g, nowSpatialPattern, where = listOf(Compare("parcel", "nowDevice", "location", Operators.ST_CONTAINS)), by = listOf(Aggregate("nowDevice", "name"), Aggregate("parcel","name")), from = 4, to = Long.MAX_VALUE)
 *    6. Possiamo specificare due validità diverse temporali nella stessa query? Don't think so, e.g., di tutti i device attivi 10 ore fa, dammi a cosa sono collegati ora.
 */
class TestWorkload{

    val staticDevicePattern = listOf(
        Step(AgriFarm, alias="farm"),
        Step(HasParcel),
        Step(AgriParcel, alias="parcel"),
        Step(HasDevice),
        Step(Device, alias="device"),
    )

    val dynamicDevicePattern = listOf(
        Step(AgriFarm, alias="farm"),
        Step(HasParcel),
        Step(AgriParcel, alias="parcel"),
        Step(HasDevice),
        Step(Device, alias="device"),
    )
    fun setup(dynamicDevices: Boolean = false): CustomGraph {
        return setup(MemoryGraph(), dynamicDevices)
    }

    fun setup(g: Graph, dynamicDevices: Boolean): CustomGraph {

        // Define a relative timestamp for debug purposes
        val maxTimestamp = 5L

        val g = CustomGraph(g)
        g.tsm = AsterixDBTSM.createDefault(g)
        g.clear()
        g.getTSM().clear()

        // Errano farm
        val errano = g.addNode(AgriFarm)
        g.addProperty(errano.id, "name", "Errano Kiwi Farm",PropType.STRING)
        g.addProperty(errano.id, "location", """{"coordinates":[[[11.798105,44.234354],[11.801217,44.237683],[11.805286,44.235809],[11.803987,44.234851],[11.804789,44.233683],[11.80268,44.231419],[11.798105,44.234354]]],"type":"Polygon"}""", PropType.GEOMETRY)

        // Errano parcels
        val erranoT1 = g.addNode(AgriParcel)
        g.addProperty(erranoT1.id, "name", "Errano T1" ,PropType.STRING)
        g.addProperty(erranoT1.id, "location", """{"coordinates":[[[11.79915,44.235384],[11.799412,44.23567],[11.801042,44.234555],[11.800681,44.234343],[11.79915,44.235384]]],"type":"Polygon"}""" ,PropType.GEOMETRY)
        val erranoT2 = g.addNode(AgriParcel)
        g.addProperty(erranoT2.id, "location", """{"coordinates":[[[11.799569,44.235609],[11.79977,44.235759],[11.801395,44.234782],[11.80108,44.234572],[11.799569,44.235609]]],"type":"Polygon"}""" ,PropType.GEOMETRY)
        g.addProperty(erranoT2.id, "name", "Errano T2" ,PropType.STRING)


        g.addEdge(HasParcel, errano.id, erranoT1.id)
        g.addEdge(HasParcel, errano.id, erranoT2.id)

        // Static devices
        val t1Moisture = g.addNode(Device)
        g.addProperty(t1Moisture.id, "location", """{"coordinates":[11.799328,44.235394],"type":"Point"}""" ,PropType.GEOMETRY)
        g.addProperty(t1Moisture.id, "name", "Errano T1 MoistureDevice" ,PropType.STRING)
        val t2Moisture = g.addNode(Device)
        g.addProperty(t2Moisture.id, "location", """{"coordinates":[11.800711,44.234904],"type":"Point"}""" ,PropType.GEOMETRY)
        g.addProperty(t2Moisture.id, "name", "Errano T2 MoistureDevice" ,PropType.STRING)

        if(dynamicDevices){
            var measurementTimestamp = 0L

            // Moving device
            val erranoDrone = g.addNode(Device)
            g.addProperty(erranoDrone.id, "name", "Errano Drone" ,PropType.STRING)
            g.addProperty(erranoDrone.id, "location", """{"coordinates":[11.799328,44.235394],"type":"Point"}""", PropType.GEOMETRY, from = 0, to = 2)

            val droneTs = g.getTSM().addTS()
            val droneNDVI = g.addNode(NDVI, value = droneTs.getTSId())

            g.addEdge(HasDevice, erranoT1.id, erranoDrone.id, from = 0, to = 2)
            g.addEdge(HasNDVI, erranoDrone.id, droneNDVI.id)

            droneTs.add(Measurement, timestamp = measurementTimestamp++, value = measurementTimestamp, location = """{"coordinates":[11.799328,44.235394],"type":"Point"}""")
            droneTs.add(Measurement, timestamp = measurementTimestamp++, value = measurementTimestamp, location = """{"coordinates":[11.799328,44.235394],"type":"Point"}""")

            //Moving device from T1 to T2
            g.addEdge(HasDevice, erranoT2.id, erranoDrone.id, from = 2, to = 5)
            g.addProperty(erranoDrone.id, "location", """{"coordinates":[11.800711,44.234904],"type":"Point"}""", PropType.GEOMETRY, from = 2, to = 5)

            droneTs.add(Measurement, timestamp = measurementTimestamp++, value = measurementTimestamp, location = """{"coordinates":[11.800711,44.234904],"type":"Point"}""")
            droneTs.add(Measurement, timestamp = measurementTimestamp++, value = measurementTimestamp, location = """{"coordinates":[11.800711,44.234904],"type":"Point"}""")

        }

        // Weather station, not linked to anything
        val weatherStation = g.addNode(Device)
        g.addProperty(weatherStation.id, "name", """Errano Weather Station""" ,PropType.GEOMETRY)
        g.addProperty(weatherStation.id, "location", """{"coordinates":[11.80164,44.234831],"type":"Point"}""" ,PropType.GEOMETRY)

        // g.addEdge(hasDevice, errano.id, weatherStation.id)
        g.addEdge(HasDevice, erranoT1.id, t1Moisture.id)
        g.addEdge(HasDevice, erranoT2.id, t2Moisture.id)

        // Time series
        val t1TS = g.getTSM().addTS()
        val t2TS = g.getTSM().addTS()
        val weatherTS = g.getTSM().addTS()

        for (timestamp in 0L..5){
            t1TS.add(Measurement, timestamp = timestamp, value = timestamp, location = """{"coordinates":[11.799328,44.235394],"type":"Point"}""")
            t2TS.add(Measurement, timestamp = timestamp, value = timestamp, location = """{"coordinates":[11.800711,44.234904],"type":"Point"}""")
            weatherTS.add(Measurement, timestamp = timestamp, value = timestamp, location = """{"coordinates":[11.80164,44.234831],"type":"Point"}""")
        }

        val weatherTemperature = g.addNode(Humidity, value = weatherTS.getTSId())
        val t1Humidity = g.addNode(Humidity, value = t1TS.getTSId())
        val t2Humidity = g.addNode(Humidity, value = t2TS.getTSId())


        g.addEdge(HasHumidity, t1Moisture.id, t1Humidity.id)
        g.addEdge(HasHumidity, t2Moisture.id, t2Humidity.id)
        g.addEdge(HasHumidity, weatherStation.id, weatherTemperature.id)

        return g
    }

    @Test
    fun testSum() {
        assertEquals(42, 40 + 2)
    }

    @Test
    fun testParcelInFarm() {
        val g = setup()
        // Parcels in farm
        kotlin.test.assertEquals(2, search(g, staticDevicePattern, listOf(Compare("farm", "parcel", "location", Operators.ST_CONTAINS)), timeaware = false).size)
        // Devices in Farm
        kotlin.test.assertEquals(2, search(g, staticDevicePattern, listOf(Compare("parcel", "device", "location", Operators.ST_CONTAINS)), timeaware = false).size)
    }

    @Test
    fun devicesInFarms() {
        val g = setup()
        val pattern = listOf(
                listOf(Step(AgriFarm, alias="farm")),
                listOf(Step(Device, alias="device"))
            )

        // Devices in farm throuh spatial join
        kotlin.test.assertEquals(3, query(g, pattern, listOf(Compare("farm","device", "location", Operators.ST_CONTAINS)), timeaware = false).size)
    }

    // E* -> A*
    /*
     * EnvironmentCoverage(L, 𝜏), where L is a list of loca-
     * tions, and 𝜏 is a measurement type: lists all agents that
     * can generate measurements of a given type 𝜏 that can
     * cover the environments/locations specified in L.
     */

    // v.1 Following graph edges
    /**
     * MATCH (:AgriFarm ) -> [:hasAgriParcel ] -> (:Parcel ) -> [:hasDevice ] -> (:Device) -> [:hasHumidity] -> (:Humidity) -> [:hasTS]
     */
    // v.2 Following spatial contains
    /**
     * MATCH (env: AgriFarm), (dev:Device)->[:hasHumidity]->(:Humidity)-[:HasTS]
     * WHERE ST_CONTAINS (env.location, dev.location)
     * RETURN env, dev
     */
    @Test
    fun environmentCoverage() {
        val g = setup()
        val tau = Humidity
        val searchLocation = """{"coordinates":[[[11.798105,44.234354],[11.801217,44.237683],[11.805286,44.235809],[11.803987,44.234851],[11.804789,44.233683],[11.80268,44.231419],[11.798105,44.234354]]],"type":"Polygon"}"""

        val pattern = staticDevicePattern + listOf(
            Step(HasHumidity),
            Step(tau),
            Step(HasTS)
        )

        val targetLocation = g.addNode(TargetLocation)
        g.addProperty(targetLocation.id, "location", searchLocation, PropType.GEOMETRY)

        val spatialPattern = listOf(
            listOf(Step(TargetLocation, alias="targetLocation")),
            listOf(Step(Device, alias="device"),
                Step(HasHumidity),
                Step(tau),
                Step(HasTS)
            )
        )

        // v.1
        kotlin.test.assertEquals(2, search(g, pattern, timeaware = false).size)

        // v.2
        kotlin.test.assertEquals(3, query(g, spatialPattern, listOf(Compare("targetLocation","device", "location", Operators.ST_CONTAINS)), timeaware = false).size)
    }

    // E* ->  A* -> M
    /*
     *  EnvironmentAggregate(𝜖, 𝜏, 𝑡𝑎 , 𝑡𝑏 ): where 𝜖 is an
     * environment. List, for each agent currently in the envi-
     * ronment 𝜖, the average hourly value of type 𝜏 during
     * the period [𝑡𝑎, 𝑡𝑏 [.
     */
    @Test
    fun environmentAggregate(){
        val g = setup(dynamicDevices=true)
        val tau = NDVI

        // TODO

        // v.1 Following graph edges
        val pattern = dynamicDevicePattern + listOf(
            null,
            Step(tau),
            Step(HasTS),
        )

        val result = search(g, pattern, timeaware = true)
        kotlin.test.assertEquals(2, result.size)
    }

    // A -> E -> M
    /*
     *     AgentHistory(𝐴): where A is a set of agents. List, for
     *     each 𝛼 ∈ 𝐴, the average value for measurements for
     *     each environment in the past 24 hours.
    */
    /**
     * // v.1 Graph traversal
     *
     * MATCH (env:AgriParcel) - [] -> (dev:Device) - [] - () - [:hasTS] - (meas:Measurement)
     * WHERE dev.name in (A)
     * VALID FROM 0 TO 5
     * RETURN dev, env, AVG(meas)
     *
     * // v.2 Spatial Contains
     *
     * MATCH (env:AgriParcel),
     * (dev:Device) - [] - () - [:hasTS] - (meas:Measurement)
     * WHERE dev.name in (A)
     * AND ST_CONTAINS(env.location, meas.location)
     * VALID FROM 0 TO 5
     * RETURN env, dev, AVG(meas)
     *
     */
    @Test
    fun agentHistory(){
        val g = setup(dynamicDevices = true)
        val agents = listOf(Triple(2.0,"Errano T1 MoistureDevice", "Errano T1"), Triple(2.0,"Errano T2 MoistureDevice", "Errano T2"), Triple(1.5,"Errano Drone", "Errano T1"), Triple(3.5, "Errano Drone", "Errano T2"))

        val pattern = listOf(Step(AgriParcel, alias = "env"), null, Step(Device, alias = "dev"), null, null, Step(HasTS), Step(Measurement, alias = "meas"))

        val spatialPattern = listOf(
            listOf(Step(AgriParcel, alias = "env")),
            listOf(
                Step(Device,alias="dev"),
                null,
                null,
                Step(HasTS),
                Step(Measurement, alias = "meas")
            )
        )

        val query = query(g, pattern, by = listOf(Aggregate("dev","name"), Aggregate("env","name"), Aggregate("meas", "value", AggOperator.AVG)), from = 0, to = 5)//.chunked(3).map{Triple(it[0],it[1],it[2])}

        val spatialQuery = query(g, spatialPattern,
            where = listOf(Compare("env","meas","location",Operators.ST_CONTAINS)),
            by = listOf(Aggregate("dev","name"), Aggregate("env","name"), Aggregate("meas", "value", AggOperator.AVG)),
            from = 0, to = 5)

        agents.forEach{ elem ->
            val queryResult = query.map { it as List<Any> }.findLast{ it[0] == elem.second && it[1] == elem.third }
            val spatialQueryResult = spatialQuery.map { it as List<Any> }.findLast{ it[0] == elem.second && it[1] == elem.third }
            if (queryResult != null && spatialQueryResult != null) {
                kotlin.test.assertEquals(queryResult[2], elem.first)
                kotlin.test.assertEquals(spatialQueryResult[2], elem.first)
            }
        }
    }


    // A -> M -> E
    /*
     * AgentCoverage(𝐴): where 𝐴 is a set of agents. For each
     * 𝛼 ∈ 𝐴, list all environments 𝜖 for which 𝛼 generated
     * measurements in.
     */
    /**
     *      // v.1 graph traversal
     *
     *  MATCH (farm:Farm) - [] - (env:Parcel) - [:hasDevice] - (device) - [] - () - [:hasTS] - (meas:Measurement)
     *  WHERE ST_CONTAINS(env.location, meas.location)
     *  AND device.name IN ["Errano T1 MoistureDevice", "Errano T1 MoistureDevice", "Errano Drone"]
     *  RETURN env, device, meas
     *
     *    // v.2 Spatial contains
     *
     *  MATCH (env:AgriParcel),
     *        (device) - [] - () - [:HasTS] - (meas:Measurement)
     *  WHERE ST_CONTAINS(env.location, meas.location)
     *  AND device.name IN ["Errano T1 MoistureDevice", "Errano T1 MoistureDevice", "Errano Drone"]
     *  RETURN env, device, meas
     */
    @Test
    fun agentCoverage(){
        val g = setup(dynamicDevices=true)
        val devices = listOf(Pair(1,"Errano T1 MoistureDevice"), Pair(1,"Errano T1 MoistureDevice"), Pair(2,"Errano Drone"))
        devices.forEach {

            val traversalPattern = listOf(
                Step(AgriFarm, alias="farm"),
                null,
                Step(AgriParcel, alias = "env"),
                Step(HasDevice),
                Step(null, listOf(Filter("name", Operators.EQ, it.second)), alias = "device"),
                null,
                null,
                Step(HasTS),
                Step(Measurement, alias = "meas")
            )

            val pattern = listOf(
                listOf(Step(AgriParcel, alias = "env")),
                listOf(Step(null, listOf(Filter("name", Operators.EQ, it.second)), alias = "device"),
                    null,
                    null,
                    Step(HasTS),
                    Step(Measurement, alias = "meas")
                )
            )
            val spatialResult = query(g, pattern, where = listOf(Compare("env", "meas", "location", Operators.ST_CONTAINS)), by = listOf(Aggregate("device", "name"), Aggregate("env","name")))
            val traversalResult = query(g, traversalPattern, by = listOf(Aggregate("device", "name"), Aggregate("env","name")), timeaware = true)

            kotlin.test.assertEquals(it.first, spatialResult.size)
            kotlin.test.assertEquals(it.first, traversalResult.size)
        }
    }

    // M -> A -> E
    /*
     * CurrenteAgentLocation(𝑡𝑎 , 𝑡𝑏 ): list the current location
     * for the agents that performed measurements during
     * [𝑡𝑎, 𝑡𝑏 [.
     */
     /**
      *     // v.1 Following path traversal
      *
      *
      *  MATCH    (device: Device),
      *          (nowEnv:AgriParcel) - [:HasDevice] - (nowDevice: Device)
      *  WHERE device.name IN (
      *          MATCH (oldDevice:Device) - [] - () - [:HasTS] - ()
      *          VALID FROM 0 TO 2
      *          RETURN oldDevice.name)
      *  AND device.name == nowDevice.name
      *  RETURN nowDevice, nowEnv
      *
      *
      *     // v.2 Following spatial contains
      *
      *  MATCH    (device: Device),
      *          (nowEnv:AgriParcel)
      *  WHERE device.name IN (
      *          MATCH (oldDevice:Device) - [] - () - [:HasTS] - ()
      *          VALID FROM 0 TO 2
      *          RETURN oldDevice.name)
      *  AND ST_CONTAINS(nowEnv, device)
      *  RETURN nowDevice, nowEnv
     */
    @Test
    fun currentAgentLocation(){
        val g = setup(dynamicDevices = true)
        val resultMap = mapOf(
            "Errano T1 MoistureDevice" to "Errano T1",
            "Errano T2 MoistureDevice" to "Errano T2",
            "Errano Drone" to "Errano T2",
            "Errano Weather Station" to ""
        )
        val historicalPattern = listOf(
            Step(Device, alias="oldDevice"),
            null,
            null,
            Step(HasTS),
            null
        )

        val nowPattern = listOf(
                Step(AgriParcel, alias = "env"),
                Step(HasDevice),
                Step(Device, alias = "nowDevice")
            )

        val devicesInTime = query(g, historicalPattern, by = listOf(Aggregate("oldDevice", "name")), from = 0, to = 2, timeaware = true)

        kotlin.test.assertEquals(resultMap.size, devicesInTime.size)

        devicesInTime.forEach{
            val pattern = listOf(
                listOf(Step(Device, listOf(Filter("name", Operators.EQ, it)), alias = "device")),
                nowPattern
            )
            val nowSpatialPattern = listOf(
                    listOf(Step(AgriParcel, alias = "parcel")),
                    listOf(Step(Device, listOf(Filter("name",Operators.EQ, it)), alias = "nowDevice")),
                )

            val actualLocation = query(g, pattern, where=listOf(Compare("device", "nowDevice","name",Operators.EQ)), by = listOf(Aggregate("device", "name"), Aggregate("env","name")), from = 4, to = Long.MAX_VALUE)

            val actualLocationBySpatial = query(g, nowSpatialPattern, where = listOf(Compare("parcel", "nowDevice", "location", Operators.ST_CONTAINS)), by = listOf(Aggregate("nowDevice", "name"), Aggregate("parcel","name")), from = 4, to = Long.MAX_VALUE)

            kotlin.test.assertEquals(resultMap[it].toString(),  (actualLocation as List<List<Any>>).firstOrNull()?.getOrNull(1)?.toString() ?: "")
            kotlin.test.assertEquals(resultMap[it].toString(),  (actualLocationBySpatial as List<List<Any>>).firstOrNull()?.getOrNull(1)?.toString() ?: "")
        }
    }

    // M -> E -> A
    /*
     * ActiveAgents(𝑡𝑎 , 𝑡𝑏 ): list all the agents currently in the
     * environments in which were performed some measurements in the period [𝑡𝑎, 𝑡𝑏 [
     */
    /**
     * // v.1 Following path traversal
     *
     * MATCH (env:AgriParcel) - [:HasDevice] -> (dev:Device)
     * WHERE env.name in (
     *      MATCH (env:Agriparcel),
     *            () - [:HasTS] -> (meas)
     *      WHERE ST_CONTAINS(env.location, meas.location)
     *      VALID FROM 0 to 2
     *      RETURN env.name)
     * RETURN env, dev
     *
     * // v.2 Following spatial contains
     * MATCH    (env:AgriParcel),
     *          (dev:Device)
     * WHERE env.name in (
     *      MATCH (env:Agriparcel),
     *            () - [:HasTS] -> (meas)
     *      WHERE ST_CONTAINS(env.location, meas.location)
     *      VALID FROM 0 to 2
     *      RETURN env.name)
     * AND ST_CONTAINS(env.location, dev.location)
     * RETURN env, dev
     */

    @Test
    fun activeAgents(){
        val resultMap = mapOf(
            "Errano T1" to listOf("Errano T1 MoistureDevice"),
            "Errano T2" to listOf("Errano T2 MoistureDevice", "Errano Drone"),
        )
        val g = setup(dynamicDevices = true)
        val tA = 0L
        val tB = 2L

        // Adding a new parcel that should not be considered
        val erranoT0 = g.addNode(AgriParcel)
        g.addProperty(erranoT0.id, "location", T0_LOCATION, PropType.GEOMETRY)
        val t0Device = g.addNode(Device)
        g.addEdge(HasDevice,erranoT0.id,t0Device.id)

        val t0TS = g.getTSM().addTS()
        t0TS.add(Measurement, 4,4,"{\"coordinates\":[11.798998,44.235024],\"type\":\"Point\"}")

        val t0Hum = g.addNode(Humidity, value = t0TS.getTSId())
        g.addEdge(HasHumidity, t0Device.id, t0Hum.id)

        val oldMeasurementsLocationPattern = listOf(
            listOf(
                Step(AgriParcel, alias="env")
            ),
            listOf(
                null,
                Step(HasTS),
                Step(Measurement, alias = "meas")
            ),
        )
        val oldMeasurementsLocations = query(g, oldMeasurementsLocationPattern, where = listOf(Compare("env","meas","location",Operators.ST_CONTAINS)), by = listOf(Aggregate("env", "name")), from = tA, to = tB, timeaware = true)

        oldMeasurementsLocations.forEach{
            val activeAgentsPattern = listOf(
                Step(AgriParcel, listOf(Filter("name", Operators.EQ, it.toString())), alias = "env"),
                Step(HasDevice),
                Step(Device, alias = "dev")
            )
            val activeAgentsSpatialPattern = listOf(
                listOf(Step(AgriParcel, listOf(Filter("name", Operators.EQ, it.toString())), alias = "env")),
                listOf(Step(Device, alias = "dev"))
            )

            val activeAgents = query(g, activeAgentsPattern, by = listOf(Aggregate("env", "name"), Aggregate("dev","name")), from = 4)
            val activeSpatialAgents = query(g, activeAgentsSpatialPattern, where = listOf(Compare("env","dev","location",Operators.ST_CONTAINS)), by = listOf(Aggregate("env", "name"), Aggregate("dev","name")), from = 4)

            var result = (activeAgents as List<List<Any>>)
                .filter { it.isNotEmpty() }
                .groupBy({ it[0] }, { it.drop(1) })
                .mapValues { it.value.flatten() }

            kotlin.test.assertEquals(resultMap[it].toString(),  result[it].toString())

            result = (activeSpatialAgents as List<List<Any>>)
                .filter { it.isNotEmpty() }
                .groupBy({ it[0] }, { it.drop(1) })
                .mapValues { it.value.flatten() }

            kotlin.test.assertEquals(resultMap[it].toString(),  result[it].toString())
        }

    }


}