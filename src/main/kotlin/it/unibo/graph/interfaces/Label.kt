package it.unibo.graph.interfaces

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

interface Label

@Serializable(with = LabelSerializer::class)

//TODO: Fix this enum, should be a sealed class
enum class Labels : Label {
    AgriFarm, AgriParcel, Device, Person, Humidity, SolarRadiation, Measurement, Environment, A, B, C, D, E, F, G,
    HasTS, HasParcel, HasDevice, HasOwner, HasManutentor, HasFriend, HasHumidity, HasTemperature, HasSolarRadiation, Foo, TargetLocation, NDVI, HasNDVI, Drone, HasDrone, User, Infrastructure, InfrastructureType, Group,
    Observation, Platform, PlatformType, Sensor, SensorType, hasOwner, hasType, hasInfrastructure, hasSensor, hasObservation, hasGroups, hasCoverage, Presence, Occupancy, SemanticObservationType, VirtualSensorType,
    hasInputType, hasSemanticObservationType, VirtualSensor, hasPresence, hasLocation, location, hasOccupancy, occupancy, hasType_, hasTemperature, Temperature
}

// Custom serializer for numbers
object LabelSerializer : KSerializer<Labels> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("Label", PrimitiveKind.INT)

    override fun serialize(encoder: Encoder, value: Labels) {
        encoder.encodeInt(value.ordinal)  // Serialize as number (0, 1, 2)
    }

    override fun deserialize(decoder: Decoder): Labels {
        return Labels.entries[decoder.decodeInt()]  // Deserialize from number
    }
}

fun labelFromString(label: String): Label = enumValueOf<Labels>(label)