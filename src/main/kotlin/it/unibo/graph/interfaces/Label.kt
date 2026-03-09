package it.unibo.graph.interfaces

//TODO: Fix this enum, should be a sealed class
enum class Label {
    AgriFarm, AgriParcel, Device, Person, Humidity, SolarRadiation, Measurement, Environment, A, B, C, D, E, F, G,
    HasTS, HasParcel, HasDevice, HasOwner, HasManutentor, HasFriend, HasHumidity, HasTemperature, HasSolarRadiation, Foo, TargetLocation, NDVI, HasNDVI, Drone, HasDrone, User, Infrastructure, InfrastructureType, Group,
    Observation, Platform, PlatformType, Sensor, SensorType, hasOwner, hasType, hasInfrastructure, hasSensor, hasObservation, hasGroups, hasCoverage, Presence, Occupancy, SemanticObservationType, VirtualSensorType,
    hasInputType, hasSemanticObservationType, VirtualSensor, hasPresence, hasLocation, location, hasOccupancy, occupancy, hasType_, hasTemperature, Temperature, Comment, Post
}

fun labelFromString(label: String): Label = enumValueOf<Label>(label)