package com.example.biocycle_watch

import android.hardware.Sensor
import android.hardware.SensorEvent
import android.hardware.SensorEventListener
import android.hardware.SensorManager
import io.flutter.embedding.android.FlutterActivity
import io.flutter.embedding.engine.FlutterEngine
import io.flutter.plugin.common.EventChannel

class MainActivity: FlutterActivity() {
    private val CHANNEL = "biocycle.watch/heart_rate"
    private var sensorManager: SensorManager? = null
    private var heartRateSensor: Sensor? = null

    override fun configureFlutterEngine(flutterEngine: FlutterEngine) {
        super.configureFlutterEngine(flutterEngine)

        // Ne conectăm la sistemul nervos al ceasului
        sensorManager = getSystemService(SENSOR_SERVICE) as SensorManager
        heartRateSensor = sensorManager?.getDefaultSensor(Sensor.TYPE_HEART_RATE)

        // Deschidem tunelul către Flutter
        EventChannel(flutterEngine.dartExecutor.binaryMessenger, CHANNEL).setStreamHandler(
            object : EventChannel.StreamHandler, SensorEventListener {
                private var eventSink: EventChannel.EventSink? = null

                override fun onListen(arguments: Any?, events: EventChannel.EventSink?) {
                    eventSink = events
                    heartRateSensor?.let {
                        sensorManager?.registerListener(this, it, SensorManager.SENSOR_DELAY_NORMAL)
                    }
                }

                override fun onCancel(arguments: Any?) {
                    sensorManager?.unregisterListener(this)
                    eventSink = null
                }

                override fun onSensorChanged(event: SensorEvent?) {
                    if (event?.sensor?.type == Sensor.TYPE_HEART_RATE) {
                        val heartRate = event.values[0]
                        eventSink?.success(heartRate)
                    }
                }

                override fun onAccuracyChanged(sensor: Sensor?, accuracy: Int) {
                    // Nu avem nevoie să procesăm modificările de acuratețe acum
                }
            }
        )
    }
}