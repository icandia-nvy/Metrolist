/**
 * Metrolist Project (C) 2026
 * Licensed under GPL-3.0 | See git history for contributors
 */

package com.metrolist.music.listentogether

import android.content.Context
import android.net.wifi.WifiManager
import dagger.hilt.android.qualifiers.ApplicationContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import timber.log.Timber
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.net.SocketTimeoutException
import java.util.UUID
import javax.inject.Inject
import javax.inject.Singleton

data class LanDevice(
    val deviceId: String,
    val deviceName: String,
    val app: String,
    val ip: String,
    val wsPort: Int,
    val roomCode: String,
    val serverUrl: String,
    val lastSeenMs: Long,
)

@Serializable
private data class LanBeaconPacket(
    val magic: String = BEACON_MAGIC,
    val device_id: String,
    val device_name: String,
    val app: String = "metrolist",
    val room_code: String = "",
    val ws_port: Int = 0,
    val server_url: String = "",
)

@Serializable
private data class LanDiscoverProbe(
    val magic: String = DISCOVER_MAGIC,
)

private const val LAN_DISCOVERY_PORT = 28474
private const val BEACON_MAGIC = "metro_connect_lan_v1"
private const val DISCOVER_MAGIC = "metro_connect_discover"
private const val DEVICE_TTL_MS = 12_000L

@Singleton
class LanDiscoveryManager
    @Inject
    constructor(
        @ApplicationContext private val context: Context,
    ) {
        companion object {
            private const val TAG = "LanDiscoveryManager"
        }

        private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }
        private val instanceId = UUID.randomUUID().toString().replace("-", "").take(16)

        private val _devices = MutableStateFlow<List<LanDevice>>(emptyList())
        val devices: StateFlow<List<LanDevice>> = _devices.asStateFlow()

        private val discoveredMap = mutableMapOf<String, LanDevice>()
        private var socket: DatagramSocket? = null
        private var listenJob: Job? = null
        private var probeJob: Job? = null
        private var multicastLock: WifiManager.MulticastLock? = null

        @Volatile
        private var hostingBeacon: LanBeaconPacket? = null

        fun setHostingBeacon(deviceName: String, roomCode: String, serverUrl: String) {
            hostingBeacon =
                if (roomCode.isNotBlank()) {
                    LanBeaconPacket(
                        device_id = instanceId,
                        device_name = deviceName.ifBlank { "Metrolist Android" },
                        app = "metrolist",
                        room_code = roomCode,
                        ws_port = 0,
                        server_url = serverUrl,
                    )
                } else {
                    null
                }
            if (hostingBeacon != null) {
                scope.launch { broadcastBeaconOnce() }
            }
        }

        fun clearHostingBeacon() {
            hostingBeacon = null
        }

        fun startDiscovery() {
            if (listenJob?.isActive == true) {
                scanNow()
                return
            }
            acquireMulticastLock()
            listenJob =
                scope.launch {
                    val sock = openSocket() ?: return@launch
                    socket = sock
                    val buf = ByteArray(2048)
                    val packet = DatagramPacket(buf, buf.size)
                    while (isActive) {
                        try {
                            sock.receive(packet)
                            val text = String(packet.data, 0, packet.length, Charsets.UTF_8)
                            handlePacket(text, packet.address)
                        } catch (_: SocketTimeoutException) {
                            pruneExpired()
                        } catch (e: Exception) {
                            if (isActive) {
                                Timber.tag(TAG).d(e, "UDP receive error")
                            }
                            break
                        }
                    }
                    try {
                        sock.close()
                    } catch (_: Exception) {
                    }
                    socket = null
                }

            probeJob =
                scope.launch {
                    while (isActive) {
                        sendDiscoverProbe()
                        broadcastBeaconOnce()
                        pruneExpired()
                        delay(2_500L)
                    }
                }
        }

        fun stopDiscovery() {
            probeJob?.cancel()
            probeJob = null
            listenJob?.cancel()
            listenJob = null
            try {
                socket?.close()
            } catch (_: Exception) {
            }
            socket = null
            releaseMulticastLock()
        }

        fun scanNow() {
            scope.launch {
                sendDiscoverProbe()
                broadcastBeaconOnce()
            }
        }

        private fun openSocket(): DatagramSocket? =
            try {
                DatagramSocket(null).apply {
                    reuseAddress = true
                    broadcast = true
                    soTimeout = 1500
                    bind(InetSocketAddress(LAN_DISCOVERY_PORT))
                }
            } catch (e: Exception) {
                Timber.tag(TAG).w(e, "Port $LAN_DISCOVERY_PORT busy, falling back to ephemeral port")
                try {
                    DatagramSocket().apply {
                        broadcast = true
                        soTimeout = 1500
                    }
                } catch (e2: Exception) {
                    Timber.tag(TAG).e(e2, "Failed to open UDP socket")
                    null
                }
            }

        private fun handlePacket(text: String, sender: InetAddress?) {
            val senderIp = sender?.hostAddress ?: return
            if (text.contains(DISCOVER_MAGIC)) {
                val beacon = hostingBeacon ?: return
                val bytes = json.encodeToString(beacon).toByteArray(Charsets.UTF_8)
                try {
                    socket?.send(DatagramPacket(bytes, bytes.size, sender, LAN_DISCOVERY_PORT))
                } catch (_: Exception) {
                }
                return
            }
            if (!text.contains(BEACON_MAGIC)) return
            val beacon =
                try {
                    json.decodeFromString<LanBeaconPacket>(text)
                } catch (_: Exception) {
                    return
                }
            if (beacon.device_id == instanceId || beacon.room_code.isBlank()) return
            val now = System.currentTimeMillis()
            synchronized(discoveredMap) {
                discoveredMap[beacon.device_id] =
                    LanDevice(
                        deviceId = beacon.device_id,
                        deviceName = beacon.device_name,
                        app = beacon.app,
                        ip = senderIp,
                        wsPort = beacon.ws_port,
                        roomCode = beacon.room_code,
                        serverUrl = beacon.server_url,
                        lastSeenMs = now,
                    )
                publishLocked(now)
            }
        }

        private fun pruneExpired() {
            val now = System.currentTimeMillis()
            synchronized(discoveredMap) {
                val removed = discoveredMap.entries.removeAll { now - it.value.lastSeenMs > DEVICE_TTL_MS }
                if (removed) publishLocked(now)
            }
        }

        private fun publishLocked(now: Long) {
            discoveredMap.entries.removeAll { now - it.value.lastSeenMs > DEVICE_TTL_MS }
            _devices.value = discoveredMap.values.sortedBy { it.deviceName.lowercase() }
        }

        private fun sendDiscoverProbe() {
            val bytes = json.encodeToString(LanDiscoverProbe()).toByteArray(Charsets.UTF_8)
            sendToBroadcastTargets(bytes)
        }

        private fun broadcastBeaconOnce() {
            val beacon = hostingBeacon ?: return
            val bytes = json.encodeToString(beacon).toByteArray(Charsets.UTF_8)
            sendToBroadcastTargets(bytes)
        }

        private fun sendToBroadcastTargets(bytes: ByteArray) {
            val sock = socket ?: return
            val targets = mutableSetOf<InetAddress>()
            try {
                targets.add(InetAddress.getByName("255.255.255.255"))
            } catch (_: Exception) {
            }
            try {
                val interfaces = NetworkInterface.getNetworkInterfaces()
                while (interfaces.hasMoreElements()) {
                    val ni = interfaces.nextElement()
                    if (!ni.isUp || ni.isLoopback) continue
                    for (ia in ni.interfaceAddresses) {
                        ia.broadcast?.let { targets.add(it) }
                    }
                }
            } catch (_: Exception) {
            }
            for (addr in targets) {
                try {
                    sock.send(DatagramPacket(bytes, bytes.size, addr, LAN_DISCOVERY_PORT))
                } catch (_: Exception) {
                }
            }
        }

        private fun acquireMulticastLock() {
            try {
                val wifi = context.applicationContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager
                if (multicastLock == null) {
                    multicastLock = wifi?.createMulticastLock("MetrolistLanDiscovery")?.apply {
                        setReferenceCounted(false)
                    }
                }
                if (multicastLock?.isHeld == false) {
                    multicastLock?.acquire()
                }
            } catch (e: Exception) {
                Timber.tag(TAG).w(e, "Could not acquire MulticastLock")
            }
        }

        private fun releaseMulticastLock() {
            try {
                if (multicastLock?.isHeld == true) {
                    multicastLock?.release()
                }
            } catch (_: Exception) {
            }
        }
    }
