package com.metrolist.music.playback

import android.content.Context
import com.metrolist.music.models.MediaMetadata
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

@Suppress("UNUSED_PARAMETER")
class CastConnectionHandler(
    context: Context,
    scope: CoroutineScope,
    musicService: MusicService,
) {
    private val _isCasting = MutableStateFlow(false)
    val isCasting: StateFlow<Boolean> = _isCasting.asStateFlow()

    private val _castDeviceName = MutableStateFlow<String?>(null)
    val castDeviceName: StateFlow<String?> = _castDeviceName.asStateFlow()

    private val _castPosition = MutableStateFlow(0L)
    val castPosition: StateFlow<Long> = _castPosition.asStateFlow()

    private val _castDuration = MutableStateFlow(0L)
    val castDuration: StateFlow<Long> = _castDuration.asStateFlow()

    private val _castIsPlaying = MutableStateFlow(false)
    val castIsPlaying: StateFlow<Boolean> = _castIsPlaying.asStateFlow()

    private val _castIsBuffering = MutableStateFlow(false)
    val castIsBuffering: StateFlow<Boolean> = _castIsBuffering.asStateFlow()

    private val _castVolume = MutableStateFlow(1f)
    val castVolume: StateFlow<Float> = _castVolume.asStateFlow()

    val isSyncingFromCast = false

    var onPlayAction: (() -> Unit)? = null
    var onPauseAction: (() -> Unit)? = null
    var onSeekAction: ((Long) -> Unit)? = null
    var onSkipNextAction: (() -> Unit)? = null
    var onSkipPreviousAction: (() -> Unit)? = null
    var onSetVolumeAction: ((Float) -> Unit)? = null

    fun setRemoteMode(
        active: Boolean,
        deviceName: String? = null,
        isPlaying: Boolean = false,
        position: Long = 0L,
        duration: Long = 0L,
    ) {
        _isCasting.value = active
        _castDeviceName.value = deviceName
        _castIsPlaying.value = isPlaying
        _castPosition.value = position
        _castDuration.value = duration
    }

    fun updatePosition(pos: Long) {
        _castPosition.value = pos
    }

    fun updateIsPlaying(playing: Boolean) {
        _castIsPlaying.value = playing
    }

    fun updateDuration(duration: Long) {
        _castDuration.value = duration
    }

    fun initialize() = false

    fun disconnect() {
        _isCasting.value = false
        _castDeviceName.value = null
        _castIsPlaying.value = false
        _castPosition.value = 0L
    }

    fun loadCurrentMedia() = Unit

    fun loadMedia(metadata: MediaMetadata) = Unit

    fun play() {
        _castIsPlaying.value = true
        onPlayAction?.invoke()
    }

    fun pause() {
        _castIsPlaying.value = false
        onPauseAction?.invoke()
    }

    fun seekTo(position: Long) {
        _castPosition.value = position
        onSeekAction?.invoke(position)
    }

    fun setVolume(volume: Float) {
        _castVolume.value = volume
        onSetVolumeAction?.invoke(volume)
    }

    fun skipToNext() {
        onSkipNextAction?.invoke()
    }

    fun skipToPrevious() {
        onSkipPreviousAction?.invoke()
    }

    fun navigateToMediaIfInQueue(mediaId: String) = false

    fun release() {
        disconnect()
    }
}
