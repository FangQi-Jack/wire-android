/**
 * Wire
 * Copyright (C) 2016 Wire Swiss GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.waz.zclient.calling.controllers

import com.waz.api.VoiceChannelState
import com.waz.model.ConvId
import com.waz.service.call.CallingService
import com.waz.zclient._
import com.waz.zclient.common.controllers.{CameraPermission, PermissionsController, RecordAudioPermission}

/**
  * This class is intended to be a relatively small controller that every PermissionsActivity can have access to in order
  * to start and accept calls. This controller requires a PermissionsActivity so that it can request and display the
  * related permissions dialogs, that's why it can't be in the GlobalCallController
  */
class CallPermissionsController(implicit inj: Injector, cxt: WireContext) extends Injectable {

  private implicit val eventContext = cxt.eventContext

  val globController = inject[GlobalCallingController]
  import globController._

  val permissionsController = inject[PermissionsController]

  val autoAnswerPreference = prefs.flatMap(p => p.uiPreferenceBooleanSignal(p.autoAnswerCallPrefKey).signal)
  val callV3Preference = prefs.flatMap(p => p.uiPreferenceBooleanSignal(p.callingV3Key).signal)

  val incomingCall = callState.map {
    case VoiceChannelState.OTHER_CALLING => true
    case _ => false
  }

  incomingCall.zip(autoAnswerPreference) {
    case (true, true) => acceptCall()
    case _ =>
  }

  private var _isV3Call = false
  isV3Call(_isV3Call = _)

  private var v3Pref = false
  callV3Preference(v3Pref = _)

  private var _v3Service = Option.empty[CallingService]
  v3Service(s => _v3Service = Some(s))

  private var _v3ServiceAndCurrentConvId = Option.empty[(CallingService, ConvId)]
  v3ServiceAndCurrentConvId(v => _v3ServiceAndCurrentConvId = Some(v))

  private var _convId = Option.empty[ConvId]
  convId (c => _convId = Some(c))

  def startCall(convId: ConvId, withVideo: Boolean): Unit = {
    permissionsController.requiring(if (withVideo) Set(CameraPermission, RecordAudioPermission) else Set(RecordAudioPermission)) {
      if (v3Pref)
        _v3Service.foreach(_.startCall(convId, withVideo))
      else
        v2Service.currentValue.foreach(_.joinVoiceChannel(convId, withVideo))

    }(R.string.calling__cannot_start__title,
      if (withVideo) R.string.calling__cannot_start__no_video_permission__message else R.string.calling__cannot_start__no_permission__message)
  }

  def acceptCall(): Unit = {
    //TODO handle permissions for v3
    if (_isV3Call) {
      (videoCall.currentValue.getOrElse(false), _v3ServiceAndCurrentConvId) match {
        case (withVideo, Some((vcs, id))) =>
          permissionsController.requiring(if (withVideo) Set(CameraPermission, RecordAudioPermission) else Set(RecordAudioPermission)) {
            vcs.acceptCall(id)
          }(R.string.calling__cannot_start__title,
            if (withVideo) R.string.calling__cannot_start__no_video_permission__message else R.string.calling__cannot_start__no_permission__message,
            vcs.endCall(id))
        case _ =>
      }
    } else {
      (videoCall.currentValue.getOrElse(false), v2ServiceAndCurrentConvId.currentValue) match {
        case (withVideo, Some((vcs, id))) =>
          permissionsController.requiring(if (withVideo) Set(CameraPermission, RecordAudioPermission) else Set(RecordAudioPermission)) {
            vcs.joinVoiceChannel(id, withVideo)
          }(R.string.calling__cannot_start__title,
            if (withVideo) R.string.calling__cannot_start__no_video_permission__message else R.string.calling__cannot_start__no_permission__message,
            vcs.silenceVoiceChannel(id))
        case _ =>
      }
    }
  }
}
