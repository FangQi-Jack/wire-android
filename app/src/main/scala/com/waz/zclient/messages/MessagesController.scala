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
package com.waz.zclient.messages

import com.waz.model.{MessageData, MessageId}
import com.waz.service.ZMessaging
import com.waz.utils.events.{EventContext, EventStream, Signal}
import com.waz.zclient.controllers.global.SelectionController
import com.waz.zclient.{Injectable, Injector}
import org.threeten.bp.Instant

class MessagesController()(implicit injector: Injector, ev: EventContext) extends Injectable {
  import com.waz.threading.Threading.Implicits.Background

  val zms = inject[Signal[ZMessaging]]
  val selectedConversation = inject[SelectionController].selectedConv

  val onScrollToBottomRequested = EventStream[Int]

  val currentConvIndex = for {
    z       <- zms
    convId  <- selectedConversation
    index   <- Signal.future(z.messagesStorage.msgsIndex(convId))
  } yield
    index

  val lastMessage: Signal[MessageData] =
    currentConvIndex.flatMap { _.signals.lastMessage } map { _.getOrElse(MessageData.Empty) }

  val lastSelfMessage: Signal[MessageData] =
    currentConvIndex.flatMap { _.signals.lastMessageFromSelf } map { _.getOrElse(MessageData.Empty) }

  def isLastSelf(id: MessageId) = lastSelfMessage.currentValue.exists(_.id == id)

  @volatile
  private var lastReadTime = Instant.EPOCH

  currentConvIndex.flatMap(_.signals.lastReadTime) { lastReadTime = _ }

  def onMessageRead(msg: MessageData) = {
    if (msg.isEphemeral && !msg.expired)
      zms.head foreach  { _.ephemeral.onMessageRead(msg.id) }

    if (msg.time isAfter lastReadTime)
      zms.head.foreach { _.convsUi.setLastRead(msg.convId, msg) }
  }

  def getMessage(messageId: MessageId): Signal[Option[MessageData]] = {
    zms.flatMap(z => Signal.future(z.messagesStorage.get(messageId)))
  }
}
