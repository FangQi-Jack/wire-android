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
package com.waz.zclient.messages.parts

import android.content.Context
import android.content.res.ColorStateList
import android.util.{AttributeSet, TypedValue}
import android.widget.TextView
import com.waz.api.{AccentColor, Message}
import com.waz.model.{MessageContent, MessageData}
import com.waz.service.messages.MessageAndLikes
import com.waz.utils.events.Signal
import com.waz.zclient.controllers.global.AccentColorController
import com.waz.zclient.messages.MessageView.MsgBindOptions
import com.waz.zclient.messages.{ClickableViewPart, MessageViewPart, MsgPart}
import com.waz.zclient.ui.text.LinkTextView
import com.waz.zclient.ui.utils.TypefaceUtils
import com.waz.zclient.{R, ViewHelper}

class TextPartView(context: Context, attrs: AttributeSet, style: Int) extends LinkTextView(context, attrs, style) with ViewHelper with ClickableViewPart with EphemeralTextPart {
  def this(context: Context, attrs: AttributeSet) = this(context, attrs, 0)
  def this(context: Context) = this(context, null, 0)

  override val tpe: MsgPart = MsgPart.Text
  override val textView: TextView = this

  val textSizeRegular = context.getResources.getDimensionPixelSize(R.dimen.wire__text_size__regular)
  val textSizeEmoji = context.getResources.getDimensionPixelSize(R.dimen.wire__text_size__emoji)

  override def set(msg: MessageAndLikes, part: Option[MessageContent], opts: MsgBindOptions): Unit = {
    super.set(msg, part, opts)
    setTextSize(TypedValue.COMPLEX_UNIT_PX, if (isEmojiOnly(msg.message, part)) textSizeEmoji else textSizeRegular)
    setTextLink(part.fold(msg.message.contentString)(_.content))
  }

  def isEmojiOnly(msg: MessageData, part: Option[MessageContent]) =
    part.fold(msg.msgType == Message.Type.TEXT_EMOJI_ONLY)(_.tpe == Message.Part.Type.TEXT_EMOJI_ONLY)
}


trait EphemeralTextPart extends MessageViewPart { self: ViewHelper =>

  val textView: TextView

  lazy val originalTypeface = textView.getTypeface
  lazy val originalColor = textView.getTextColors
  lazy val redactedTypeface = TypefaceUtils.getTypeface(TypefaceUtils.getRedactedTypedaceName)

  lazy val accentController = inject[AccentColorController]

  val expired = message map { m => m.isEphemeral && m.expired }
  val typeface = expired map { if (_) redactedTypeface else originalTypeface }
  val color = expired flatMap[Either[ColorStateList, AccentColor]] {
    case true => accentController.accentColor.map { Right(_) }
    case false => Signal const Left(originalColor)
  }

  typeface { textView.setTypeface }
  color {
    case Left(csl) => textView.setTextColor(csl)
    case Right(ac) => textView.setTextColor(ac.getColor())
  }

  override def set(msg: MessageAndLikes, part: Option[MessageContent], opts: MsgBindOptions): Unit = {
    super.set(msg, part, opts)

    originalTypeface // ensure lazy is initialized before first message update
    originalColor
  }
}
