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
package com.waz.zclient.conversation

import android.content.Context
import android.os.Bundle
import android.support.v4.app.FragmentManager
import android.support.v7.widget.Toolbar
import android.view.MenuItem.OnMenuItemClickListener
import android.view.View.OnClickListener
import android.view.{LayoutInflater, MenuItem, View, ViewGroup}
import android.widget.TextView
import com.waz.ZLog._
import com.waz.api.Message
import com.waz.model.{AssetId, MessageData}
import com.waz.threading.Threading
import com.waz.utils.events.Signal
import com.waz.zclient.controllers.collections.CollectionsObserver
import com.waz.zclient.conversation.CollectionAdapter.AdapterState
import com.waz.zclient.conversation.CollectionController._
import com.waz.zclient.pages.BaseFragment
import com.waz.zclient.utils.ViewUtils
import com.waz.zclient.{FragmentHelper, OnBackPressedListener, R}
import org.threeten.bp.{LocalDateTime, ZoneId}

class CollectionFragment extends BaseFragment[CollectionFragment.Container] with FragmentHelper with OnBackPressedListener with CollectionsObserver  {

  private implicit lazy val context: Context = getContext

  private implicit val tag: LogTag = logTagFor[CollectionFragment]

  lazy val controller = getControllerFactory.getCollectionsController
  var adapter: CollectionAdapter = null


  override def onStart(): Unit = {
    super.onStart()
    controller.addObserver(this)
  }

  override def onStop(): Unit = {
    super.onStop()
    controller.removeObserver(this)
  }


  override def onDestroy(): Unit = {
    if (adapter != null) adapter.closeCursors()
    super.onDestroy()
  }

  private def showSingleImage(assetId: AssetId) = {
    getChildFragmentManager.findFragmentByTag(SingleImageCollectionFragment.TAG) match {
      case null => getChildFragmentManager.beginTransaction.add(R.id.fl__collection_content, SingleImageCollectionFragment.newInstance(assetId), SingleImageCollectionFragment.TAG).addToBackStack(SingleImageCollectionFragment.TAG).commit
      case fragment: SingleImageCollectionFragment => fragment.setAsset(assetId)
      case _ =>
    }
  }

  private def closeSingleImage() = {
    getChildFragmentManager.findFragmentByTag(SingleImageCollectionFragment.TAG) match {
      case null =>
      case _ => getChildFragmentManager.popBackStackImmediate(SingleImageCollectionFragment.TAG, FragmentManager.POP_BACK_STACK_INCLUSIVE)
    }
  }

  private def textIdForContentMode(contentType: ContentType) = contentType match {
    case Images => R.string.collection_header_pictures
    case Files => R.string.collection_header_files
    case Links => R.string.collection_header_links
    case _ => R.string.collection_header_all
  }

  override def onCreateView(inflater: LayoutInflater, container: ViewGroup, savedInstanceState: Bundle): View = {
    val view = inflater.inflate(R.layout.fragment_collection, container, false)
    val name: TextView  = ViewUtils.getView(view, R.id.tv__collection_toolbar__name)
    val recyclerView: CollectionRecyclerView = ViewUtils.getView(view, R.id.rv__collection)
    val emptyView: View = ViewUtils.getView(view, R.id.ll__collection__empty)
    val toolbar: Toolbar = ViewUtils.getView(view, R.id.t_toolbar)
    emptyView.setVisibility(View.GONE)

    controller.focusedItem.on(Threading.Ui) {
      case Some(md) if md.msgType == Message.Type.ASSET => showSingleImage(md.assetId)
      case _ => closeSingleImage()
    }

    val columns = 4
    adapter = new CollectionAdapter(recyclerView.viewDim, columns, controller)
    recyclerView.init(adapter)

    Signal(adapter.adapterState, controller.focusedItem, controller.conversationName).on(Threading.Ui) {
      case (AdapterState(_, _, _), Some(messageData), conversationName) =>
        name.setText(LocalDateTime.ofInstant(messageData.time, ZoneId.systemDefault()).toLocalDate.toString)
      case (AdapterState(contentMode, 0, false), None, conversationName) =>
        emptyView.setVisibility(View.VISIBLE)
        recyclerView.setVisibility(View.GONE)
        name.setText(conversationName)
      case (AdapterState(contentMode, _, _), None, conversationName) =>
        emptyView.setVisibility(View.GONE)
        recyclerView.setVisibility(View.VISIBLE)
        name.setText(conversationName)
      case _ =>
    }

    adapter.contentMode.on(Threading.Ui){
      _ => recyclerView.smoothScrollToPosition(0)
    }

    toolbar.inflateMenu(R.menu.toolbar_collection)
    toolbar.setNavigationIcon(null)
    toolbar.setOnMenuItemClickListener(new Toolbar.OnMenuItemClickListener {
      override def onMenuItemClick(item: MenuItem): Boolean = {
        item.getItemId match {
          case R.id.close => onBackPressed(); return true
        }
        false
      }
    })

    view
  }

  override def onBackPressed(): Boolean = {
    getChildFragmentManager.findFragmentByTag(SingleImageCollectionFragment.TAG) match {
      case fragment: SingleImageCollectionFragment => controller.focusedItem ! None; return true
      case _ =>
    }
    if (!adapter.onBackPressed)
      getControllerFactory.getCollectionsController.closeCollection
    true
  }

  override def openCollection(): Unit = {}

  override def shareCollectionItem(messageData: MessageData): Unit = {}

  override def closeCollectionShare(): Unit = {}

  override def previousItemRequested(): Unit =
    controller.focusedItem mutate {
      case Some(messageData) => None
      case _ => None
    }

  override def nextItemRequested(): Unit =
    controller.focusedItem mutate {
      case Some(messageData) => None
      case _ => None
    }

  override def closeCollection(): Unit = {}
}

object CollectionFragment {

  val TAG = CollectionFragment.getClass.getSimpleName

  val MAX_DELTA_TOUCH = 30

  def newInstance() = new CollectionFragment

  trait Container

}
