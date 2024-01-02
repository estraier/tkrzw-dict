'use strict';

let union_dict_search_url = "https://dbmx.net/dict/search_union.cgi"

let union_dict_pane = document.createElement("div");
union_dict_pane.id = "union_dict_pane";
union_dict_pane.last_query = "";
union_dict_pane.addEventListener("mouseup", function(event) {
  event.stopPropagation();
}, false);

function union_dict_resize(level) {
  for (let name of union_dict_pane.classList) {
    if (name.startsWith("popup_size_")) {
      union_dict_pane.classList.remove(name);
    }
  }
  union_dict_pane.classList.add("popup_size_" + level);
}

function union_dict_activate() {
  document.addEventListener("mouseup", union_dict_mouseup, false);
  document.removeEventListener("mouseup", union_dict_hide_popup, false);
}

function union_dict_deactivate() {
  document.removeEventListener("mouseup", union_dict_mouseup, false);
  document.addEventListener("mouseup", union_dict_hide_popup, false);
  union_dict_hide_popup();
}

function union_dict_mouseup(event) {
  union_dict_toggle_popup(true);
}

function union_dict_toggle_popup(dom_check) {
  if (!document.has_union_dict) {
    if (document.location.href.startsWith(union_dict_search_url)) {
      return;
    }
    document.body.appendChild(union_dict_pane);
    document.body.has_union_dict = true;
  }
  union_dict_pane.style.display = "none";
  let selection = window.getSelection();
  if (selection.rangeCount < 1) {
    return;
  }
  if (dom_check) {
    if (selection.focusNode) {
      for (let elem of selection.focusNode.childNodes) {
        let editable = elem.contentEditable;
        if (editable == "true") {
          return;
        }
        let node_name = elem.nodeName.toLowerCase();
        if (node_name == "input" || node_name == "textarea") {
          return;
        }
      }
      let elem = selection.focusNode.parentNode;
      while (elem != undefined) {
        let editable = elem.contentEditable;
        if (editable == "true") {
          return;
        }
        if (editable == "false") {
          break;
        }
        elem = elem.parentNode;
      }
    }
  }
  let range = selection.getRangeAt(0);
  let rect = range.getBoundingClientRect();
  let left = rect.x;
  let top = rect.y + rect.height;
  let text = selection.toString().trim();
  if (text.length <= 48) {
    text = text.replaceAll(
        /[^-'\d\p{Script=Latin}\p{Script=Han}\p{Script=Hiragana}\p{Script=Katakana}ー]+/gu, " ");
    text = text.trim();
  }
  if (text.length == 0) {
    return;
  }
  if (text.match(/^https?:\/\//)) {
    return;
  }
  let max_length = 4096;
  if (text.length > max_length) {
    text = text.substring(0, max_length);
    text = text.replace(/[A-Za-z0-9]+$/, "").trim();
  }
  let non_alnum = text.replaceAll(/[- '\d\p{Script=Latin},.!?;:"]+/gu, "");
  if (non_alnum.length > 1024) {
    return;
  }
  let ja_text = text.replaceAll(/[^\p{Script=Han}\p{Script=Hiragana}\p{Script=Katakana}ー]+/gu, "");
  if (ja_text.length > 48) {
    return;
  }
  union_dict_pane.style.display = "block";
  let pane_left = Math.min(rect.left, window.innerWidth - union_dict_pane.offsetWidth - 8);
  let pane_top = rect.top + rect.height + 5;
  if (pane_top + union_dict_pane.offsetHeight + 3 > window.innerHeight) {
    pane_top = rect.top - union_dict_pane.offsetHeight - 8;
  }
  if (pane_top < 0) {
    pane_left = rect.left + rect.width / 2.2 - union_dict_pane.offsetWidth / 2;
    pane_top = rect.top + rect.height / 2 - union_dict_pane.offsetHeight / 2;
  }
  if (pane_top < 5) {
    pane_top = 5;
  }
  pane_left += window.pageXOffset;
  pane_top += window.pageYOffset;
  union_dict_pane.style.left = pane_left + "px";
  union_dict_pane.style.top = pane_top + "px";
  union_dict_update_pane(text);
}

function union_dict_update_pane(query) {
  if (query == union_dict_pane.last_query) {
    return;
  }
  while (union_dict_pane.firstChild) {
    union_dict_pane.removeChild(union_dict_pane.firstChild);
  }
  let sub_pane = document.createElement("iframe");
  sub_pane.id = "union_dict_sub_pane";
  sub_pane.src = union_dict_search_url + "?x=popup&q=" + encodeURIComponent(query);
  union_dict_pane.appendChild(sub_pane);
  return;
}

function union_dict_hide_popup(event) {
  union_dict_pane.style.display = "none";
}
