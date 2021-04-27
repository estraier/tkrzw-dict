'use strict';

//let union_dict_search_url = "https://dbmx.net/dict/search_union.cgi"
let union_dict_search_url = "https://dbmx.net/dev/tkrzw-dict/search_union.cgi"
let pos_labels = new Map([
  ["noun", "名"],
  ["verb", "動"],
  ["adjective", "形"],
  ["adverb", "副"],
  ["pronoun", "代名"],
  ["auxverb", "助動"],
  ["preposition", "前置"],
  ["determiner", "限定"],
  ["article", "冠"],
  ["interjection", "間投"],
  ["conjunction", "接続"],
  ["prefix", "接頭"],
  ["suffix", "接尾"],
  ["abbreviation", "省略"],
  ["misc", "他"],
]);
let mark_labels = new Map([
  ["_translation", "翻訳"],
  ["可算", "c"],
  ["不可算", "u"],
  ["自動詞", "vi"],
  ["他動詞", "vt"],
  ["countable", "c"],
  ["uncountable", "u"],
  ["intransitive", "vi"],
  ["transitive", "vt"],
]);

let union_dict_pane = document.createElement("div");
union_dict_pane.id = "union_dict_pane";
union_dict_pane.last_query = "";
union_dict_pane.addEventListener("mouseup", function(event) {
  event.stopPropagation();
}, false);

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
  let text = selection.toString();
  text = text.replaceAll(/[^-'\d\p{Script=Latin}\p{Script=Han}\p{Script=Hiragana}\p{Script=Katakana}ー]+/gu, " ");
  text = text.trim();
  if (text.length == 0 || text.length > 50) {
    return;
  }
  union_dict_pane.style.display = "block";
  let pane_left = Math.min(rect.left, window.innerWidth - union_dict_pane.offsetWidth - 8);
  pane_left += window.pageXOffset;
  union_dict_pane.style.left = pane_left + "px";
  let pane_top = rect.top + rect.height * 1.5;
  if (pane_top + union_dict_pane.offsetHeight + 3 > window.innerHeight) {
    pane_top = rect.top - union_dict_pane.offsetHeight - 8;
  }
  pane_top += window.pageYOffset;
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
  sub_pane.src = union_dict_search_url + "?x=popup&q=" + encodeURI(query);
  union_dict_pane.appendChild(sub_pane);
  return;
}

function union_dict_hide_popup(event) {
  union_dict_pane.style.display = "none";
}
