'use strict';

let union_dict_search_url = "https://dbmx.net/dict/search_union.cgi"
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
  let article = document.createElement("div");
  article.className = "union_dict_article";
  let header = document.createElement("h2");
  header.textContent = query;
  header.className = "union_dict_header";
  article.appendChild(header);
  let message = document.createElement("p");
  message.className = "union_dict_message";
  article.appendChild(message);
  union_dict_pane.appendChild(article);
  let url = union_dict_search_url + "?x=popup&q=" + encodeURI(query);
  let xhr = new XMLHttpRequest();
  let reporter = function(event) {
    if (xhr.readyState == 1) {
      message.textContent = "connecting";
    }
    if (xhr.readyState == 2) {
      message.textContent = "header received";
    }
    if (xhr.readyState == 3) {
      message.textContent = "loading";
    }
  };
  xhr.addEventListener("readystatechange", reporter, false);
  let renderer = function(event) {
    if (xhr.status == 200) {
      let result = JSON.parse(xhr.responseText);
      if (result.length > 0) {
        while (union_dict_pane.firstChild) {
          union_dict_pane.removeChild(union_dict_pane.firstChild);
        }
        let max_items = 1000;
        if (result.length >= 4) {
          max_items = 2;
        } else if (result.length > 1) {
          max_items = 4;
        }
        for (let entry of result) {
          union_dict_fill_entry(entry, max_items);
        }
      } else {
        let note = document.createElement("p");
        note.className = "union_dict_note";
        note.textContent = "No results.";
        article.appendChild(note);
      }
      union_dict_pane.last_query = query;
    } else {
      let note = document.createElement("p");
      note.className = "union_dict_note";
      note.textContent = "Error: " + xhr.status;
      article.appendChild(note);
    }
  };
  xhr.addEventListener('load', renderer, false);
  xhr.open('GET', url);
  xhr.send(null);
}

function union_dict_fill_entry(entry, max_items) {
  let article = document.createElement("div");
  article.className = "union_dict_article";
  let header = document.createElement("h2");
  header.className = "union_dict_header";
  let header_link = document.createElement("a");
  header_link.href = union_dict_search_url + "?q=" + encodeURI(entry.word);
  header_link.target = "_blank";
  header_link.textContent = entry.word;
  header.appendChild(header_link);
  if (entry.pronunciation) {
    header.appendChild(document.createTextNode(" "));
    let header_pron = document.createElement("span");
    header_pron.className = "union_dict_pron";
    header_pron.textContent = entry.pronunciation;
    header.appendChild(header_pron);
  }
  article.appendChild(header);
  if (entry.translation) {
    let tran_line = document.createElement("p");
    tran_line.className = "union_dict_tran";
    for (let tran_index = 0; tran_index < entry.translation.length; tran_index++) {
      if (tran_index > 4) {
        break;
      }
      if (tran_index > 0) {
        tran_line.appendChild(document.createTextNode(", "));
      }
      let tran = entry.translation[tran_index];
      let tran_word = document.createElement("span");
      tran_word.className = "union_dict_tran_word";
      tran_word.textContent = tran;
      tran_line.appendChild(tran_word);
    }
    article.appendChild(tran_line);
  }
  if (entry.item) {
    let is_hidden = false;
    for (let item_index = 0; item_index < entry.item.length; item_index++) {
      if (!is_hidden && item_index >= max_items) {
        is_hidden = true;
        let omit_line = document.createElement("p");
        let omit_link = document.createElement("span");
        omit_link.className = "union_dict_item_omit_link";
        omit_link.textContent = "......";
        omit_link.addEventListener("click", function(event) {
          for (let item_line of article.getElementsByClassName("union_dict_item")) {
            item_line.style.display = "block";
          }
          omit_line.style.display = "none";
          event.stopPropagation();
        }, false);
        omit_line.appendChild(omit_link);
        article.appendChild(omit_line);
      }
      let item = entry.item[item_index];
      let item_line = document.createElement("p");
      item_line.className = "union_dict_item";
      let item_label = document.createElement("span");
      item_label.className = "union_dict_item_label";
      item_label.classList.add("union_dict_item_label_" + item.label);
      item_label.textContent = item.label.toUpperCase();
      item_line.appendChild(item_label);
      let item_pos = document.createElement("span");
      item_pos.className = "union_dict_item_pos";
      let pos_label = pos_labels.get(item.pos);
      if (!pos_label) {
        pos_label = item.pos;
      }
      item_pos.textContent = pos_label;
      item_line.appendChild(item_pos);
      let text = item.text.replace(/\[-+\].*/, "");
      let mark_names = [];
      while (true) {
        let match = text.match(/^\[([a-z]+)\]: /);
        if (!match) {
          break;
        }
        text = text.substr(match.index + match[0].length).trim();
        if (match[1] == "translation") {
          mark_names.push("_translation");
        } else {
          mark_names.push(match[1]);
        }
      }
      while (true) {
        let match = text.match(/^\(([^\)]+)\)/);
        if (!match) {
          break;
        }
        text = text.substr(match.index + match[0].length).trim();
        mark_names.push(match[1]);
      }
      while (true) {
        let match = text.match(/^（([^）]+)）/);
        if (!match) {
          break;
        }
        text = text.substr(match.index + match[0].length).trim();
        mark_names.push(match[1]);
      }
      for (let mark_name of mark_names) {
        let mark_label = mark_labels.get(mark_name);
        let mark = document.createElement("span");
        if (mark_label) {
          mark.className = "union_dict_item_mark";
        } else {
          mark.className = "union_dict_item_note";
          mark_label = mark_name;
        }
        mark.textContent = mark_label;
        item_line.appendChild(mark);
      }
      let item_text = document.createElement("span");
      item_text.className = "union_dict_item_text";
      item_text.textContent = text;
      item_line.appendChild(item_text);
      if (is_hidden) {
        item_line.style.display = "none";
      }
      article.appendChild(item_line);
    }
  }
  union_dict_pane.appendChild(article);
  let close_button = document.createElement("div");
  close_button.className = "union_dict_close_button";
  close_button.textContent = "️X";
  close_button.addEventListener("click", function(event) {
    union_dict_pane.style.display = "none";
  }, false);
  union_dict_pane.appendChild(close_button);
}

function union_dict_hide_popup(event) {
  union_dict_pane.style.display = "none";
}
