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

function tokenize_text(text) {
  let tokens = [];
  for (let token of text.split(/\s/)) {
    token = token.trim();
    if (token.length > 0) {
      tokens.push(token);
    }
  }
  return tokens;
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
  let context_core = "";
  let context_prefix = "";
  let context_suffix = "";
  if (text.length < 48 && ja_text.length < 1 && range.startContainer == range.endContainer) {
    let whole_text = range.startContainer.textContent;
    let start = range.startOffset;
    let end = range.endOffset;
    if (whole_text.charAt(start).match(/[-A-Za-z0-9]/)) {
      while (start > 0 && whole_text.charAt(start - 1).match(/[-A-Za-z0-9']/)) {
        if (start < range.startOffset - 50) break;
        start--;
      }
    }
    if (whole_text.charAt(end - 1).match(/[-A-Za-z0-9]/)) {
      while (end < whole_text.length - 1 && whole_text.charAt(end).match(/[-A-Za-z0-9']/)) {
        if (end > range.endOffset + 50) break;
        end++;
      }
    }
    context_core = whole_text.substring(start, end);
    let seg_start = start;
    while (seg_start > 0 &&
           whole_text.charAt(seg_start - 1).match(/[-A-Za-z0-9' ]/)) {
      if (seg_start < start - 100) break;
      seg_start--;
    }
    let seg_end = end;
    while (seg_end < whole_text.length &&
           whole_text.charAt(seg_end).match(/[-A-Za-z0-9' ]/)) {
      if (seg_end > end + 100) break;
      seg_end++;
    }
    let prefix = whole_text.substring(seg_start, start).trim();
    let suffix = whole_text.substring(end, seg_end).trim();
    let prefix_tokens = tokenize_text(prefix);
    let suffix_tokens = tokenize_text(suffix);
    let num_affix_tokens = 3;
    prefix_tokens = prefix_tokens.slice(Math.max(0, prefix_tokens.length - num_affix_tokens),
                                                 prefix_tokens.length);
    suffix_tokens = suffix_tokens.slice(0, Math.min(num_affix_tokens, suffix_tokens.length));
    context_prefix = prefix_tokens.join(" ");
    context_suffix = suffix_tokens.join(" ");
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
  union_dict_update_pane(text, context_core, context_prefix, context_suffix);
}

function union_dict_update_pane(text, context_core, context_prefix, context_suffix) {
  let joined_query = text + ":" + context_core + ":" + context_prefix + ":" + context_suffix;
  if (joined_query == union_dict_pane.last_query) {
    return;
  }
  union_dict_pane.last_query = joined_query;
  while (union_dict_pane.firstChild) {
    union_dict_pane.removeChild(union_dict_pane.firstChild);
  }
  let search_url = union_dict_search_url + "?x=popup&q=" + encodeURIComponent(text);
  if (context_core.length > 0 && context_core != text) {
    search_url += "&xc=" + encodeURIComponent(context_core);
  }
  if (context_prefix.length > 0) {
    search_url += "&xp=" + encodeURIComponent(context_prefix);
  }
  if (context_suffix.length > 0) {
    search_url += "&xs=" + encodeURIComponent(context_suffix);
  }
  let sub_pane = document.createElement("iframe");
  sub_pane.id = "union_dict_sub_pane";
  sub_pane.src = search_url;
  union_dict_pane.appendChild(sub_pane);
  return;
}

function union_dict_hide_popup(event) {
  union_dict_pane.style.display = "none";
}
