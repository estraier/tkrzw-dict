'use strict';

function update_page_state() {
  chrome.storage.local.get(["popup_enable"], function(value) {
    if (value.popup_enable == "off") {
      union_dict_deactivate();
    } else {
      union_dict_activate();
    }
  });
}

function entry_word_handler(entry_word) {
  let message = "search:" + entry_word;
  chrome.extension.sendMessage(message, function() {
    chrome.runtime.lastError;
  });
}

chrome.extension.onMessage.addListener(function(request, sender, send_response) {
  if (request == "union_dict_update_config") {
    update_page_state();
  }
  if (request == "union_dict_popup") {
    union_dict_toggle_popup(false);
  }
  send_response("OK");
});

update_page_state();
