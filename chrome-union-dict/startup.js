function update_page_state() {
  chrome.storage.local.get(["popup_enable"], function(value) {
    if (value.popup_enable == "off") {
      union_dict_deactivate();
    } else {
      union_dict_activate();
    }
  });
}

update_page_state();
