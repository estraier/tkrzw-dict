'use strict';

let config_form = document.getElementById("config_form");

function update_config(notify) {
  chrome.storage.local.set({"popup_enable": config_form.popup_enable.value}, function(value) {});
  chrome.runtime.getBackgroundPage(function(background_page) {
    background_page.update_config();
  });
}

config_form.addEventListener("change", function(event) {
  update_config();
});

chrome.storage.local.get(["popup_enable"], function(value) {
  if (value.popup_enable == "off") {
    config_form.popup_enable.value = "off";
  } else {
    config_form.popup_enable.value = "on";
  }
});
