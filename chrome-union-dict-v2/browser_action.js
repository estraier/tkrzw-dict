'use strict';

let config_form = document.getElementById("config_form");

function update_config(notify) {
  chrome.storage.local.set({
    "popup_size": config_form.popup_size.value,
    "popup_enable": config_form.popup_enable.value,
  }, function(value) {
    chrome.runtime.sendMessage("update_config");
  });
}

config_form.addEventListener("change", function(event) {
  update_config();
});

chrome.storage.local.get(["popup_size"], function(value) {
  if (value.popup_size == "3") {
    config_form.popup_size.value = "3";
  } else if (value.popup_size == "1") {
    config_form.popup_size.value = "1";
  } else {
    config_form.popup_size.value = "2";
  }
});

chrome.storage.local.get(["popup_enable"], function(value) {
  if (value.popup_enable == "off") {
    config_form.popup_enable.value = "off";
  } else {
    config_form.popup_enable.value = "on";
  }
});

let root = document.documentElement;
if (root.clientWidth < 500) {
  document.documentElement.style.width = (root.clientWidth - 1) + "px";
}
if (root.clientWidth < 400) {
  document.documentElement.style.height = (root.clientHeight - 1) + "px";
}

let help_icon = document.getElementById("help_icon");
let help = document.getElementById("help");
help_icon.onmouseenter = () => {
  help.style.display = 'block';
};
help_icon.onmouseout = () => {
  setTimeout(() => {
    help.style.display = 'none';
  }, 100);
};

setTimeout(function() {
  config_form.classList.add("config_form_timed");
}, 5000);
