'use strict';

function update_config() {
  chrome.tabs.query({}, function(tabs) {
    for (let tab of tabs) {
      if ((tab.url.startsWith("http://") || tab.url.startsWith("https://")) &&
          tab.status == 'complete') {
        chrome.tabs.sendMessage(tab.id, "union_dict_update_config", function(response) {
          chrome.runtime.lastError;
        });
      }
    }
  });
}

chrome.runtime.onInstalled.addListener(function() {
  chrome.contextMenus.create({
    "title": "Popup Dictionary",
    "contexts": ["selection"],
    "id": "union_dict_popup"
 });
});

chrome.contextMenus.onClicked.addListener(function(info, tab) {
  switch (info.menuItemId) {
  case "union_dict_popup":
    if ((tab.url.startsWith("http://") || tab.url.startsWith("https://")) &&
        tab.status == 'complete') {
      chrome.tabs.sendMessage(tab.id, "union_dict_popup", function(response) {
        chrome.runtime.lastError;
      });
    }
    break;
  }
});

chrome.commands.onCommand.addListener(function(command) {
  switch (command) {
  case "union_dict_popup":
    chrome.tabs.query({active: true}, function(tabs) {
      for (let tab of tabs) {
        if ((tab.url.startsWith("http://") || tab.url.startsWith("https://")) &&
            tab.status == 'complete') {
          chrome.tabs.sendMessage(tab.id, "union_dict_popup", function(response) {
            chrome.runtime.lastError;
          });
        }
      }
    });
    break;
  }
});
