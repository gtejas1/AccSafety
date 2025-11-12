(function () {
  const existing = document.querySelector('script[data-assistant-loader="1"]');
  if (existing) {
    return;
  }
  const script = document.createElement("script");
  script.src = "/static/assistant.js";
  script.async = true;
  script.dataset.assistantLoader = "1";
  document.head.appendChild(script);
})();
