(function () {
  const SELECTOR = ".assistant-launcher";

  class AssistantWidget {
    constructor(anchor) {
      this.anchor = anchor;
      this.endpoint = anchor.dataset.endpoint || "/assistant/chat";
      this.stream = anchor.dataset.stream !== "false";
      this.systemPrompt =
        anchor.dataset.systemPrompt ||
        "You are the AccSafety virtual assistant. Provide concise, actionable answers about the mobility dashboards, data sources, and safety insights available in the portal. If you are unsure, encourage the user to consult official documentation or program contacts.";
      this.history = [];
      this.isSending = false;

      this._build();
    }

    _build() {
      this.root = document.createElement("div");
      this.root.className = "assistant-widget";

      this.toggle = document.createElement("button");
      this.toggle.className = "assistant-widget__toggle";
      this.toggle.type = "button";
      this.toggle.setAttribute("aria-expanded", "false");
      this.toggle.setAttribute("aria-controls", "assistant-panel");
      this.toggle.setAttribute("aria-label", "Open assistant chat");
      this.toggle.innerHTML = "\u{1F4AC}";

      this.panel = document.createElement("div");
      this.panel.className = "assistant-widget__panel";
      this.panel.id = "assistant-panel";

      const header = document.createElement("div");
      header.className = "assistant-widget__header";
      header.innerHTML = "<h2>Ask AccSafety</h2>";

      this.messagesList = document.createElement("div");
      this.messagesList.className = "assistant-widget__messages";
      this.messagesList.setAttribute("role", "log");
      this.messagesList.setAttribute("aria-live", "polite");

      const composer = document.createElement("form");
      composer.className = "assistant-widget__composer";
      composer.setAttribute("aria-label", "Send a message to the assistant");

      this.input = document.createElement("textarea");
      this.input.required = true;
      this.input.placeholder = "Ask about datasets, dashboards, or guidance";

      this.sendButton = document.createElement("button");
      this.sendButton.type = "submit";
      this.sendButton.textContent = "Send";

      composer.appendChild(this.input);
      composer.appendChild(this.sendButton);

      this.status = document.createElement("div");
      this.status.className = "assistant-widget__status";
      this.status.textContent = "You're chatting with the AccSafety assistant.";

      this.panel.appendChild(header);
      this.panel.appendChild(this.messagesList);
      this.panel.appendChild(composer);
      this.panel.appendChild(this.status);

      this.root.appendChild(this.toggle);
      this.root.appendChild(this.panel);

      document.body.appendChild(this.root);

      this.toggle.addEventListener("click", () => this._togglePanel());
      composer.addEventListener("submit", (event) => {
        event.preventDefault();
        this._submit();
      });

      document.addEventListener("keydown", (event) => {
        if (event.key === "Escape" && this.panel.classList.contains("is-open")) {
          this._togglePanel(false);
        }
      });
    }

    _togglePanel(force) {
      const shouldOpen = typeof force === "boolean" ? force : !this.panel.classList.contains("is-open");
      this.panel.classList.toggle("is-open", shouldOpen);
      this.toggle.setAttribute("aria-expanded", shouldOpen ? "true" : "false");
      if (shouldOpen) {
        setTimeout(() => this.input.focus(), 100);
      }
    }

    _appendMessage(role, text) {
      const bubble = document.createElement("div");
      bubble.className = `assistant-widget__message assistant-widget__message--${role}`;
      bubble.textContent = text;
      this.messagesList.appendChild(bubble);
      this.messagesList.scrollTop = this.messagesList.scrollHeight;
      return bubble;
    }

    async _submit() {
      const text = (this.input.value || "").trim();
      if (!text || this.isSending) {
        return;
      }

      this.isSending = true;
      this.sendButton.disabled = true;
      this.input.value = "";
      this.status.textContent = "Sending...";

      this.history.push({ role: "user", content: text });
      this._appendMessage("user", text);

      const assistantBubble = this._appendMessage("assistant", "");

      try {
        const response = await fetch(this.endpoint, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            messages: this.history,
            stream: this.stream,
            system_prompt: this.systemPrompt,
          }),
        });

        if (!response.ok) {
          throw new Error(`Assistant error (${response.status})`);
        }

        let assistantText = "";

        if (this.stream && response.body) {
          const reader = response.body.getReader();
          const decoder = new TextDecoder();
          let buffer = "";

          while (true) {
            const { value, done } = await reader.read();
            if (done) {
              break;
            }
            buffer += decoder.decode(value, { stream: true });
            const parts = buffer.split("\n\n");
            buffer = parts.pop() || "";

            for (const part of parts) {
              if (!part.trim()) {
                continue;
              }
              if (part.trim() === "data: [DONE]") {
                buffer = "";
                break;
              }
              const normalized = part.startsWith("data:") ? part.slice(5) : part;
              try {
                const payload = JSON.parse(normalized);
                if (payload && payload.content) {
                  assistantText += payload.content;
                  assistantBubble.textContent = assistantText;
                  this.messagesList.scrollTop = this.messagesList.scrollHeight;
                }
              } catch (error) {
                console.debug("assistant chunk parse error", error);
              }
            }
          }
        } else {
          const payload = await response.json();
          assistantText = payload.content || "";
          assistantBubble.textContent = assistantText;
        }

        if (!assistantText) {
          assistantText = "I'm not sure how to respond right now.";
          assistantBubble.textContent = assistantText;
        }

        this.history.push({ role: "assistant", content: assistantText });
        this.status.textContent = "Ready for your next question.";
      } catch (error) {
        console.error(error);
        assistantBubble.textContent = "Sorry, I couldn't reach the assistant. Please try again.";
        this.status.textContent = error.message || "Assistant is unavailable.";
      } finally {
        this.isSending = false;
        this.sendButton.disabled = false;
      }
    }
  }

  function init() {
    const anchors = document.querySelectorAll(SELECTOR);
    if (!anchors.length) {
      return;
    }
    anchors.forEach((anchor) => {
      if (anchor.dataset.assistantReady) {
        return;
      }
      anchor.dataset.assistantReady = "true";
      new AssistantWidget(anchor);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
