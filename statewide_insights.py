"""Statewide ArcGIS map view for the AccSafety portal."""

from flask import Flask, redirect, render_template_string


def register_statewide_insights(app: Flask) -> None:
    """Attach the statewide insights routes to the provided Flask app."""

    @app.route("/statewide-map")
    def statewide_map():
        return render_template_string(
            """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Statewide Map &amp; Insights · AccSafety</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" href="/static/theme.css">
  <link rel="stylesheet" href="https://js.arcgis.com/4.33/esri/themes/light/main.css">
  <script type="module" src="https://js.arcgis.com/embeddable-components/4.33/arcgis-embeddable-components.esm.js"></script>
  <script nomodule src="https://js.arcgis.com/embeddable-components/4.33/arcgis-embeddable-components.js"></script>
  <style>
    .map-frame {
      width: 100%;
      border-radius: 18px;
      overflow: hidden;
      box-shadow: 0 20px 44px rgba(15, 23, 42, 0.15);
    }
  </style>
</head>
<body>
  <div class="app-shell">
    <header class="app-header">
      <div class="app-header-title">
        <span class="app-brand">AccSafety</span>
        <span class="app-subtitle">Statewide Map &amp; Insights</span>
      </div>
      <nav class="app-nav" aria-label="Main navigation">
        <a class="app-link" href="/">Back to Portal</a>
      </nav>
    </header>

    <main class="app-content">
      <section class="app-card">
        <div class="map-frame">
          <arcgis-embedded-map style="height:600px;width:100%;" item-id="5badd855f3384cb1ab03eb0470a93f20" theme="light" bookmarks-enabled heading-enabled legend-enabled information-enabled center="-88.01456655273279,42.991659663963226" scale="1155581.108577" portal-url="https://uwm.maps.arcgis.com"></arcgis-embedded-map>
        </div>
      </section>
    </main>
  </div>

</body>
</html>
            """,
        )

    @app.route("/statewide-map/")
    def statewide_map_slash():
        return redirect("/statewide-map", code=302)
