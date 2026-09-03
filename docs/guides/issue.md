```{warning}
Technical challenge: Marimo HTML exports are not plain static html, but a JavaScript based web-application.
Workarounds including the exported html have their own caveats:
- Inclduding by way of the raw html directive as for the [quick start guide](quickstart), conflicts with the Furo template + crashes with z-order.
- Including IFRAME sourced from <projectroot>/notebooks/html/calculations.html", works, but is tricky to scale vertically.
```
