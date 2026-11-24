// The script of the pipeline diagram's notebook view (ITL-1197).
//
// Embedded inline in every diagram's HTML (``PipelineDiagram.to_html``), with comment
// lines and indentation removed: a notebook output cannot fetch a package file. It is
// an enhancement only: the SVG picture is complete without it.
//
// It finds its own container (the element just before it; or, where scripts run
// without ``document.currentScript``, as in the classic Notebook, the last container
// not set up yet), reads the model from the JSON block and adds pan (drag, arrow keys),
// zoom (buttons, + and - keys, Ctrl or Cmd with the wheel), fit (button, 0 key), a
// details panel built with ``textContent`` only and, when the output holds the other
// layouts (ITL-1232), a ``vertical`` checkbox that switches between the left-to-right
// and top-to-bottom layouts and a ``collapse`` checkbox that switches between the
// folded and full ones, keeping the selected node if it is in both and fitting the
// new picture as on first display.
//
// Scale (ITL-1231): the view keeps a scale (screen pixels per picture unit) and the
// picture point at its centre, and derives the viewBox from its own size each time, so
// the picture is never distorted or shrunk by a size the view had earlier. Fit (the
// button, the 0 key) shows the whole picture, scaled up or down, at most MAX_SCALE. The
// first display fits too, but never below MIN_FIRST_SCALE: a picture wider or taller
// than that starts at its left or top end. Where the picture goes on beyond an edge of
// the view, a cue at that edge says so and moves it on a click. The view's height
// follows the picture at its first-display scale. Until the reader zooms or pans, a
// change of the view's width (the notebook laying the cell out, a side bar closing)
// draws it again as on first display.
//
// Rules for this file:
// - Nothing from the user is ever written into it; the model is read from the JSON
//   blocks with ``textContent`` and shown with ``textContent``, never as markup.
// - Comments are whole lines starting with two slashes: the embedding step drops those
//   lines and the indentation, and keeps every line break. No comment after code, no
//   block comment, no template literal, no string that spans lines.
// - It must never contain the end tag of a script element, nor the start of an HTML
//   comment: the diagram refuses to embed it then (``diagram._asset``).
(function () {
  var s = document.currentScript;
  var root = s && s.previousElementSibling;
  if (!root || !root.classList || !root.classList.contains('op-dag')) {
    var all = document.querySelectorAll('.op-dag:not([data-op-dag-ready])');
    root = all[all.length - 1];
  }
  if (!root || root.hasAttribute('data-op-dag-ready')) return;
  root.setAttribute('data-op-dag-ready', '');
  var img = root.querySelector('img.op-dag-picture');
  if (!img) return;

  // The pictures are base64-encoded UTF-8 SVG.
  function decode(b64) {
    return new TextDecoder().decode(
      Uint8Array.from(atob(b64), function (c) { return c.charCodeAt(0); }));
  }

  // The layouts, by direction and collapse state: the shown one (the image and the
  // model block) and those the views block holds, whose model is the shown one's or,
  // for the other collapse state, the block's.
  var shown = JSON.parse(root.querySelector('script.op-dag-model').textContent);
  var block = root.querySelector('script.op-dag-views');
  var extra = block ? JSON.parse(block.textContent) : null;
  var dir = extra ? extra.direction : 'LR', col = extra ? extra.collapse : false;
  var layouts = {};
  function key(d, c) {
    return d + (c ? ' folded' : '');
  }
  layouts[key(dir, col)] = {model: shown, picture: img.getAttribute('src').split(',')[1]};
  if (extra) {
    extra.views.forEach(function (v) {
      layouts[key(v.direction, v.collapse)] = {
        model: v.collapse === col ? shown : extra.models[0],
        picture: v.picture
      };
    });
  }
  function flip(d) {
    return d === 'LR' ? 'TB' : 'LR';
  }

  var view = root.querySelector('.op-dag-view');
  var panel = root.querySelector('.op-dag-panel');
  var count = root.querySelector('.op-dag-count');
  var model = shown, svg = img, selected = null;
  // The largest scale (also the no-script image's, ``diagram._MAX_SCALE``): the node
  // labels (13 pixels) at 16 pixels and the kind line (11) at 13.75, about a notebook's
  // own text size; so a two-node job is not blown up. The smallest scale of the first
  // display: labels at 10.4 pixels, still readable.
  var MAX_SCALE = 1.25, MIN_FIRST_SCALE = .8;
  // The picture's size; the scale; the picture point at the view's centre; and how a
  // change of the view's width redraws it: 'first' as on first display, 'fit' as fit,
  // null (after a zoom or pan) keeping the scale and centre.
  var W = 1, H = 1, z = 1, cx = .5, cy = .5, mode = 'first';

  // The tools, top right: the direction and collapse checkboxes (where the output
  // holds the layouts they switch to) and the zoom buttons.
  var tools = document.createElement('div');
  tools.className = 'op-dag-tools';
  function checkbox(name, text, title, checked, change) {
    root.classList.add('op-dag-has-' + name);
    var label = document.createElement('label');
    label.className = 'op-dag-check op-dag-' + name;
    label.title = title;
    var box = document.createElement('input');
    box.type = 'checkbox';
    box.checked = checked;
    label.appendChild(box);
    label.appendChild(document.createTextNode(text));
    tools.appendChild(label);
    box.addEventListener('change', function () { change(box.checked); });
  }
  if (layouts[key(flip(dir), col)]) {
    checkbox('direction', 'vertical', 'Lay the graph out top to bottom',
      dir === 'TB', function (on) { show(on ? 'TB' : 'LR', col); });
  }
  if (layouts[key(dir, !col)]) {
    checkbox('collapse', 'collapse',
      'Fold the steps that only join, choose or rename columns into the edges',
      col, function (on) { show(dir, on); });
  }
  [['in', '+', 'Zoom in'], ['out', '\u2212', 'Zoom out'], ['fit', 'fit', 'Fit to view']].forEach(
    function (b) {
      var e = document.createElement('button');
      e.type = 'button';
      e.setAttribute('data-op-dag-act', b[0]);
      e.textContent = b[1];
      e.title = b[2];
      e.setAttribute('aria-label', b[2]);
      tools.appendChild(e);
    });
  root.appendChild(tools);

  // The cues at the edges of the view beyond which the picture goes on; a click moves
  // the picture half a view that way.
  var sides = [['left', '\u25c2', -.5, 0], ['right', '\u25b8', .5, 0],
    ['top', '\u25b4', 0, -.5], ['bottom', '\u25be', 0, .5]];
  sides.forEach(function (d) {
    var e = document.createElement('button');
    e.type = 'button';
    e.className = 'op-dag-more op-dag-more-' + d[0];
    e.textContent = d[1];
    e.title = 'More of the diagram this way: click, drag, or fit to see all of it';
    e.setAttribute('aria-label', 'Move the diagram to show more on the ' + d[0]);
    e.addEventListener('click', function () { pan(d[2], d[3]); });
    view.appendChild(e);
  });

  // The viewBox, from the scale, the centre and the view's size now, and the cues of
  // the edges the picture goes on beyond; nothing while the view is not laid out (a
  // width or height of 0).
  function apply() {
    var r = view.getBoundingClientRect();
    if (!r.width || !r.height) return;
    var w = r.width / z, h = r.height / z, x = cx - w / 2, y = cy - h / 2, e = 1 / z;
    svg.setAttribute('viewBox', [x, y, w, h].join(' '));
    root.classList.toggle('op-dag-clip-left', x > e);
    root.classList.toggle('op-dag-clip-right', x + w < W - e);
    root.classList.toggle('op-dag-clip-top', y > e);
    root.classList.toggle('op-dag-clip-bottom', y + h < H - e);
  }
  // The scale of the first display at a view width: fitted to it, within the limits.
  function firstScale(cw) {
    return Math.max(Math.min(cw / W, MAX_SCALE), MIN_FIRST_SCALE);
  }
  // The view's height: the picture's at its first-display scale, within 560 pixels; at
  // least 120 pixels unless the picture is shorter at MAX_SCALE.
  function height() {
    var h = H * firstScale(view.clientWidth || W);
    h = Math.min(Math.max(h, Math.min(120, H * MAX_SCALE)), 560);
    root.style.setProperty('--op-dag-h', Math.round(h) + 'px');
  }
  // The scale at which the whole picture fits the view, at most MAX_SCALE.
  function fitScale() {
    var r = view.getBoundingClientRect();
    if (!r.width || !r.height) return z;
    return Math.min(r.width / W, r.height / H, MAX_SCALE);
  }
  function fit() {
    z = fitScale();
    cx = W / 2;
    cy = H / 2;
    mode = 'fit';
    apply();
  }
  // As fit, but never below MIN_FIRST_SCALE: along an axis where the picture is then
  // larger than the view, it starts at its left or top end.
  function first() {
    var r = view.getBoundingClientRect();
    z = Math.max(fitScale(), Math.min(MIN_FIRST_SCALE, MAX_SCALE));
    cx = W * z <= r.width ? W / 2 : r.width / z / 2;
    cy = H * z <= r.height ? H / 2 : r.height / z / 2;
    mode = 'first';
    apply();
  }
  // Zooms by the factor f about a screen point (the view's centre by default), within
  // the scales .02 to 20; a scale already beyond a limit (a very large picture fitted
  // below .02) never moves against the zoom's direction.
  function zoom(f, px, py) {
    var r = view.getBoundingClientRect();
    var ox = px === undefined ? 0 : px - (r.left + r.width / 2);
    var oy = py === undefined ? 0 : py - (r.top + r.height / 2);
    var nz = f < 1 ? Math.max(z * f, Math.min(.02, z)) : Math.min(z * f, Math.max(20, z));
    cx += ox / z - ox / nz;
    cy += oy / z - oy / nz;
    z = nz;
    mode = null;
    apply();
  }
  tools.addEventListener('click', function (ev) {
    var a = ev.target.getAttribute('data-op-dag-act');
    if (a === 'in') zoom(1.25);
    else if (a === 'out') zoom(1 / 1.25);
    else if (a === 'fit') fit();
  });
  view.addEventListener('wheel', function (ev) {
    if (!(ev.ctrlKey || ev.metaKey)) return;
    ev.preventDefault();
    zoom(ev.deltaY > 0 ? 1 / 1.1 : 1.1, ev.clientX, ev.clientY);
  }, {passive: false});

  // A change of the view's width (the notebook laying the cell out, a side bar
  // opening): its height follows, and the picture is drawn again. Observed on the
  // title line, as wide as the view, whose size the new height does not change.
  var lastWidth = -1;
  function resized() {
    var cw = view.clientWidth;
    if (cw === lastWidth) return;
    lastWidth = cw;
    height();
    if (mode === 'first') first();
    else if (mode === 'fit') fit();
    else apply();
  }
  var head = root.querySelector('.op-dag-head') || root;
  if (window.ResizeObserver) new ResizeObserver(resized).observe(head);
  else window.addEventListener('resize', resized);

  // Drag to pan; a drag never starts on a node, so a click selects it.
  var drag = null;
  view.addEventListener('pointerdown', function (ev) {
    if (ev.target.closest && ev.target.closest('.op-dag-node, .op-dag-more')) return;
    drag = {x: ev.clientX, y: ev.clientY, cx: cx, cy: cy};
    view.setPointerCapture(ev.pointerId);
    view.style.cursor = 'grabbing';
  });
  view.addEventListener('pointermove', function (ev) {
    if (!drag) return;
    cx = drag.cx - (ev.clientX - drag.x) / z;
    cy = drag.cy - (ev.clientY - drag.y) / z;
    mode = null;
    apply();
  });
  function stop() {
    drag = null;
    view.style.cursor = '';
  }
  view.addEventListener('pointerup', stop);
  view.addEventListener('pointercancel', stop);

  // The keyboard: arrow keys pan (Shift for larger steps), + and - zoom, 0 fits; a node
  // reached with Tab is brought into view. A key the diagram uses goes no further, so
  // a notebook's own shortcuts (in JupyterLab the arrow keys move between cells, and
  // 0 twice restarts the kernel) do not act on it too.
  view.tabIndex = 0;
  view.setAttribute('role', 'group');
  view.setAttribute('aria-label',
    'Pipeline diagram: arrow keys move it, + and \u2212 zoom, 0 fits; Tab reaches the nodes');
  function pan(dx, dy) {
    var r = view.getBoundingClientRect();
    cx += dx * r.width / z;
    cy += dy * r.height / z;
    mode = null;
    apply();
  }
  view.addEventListener('keydown', function (ev) {
    if (ev.altKey || ev.ctrlKey || ev.metaKey) return;
    var k = ev.key, step = ev.shiftKey ? .5 : .1;
    if (k === 'ArrowLeft') pan(-step, 0);
    else if (k === 'ArrowRight') pan(step, 0);
    else if (k === 'ArrowUp') pan(0, -step);
    else if (k === 'ArrowDown') pan(0, step);
    else if (k === '+' || k === '=') zoom(1.25);
    else if (k === '-' || k === '\u2212') zoom(1 / 1.25);
    else if (k === '0') fit();
    else return;
    ev.preventDefault();
    ev.stopPropagation();
  });
  view.addEventListener('focusin', function (ev) {
    var g = ev.target.closest && ev.target.closest('.op-dag-node');
    if (!g) return;
    var b = g.getBoundingClientRect(), r = view.getBoundingClientRect();
    if (b.left < r.left || b.top < r.top || b.right > r.right || b.bottom > r.bottom) {
      cx += ((b.left + b.right) / 2 - (r.left + r.right) / 2) / z;
      cy += ((b.top + b.bottom) / 2 - (r.top + r.bottom) / 2) / z;
      mode = null;
      apply();
    }
  });

  // The details panel of the selected node, built with textContent only.
  function schema(cols) {
    return cols.map(function (c) { return c[0] + ': ' + c[1]; }).join(', ') || 'none';
  }
  function row(dl, name, text) {
    var dt = document.createElement('dt'), dd = document.createElement('dd');
    dt.textContent = name;
    dd.textContent = text;
    dl.appendChild(dt);
    dl.appendChild(dd);
  }
  function select(i) {
    var n = model.nodes[i], inputs = [];
    selected = n.label;
    model.edges.forEach(function (e) {
      if (e.target === i) inputs.push(model.nodes[e.source].label);
    });
    svg.querySelectorAll('.op-dag-node').forEach(function (g) {
      g.classList.toggle('op-dag-selected', +g.getAttribute('data-index') === i);
    });
    svg.querySelectorAll('.op-dag-edges > g').forEach(function (g) {
      g.classList.toggle('op-dag-lit',
        +g.getAttribute('data-source') === i || +g.getAttribute('data-target') === i);
    });
    panel.textContent = '';
    var head = document.createElement('div');
    head.style.fontWeight = '600';
    head.textContent = n.label + ' \u2014 ' + [n.kind_text, n.detail, n.status].filter(Boolean).join(' \u00b7 ');
    panel.appendChild(head);
    var dl = document.createElement('dl');
    row(dl, 'tags', schema(n.tag_schema));
    row(dl, 'data', schema(n.data_schema));
    row(dl, 'inputs', inputs.join(', ') || 'none');
    if (n.mark) row(dl, 'highlight', n.mark);
    n.facts.forEach(function (f) { row(dl, f[0], f[1]); });
    panel.appendChild(dl);
    panel.classList.add('op-dag-open');
  }

  // Draws a layout: its SVG inline (in the page's colours, without the background the
  // image needs), sized to the view, its nodes selectable; keeps the selected node if
  // it is in this layout too.
  // Draws the layout of a direction and collapse state (when the output holds it).
  function show(d, c) {
    var layout = layouts[key(d, c)];
    if (!layout) return;
    dir = d;
    col = c;
    model = layout.model;
    var el = document.importNode(new DOMParser().parseFromString(
      decode(layout.picture), 'image/svg+xml').documentElement, true);
    var bg = el.querySelector('.op-dag-bg');
    if (bg) bg.parentNode.removeChild(bg);
    el.removeAttribute('color');
    W = +el.getAttribute('width');
    H = +el.getAttribute('height');
    el.removeAttribute('width');
    el.removeAttribute('height');
    el.style.width = '100%';
    el.style.height = '100%';
    el.style.maxWidth = 'none';
    svg.parentNode.replaceChild(el, svg);
    svg = el;
    if (count) count.textContent = String(model.nodes.length);
    svg.querySelectorAll('.op-dag-node').forEach(function (g) {
      var i = +g.getAttribute('data-index');
      g.addEventListener('click', function () { select(i); });
      g.addEventListener('keydown', function (ev) {
        if (ev.key === 'Enter' || ev.key === ' ') {
          ev.preventDefault();
          ev.stopPropagation();
          select(i);
        }
      });
    });
    var keep = -1;
    model.nodes.forEach(function (n, i) { if (n.label === selected) keep = i; });
    if (keep >= 0) {
      select(keep);
    } else {
      selected = null;
      panel.textContent = '';
      panel.classList.remove('op-dag-open');
    }
    root.classList.add('op-dag-live');
    lastWidth = view.clientWidth;
    height();
    first();
  }
  show(dir, col);
})();
