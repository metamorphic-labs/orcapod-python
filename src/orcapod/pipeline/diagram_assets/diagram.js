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
// layout, a collapse checkbox that switches to it (keeping the selected node if it is
// in both).
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

  // The shown layout, and the other one (folded or not) when the output holds it.
  var views = [{
    model: JSON.parse(root.querySelector('script.op-dag-model').textContent),
    picture: img.getAttribute('src').split(',')[1]
  }];
  var otherBlock = root.querySelector('script.op-dag-other');
  var other = otherBlock && JSON.parse(otherBlock.textContent);
  if (other) views.push({model: other.model, picture: other.picture});

  var view = root.querySelector('.op-dag-view');
  var panel = root.querySelector('.op-dag-panel');
  var count = root.querySelector('.op-dag-count');
  var cur = 0, model = views[0].model, svg = img, selected = null;
  var W = 1, H = 1, vb = {x: 0, y: 0, w: 1, h: 1};

  // The tools, top right: the collapse checkbox (with the other layout) and the zoom
  // buttons.
  var tools = document.createElement('div');
  tools.className = 'op-dag-tools';
  if (other) {
    root.classList.add('op-dag-has-other');
    var label = document.createElement('label');
    label.className = 'op-dag-collapse';
    label.title = 'Fold the steps that only join, choose or rename columns into the edges';
    var box = document.createElement('input');
    box.type = 'checkbox';
    box.checked = !other.collapse;
    label.appendChild(box);
    label.appendChild(document.createTextNode('collapse'));
    tools.appendChild(label);
    box.addEventListener('change', function () { show(1 - cur); });
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

  // Pan and zoom move the SVG's viewBox.
  function apply() {
    svg.setAttribute('viewBox', [vb.x, vb.y, vb.w, vb.h].join(' '));
  }
  function fit() {
    var r = view.getBoundingClientRect(), rw = r.width || W, rh = r.height || H;
    var k = Math.max(W / rw, H / rh, 1);
    vb.w = rw * k;
    vb.h = rh * k;
    vb.x = (W - vb.w) / 2;
    vb.y = (H - vb.h) / 2;
    apply();
  }
  function zoom(f, cx, cy) {
    var r = view.getBoundingClientRect();
    var px = cx === undefined ? .5 : (cx - r.left) / r.width;
    var py = cy === undefined ? .5 : (cy - r.top) / r.height;
    var nw = vb.w * f, nh = vb.h * f;
    vb.x += (vb.w - nw) * px;
    vb.y += (vb.h - nh) * py;
    vb.w = nw;
    vb.h = nh;
    apply();
  }
  tools.addEventListener('click', function (ev) {
    var a = ev.target.getAttribute('data-op-dag-act');
    if (a === 'in') zoom(1 / 1.25);
    else if (a === 'out') zoom(1.25);
    else if (a === 'fit') fit();
  });
  view.addEventListener('wheel', function (ev) {
    if (!(ev.ctrlKey || ev.metaKey)) return;
    ev.preventDefault();
    zoom(ev.deltaY > 0 ? 1.1 : 1 / 1.1, ev.clientX, ev.clientY);
  }, {passive: false});

  // Drag to pan; a drag never starts on a node, so a click selects it.
  var drag = null;
  view.addEventListener('pointerdown', function (ev) {
    if (ev.target.closest && ev.target.closest('.op-dag-node')) return;
    drag = {x: ev.clientX, y: ev.clientY, vx: vb.x, vy: vb.y};
    view.setPointerCapture(ev.pointerId);
    view.style.cursor = 'grabbing';
  });
  view.addEventListener('pointermove', function (ev) {
    if (!drag) return;
    var k = vb.w / (view.getBoundingClientRect().width || W);
    vb.x = drag.vx - (ev.clientX - drag.x) * k;
    vb.y = drag.vy - (ev.clientY - drag.y) * k;
    apply();
  });
  function stop() {
    drag = null;
    view.style.cursor = '';
  }
  view.addEventListener('pointerup', stop);
  view.addEventListener('pointercancel', stop);

  // The keyboard: arrow keys pan (Shift for larger steps), + and - zoom, 0 fits; a node
  // reached with Tab is brought into view.
  view.tabIndex = 0;
  view.setAttribute('role', 'group');
  view.setAttribute('aria-label',
    'Pipeline diagram: arrow keys move it, + and \u2212 zoom, 0 fits; Tab reaches the nodes');
  function pan(dx, dy) {
    vb.x += dx * vb.w;
    vb.y += dy * vb.h;
    apply();
  }
  view.addEventListener('keydown', function (ev) {
    if (ev.altKey || ev.ctrlKey || ev.metaKey) return;
    var k = ev.key, step = ev.shiftKey ? .5 : .1;
    if (k === 'ArrowLeft') pan(-step, 0);
    else if (k === 'ArrowRight') pan(step, 0);
    else if (k === 'ArrowUp') pan(0, -step);
    else if (k === 'ArrowDown') pan(0, step);
    else if (k === '+' || k === '=') zoom(1 / 1.25);
    else if (k === '-' || k === '\u2212') zoom(1.25);
    else if (k === '0') fit();
    else return;
    ev.preventDefault();
  });
  view.addEventListener('focusin', function (ev) {
    var g = ev.target.closest && ev.target.closest('.op-dag-node');
    if (!g) return;
    var b = g.getBoundingClientRect(), r = view.getBoundingClientRect();
    if (b.left < r.left || b.top < r.top || b.right > r.right || b.bottom > r.bottom) {
      var k = vb.w / (r.width || W);
      vb.x += ((b.left + b.right) / 2 - (r.left + r.right) / 2) * k;
      vb.y += ((b.top + b.bottom) / 2 - (r.top + r.bottom) / 2) * k;
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
    n.facts.forEach(function (f) { row(dl, f[0], f[1]); });
    panel.appendChild(dl);
    panel.classList.add('op-dag-open');
  }

  // Draws a layout: its SVG inline (in the page's colours, without the background the
  // image needs), sized to the view, its nodes selectable; keeps the selected node if
  // it is in this layout too.
  function show(v) {
    cur = v;
    model = views[v].model;
    var el = document.importNode(new DOMParser().parseFromString(
      decode(views[v].picture), 'image/svg+xml').documentElement, true);
    var bg = el.querySelector('.op-dag-bg');
    if (bg) bg.parentNode.removeChild(bg);
    el.removeAttribute('color');
    W = +el.getAttribute('width');
    H = +el.getAttribute('height');
    var cw = view.clientWidth || W, k0 = Math.max(W / cw, 1);
    root.style.setProperty('--op-dag-h', Math.round(Math.min(Math.max(H / k0, 120), 560)) + 'px');
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
    fit();
  }
  show(0);
})();
