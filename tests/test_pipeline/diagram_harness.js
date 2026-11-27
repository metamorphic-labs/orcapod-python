// Runs the diagram's script (diagram_assets/diagram.js, as embedded) against a small
// stand-in for a browser page, for tests/test_pipeline/test_diagram.py (ITL-1231).
//
// Usage: node diagram_harness.js <case.json>. The case holds the HTML of the output as a
// tree (parsed in Python), the embedded script, and steps; it prints one observation per
// "observe" step as a JSON list. Layout is reduced to what the script reads: the view is
// as wide as the step "width" says, and as tall as the --op-dag-h the script sets (at
// most 560 pixels, as the style sheet says).
'use strict';
const fs = require('fs');
const kase = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));

let viewWidth = 0;
const observers = [];

class ClassList {
  constructor(el) { this.el = el; }
  get set() { return new Set((this.el.attrs['class'] || '').split(/\s+/).filter(Boolean)); }
  contains(c) { return this.set.has(c); }
  add(c) { const s = this.set; s.add(c); this.el.attrs['class'] = [...s].join(' '); }
  remove(c) { const s = this.set; s.delete(c); this.el.attrs['class'] = [...s].join(' '); }
  toggle(c, on) { if (on) this.add(c); else this.remove(c); }
}

class El {
  constructor(tag, attrs) {
    this.tagName = tag.toUpperCase();
    this.tag = tag.toLowerCase();
    this.attrs = Object.assign({}, attrs || {});
    this.children = [];
    this.parentNode = null;
    this.listeners = {};
    this.text = '';
    this.classList = new ClassList(this);
    const props = {};
    this.style = {props, setProperty(k, v) { props[k] = v; }};
  }
  getAttribute(k) { return k in this.attrs ? this.attrs[k] : null; }
  setAttribute(k, v) { this.attrs[k] = String(v); }
  hasAttribute(k) { return k in this.attrs; }
  removeAttribute(k) { delete this.attrs[k]; }
  set className(v) { this.attrs['class'] = v; }
  get className() { return this.attrs['class'] || ''; }
  set textContent(v) { this.children = []; this.text = String(v); }
  get textContent() { return this.text + this.children.map((c) => c.textContent).join(''); }
  appendChild(c) { c.parentNode = this; this.children.push(c); return c; }
  removeChild(c) { this.children = this.children.filter((x) => x !== c); c.parentNode = null; }
  replaceChild(n, o) {
    const i = this.children.indexOf(o);
    this.children[i] = n;
    n.parentNode = this;
    o.parentNode = null;
  }
  get previousElementSibling() {
    const sib = this.parentNode ? this.parentNode.children : [];
    const i = sib.indexOf(this);
    return i > 0 ? sib[i - 1] : null;
  }
  addEventListener(type, fn) { (this.listeners[type] = this.listeners[type] || []).push(fn); }
  dispatch(type, ev) {
    ev = Object.assign({
      type, target: this, defaultPrevented: false, propagationStopped: false,
      preventDefault() { this.defaultPrevented = true; },
      stopPropagation() { this.propagationStopped = true; },
    }, ev || {});
    for (let el = this; el && !ev.propagationStopped; el = el.parentNode) {
      for (const fn of el.listeners[type] || []) fn.call(el, ev);
    }
    return ev;
  }
  all() { return this.children.flatMap((c) => [c, ...c.all()]); }
  querySelectorAll(sel) {
    const parts = sel.split(/\s*>\s*/);
    let found = this.all().filter((e) => matches(e, parts[parts.length - 1]));
    if (parts.length === 2) found = found.filter((e) => e.parentNode && matches(e.parentNode, parts[0]));
    return found;
  }
  querySelector(sel) { return this.querySelectorAll(sel)[0] || null; }
  closest(sel) {
    for (let el = this; el; el = el.parentNode) if (el.tag && matches(el, sel)) return el;
    return null;
  }
  setPointerCapture() {}
  get isView() { return this.classList.contains('op-dag-view'); }
  get clientWidth() { return this.isView ? viewWidth : 0; }
  get clientHeight() { return this.isView ? viewHeight() : 0; }
  getBoundingClientRect() {
    const w = this.clientWidth, h = this.clientHeight;
    return {left: 0, top: 0, width: w, height: h, right: w, bottom: h};
  }
}

// Selectors the script uses: tag, classes, and :not([attribute]); a comma list of them.
function matches(el, sel) {
  if (sel.includes(',')) return sel.split(',').some((one) => matches(el, one));
  const m = /^([a-z]*)((?:\.[\w-]+)*)(?::not\(\[([\w-]+)\]\))?$/.exec(sel.trim());
  if (!m) throw new Error('selector not supported: ' + sel);
  if (m[1] && el.tag !== m[1]) return false;
  for (const c of m[2].split('.').filter(Boolean)) if (!el.classList.contains(c)) return false;
  if (m[3] && el.hasAttribute(m[3])) return false;
  return true;
}

function build(node) {
  if (typeof node === 'string') return null;
  const el = new El(node.tag, node.attrs);
  for (const c of node.children) {
    if (typeof c === 'string') el.text += c;
    else el.appendChild(build(c));
  }
  return el;
}

// A minimal XML reader for the diagram's own SVG: elements and attributes, no text.
function parseSvg(text) {
  const stack = [new El('#document')];
  const re = /<(\/?)([\w:-]+)((?:\s+[\w:-]+="[^"]*")*)\s*(\/?)>/g;
  let m;
  while ((m = re.exec(text))) {
    if (m[1]) { stack.pop(); continue; }
    const attrs = {};
    for (const a of m[3].matchAll(/([\w:-]+)="([^"]*)"/g)) attrs[a[1]] = a[2];
    const el = new El(m[2], attrs);
    if (m[2] === 'svg') el.natural = [+attrs.width, +attrs.height];
    stack[stack.length - 1].appendChild(el);
    if (!m[4]) stack.push(el);
  }
  return {documentElement: stack[0].children[0]};
}

const body = new El('body');
const tree = build(kase.tree);
for (const c of tree.children.slice()) body.appendChild(c);
const root = body.querySelector('.op-dag');
const view = root.querySelector('.op-dag-view');
function viewHeight() {
  const h = parseFloat(root.style.props['--op-dag-h']);
  return Number.isNaN(h) ? 0 : Math.min(h, 560);
}
const script = new El('script');
body.appendChild(script);

global.window = {ResizeObserver: true, addEventListener() {}};
global.ResizeObserver = class {
  constructor(fn) { this.fn = fn; observers.push(this); }
  observe(el) { this.el = el; }
};
global.DOMParser = class { parseFromString(t) { return parseSvg(t); } };
global.document = {
  currentScript: script,
  querySelectorAll: (sel) => body.querySelectorAll(sel),
  createElement: (tag) => new El(tag),
  createTextNode: (t) => { const e = new El('#text'); e.text = t; return e; },
  importNode: (n) => n,
};

function svgNow() { return view.querySelector('svg'); }
function observe() {
  const svg = svgNow();
  const vb = svg ? svg.getAttribute('viewBox').split(' ').map(Number) : null;
  return {
    viewBox: vb,
    width: viewWidth,
    height: viewHeight(),
    scale: vb ? viewWidth / vb[2] : null,
    centre: vb ? [vb[0] + vb[2] / 2, vb[1] + vb[3] / 2] : null,
    observed: observers.map((o) => o.el && o.el.className),
    clipped: ['left', 'right', 'top', 'bottom'].filter(
      (side) => root.classList.contains('op-dag-clip-' + side)),
    picture: svg ? svg.natural || null : null,
    selected: svg ? svg.querySelectorAll('.op-dag-node')
      .filter((g) => g.classList.contains('op-dag-selected'))
      .map((g) => g.getAttribute('aria-label').split(', ')[0]) : [],
    panel: root.querySelector('.op-dag-panel').classList.contains('op-dag-open'),
    panelText: root.querySelector('.op-dag-panel').textContent,
    count: root.querySelector('.op-dag-count').textContent,
    controls: Object.fromEntries(['direction', 'collapse']
      .filter((name) => root.querySelector('.op-dag-' + name))
      .map((name) => [name, root.querySelector('.op-dag-' + name).children[0].checked])),
  };
}

const out = [];
for (const step of kase.steps) {
  if (step.op === 'width') {
    viewWidth = step.width;
    for (const o of observers) o.fn([]);
  } else if (step.op === 'run') {
    viewWidth = step.width;
    new Function(kase.script)();
  } else if (step.op === 'click') {
    root.querySelector('.op-dag-tools').children
      .find((b) => b.getAttribute('data-op-dag-act') === step.act).dispatch('click');
  } else if (step.op === 'cue') {
    view.querySelector('.op-dag-more-' + step.side).dispatch('click');
  } else if (step.op === 'key') {
    const ev = view.dispatch('keydown', {key: step.key, shiftKey: false});
    out.push({key: step.key, stopped: ev.propagationStopped, prevented: ev.defaultPrevented});
  } else if (step.op === 'drag') {
    view.dispatch('pointerdown', {clientX: 0, clientY: 0, pointerId: 1});
    view.dispatch('pointermove', {clientX: step.dx, clientY: step.dy, pointerId: 1});
    view.dispatch('pointerup', {pointerId: 1});
  } else if (step.op === 'collapse' || step.op === 'direction') {
    const box = root.querySelector('.op-dag-' + step.op).children[0];
    box.checked = !box.checked;
    box.dispatch('change');
  } else if (step.op === 'select') {
    svgNow().querySelectorAll('.op-dag-node')
      .find((g) => g.getAttribute('aria-label').startsWith(step.label + ', '))
      .dispatch('click');
  } else if (step.op === 'observe') {
    out.push(observe());
  } else {
    throw new Error('unknown step ' + step.op);
  }
}
process.stdout.write(JSON.stringify(out));
