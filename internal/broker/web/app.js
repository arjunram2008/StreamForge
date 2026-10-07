'use strict';
const $ = id => document.getElementById(id);
const num = value => Number(value).toLocaleString();
// All broker-supplied strings go through textContent, including message/topic names.
function element(tag, text, className) {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = text;
  if (className) node.className = className;
  return node;
}
function emptyRow(body, text, columns) {
  const row = element('tr'); const cell = element('td', text, 'empty');
  cell.colSpan = columns; row.append(cell); body.append(row);
}
let loading = false;
async function refresh() {
  if (loading) return;
  loading = true;
  try {
    const response = await fetch('/metrics', {signal: AbortSignal.timeout(15000)});
    if (!response.ok) throw new Error(`Metrics request failed (${response.status})`);
    const data = await response.json();
    $('messages').textContent = num(data.total_messages);
    $('rate').textContent = data.messages_per_second.toFixed(1);
    $('topics').textContent = num(data.active_topics);
    $('lag').textContent = num(data.groups.reduce((sum,g) => sum + g.lag, 0));
    $('notice').hidden = data.complete;
    $('notice').textContent = 'A broker is unavailable. Message counts and lag only include reachable partitions. Restart that broker with its original data directory.';
    $('connection').textContent = data.complete ? '● System connected' : '● Partial availability';
    $('brokers').replaceChildren();
    for (const broker of data.brokers) {
      const card = element('div', undefined, 'broker'); const top = element('div', undefined, 'broker-top');
      top.append(element('strong', `broker-${broker.broker_id}`));
      top.append(element('span', broker.status, `pill ${broker.status === 'healthy' ? '' : broker.status === 'unknown' ? 'unknown' : 'bad'}`));
      card.append(top, element('p', broker.url));
      card.append(element('p', broker.checked_at.startsWith('0001') ? 'First heartbeat pending' : `Last probe ${new Date(broker.checked_at).toLocaleTimeString()}`));
      $('brokers').append(card);
    }
    $('partitions').replaceChildren();
    const partitions = [...data.partitions].sort((a,b) => a.topic.localeCompare(b.topic) || a.partition-b.partition);
    $('partition-count').textContent = `${partitions.length} reachable partitions`;
    for (const p of partitions) {
      const row = element('tr');
      row.append(element('td',p.topic),element('td',`P${p.partition}`),element('td',`broker-${p.broker_id}`),element('td',num(p.messages),'number'));
      $('partitions').append(row);
    }
    if (!partitions.length) emptyRow($('partitions'), 'No partitions yet. Create a topic with the topic command.', 4);
    $('groups').replaceChildren();
    for (const group of data.groups) {
      const row = element('tr'); const name = element('td'); name.append(element('strong',group.group),element('small',group.topic));
      const offsets = Object.entries(group.offsets).sort((a,b)=>Number(a[0])-Number(b[0])).map(([p,n])=>`P${p}: ${n}`).join(' · ') || 'No offsets committed';
      const cell = element('td'); cell.append(element('code',offsets));
      row.append(name,element('td',group.members.join(', ') || 'No live members'),cell,element('td',num(group.lag),'number')); $('groups').append(row);
    }
    if (!data.groups.length) emptyRow($('groups'), 'No consumer groups yet. Start a consumer to see its offsets and lag.', 4);
    $('uptime').textContent = `Coordinator uptime ${Math.floor(data.uptime_seconds)}s`;
    $('updated').textContent = `Updated ${new Date().toLocaleTimeString()} · every 3s`;
  } catch (error) {
    $('connection').textContent = '● Connection unavailable';
    $('notice').hidden = false;
    $('notice').textContent = `${error.message}. Displayed values may be stale. Check that broker 0 is running.`;
  } finally { loading = false; }
}
$('refresh').addEventListener('click', refresh);
refresh(); setInterval(refresh, 3000);
